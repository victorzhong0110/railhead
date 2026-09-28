package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/victorzhong0110/railhead/internal/billing"
	"github.com/victorzhong0110/railhead/internal/cache"
	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/provider"
	"github.com/victorzhong0110/railhead/internal/router"
	"github.com/victorzhong0110/railhead/internal/tokens"
)

// chatCompletions 是一条请求的完整生命周期。顺序固定：
// 校验 → RPM → 缓存 → TPM → 并发限制 → 预扣 → 上游（含故障转移）→ 结算或退款 → 写响应。
func (s *Server) chatCompletions(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	result := "error"
	stream := false
	s.metrics.InflightAdd(1)
	defer func() {
		s.metrics.InflightAdd(-1)
		s.metrics.ObserveRequest(result, stream, time.Since(start))
	}()

	key := apiKeyFrom(r.Context())
	var req domain.ChatRequest
	// 允许未知字段。官方 SDK 会带上 presence_penalty 这类我们没有单独建模的参数，
	// 拒绝它们会让兼容接口名不副实。我们只解释自己认识的字段。
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		result = "bad_request"
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_json", "请求体不是合法的 JSON")
		return
	}
	stream = req.Stream
	if req.Model == "" || len(req.Messages) == 0 {
		result = "bad_request"
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_request", "model 和 messages 不能为空")
		return
	}
	maxTokens, ok := s.effectiveMaxTokens(req.MaxTokens)
	if !ok {
		result = "bad_request"
		writeError(w, http.StatusBadRequest, "invalid_request_error", "invalid_max_tokens", "max_tokens 超出允许范围")
		return
	}
	// 把实际采用的上限写回请求，上游和预扣用的是同一个数。
	req.MaxTokens = &maxTokens

	ctx := r.Context()
	if s.cfg.RequestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.cfg.RequestTimeout)
		defer cancel()
	}

	if !s.limiter.Allow(ctx, fmt.Sprintf("railhead:rl:%d:rpm", key.ID), 1, key.RPMLimit, key.RPMLimit, time.Minute) {
		result = "rate_limited"
		s.metrics.Limited("rpm")
		writeError(w, http.StatusTooManyRequests, "rate_limit_error", "rate_limit_exceeded", "超过每分钟请求数限制")
		return
	}

	bypass := strings.EqualFold(r.Header.Get("X-Railhead-Cache"), "bypass")
	cacheable := cache.Eligible(s.cfg.CacheEnabled, req) && !bypass
	var cacheKey string
	if cacheable {
		var err error
		cacheKey, err = cache.Key(key.ID, req)
		if err != nil {
			cacheable = false
		}
	}
	if cacheable {
		body, hit, err := s.cache.Get(ctx, cacheKey)
		if err != nil {
			slog.Warn("读取响应缓存失败", "err", err, "request_id", requestIDFrom(ctx))
			s.metrics.Cache("bypass")
		} else if hit {
			result = "cache_hit"
			s.metrics.Cache("hit")
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Railhead-Cache", "HIT")
			w.Header().Set("X-Railhead-Channel", "cache")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(body)
			return
		} else {
			s.metrics.Cache("miss")
		}
	} else {
		s.metrics.Cache("bypass")
	}

	_, quoted := tokens.Quote(req.Messages, maxTokens, s.cfg.ReserveMargin)
	reserve := int64(quoted)
	if !s.limiter.Allow(ctx, fmt.Sprintf("railhead:rl:%d:tpm", key.ID), quoted, key.TPMLimit, key.TPMLimit, time.Minute) {
		result = "rate_limited"
		s.metrics.Limited("tpm")
		writeError(w, http.StatusTooManyRequests, "rate_limit_error", "tokens_rate_limit_exceeded", "超过每分钟 token 数限制")
		return
	}

	requestID := requestIDFrom(ctx)
	leaseFor := s.cfg.RequestTimeout
	if leaseFor <= 0 {
		leaseFor = 2 * time.Minute
	}
	lease, ok := s.conc.Acquire(ctx, fmt.Sprintf("railhead:conc:%d", key.ID), requestID, key.ConcurrencyLimit, leaseFor)
	if !ok {
		result = "concurrency_limited"
		s.metrics.Limited("concurrency")
		writeError(w, http.StatusTooManyRequests, "rate_limit_error", "concurrency_limit_exceeded", "超过并发请求数限制")
		return
	}
	defer lease.Release()

	if err := s.billing.Reserve(ctx, key.ID, requestID, reserve, req.Model); err != nil {
		result = writeReserveError(w, err)
		return
	}

	if req.Stream {
		s.serveStream(ctx, w, req, requestID, &result)
		return
	}
	s.serveUnary(ctx, w, req, requestID, cacheable, cacheKey, &result)
}

func (s *Server) effectiveMaxTokens(requested *int) (int, bool) {
	n := s.cfg.DefaultMaxTokens
	if n < 1 {
		n = 64
	}
	if requested != nil {
		if *requested < 1 {
			return 0, false
		}
		n = *requested
	}
	capN := s.cfg.MaxTokensCap
	if capN <= 0 {
		capN = 4096
	}
	if n > capN {
		return 0, false
	}
	return n, true
}

func (s *Server) serveUnary(ctx context.Context, w http.ResponseWriter, req domain.ChatRequest, requestID string, cacheable bool, cacheKey string, result *string) {
	resp, ch, err := s.router.Chat(ctx, req)
	if err != nil {
		if refundErr := s.billing.Refund(ctx, requestID); refundErr != nil {
			slog.Error("退款失败", "err", refundErr, "request_id", requestID)
		}
		*result = writeUpstreamFailure(w, err)
		slog.Warn("上游失败", "request_id", requestID, "model", req.Model, "err", err, "result", *result)
		return
	}
	prompt, completion := usageFromResponse(resp, req)
	if err := s.settle(ctx, requestID, int64(prompt+completion), ch.Name, prompt, completion); err != nil {
		slog.Error("结算失败", "err", err, "request_id", requestID)
	}
	s.metrics.Tokens(prompt, completion)
	body, err := responseBytes(resp)
	if err != nil {
		*result = "error"
		writeError(w, http.StatusInternalServerError, "internal_error", "encode_error", "编码响应失败")
		return
	}
	if cacheable {
		if err := s.cache.Set(ctx, cacheKey, body, s.cfg.CacheTTL); err != nil {
			slog.Warn("写入响应缓存失败", "err", err, "request_id", requestID)
		}
	}
	*result = "success"
	cacheHeader := "BYPASS"
	if cacheable {
		cacheHeader = "MISS"
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Railhead-Channel", ch.Name)
	w.Header().Set("X-Railhead-Cache", cacheHeader)
	copyMockTiming(w, resp.MockTTFTUs, resp.MockUpstreamUs)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) serveStream(ctx context.Context, w http.ResponseWriter, req domain.ChatRequest, requestID string, result *string) {
	st, ch, err := s.router.ChatStream(ctx, req)
	if err != nil {
		if refundErr := s.billing.Refund(ctx, requestID); refundErr != nil {
			slog.Error("退款失败", "err", refundErr, "request_id", requestID)
		}
		*result = writeUpstreamFailure(w, err)
		slog.Warn("上游流建立失败", "request_id", requestID, "model", req.Model, "err", err)
		return
	}
	defer st.Close()

	// 响应头一旦发出就不能再改状态码，所以只有流真正建立之后才开始写 SSE。
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Railhead-Channel", ch.Name)
	if hs, ok := st.(interface{ Header(string) string }); ok {
		if v := hs.Header("X-Mock-TTFT-Us"); v != "" {
			w.Header().Set("X-Mock-TTFT-Us", v)
		}
		if v := hs.Header("X-Mock-Upstream-Us"); v != "" {
			w.Header().Set("X-Mock-Upstream-Us", v)
		}
	}
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)

	var acc strings.Builder
	var usage *domain.Usage
	var streamErr error
	for {
		chunk, err := st.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			streamErr = err
			break
		}
		if chunk.Usage != nil {
			copied := *chunk.Usage
			usage = &copied
		}
		if len(chunk.Choices) > 0 && chunk.Choices[0].Delta != nil {
			// 推理有两种写法：content 里的 <think>，或单独的 reasoning_content。
			// 没有上游 usage 时，两种都要算进估算，不能只留下可见答案。
			delta := chunk.Choices[0].Delta
			acc.WriteString(delta.Content)
			acc.WriteString(delta.ReasoningContent)
		}
		b, err := chunkBytes(chunk)
		if err != nil {
			streamErr = err
			break
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", b); err != nil {
			streamErr = err
			break
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
	if streamErr == nil {
		if _, err := io.WriteString(w, "data: [DONE]\n\n"); err == nil && flusher != nil {
			flusher.Flush()
		}
	}

	prompt, completion := usageFromStream(usage, req, acc.String())
	// 一个字节都没给客户端，且流是坏的：全额退款。已经吐出内容则按已生成的 token 结算。
	if streamErr != nil && usage == nil && acc.Len() == 0 {
		if err := s.billing.Refund(ctx, requestID); err != nil {
			slog.Error("流式退款失败", "err", err, "request_id", requestID)
		}
		*result = "stream_error"
		slog.Warn("流中断且没有产出", "request_id", requestID, "channel", ch.Name, "err", streamErr)
		return
	}
	if err := s.settle(ctx, requestID, int64(prompt+completion), ch.Name, prompt, completion); err != nil {
		slog.Error("流式结算失败", "err", err, "request_id", requestID)
	}
	s.metrics.Tokens(prompt, completion)
	if streamErr != nil {
		*result = "stream_interrupted"
		slog.Warn("流中断，已按已生成 token 结算", "request_id", requestID, "channel", ch.Name, "err", streamErr, "tokens", prompt+completion)
		return
	}
	*result = "success"
}

func copyMockTiming(w http.ResponseWriter, ttftUs, upstreamUs int64) {
	if ttftUs > 0 {
		w.Header().Set("X-Mock-TTFT-Us", fmt.Sprintf("%d", ttftUs))
	}
	if upstreamUs > 0 {
		w.Header().Set("X-Mock-Upstream-Us", fmt.Sprintf("%d", upstreamUs))
	}
}

func (s *Server) settle(ctx context.Context, requestID string, actual int64, channel string, prompt, completion int) error {
	var err error
	for i := 0; i < 3; i++ {
		err = s.billing.Settle(ctx, requestID, actual, channel, prompt, completion)
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(i+1) * 10 * time.Millisecond)
	}
	return err
}

// responseBytes 优先转发上游原文，这样 <think> 和 reasoning_tokens 不会在重新编码时丢掉。
func responseBytes(resp *domain.ChatResponse) ([]byte, error) {
	if resp != nil && len(resp.Raw) > 0 {
		return resp.Raw, nil
	}
	return json.Marshal(resp)
}

func chunkBytes(chunk *domain.Chunk) ([]byte, error) {
	if chunk != nil && len(chunk.Raw) > 0 {
		return chunk.Raw, nil
	}
	return json.Marshal(chunk)
}

func usageFromResponse(resp *domain.ChatResponse, req domain.ChatRequest) (int, int) {
	if resp == nil {
		return tokens.CountPrompt(req.Messages), 0
	}
	if resp.Usage.PromptTokens > 0 || resp.Usage.CompletionTokens > 0 {
		return resp.Usage.PromptTokens, resp.Usage.CompletionTokens
	}
	if resp.Usage.TotalTokens > 0 {
		return 0, resp.Usage.TotalTokens
	}
	text := ""
	if len(resp.Choices) > 0 && resp.Choices[0].Message != nil {
		msg := resp.Choices[0].Message
		text = msg.Content.Text + msg.ReasoningContent
	}
	return tokens.CountPrompt(req.Messages), tokens.CountText(text)
}

func usageFromStream(usage *domain.Usage, req domain.ChatRequest, acc string) (int, int) {
	if usage != nil && (usage.PromptTokens > 0 || usage.CompletionTokens > 0 || usage.TotalTokens > 0) {
		if usage.PromptTokens == 0 && usage.CompletionTokens == 0 {
			return 0, usage.TotalTokens
		}
		return usage.PromptTokens, usage.CompletionTokens
	}
	return tokens.CountPrompt(req.Messages), tokens.CountText(acc)
}

func writeReserveError(w http.ResponseWriter, err error) string {
	switch {
	case errors.Is(err, billing.ErrInsufficientQuota):
		writeError(w, http.StatusTooManyRequests, "insufficient_quota", "insufficient_quota", "额度不足")
		return "insufficient_quota"
	case errors.Is(err, billing.ErrDuplicateRequest):
		writeError(w, http.StatusConflict, "invalid_request_error", "duplicate_request", "重复的请求 ID")
		return "duplicate_request"
	case errors.Is(err, billing.ErrKeyDisabled):
		writeError(w, http.StatusUnauthorized, "authentication_error", "invalid_api_key", "密钥已停用")
		return "unauthorized"
	default:
		slog.Error("预扣失败", "err", err)
		writeError(w, http.StatusInternalServerError, "internal_error", "billing_error", "预扣额度失败")
		return "error"
	}
}

func writeUpstreamFailure(w http.ResponseWriter, err error) string {
	if errors.Is(err, router.ErrNoChannel) {
		writeError(w, http.StatusNotFound, "invalid_request_error", "model_not_found", "没有可用渠道能服务这个模型")
		return "no_channel"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		writeError(w, http.StatusGatewayTimeout, "timeout_error", "timeout", "请求超时")
		return "timeout"
	}
	var ue *provider.UpstreamError
	if errors.As(err, &ue) && !ue.Retryable {
		status := ue.Status
		if status < 400 || status > 599 {
			status = http.StatusBadRequest
		}
		writeError(w, status, "invalid_request_error", "upstream_rejected", ue.Error())
		return "upstream_rejected"
	}
	writeError(w, http.StatusBadGateway, "upstream_error", "upstream_unavailable", "上游渠道不可用")
	return "upstream_error"
}
