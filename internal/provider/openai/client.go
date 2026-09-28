// Package openai 把渠道上的 HTTP 服务当成 OpenAI Chat Completions 来调用。
// base_url 可配置，所以同一份代码能打到官方 API、DeepSeek、vLLM、Ollama 和本仓库的 mock。
package openai

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/provider"
)

// Client 复用连接。超时由调用方的 context 控制，Client.Timeout 保持为 0，
// 否则它会和每次尝试自己的超时打架。
type Client struct {
	HTTP *http.Client
}

// New 返回一个适合网关使用的客户端。
func New() *Client {
	return &Client{HTTP: &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 128,
			IdleConnTimeout:     90 * time.Second,
		},
	}}
}

// Chat 发送非流式请求。
func (c *Client) Chat(ctx context.Context, ch domain.Channel, req domain.ChatRequest) (*domain.ChatResponse, error) {
	req.Stream = false
	resp, err := c.do(ctx, ch, req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, &provider.UpstreamError{Status: 502, Retryable: true, Message: "读取上游响应失败", Err: err}
	}
	if resp.StatusCode >= 400 {
		return nil, httpStatusError(resp.StatusCode, body)
	}
	var out domain.ChatResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, &provider.UpstreamError{Status: 502, Retryable: true, Message: "上游响应不是合法 JSON", Err: err}
	}
	out.Raw = append(json.RawMessage(nil), body...)
	return &out, nil
}

// ChatStream 在收到 200 之后把连接交给调用方。非 200 会读完错误体再返回，此时还没有开始向客户端写 SSE。
func (c *Client) ChatStream(ctx context.Context, ch domain.Channel, req domain.ChatRequest) (provider.Stream, error) {
	req.Stream = true
	req.StreamOptions = &domain.StreamOptions{IncludeUsage: true}
	resp, err := c.do(ctx, ch, req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		return nil, httpStatusError(resp.StatusCode, body)
	}
	return &sseStream{body: resp.Body, sc: bufio.NewScanner(resp.Body), hdr: resp.Header}, nil
}

func (c *Client) do(ctx context.Context, ch domain.Channel, req domain.ChatRequest) (*http.Response, error) {
	if ch.UpstreamModel != "" {
		req.Model = ch.UpstreamModel
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return nil, &provider.UpstreamError{Status: 400, Retryable: false, Message: "序列化请求失败", Err: err}
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint(ch.BaseURL), bytes.NewReader(payload))
	if err != nil {
		return nil, &provider.UpstreamError{Status: 500, Retryable: false, Message: "构造上游请求失败", Err: err}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")
	if ch.APIKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+ch.APIKey)
	}
	resp, err := c.HTTP.Do(httpReq)
	if err != nil {
		return nil, classifyTransport(err)
	}
	return resp, nil
}

func endpoint(base string) string {
	base = strings.TrimRight(base, "/")
	return base + "/chat/completions"
}

func classifyTransport(err error) *provider.UpstreamError {
	switch {
	case errors.Is(err, context.Canceled):
		// 调用方取消。路由器会看父 context，决定要不要重试。
		return &provider.UpstreamError{Status: 499, Retryable: false, Message: "上游请求已取消", Err: err}
	case errors.Is(err, context.DeadlineExceeded):
		return &provider.UpstreamError{Status: 504, Retryable: true, Message: "上游超时", Err: err}
	default:
		return &provider.UpstreamError{Status: 502, Retryable: true, Message: "连接上游失败", Err: err}
	}
}

func httpStatusError(status int, body []byte) *provider.UpstreamError {
	msg := strings.TrimSpace(string(body))
	if len(msg) > 300 {
		msg = msg[:300]
	}
	if msg == "" {
		msg = fmt.Sprintf("上游返回 %d", status)
	}
	retryable := status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500
	return &provider.UpstreamError{Status: status, Retryable: retryable, Message: msg}
}

type sseStream struct {
	body io.ReadCloser
	sc   *bufio.Scanner
	hdr  http.Header
	init bool
}

// Header 读上游响应头。网关用它把 mock 的计时头原样交给客户端。
func (s *sseStream) Header(key string) string {
	if s == nil || s.hdr == nil {
		return ""
	}
	return s.hdr.Get(key)
}

func (s *sseStream) Recv() (*domain.Chunk, error) {
	if !s.init {
		s.init = true
		s.sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	}
	for s.sc.Scan() {
		line := strings.TrimSpace(s.sc.Text())
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if data == "[DONE]" {
			return nil, io.EOF
		}
		var chunk domain.Chunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			return nil, err
		}
		chunk.Raw = append(json.RawMessage(nil), data...)
		return &chunk, nil
	}
	if err := s.sc.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (s *sseStream) Close() error {
	return s.body.Close()
}
