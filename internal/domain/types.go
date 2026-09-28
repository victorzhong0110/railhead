// Package domain 放网关内部流通的数据结构。
// 这些类型只覆盖 OpenAI Chat Completions 的文本子集，不处理图片和 tools。
package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// Content 同时接受两种 OpenAI 写法：纯字符串，或 [{"type":"text","text":"..."}]。
// 网关只保留文本，不处理图片。这样客户端用官方 SDK 发来的文本请求不会被拒绝。
type Content struct {
	Text string
}

func (c Content) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Text)
}

func (c *Content) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || string(b) == "null" {
		c.Text = ""
		return nil
	}
	if b[0] == '"' {
		return json.Unmarshal(b, &c.Text)
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(b, &parts); err != nil {
		return fmt.Errorf("content 必须是字符串或文本片段数组")
	}
	var sb strings.Builder
	for _, p := range parts {
		sb.WriteString(p.Text)
	}
	c.Text = sb.String()
	return nil
}

// Message 是对话里的一轮。
// ReasoningContent 是 MiniMax-M3.1-Flash-Preview 这类模型的独立推理字段。
// 另一些模型把推理放在 content 的 <think> 标签里。两种都要保留。
type Message struct {
	Role             string  `json:"role"`
	Content          Content `json:"content"`
	ReasoningContent string  `json:"reasoning_content,omitempty"`
}

// StreamOptions 对应 OpenAI 的 stream_options。网关向上游请求时会强制打开 include_usage，
// 这样流结束时能拿到真实 token 数，而不是只靠估算。
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

// ChatRequest 是 /v1/chat/completions 的请求体。
type ChatRequest struct {
	Model         string         `json:"model"`
	Messages      []Message      `json:"messages"`
	Temperature   *float64       `json:"temperature,omitempty"`
	TopP          *float64       `json:"top_p,omitempty"`
	MaxTokens     *int           `json:"max_tokens,omitempty"`
	Stream        bool           `json:"stream,omitempty"`
	Stop          []string       `json:"stop,omitempty"`
	User          string         `json:"user,omitempty"`
	StreamOptions *StreamOptions `json:"stream_options,omitempty"`
}

// Usage 是计费依据。TotalTokens = PromptTokens + CompletionTokens。
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ChatResponse 是非流式响应，字段名与 OpenAI 对齐，方便官方 SDK 直接解析。
type ChatResponse struct {
	ID      string   `json:"id"`
	Object  string   `json:"object"`
	Created int64    `json:"created"`
	Model   string   `json:"model"`
	Choices []Choice `json:"choices"`
	Usage   Usage    `json:"usage"`
	// 下面两个字段只有 mock 上游会填，用来把「上游自己花了多久」和网关耗时分开。
	// 真实供应商不返回它们，omitempty 会让字段从响应里消失。
	MockUpstreamUs int64 `json:"mock_upstream_us,omitempty"`
	MockTTFTUs     int64 `json:"mock_ttft_us,omitempty"`
	// Raw 是上游原始 JSON。MiniMax 会在 content 里放 <think> 块，并在 usage 里带
	// reasoning_tokens。重新编码会丢掉这些字段，所以能转发原文时就转发原文。
	Raw json.RawMessage `json:"-"`
}

// Choice 同时服务于非流式（Message）和流式（Delta）。
// FinishReason 用指针，是为了让流式中间块序列化成 "finish_reason": null。
type Choice struct {
	Index        int      `json:"index"`
	Message      *Message `json:"message,omitempty"`
	Delta        *Delta   `json:"delta,omitempty"`
	FinishReason *string  `json:"finish_reason"`
}

// Delta 是流式增量。推理可能在 content 里，也可能在 reasoning_content 里。
type Delta struct {
	Role             string `json:"role,omitempty"`
	Content          string `json:"content,omitempty"`
	ReasoningContent string `json:"reasoning_content,omitempty"`
}

// Chunk 是 SSE 里 data: 后面的 JSON。
type Chunk struct {
	ID      string          `json:"id"`
	Object  string          `json:"object"`
	Created int64           `json:"created"`
	Model   string          `json:"model"`
	Choices []Choice        `json:"choices"`
	Usage   *Usage          `json:"usage,omitempty"`
	Raw     json.RawMessage `json:"-"`
}

// APIError / ErrorResponse 对齐 OpenAI 的错误信封，调用方可以按 type 和 code 分支。
type APIError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code,omitempty"`
}

type ErrorResponse struct {
	Error APIError `json:"error"`
}

// APIKey 是鉴权之后放进内存的密钥元数据。
// QuotaBalance 只作为展示，扣费时以数据库里加锁读到的值为准，避免用缓存余额做决定。
type APIKey struct {
	ID               int64
	KeyHash          string
	KeyPrefix        string
	Name             string
	QuotaBalance     int64
	QuotaGranted     int64
	RPMLimit         int
	TPMLimit         int
	ConcurrencyLimit int
	Enabled          bool
}

// Channel 是一条上游渠道。Priority 越大越优先；同一优先级内按 Weight 加权随机。
// UpstreamModel 非空时，发给上游的 model 用这个值，客户端看到的模型名可以是别名。
// 这样同一个别名可以在两条 MiniMax 渠道之间故障转移，而两边实际调用的模型不同。
type Channel struct {
	ID            int64
	Name          string
	Kind          string
	BaseURL       string
	APIKey        string
	Models        []string
	UpstreamModel string
	Priority      int
	Weight        int
	Timeout       time.Duration
	Enabled       bool
}

// Supports 判断这条渠道能不能接这个模型。
func (c Channel) Supports(model string) bool {
	for _, m := range c.Models {
		if m == "*" || m == model {
			return true
		}
	}
	return false
}

// ReconcileResult 是一条密钥的对账结果。Drift 必须为 0。
// 不变量：quota_granted = quota_balance + settled + inflight。
type ReconcileResult struct {
	APIKeyID      int64 `json:"api_key_id"`
	QuotaGranted  int64 `json:"quota_granted"`
	QuotaBalance  int64 `json:"quota_balance"`
	Settled       int64 `json:"settled"`
	Inflight      int64 `json:"inflight"`
	Unbilled      int64 `json:"unbilled"`
	ReservedCount int64 `json:"reserved_count"`
	SettledCount  int64 `json:"settled_count"`
	RefundedCount int64 `json:"refunded_count"`
	Drift         int64 `json:"drift"`
}
