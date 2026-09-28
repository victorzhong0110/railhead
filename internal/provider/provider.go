// Package provider 定义上游调用的接口和错误。
// 网关其余部分只依赖这里的类型，所以换一家供应商时改适配器，不改计费和路由。
package provider

import (
	"context"
	"fmt"

	"github.com/victorzhong0110/railhead/internal/domain"
)

// UpstreamError 是一次上游失败。Retryable 为 false 时路由器不再换渠道，
// 因为这通常是调用方的请求本身有问题（例如 400），换一条渠道还会得到同样的结果。
type UpstreamError struct {
	Status    int
	Retryable bool
	Message   string
	Err       error
}

func (e *UpstreamError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("upstream status %d", e.Status)
}

func (e *UpstreamError) Unwrap() error { return e.Err }

// Stream 是已经建立好的上游流。Recv 在结束时返回 io.EOF。
type Stream interface {
	Recv() (*domain.Chunk, error)
	Close() error
}

// Upstream 是供应商适配器。channel 里带有地址和上游密钥。
type Upstream interface {
	Chat(ctx context.Context, ch domain.Channel, req domain.ChatRequest) (*domain.ChatResponse, error)
	ChatStream(ctx context.Context, ch domain.Channel, req domain.ChatRequest) (Stream, error)
}
