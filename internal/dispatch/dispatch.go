// Package dispatch 按渠道类型选择适配器。
// 它单独成包，是因为适配器实现（openai、mock）需要引用 provider 里的错误类型，
// 如果选择逻辑也放在 provider 里，就会变成 provider → openai → provider 的循环依赖。
package dispatch

import (
	"context"
	"errors"

	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/provider"
	"github.com/victorzhong0110/railhead/internal/provider/mock"
	"github.com/victorzhong0110/railhead/internal/provider/openai"
)

// Dispatcher 按渠道 kind 选择适配器。
// openai：任意兼容 OpenAI 的 HTTP 服务（官方、DeepSeek、vLLM、Ollama、本仓库的 mock）。
// mock：进程内模拟，单测不必起 HTTP。
type Dispatcher struct {
	OpenAI *openai.Client
	Mock   *mock.Engine
}

// Chat 发起非流式调用。
func (d *Dispatcher) Chat(ctx context.Context, ch domain.Channel, req domain.ChatRequest) (*domain.ChatResponse, error) {
	switch ch.Kind {
	case "mock":
		resp, err := d.Mock.Complete(ctx, req)
		return resp, wrapMock(err)
	case "openai", "":
		return d.OpenAI.Chat(ctx, ch, req)
	default:
		return nil, &provider.UpstreamError{Status: 500, Retryable: true, Message: "未知渠道类型 " + ch.Kind}
	}
}

// ChatStream 发起流式调用。
func (d *Dispatcher) ChatStream(ctx context.Context, ch domain.Channel, req domain.ChatRequest) (provider.Stream, error) {
	switch ch.Kind {
	case "mock":
		st, err := d.Mock.Stream(ctx, req)
		return st, wrapMock(err)
	case "openai", "":
		return d.OpenAI.ChatStream(ctx, ch, req)
	default:
		return nil, &provider.UpstreamError{Status: 500, Retryable: true, Message: "未知渠道类型 " + ch.Kind}
	}
}

func wrapMock(err error) error {
	if err == nil {
		return nil
	}
	var me *mock.Error
	if errors.As(err, &me) {
		return &provider.UpstreamError{Status: me.Status, Retryable: me.Retryable, Message: me.Message, Err: me.Err}
	}
	return err
}
