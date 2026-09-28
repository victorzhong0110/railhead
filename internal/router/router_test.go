package router

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/victorzhong0110/railhead/internal/breaker"
	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/provider"
)

type fake struct {
	mu    sync.Mutex
	fail  map[string]error
	calls []string
}

func (f *fake) Chat(_ context.Context, ch domain.Channel, req domain.ChatRequest) (*domain.ChatResponse, error) {
	f.mu.Lock()
	f.calls = append(f.calls, ch.Name)
	err := f.fail[ch.Name]
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &domain.ChatResponse{
		Model: req.Model,
		Usage: domain.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2},
	}, nil
}

func (f *fake) ChatStream(ctx context.Context, ch domain.Channel, req domain.ChatRequest) (provider.Stream, error) {
	_, err := f.Chat(ctx, ch, req)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (f *fake) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

func TestPrefersHigherPriority(t *testing.T) {
	f := &fake{}
	r := New(f, breaker.NewGroup(breaker.Options{Threshold: 5, Cooldown: time.Second}), nil, 3, time.Millisecond)
	r.SetChannels([]domain.Channel{
		{Name: "low", Kind: "mock", Models: []string{"m"}, Priority: 1, Weight: 100, Enabled: true},
		{Name: "high", Kind: "mock", Models: []string{"m"}, Priority: 10, Weight: 1, Enabled: true},
	})
	_, ch, err := r.Chat(context.Background(), domain.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if ch.Name != "high" {
		t.Fatalf("channel %s", ch.Name)
	}
}

func TestFailoverSkipsRetryableAndStopsOnClientError(t *testing.T) {
	f := &fake{fail: map[string]error{
		"primary": &provider.UpstreamError{Status: 500, Retryable: true, Message: "down"},
		"backup":  &provider.UpstreamError{Status: 400, Retryable: false, Message: "bad request"},
	}}
	r := New(f, breaker.NewGroup(breaker.Options{Threshold: 5, Cooldown: time.Minute}), nil, 3, time.Millisecond)
	r.SetChannels([]domain.Channel{
		{Name: "primary", Models: []string{"m"}, Priority: 10, Weight: 1, Enabled: true, Timeout: time.Second},
		{Name: "backup", Models: []string{"m"}, Priority: 1, Weight: 1, Enabled: true, Timeout: time.Second},
	})
	_, ch, err := r.Chat(context.Background(), domain.ChatRequest{Model: "m"})
	if err == nil {
		t.Fatal("expected client error")
	}
	if ch.Name != "backup" {
		t.Fatalf("channel %s calls %v", ch.Name, f.names())
	}
	if got := f.names(); len(got) != 2 || got[0] != "primary" || got[1] != "backup" {
		t.Fatalf("calls %v", got)
	}
}

func TestBreakerSkipsOpenChannel(t *testing.T) {
	f := &fake{fail: map[string]error{
		"primary": &provider.UpstreamError{Status: 503, Retryable: true, Message: "down"},
	}}
	g := breaker.NewGroup(breaker.Options{Threshold: 1, Cooldown: time.Hour})
	r := New(f, g, nil, 2, time.Millisecond)
	r.SetChannels([]domain.Channel{
		{Name: "primary", Models: []string{"m"}, Priority: 10, Weight: 1, Enabled: true, Timeout: time.Second},
		{Name: "backup", Models: []string{"m"}, Priority: 1, Weight: 1, Enabled: true, Timeout: time.Second},
	})
	if _, _, err := r.Chat(context.Background(), domain.ChatRequest{Model: "m"}); err != nil {
		t.Fatal(err)
	}
	// 阈值是 1，第一次失败后 primary 已经打开。第二次应该直接打 backup。
	f.mu.Lock()
	f.calls = nil
	f.mu.Unlock()
	_, ch, err := r.Chat(context.Background(), domain.ChatRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if ch.Name != "backup" {
		t.Fatalf("channel %s", ch.Name)
	}
	if got := f.names(); len(got) != 1 || got[0] != "backup" {
		t.Fatalf("open breaker should skip primary, calls %v", got)
	}
}

func TestNoChannel(t *testing.T) {
	r := New(&fake{}, nil, nil, 2, time.Millisecond)
	r.SetChannels(nil)
	_, _, err := r.Chat(context.Background(), domain.ChatRequest{Model: "missing"})
	if !errors.Is(err, ErrNoChannel) {
		t.Fatalf("err %v", err)
	}
}

func TestWeightBiasesSelection(t *testing.T) {
	f := &fake{}
	r := New(f, breaker.NewGroup(breaker.Options{Threshold: 100, Cooldown: time.Second}), nil, 1, time.Millisecond)
	r.SetChannels([]domain.Channel{
		{Name: "light", Models: []string{"m"}, Priority: 1, Weight: 1, Enabled: true},
		{Name: "heavy", Models: []string{"m"}, Priority: 1, Weight: 9, Enabled: true},
	})
	light := 0
	const n = 2000
	for i := 0; i < n; i++ {
		_, ch, err := r.Chat(context.Background(), domain.ChatRequest{Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		if ch.Name == "light" {
			light++
		}
	}
	// 权重 1:9，2000 次里 light 大约 200。放宽到 80–400，避免偶发失败。
	if light < 80 || light > 400 {
		t.Fatalf("light selections = %d", light)
	}
}
