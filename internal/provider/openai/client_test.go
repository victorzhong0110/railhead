package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/provider"
)

func TestChatAndStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Errorf("path %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer secret" {
			t.Errorf("auth %s", got)
		}
		var req domain.ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n")
			_, _ = io.WriteString(w, "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1,\"total_tokens\":4}}\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"c1","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hi"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1,"total_tokens":4}}`)
	}))
	defer srv.Close()

	c := New()
	ch := domain.Channel{BaseURL: srv.URL + "/v1", APIKey: "secret", Kind: "openai"}
	req := domain.ChatRequest{Model: "m", Messages: []domain.Message{{Role: "user", Content: domain.Content{Text: "hi"}}}}
	resp, err := c.Chat(context.Background(), ch, req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Usage.TotalTokens != 4 {
		t.Fatalf("usage %+v", resp.Usage)
	}

	req.Stream = true
	st, err := c.ChatStream(context.Background(), ch, req)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var sawUsage bool
	for {
		chunk, err := st.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if chunk.Usage != nil && chunk.Usage.TotalTokens == 4 {
			sawUsage = true
		}
	}
	if !sawUsage {
		t.Fatal("stream did not include usage")
	}
}

func TestUpstreamModelAndThinkPassthrough(t *testing.T) {
	const raw = `{"id":"c1","object":"chat.completion","model":"MiniMax-M2.5","choices":[{"index":0,"message":{"role":"assistant","name":"MiniMax AI","content":"<think>内部</think>好"},"finish_reason":"stop"}],"usage":{"prompt_tokens":40,"completion_tokens":12,"total_tokens":52,"completion_tokens_details":{"reasoning_tokens":8}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req domain.ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Model != "MiniMax-M2.5" {
			t.Errorf("upstream model %s", req.Model)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, raw)
	}))
	defer srv.Close()
	resp, err := New().Chat(context.Background(), domain.Channel{
		BaseURL: srv.URL + "/v1", UpstreamModel: "MiniMax-M2.5",
	}, domain.ChatRequest{Model: "minimax", Messages: []domain.Message{{Role: "user", Content: domain.Content{Text: "hi"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Usage.CompletionTokens != 12 || string(resp.Raw) != raw {
		t.Fatalf("usage=%+v raw_kept=%v", resp.Usage, string(resp.Raw) == raw)
	}
	if !json.Valid(resp.Raw) || !bytesContains(resp.Raw, "<think>") || !bytesContains(resp.Raw, "reasoning_tokens") {
		t.Fatal("raw response dropped think or reasoning_tokens")
	}
}

func bytesContains(b []byte, sub string) bool {
	return bytes.Contains(b, []byte(sub))
}

func TestReasoningContentPassthrough(t *testing.T) {
	const unary = `{"id":"c1","object":"chat.completion","model":"MiniMax-M3.1-Flash-Preview","choices":[{"index":0,"message":{"role":"assistant","content":"好","reasoning_content":"先想一下"},"finish_reason":"stop"}],"usage":{"prompt_tokens":208,"completion_tokens":10,"total_tokens":218,"completion_tokens_details":{"reasoning_tokens":0}}}`
	const delta = `{"id":"c1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"reasoning_content":"先想"},"finish_reason":null}]}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req domain.ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if req.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: "+delta+"\n\n")
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
			return
		}
		_, _ = io.WriteString(w, unary)
	}))
	defer srv.Close()
	c := New()
	ch := domain.Channel{BaseURL: srv.URL + "/v1", UpstreamModel: "MiniMax-M3.1-Flash-Preview"}
	resp, err := c.Chat(context.Background(), ch, domain.ChatRequest{
		Model: "minimax", Messages: []domain.Message{{Role: "user", Content: domain.Content{Text: "hi"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Raw) != unary || resp.Choices[0].Message.ReasoningContent != "先想一下" || resp.Choices[0].Message.Content.Text != "好" {
		t.Fatalf("unary reasoning dropped: content=%q reasoning=%q", resp.Choices[0].Message.Content.Text, resp.Choices[0].Message.ReasoningContent)
	}
	st, err := c.ChatStream(context.Background(), ch, domain.ChatRequest{
		Model: "minimax", Messages: []domain.Message{{Role: "user", Content: domain.Content{Text: "hi"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	chunk, err := st.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if chunk.Choices[0].Delta.ReasoningContent != "先想" || !bytes.Contains(chunk.Raw, []byte("reasoning_content")) {
		t.Fatalf("stream reasoning dropped: %+v", chunk.Choices[0].Delta)
	}
}

func TestStatusMapping(t *testing.T) {
	for _, tc := range []struct {
		status    int
		retryable bool
	}{
		{http.StatusBadGateway, true},
		{http.StatusTooManyRequests, true},
		{http.StatusBadRequest, false},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "nope", tc.status)
		}))
		c := New()
		_, err := c.Chat(context.Background(), domain.Channel{BaseURL: srv.URL + "/v1"}, domain.ChatRequest{Model: "m"})
		srv.Close()
		var ue *provider.UpstreamError
		if !errors.As(err, &ue) {
			t.Fatalf("status %d: err=%v", tc.status, err)
		}
		if ue.Retryable != tc.retryable || ue.Status != tc.status {
			t.Fatalf("status %d: got status=%d retryable=%v", tc.status, ue.Status, ue.Retryable)
		}
	}
}
