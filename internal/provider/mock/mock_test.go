package mock

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/tokens"
)

func TestUsageMatchesSharedEstimator(t *testing.T) {
	e := New(Options{DefaultCompletion: 4})
	max := 8
	req := domain.ChatRequest{
		Model: "railhead-mock",
		Messages: []domain.Message{{
			Role:    "user",
			Content: domain.Content{Text: "hello gateway"},
		}},
		MaxTokens: &max,
	}
	resp, err := e.Complete(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Usage.PromptTokens != tokens.CountPrompt(req.Messages) {
		t.Fatalf("prompt %d", resp.Usage.PromptTokens)
	}
	if resp.Usage.CompletionTokens != 8 || resp.Usage.TotalTokens != resp.Usage.PromptTokens+8 {
		t.Fatalf("usage %+v", resp.Usage)
	}
}

func TestErrorRateOneIsDeterministic(t *testing.T) {
	e := New(Options{ErrorRate: 1})
	_, err := e.Complete(context.Background(), domain.ChatRequest{Model: "m"})
	var ue *Error
	if err == nil || !as(err, &ue) || !ue.Retryable {
		t.Fatalf("err=%v", err)
	}
}

func TestStreamHonorsCancel(t *testing.T) {
	e := New(Options{StreamDelay: time.Hour, DefaultCompletion: 4})
	max := 4
	ctx, cancel := context.WithCancel(context.Background())
	st, err := e.Stream(ctx, domain.ChatRequest{Model: "m", MaxTokens: &max, Messages: []domain.Message{{Role: "user", Content: domain.Content{Text: "a"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Recv(); err != nil {
		t.Fatal(err)
	}
	cancel()
	_, err = st.Recv()
	if err == nil {
		t.Fatal("expected cancel")
	}
}

func TestHTTPFaultInjection(t *testing.T) {
	e := New(Options{DefaultCompletion: 2})
	srv := httptest.NewServer(Handler(e))
	defer srv.Close()
	body := `{"error_rate":1}`
	res, err := http.Post(srv.URL+"/admin/fault", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	res, err = http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 500 {
		b, _ := io.ReadAll(res.Body)
		t.Fatalf("status %d body %s", res.StatusCode, b)
	}
}

func as(err error, target **Error) bool {
	ue, ok := err.(*Error)
	if !ok {
		return false
	}
	*target = ue
	return true
}
