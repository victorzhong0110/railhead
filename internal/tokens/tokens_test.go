package tokens

import (
	"testing"

	"github.com/victorzhong0110/railhead/internal/domain"
)

func TestQuoteMatchesPromptPlusMaxPlusMargin(t *testing.T) {
	msgs := []domain.Message{{
		Role:    "user",
		Content: domain.Content{Text: "hello"},
	}}
	prompt, reserve := Quote(msgs, 16, 0)
	if prompt <= 0 {
		t.Fatalf("prompt = %d", prompt)
	}
	if reserve != prompt+16 {
		t.Fatalf("reserve = %d, want %d", reserve, prompt+16)
	}
}

func TestCountTextEmpty(t *testing.T) {
	if CountText("") != 0 {
		t.Fatal("empty text should be 0 tokens")
	}
}
