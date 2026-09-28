package cache

import (
	"testing"

	"github.com/victorzhong0110/railhead/internal/domain"
)

func TestKeyIsolatesTenants(t *testing.T) {
	temp := 0.0
	max := 16
	req := domain.ChatRequest{
		Model: "m",
		Messages: []domain.Message{{
			Role:    "user",
			Content: domain.Content{Text: "hi"},
		}},
		Temperature: &temp,
		MaxTokens:   &max,
	}
	a, err := Key(1, req)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Key(2, req)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("different api keys must not share a cache entry")
	}
	again, err := Key(1, req)
	if err != nil {
		t.Fatal(err)
	}
	if a != again {
		t.Fatal("cache key should be stable")
	}
}

func TestEligibleRequiresZeroTemperature(t *testing.T) {
	req := domain.ChatRequest{Model: "m"}
	if Eligible(true, req) {
		t.Fatal("missing temperature is not cacheable")
	}
	one := 1.0
	req.Temperature = &one
	if Eligible(true, req) {
		t.Fatal("temperature 1 is not cacheable")
	}
	zero := 0.0
	req.Temperature = &zero
	if !Eligible(true, req) {
		t.Fatal("temperature 0 should be cacheable")
	}
	req.Stream = true
	if Eligible(true, req) {
		t.Fatal("streams are not cacheable")
	}
}
