package httpserver

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/victorzhong0110/railhead/internal/domain"
	"github.com/victorzhong0110/railhead/internal/tokens"
)

func TestThinkBlockIsForwardedAndBilledFromUsage(t *testing.T) {
	raw := []byte(`{"id":"c","object":"chat.completion","model":"MiniMax-M3","choices":[{"index":0,"message":{"role":"assistant","content":"<think>推理很长</think>好"},"finish_reason":"stop"}],"usage":{"prompt_tokens":180,"completion_tokens":35,"total_tokens":215,"completion_tokens_details":{"reasoning_tokens":31}}}`)
	var resp domain.ChatResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	resp.Raw = append([]byte(nil), raw...)
	body, err := responseBytes(&resp)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != string(raw) || !strings.Contains(string(body), "<think>") || !strings.Contains(string(body), "reasoning_tokens") {
		t.Fatal("gateway re-encoded the upstream body and dropped the think block")
	}
	prompt, completion := usageFromResponse(&resp, domain.ChatRequest{
		Messages: []domain.Message{{Role: "user", Content: domain.Content{Text: "hi"}}},
	})
	if prompt != 180 || completion != 35 {
		t.Fatalf("billed %d+%d, want provider usage 180+35", prompt, completion)
	}
	// 只数 </think> 后面的答案会少计。供应商把 reasoning 算进 completion_tokens。
	if tokens.CountText("好") >= completion {
		t.Fatal("fixture should show provider completion tokens above the visible answer")
	}
}

func TestReasoningContentIsKeptAndCountedWhenUsageMissing(t *testing.T) {
	raw := []byte(`{"id":"c","object":"chat.completion","model":"MiniMax-M3.1-Flash-Preview","choices":[{"index":0,"message":{"role":"assistant","content":"好","reasoning_content":"先想一下"},"finish_reason":"stop"}]}`)
	var resp domain.ChatResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatal(err)
	}
	resp.Raw = append([]byte(nil), raw...)
	body, err := responseBytes(&resp)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"reasoning_content"`) {
		t.Fatal("reasoning_content was dropped")
	}
	if resp.Choices[0].Message.ReasoningContent != "先想一下" {
		t.Fatalf("parsed reasoning %q", resp.Choices[0].Message.ReasoningContent)
	}
	_, completion := usageFromResponse(&resp, domain.ChatRequest{
		Messages: []domain.Message{{Role: "user", Content: domain.Content{Text: "hi"}}},
	})
	want := tokens.CountText("好" + "先想一下")
	if completion != want {
		t.Fatalf("fallback completion %d, want %d from content plus reasoning_content", completion, want)
	}
}

func TestStreamChunkKeepsRawThinkDelta(t *testing.T) {
	raw := []byte(`{"choices":[{"index":0,"delta":{"content":"<think>推理"},"finish_reason":null}]}`)
	chunk := &domain.Chunk{Raw: raw}
	body, err := chunkBytes(chunk)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != string(raw) {
		t.Fatal("stream chunk was re-encoded")
	}
}

func TestStreamReasoningContentParsesAndCountsWithContent(t *testing.T) {
	raw := []byte(`{"choices":[{"index":0,"delta":{"reasoning_content":"先想"},"finish_reason":null}]}`)
	var chunk domain.Chunk
	if err := json.Unmarshal(raw, &chunk); err != nil {
		t.Fatal(err)
	}
	if chunk.Choices[0].Delta == nil || chunk.Choices[0].Delta.ReasoningContent != "先想" {
		t.Fatalf("delta %+v", chunk.Choices[0].Delta)
	}
	chunk.Raw = append([]byte(nil), raw...)
	body, err := chunkBytes(&chunk)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"reasoning_content"`) {
		t.Fatal("stream reasoning_content was dropped")
	}
	// 流式累加器和网关循环一样：content 与 reasoning_content 都计入。
	acc := chunk.Choices[0].Delta.Content + chunk.Choices[0].Delta.ReasoningContent + "好"
	_, completion := usageFromStream(nil, domain.ChatRequest{
		Messages: []domain.Message{{Role: "user", Content: domain.Content{Text: "hi"}}},
	}, acc)
	want := tokens.CountText("先想" + "好")
	if completion != want {
		t.Fatalf("fallback completion %d, want %d", completion, want)
	}
	// 上游给了 usage 时只按供应商数字结算，不再把推理文本加一遍。
	_, billed := usageFromStream(&domain.Usage{PromptTokens: 10, CompletionTokens: 4, TotalTokens: 14}, domain.ChatRequest{}, acc)
	if billed != 4 {
		t.Fatalf("provider completion %d, want 4", billed)
	}
}
