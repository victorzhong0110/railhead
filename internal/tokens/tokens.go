// Package tokens 用一个故意写得很浅的估算器数 token。
// 它不是 tiktoken。预扣用估算值，结算用上游返回的 usage。
// 两者不一致时多退少补，余额不会变成负数。
package tokens

import (
	"unicode/utf8"

	"github.com/victorzhong0110/railhead/internal/domain"
)

// CountText 按「大约 2 个 Unicode 码点 = 1 个 token」估算。
// 英文会估多，中文会估少，所以它只适合做预扣上限的一部分，不能当账单。
func CountText(s string) int {
	n := utf8.RuneCountInString(s)
	if n == 0 {
		return 0
	}
	return (n + 1) / 2
}

// CountPrompt 给一段对话一个稳定的 prompt token 数。
// 每条消息加 4，最后再加 2，是为了模仿 chat 模板的固定开销，并且让网关和 mock 用同一公式。
func CountPrompt(messages []domain.Message) int {
	if len(messages) == 0 {
		return 0
	}
	n := 0
	for _, m := range messages {
		n += 4
		n += CountText(m.Role)
		n += CountText(m.Content.Text)
	}
	n += 2
	return n
}

// Quote 计算本次请求要预扣的 token。
// reserve = prompt 估算 + max_tokens + margin。
// margin 用来覆盖「真实 tokenizer 比估算更贵」的情况；和 mock 对齐时可以设为 0。
func Quote(messages []domain.Message, maxTokens, margin int) (prompt int, reserve int) {
	prompt = CountPrompt(messages)
	if maxTokens < 1 {
		maxTokens = 1
	}
	if margin < 0 {
		margin = 0
	}
	return prompt, prompt + maxTokens + margin
}
