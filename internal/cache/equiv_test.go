package cache

import (
	"encoding/json"
	"math/rand"
	"testing"

	"github.com/victorzhong0110/railhead/internal/domain"
)

// TestCanonicalMatchesJSON 守住这个不变量：
// appendCanonical 产出的字节必须与原先 json.Marshal(canon) **完全相同**，
// 否则所有缓存键都会变，Redis 里的旧条目全部失效，且行为是静默改变的。
//
// 随机化覆盖：引号、反斜杠、换行/制表、emoji、控制字符、
// 边界浮点、nil 与非 nil 空切片、omitempty 字段的各种组合。
func TestCanonicalMatchesJSON(t *testing.T) {
	r := rand.New(rand.NewSource(99))
	weird := []string{
		"", "a", "hi",
		`quote"inside`,
		`back\slash`,
		"换行\n制表\t",
		"emoji😀中文",
		"\x01\x02control",
		"三十个字符长度的测试字符串用来触发边界",
	}
	roles := []string{"user", "assistant", "system"}

	for i := 0; i < 3000; i++ {
		var msgs []domain.Message
		for j := 0; j < r.Intn(6); j++ {
			txt := weird[r.Intn(len(weird))]
			if r.Intn(3) == 0 {
				txt += string(rune('0' + r.Intn(10)))
			}
			rc := ""
			if r.Intn(3) == 0 {
				rc = weird[r.Intn(len(weird))]
			}
			role := roles[r.Intn(len(roles))]
			if r.Intn(8) == 0 {
				role = weird[r.Intn(len(weird))]
			}
			msgs = append(msgs, domain.Message{
				Role:             role,
				Content:          domain.Content{Text: txt},
				ReasoningContent: rc,
			})
		}

		var temp, topP *float64
		switch r.Intn(4) {
		case 0:
			v := r.Float64() * 2
			temp = &v
		case 1:
			v := 0.0
			temp = &v
		case 2:
			v := 1e-7
			temp = &v
		}
		if r.Intn(2) == 0 {
			v := r.Float64()
			topP = &v
		}
		var maxTok *int
		if r.Intn(2) == 0 {
			v := r.Intn(32000)
			maxTok = &v
		}
		var stop []string
		switch r.Intn(4) {
		case 0:
			stop = []string{weird[r.Intn(len(weird))], weird[r.Intn(len(weird))]}
		case 1:
			stop = []string{} // 非 nil 但为空：omitempty 应跳过整个字段
		}

		req := domain.ChatRequest{
			Model:       weird[r.Intn(len(weird))],
			Messages:    msgs,
			Temperature: temp,
			TopP:        topP,
			MaxTokens:   maxTok,
			Stop:        stop,
		}
		id := int64(r.Intn(100000))

		want := referenceCanonBytes(t, id, req)
		got := appendCanonical(nil, id, req)
		if string(want) != string(got) {
			t.Fatalf("i=%d 缓存键字节不等价\nreq=%+v\nwant=%s\ngot =%s", i, req, want, got)
		}
	}
	t.Log("3000 组随机输入：appendCanonical 与 json.Marshal 字节完全一致")
}

// referenceCanonBytes 是优化前的原始路径，保留在测试里作为对照基准。
func referenceCanonBytes(t *testing.T, apiKeyID int64, req domain.ChatRequest) []byte {
	t.Helper()
	canon := struct {
		KeyID       int64            `json:"key_id"`
		Model       string           `json:"model"`
		Messages    []domain.Message `json:"messages"`
		Temperature *float64         `json:"temperature,omitempty"`
		TopP        *float64         `json:"top_p,omitempty"`
		MaxTokens   *int             `json:"max_tokens,omitempty"`
		Stop        []string         `json:"stop,omitempty"`
	}{
		KeyID:       apiKeyID,
		Model:       req.Model,
		Messages:    req.Messages,
		Temperature: req.Temperature,
		TopP:        req.TopP,
		MaxTokens:   req.MaxTokens,
		Stop:        req.Stop,
	}
	b, err := json.Marshal(canon)
	if err != nil {
		t.Fatalf("reference marshal: %v", err)
	}
	return b
}
