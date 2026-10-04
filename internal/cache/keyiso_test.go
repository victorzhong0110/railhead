package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"
)

// 闸门 2 隔离：Key() 的成本归属。
// Key = json.Marshal(canon) + sha256 + hex.EncodeToString

func canonOf(turns int) any {
	req := benchReq(turns)
	return &struct {
		KeyID       int64    `json:"key_id"`
		Model       string   `json:"model"`
		Messages    any      `json:"messages"`
		Temperature *float64 `json:"temperature,omitempty"`
	}{
		KeyID:    123,
		Model:    req.Model,
		Messages: req.Messages,
	}
}

func BenchmarkIsoMarshalOnly(b *testing.B) {
	c := canonOf(16)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(c)
	}
}

func BenchmarkIsoSHA256Only(b *testing.B) {
	// 用 Marshal 后的真实字节量做 SHA256
	buf, _ := json.Marshal(canonOf(16))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = sha256.Sum256(buf)
	}
}

func BenchmarkIsoHexOnly(b *testing.B) {
	sum := sha256.Sum256(make([]byte, 3800))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = hex.EncodeToString(sum[:])
	}
}

// 对照：手写拼接的字节量
func BenchmarkIsoHandWritten(b *testing.B) {
	req := benchReq(16)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		n := 0
		n += len(req.Model)
		for _, m := range req.Messages {
			n += len(m.Role) + len(m.Content.Text)
		}
		_ = n
	}
}
