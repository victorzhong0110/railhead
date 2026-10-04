package cache

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/victorzhong0110/railhead/internal/domain"
)

// 基线 benchmark：响应缓存是每个可缓存请求的热路径。
// 生产条件（闸门 1）：
//   CACHE_ENABLED 默认 true（config.go:81），Eligible 命中后 chat.go:84 必调 Get
//   Memory 在 Redis 不可用时接管（server.go:63 / gateway main.go:106）
//   缓存键含 key_id，命中率低但读请求远多于写
//   真实响应是 chat completion JSON，约 1-4 KB

const respSize = 2048

func benchValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + i%26)
	}
	return b
}

func benchReq(turns int) domain.ChatRequest {
	msgs := make([]domain.Message, 0, turns+1)
	msgs = append(msgs, domain.Message{Role: "system", Content: domain.Content{Text: "你是一个有帮助的助手。"}})
	r := rand.New(rand.NewSource(1))
	for i := 0; i < turns; i++ {
		msgs = append(msgs, domain.Message{
			Role:    "user",
			Content: domain.Content{Text: fmt.Sprintf("第 %d 个问题：请详细解释一下这个概念，并给出例子。", i)},
		})
		msgs = append(msgs, domain.Message{
			Role:    "assistant",
			Content: domain.Content{Text: fmt.Sprintf("第 %d 个回答：这是对应的解释内容，长度接近真实回复。%d", i, r.Intn(1000))},
		})
	}
	return domain.ChatRequest{Model: "minimax-m3", Messages: msgs}
}

// 闸门 1：测不同对话轮数下的 Key() 成本（json.Marshal + SHA256）。
func BenchmarkKey(b *testing.B) {
	for _, turns := range []int{1, 4, 16} {
		req := benchReq(turns)
		b.Run(fmt.Sprintf("turns=%d", turns), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_, _ = Key(123, req)
			}
		})
	}
}

// Memory.Get 的读路径：每个可缓存请求都调一次。
func BenchmarkMemoryGetHit(b *testing.B) {
	m := NewMemory()
	v := benchValue(respSize)
	ctx := context.Background()
	m.Set(ctx, "k", v, time.Hour)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = m.Get(ctx, "k")
	}
}

func BenchmarkMemoryGetMiss(b *testing.B) {
	m := NewMemory()
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = m.Get(ctx, "not-exist")
	}
}

func BenchmarkMemorySet(b *testing.B) {
	m := NewMemory()
	v := benchValue(respSize)
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = m.Set(ctx, "k", v, time.Hour)
	}
}

// 核心怀疑点：Get 是只读操作用了写锁（m.mu.Lock）。
// 读多写少的缓存上，RWMutex.RLock 能让并发读真正并行。
// 按纪律陷阱 B：并发数字必须串行复现一次才采信。
func BenchmarkMemoryGetParallel(b *testing.B) {
	m := NewMemory()
	v := benchValue(respSize)
	ctx := context.Background()
	m.Set(ctx, "k", v, time.Hour)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = m.Get(ctx, "k")
		}
	})
}

// 混合负载：生产里读远多于写。
func BenchmarkMemoryMixedParallel(b *testing.B) {
	m := NewMemory()
	v := benchValue(respSize)
	ctx := context.Background()
	m.Set(ctx, "k", v, time.Hour)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%20 == 0 {
				_ = m.Set(ctx, "k", v, time.Hour)
			} else {
				_, _, _ = m.Get(ctx, "k")
			}
			i++
		}
	})
}

// 串行对照：排除并发测量污染（纪律陷阱 B 的要求）。
func BenchmarkMemoryGetSerial(b *testing.B) {
	m := NewMemory()
	v := benchValue(respSize)
	ctx := context.Background()
	m.Set(ctx, "k", v, time.Hour)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _, _ = m.Get(ctx, "k")
	}
}
