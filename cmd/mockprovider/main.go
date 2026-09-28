// Command mockprovider 是一个 OpenAI 兼容的假上游。
// 延迟、错误率、流式间隔都可以用环境变量设置，也可以在运行中 POST /admin/fault 修改。
// 它没有鉴权。只用于测试和压测，不要暴露到公网。
package main

import (
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/victorzhong0110/railhead/internal/provider/mock"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	addr := getenv("ADDR", ":8090")
	e := mock.New(mock.Options{
		Latency:           time.Duration(getenvInt("LATENCY_MS", 0)) * time.Millisecond,
		StreamDelay:       time.Duration(getenvInt("STREAM_DELAY_MS", 0)) * time.Millisecond,
		ErrorRate:         getenvFloat("ERROR_RATE", 0),
		ErrorStatus:       getenvInt("ERROR_STATUS", 500),
		DefaultCompletion: getenvInt("DEFAULT_COMPLETION_TOKENS", 16),
		TTFT:              time.Duration(getenvInt("TTFT_MS", 0)) * time.Millisecond,
		DecodePerToken:    time.Duration(getenvInt("DECODE_US", 0)) * time.Microsecond,
		TailProb:          getenvFloat("TAIL_PROB", 0),
		TailFactor:        getenvFloat("TAIL_FACTOR", 1),
		Rate429:           getenvFloat("RATE_429", 0),
		Rate500:           getenvFloat("RATE_500", 0),
		RateTimeout:       getenvFloat("RATE_TIMEOUT", 0),
		Timeout:           time.Duration(getenvInt("TIMEOUT_MS", 2000)) * time.Millisecond,
	})
	slog.Info("mock 上游监听", "addr", addr)
	if err := http.ListenAndServe(addr, mock.Handler(e)); err != nil {
		slog.Error("mock 退出", "err", err)
		os.Exit(1)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func getenvFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return n
}
