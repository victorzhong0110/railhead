package mock

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/victorzhong0110/railhead/internal/domain"
)

// Handler 把 Engine 暴露成 OpenAI 兼容 HTTP，并提供运行中修改故障参数的接口。
func Handler(e *Engine) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("GET /admin/stats", func(w http.ResponseWriter, r *http.Request) {
		requests, injected := e.Stats()
		writeJSON(w, http.StatusOK, map[string]any{"requests": requests, "errors": injected})
	})
	mux.HandleFunc("POST /admin/fault", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ErrorRate     *float64 `json:"error_rate"`
			LatencyMS     *int     `json:"latency_ms"`
			StreamDelayMS *int     `json:"stream_delay_ms"`
			ErrorStatus   *int     `json:"error_status"`
			TTFTMS        *int     `json:"ttft_ms"`
			DecodeUS      *int     `json:"decode_us"`
			TailProb      *float64 `json:"tail_prob"`
			TailFactor    *float64 `json:"tail_factor"`
			Rate429       *float64 `json:"rate_429"`
			Rate500       *float64 `json:"rate_500"`
			RateTimeout   *float64 `json:"rate_timeout"`
			TimeoutMS     *int     `json:"timeout_ms"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f := Fault{ErrorRate: body.ErrorRate, ErrorStatus: body.ErrorStatus, TailProb: body.TailProb, TailFactor: body.TailFactor, Rate429: body.Rate429, Rate500: body.Rate500, RateTimeout: body.RateTimeout}
		if body.LatencyMS != nil {
			d := time.Duration(*body.LatencyMS) * time.Millisecond
			f.Latency = &d
		}
		if body.StreamDelayMS != nil {
			d := time.Duration(*body.StreamDelayMS) * time.Millisecond
			f.StreamDelay = &d
		}
		if body.TTFTMS != nil {
			d := time.Duration(*body.TTFTMS) * time.Millisecond
			f.TTFT = &d
		}
		if body.DecodeUS != nil {
			d := time.Duration(*body.DecodeUS) * time.Microsecond
			f.DecodePerToken = &d
		}
		if body.TimeoutMS != nil {
			d := time.Duration(*body.TimeoutMS) * time.Millisecond
			f.Timeout = &d
		}
		e.Apply(f)
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		var chat domain.ChatRequest
		if err := json.NewDecoder(r.Body).Decode(&chat); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if chat.Stream {
			writeStream(w, r, e, chat)
			return
		}
		resp, err := e.Complete(r.Context(), chat)
		if err != nil {
			writeUpstream(w, err)
			return
		}
		if resp.MockTTFTUs > 0 {
			w.Header().Set("X-Mock-TTFT-Us", fmtInt(resp.MockTTFTUs))
		}
		if resp.MockUpstreamUs > 0 {
			w.Header().Set("X-Mock-Upstream-Us", fmtInt(resp.MockUpstreamUs))
		}
		writeJSON(w, http.StatusOK, resp)
	})
	return mux
}

func writeStream(w http.ResponseWriter, r *http.Request, e *Engine, chat domain.ChatRequest) {
	st, err := e.Stream(r.Context(), chat)
	if err != nil {
		writeUpstream(w, err)
		return
	}
	defer st.Close()
	if ttft, upstream := st.Timing(); ttft > 0 || upstream > 0 {
		w.Header().Set("X-Mock-TTFT-Us", fmtInt(ttft.Microseconds()))
		w.Header().Set("X-Mock-Upstream-Us", fmtInt(upstream.Microseconds()))
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	flusher, _ := w.(http.Flusher)
	for {
		chunk, err := st.Recv()
		if errors.Is(err, io.EOF) {
			_, _ = io.WriteString(w, "data: [DONE]\n\n")
			if flusher != nil {
				flusher.Flush()
			}
			return
		}
		if err != nil {
			return
		}
		b, err := json.Marshal(chunk)
		if err != nil {
			return
		}
		if _, err := w.Write([]byte("data: ")); err != nil {
			return
		}
		if _, err := w.Write(b); err != nil {
			return
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return
		}
		if flusher != nil {
			flusher.Flush()
		}
	}
}

func writeUpstream(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	msg := err.Error()
	var ue *Error
	if errors.As(err, &ue) {
		if ue.Status > 0 {
			status = ue.Status
		}
		msg = ue.Error()
	}
	writeJSON(w, status, map[string]any{
		"error": map[string]string{"message": msg, "type": "upstream_error"},
	})
}

func fmtInt(n int64) string {
	return fmt.Sprintf("%d", n)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
