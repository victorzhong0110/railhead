package config

import (
	"testing"
)

func TestChannelAPIKeyComesFromEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("MINIMAX_API_KEY", "test-secret-value")
	t.Setenv("CHANNELS_FILE", "")
	t.Setenv("CHANNELS_JSON", `[{"name":"minimax-m3","base_url":"https://api.minimaxi.com/v1","api_key":"${MINIMAX_API_KEY}","models":["MiniMax-M3"],"upstream_model":"MiniMax-M3"}]`)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Channels) != 1 {
		t.Fatalf("channels %d", len(cfg.Channels))
	}
	ch := cfg.Channels[0]
	if ch.APIKey != "test-secret-value" || ch.UpstreamModel != "MiniMax-M3" {
		t.Fatal("channel did not expand the env placeholder or upstream model")
	}
}

func TestChannelAPIKeyMissingEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example")
	t.Setenv("CHANNELS_FILE", "")
	t.Setenv("CHANNELS_JSON", `[{"name":"minimax-m3","api_key":"${DEFINITELY_UNSET_MM_KEY}","models":["MiniMax-M3"]}]`)
	if _, err := Load(); err == nil {
		t.Fatal("expected missing env to fail configuration")
	}
}
