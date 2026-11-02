package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeFile(t *testing.T, s string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "c.json")
	if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadExampleConfig(t *testing.T) {
	cfg, err := Load("../../config.example.json")
	if err != nil {
		t.Fatal(err)
	}
	r := cfg.RuleSet()["login"]
	if r.Limit != 5 || r.Window != time.Minute {
		t.Fatalf("login rule = %+v", r)
	}
}

func TestEnvOverridesAndDefaults(t *testing.T) {
	t.Setenv("THROTTLE_BACKEND", "memory")
	t.Setenv("THROTTLE_LISTEN_ADDR", ":9999")
	cfg, err := Load(writeFile(t, `{"backend":"redis","rules":[{"name":"a","algorithm":"token_bucket","limit":1,"window":"1s"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Backend != "memory" || cfg.ListenAddr != ":9999" || cfg.FailureMode != "local" {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestInvalidConfigs(t *testing.T) {
	for name, body := range map[string]string{
		"no rules":     `{}`,
		"bad duration": `{"rules":[{"name":"a","algorithm":"token_bucket","limit":1,"window":"soon"}]}`,
		"bad backend":  `{"backend":"etcd","rules":[{"name":"a","algorithm":"token_bucket","limit":1,"window":"1s"}]}`,
		"bad mode":     `{"failure_mode":"maybe","rules":[{"name":"a","algorithm":"token_bucket","limit":1,"window":"1s"}]}`,
		"duplicate":    `{"rules":[{"name":"a","algorithm":"token_bucket","limit":1,"window":"1s"},{"name":"a","algorithm":"token_bucket","limit":1,"window":"1s"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(writeFile(t, body)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
