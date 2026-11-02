// Package config loads service configuration from a JSON file and
// environment variables.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/tkpohar/throttle/pkg/limiter"
)

// Config is the full service configuration.
type Config struct {
	ListenAddr   string       `json:"listen_addr"`
	Backend      string       `json:"backend"` // "redis" or "memory"
	RedisAddr    string       `json:"redis_addr"`
	RedisTimeout Duration     `json:"redis_timeout"`
	FailureMode  string       `json:"failure_mode"` // open | closed | local
	Rules        []RuleConfig `json:"rules"`
	rules        map[string]limiter.Rule
}

// RuleConfig is the on-disk form of a limiter.Rule.
type RuleConfig struct {
	Name      string   `json:"name"`
	Algorithm string   `json:"algorithm"`
	Limit     int64    `json:"limit"`
	Window    Duration `json:"window"`
	Burst     int64    `json:"burst,omitempty"`
}

// Duration is a time.Duration that (un)marshals as a string like "1m30s".
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"1s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Load reads a JSON config file, applies env overrides and defaults, and
// validates the result.
func Load(path string) (*Config, error) {
	cfg := &Config{}
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read config: %w", err)
		}
		if err := json.Unmarshal(b, cfg); err != nil {
			return nil, fmt.Errorf("parse config: %w", err)
		}
	}
	cfg.applyEnv()
	cfg.applyDefaults()
	return cfg, cfg.validate()
}

func (c *Config) applyEnv() {
	for env, dst := range map[string]*string{
		"THROTTLE_LISTEN_ADDR":  &c.ListenAddr,
		"THROTTLE_BACKEND":      &c.Backend,
		"THROTTLE_REDIS_ADDR":   &c.RedisAddr,
		"THROTTLE_FAILURE_MODE": &c.FailureMode,
	} {
		if v, ok := os.LookupEnv(env); ok {
			*dst = v
		}
	}
}

func (c *Config) applyDefaults() {
	if c.ListenAddr == "" {
		c.ListenAddr = ":8080"
	}
	if c.Backend == "" {
		c.Backend = "memory"
	}
	if c.RedisAddr == "" {
		c.RedisAddr = "localhost:6379"
	}
	if c.RedisTimeout == 0 {
		c.RedisTimeout = Duration(50 * time.Millisecond)
	}
	if c.FailureMode == "" {
		c.FailureMode = string(limiter.FailLocal)
	}
}

func (c *Config) validate() error {
	if c.Backend != "redis" && c.Backend != "memory" {
		return fmt.Errorf("backend must be redis or memory, got %q", c.Backend)
	}
	if _, err := limiter.ParseFailureMode(c.FailureMode); err != nil {
		return err
	}
	if len(c.Rules) == 0 {
		return fmt.Errorf("at least one rule is required")
	}
	c.rules = make(map[string]limiter.Rule, len(c.Rules))
	for _, rc := range c.Rules {
		r := limiter.Rule{
			Name:      rc.Name,
			Algorithm: limiter.Algorithm(rc.Algorithm),
			Limit:     rc.Limit,
			Window:    time.Duration(rc.Window),
			Burst:     rc.Burst,
		}
		if err := r.Validate(); err != nil {
			return err
		}
		if _, dup := c.rules[r.Name]; dup {
			return fmt.Errorf("duplicate rule %q", r.Name)
		}
		c.rules[r.Name] = r
	}
	return nil
}

// RuleSet returns validated rules keyed by name.
func (c *Config) RuleSet() map[string]limiter.Rule { return c.rules }
