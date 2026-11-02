package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/tkpohar/throttle/pkg/limiter"
)

func newTestServer(t *testing.T) (*httptest.Server, *Metrics) {
	t.Helper()
	mem := limiter.NewMemory()
	t.Cleanup(func() { mem.Close() })
	m := NewMetrics()
	s := New(Options{
		Limiter: mem,
		Metrics: m,
		Rules: map[string]limiter.Rule{
			"login": {Name: "login", Algorithm: limiter.SlidingWindow, Limit: 2, Window: time.Minute},
		},
	})
	ts := httptest.NewServer(s.Handler())
	t.Cleanup(ts.Close)
	return ts, m
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url+"/v1/check", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestCheckAllowsThenDenies(t *testing.T) {
	ts, _ := newTestServer(t)
	body := `{"rule":"login","key":"alice"}`

	for i, wantRemaining := range []string{"1", "0"} {
		resp := post(t, ts.URL, body)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("req %d: status %d", i, resp.StatusCode)
		}
		if got := resp.Header.Get("RateLimit-Remaining"); got != wantRemaining {
			t.Fatalf("req %d: RateLimit-Remaining=%s want %s", i, got, wantRemaining)
		}
	}

	resp := post(t, ts.URL, body)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("missing Retry-After header")
	}
	var cr CheckResponse
	if err := json.NewDecoder(resp.Body).Decode(&cr); err != nil {
		t.Fatal(err)
	}
	if cr.Allowed || cr.RetryAfterMs <= 0 {
		t.Fatalf("unexpected body %+v", cr)
	}
}

func TestCheckValidation(t *testing.T) {
	ts, _ := newTestServer(t)
	cases := map[string]struct {
		body string
		want int
	}{
		"bad json":      {`{`, http.StatusBadRequest},
		"unknown field": {`{"rule":"login","key":"a","x":1}`, http.StatusBadRequest},
		"missing key":   {`{"rule":"login"}`, http.StatusBadRequest},
		"unknown rule":  {`{"rule":"nope","key":"a"}`, http.StatusNotFound},
		"cost too high": {`{"rule":"login","key":"a","cost":3}`, http.StatusUnprocessableEntity},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := post(t, ts.URL, tc.body).StatusCode; got != tc.want {
				t.Fatalf("status %d, want %d", got, tc.want)
			}
		})
	}
}

type errLimiter struct{}

func (errLimiter) Allow(context.Context, limiter.Rule, string, int64) (limiter.Result, error) {
	return limiter.Result{}, errors.New("boom")
}

func TestBackendErrorIs503AndNotReady(t *testing.T) {
	s := New(Options{
		Limiter: errLimiter{},
		Rules:   map[string]limiter.Rule{"r": {Name: "r", Algorithm: limiter.TokenBucket, Limit: 1, Window: time.Second}},
		Ready:   func(context.Context) error { return errors.New("redis down") },
	})
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	if got := post(t, ts.URL, `{"rule":"r","key":"k"}`).StatusCode; got != http.StatusServiceUnavailable {
		t.Fatalf("check status %d, want 503", got)
	}
	resp, err := http.Get(ts.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz %d, want 503", resp.StatusCode)
	}
}

func TestMetricsExposition(t *testing.T) {
	ts, _ := newTestServer(t)
	for i := 0; i < 3; i++ {
		post(t, ts.URL, `{"rule":"login","key":"bob"}`)
	}
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var sb strings.Builder
	buf := make([]byte, 4096)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			break
		}
	}
	out := sb.String()
	for _, want := range []string{
		`throttle_decisions_total{rule="login",outcome="allowed"} 2`,
		`throttle_decisions_total{rule="login",outcome="denied"} 1`,
		`throttle_decision_duration_seconds_count{rule="login"} 3`,
		`throttle_backend_errors_total 0`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q\n%s", want, out)
		}
	}
}
