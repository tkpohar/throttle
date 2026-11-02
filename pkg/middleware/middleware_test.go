package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/tkpohar/throttle/pkg/limiter"
)

func TestRateLimitMiddleware(t *testing.T) {
	mem := limiter.NewMemory()
	defer mem.Close()
	rule := limiter.Rule{Name: "mw", Algorithm: limiter.TokenBucket, Limit: 2, Window: time.Minute}

	h := RateLimit(mem, rule, ByHeader("X-API-Key"))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	do := func(key string) int {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if key != "" {
			req.Header.Set("X-API-Key", key)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}

	for _, want := range []int{204, 204, 429} {
		if got := do("k1"); got != want {
			t.Fatalf("got %d want %d", got, want)
		}
	}
	if got := do("k2"); got != 204 {
		t.Fatalf("separate key: got %d", got)
	}
	for i := 0; i < 5; i++ {
		if got := do(""); got != 204 {
			t.Fatalf("empty key should bypass, got %d", got)
		}
	}
}

func TestByClientIP(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "203.0.113.7:51234"
	if got := ByClientIP(req); got != "203.0.113.7" {
		t.Fatalf("got %q", got)
	}
}
