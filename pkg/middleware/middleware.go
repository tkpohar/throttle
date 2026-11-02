// Package middleware provides net/http middleware that enforces a rate
// limit in-process, for services that embed throttle as a library instead
// of calling the HTTP decision service.
//
//	rule := limiter.Rule{Name: "api", Algorithm: limiter.TokenBucket, Limit: 100, Window: time.Minute}
//	h := middleware.RateLimit(lim, rule, middleware.ByClientIP)(mux)
package middleware

import (
	"log/slog"
	"net"
	"net/http"

	"github.com/tkpohar/throttle/pkg/limiter"
)

// KeyFunc extracts the rate-limit key (client identity) from a request.
// Returning "" skips limiting for that request.
type KeyFunc func(*http.Request) string

// ByClientIP keys on the connection's remote IP. Behind a trusted proxy,
// prefer a KeyFunc that reads the proxy's forwarding header instead.
func ByClientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// ByHeader keys on the value of a request header, e.g. "X-API-Key".
func ByHeader(name string) KeyFunc {
	return func(r *http.Request) string { return r.Header.Get(name) }
}

// RateLimit returns middleware enforcing rule per key. When the limiter
// returns an error the request is let through (fail-open) and logged; wrap
// the limiter in limiter.Failover for other policies.
func RateLimit(l limiter.Limiter, rule limiter.Rule, key KeyFunc) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			k := key(r)
			if k == "" {
				next.ServeHTTP(w, r)
				return
			}
			res, err := l.Allow(r.Context(), rule, k, 1)
			if err != nil {
				slog.Warn("rate limiter error; allowing request", "err", err)
				next.ServeHTTP(w, r)
				return
			}
			res.WriteHeaders(w.Header())
			if !res.Allowed {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
