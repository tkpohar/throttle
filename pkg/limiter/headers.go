package limiter

import (
	"math"
	"net/http"
	"strconv"
	"time"
)

// WriteHeaders sets standard rate-limit response headers following the
// IETF httpapi "RateLimit header fields" draft, plus Retry-After on denial.
func (r Result) WriteHeaders(h http.Header) {
	h.Set("RateLimit-Limit", strconv.FormatInt(r.Limit, 10))
	h.Set("RateLimit-Remaining", strconv.FormatInt(r.Remaining, 10))
	h.Set("RateLimit-Reset", strconv.FormatInt(ceilSeconds(r.ResetAfter), 10))
	if !r.Allowed {
		h.Set("Retry-After", strconv.FormatInt(max(1, ceilSeconds(r.RetryAfter)), 10))
	}
}

func ceilSeconds(d time.Duration) int64 { return int64(math.Ceil(d.Seconds())) }
