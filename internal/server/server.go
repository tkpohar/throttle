// Package server exposes the rate limiter as an HTTP decision service.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/tkpohar/throttle/pkg/limiter"
)

const maxBodyBytes = 4 << 10

// Server is the HTTP API.
type Server struct {
	limiter limiter.Limiter
	rules   map[string]limiter.Rule
	metrics *Metrics
	ready   func(context.Context) error
	log     *slog.Logger
}

// Options configures a Server.
type Options struct {
	Limiter limiter.Limiter
	Rules   map[string]limiter.Rule
	Metrics *Metrics
	// Ready reports backend readiness for /readyz. Nil means always ready.
	Ready  func(context.Context) error
	Logger *slog.Logger
}

// New builds a Server.
func New(o Options) *Server {
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Metrics == nil {
		o.Metrics = NewMetrics()
	}
	return &Server{limiter: o.Limiter, rules: o.Rules, metrics: o.Metrics,
		ready: o.Ready, log: o.Logger}
}

// Handler returns the routed http.Handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/check", s.handleCheck)
	mux.HandleFunc("GET /v1/rules", s.handleRules)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /metrics", s.metrics)
	return mux
}

// CheckRequest is the body of POST /v1/check.
type CheckRequest struct {
	Rule string `json:"rule"`
	Key  string `json:"key"`
	Cost int64  `json:"cost,omitempty"` // defaults to 1
}

// CheckResponse is returned by POST /v1/check.
type CheckResponse struct {
	Allowed      bool    `json:"allowed"`
	Limit        int64   `json:"limit"`
	Remaining    int64   `json:"remaining"`
	RetryAfterMs float64 `json:"retry_after_ms"`
	ResetAfterMs float64 `json:"reset_after_ms"`
}

func (s *Server) handleCheck(w http.ResponseWriter, r *http.Request) {
	var req CheckRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}
	if req.Cost == 0 {
		req.Cost = 1
	}
	rule, ok := s.rules[req.Rule]
	if !ok {
		writeError(w, http.StatusNotFound, "unknown rule "+strconv.Quote(req.Rule))
		return
	}

	start := time.Now()
	res, err := s.limiter.Allow(r.Context(), rule, req.Key, req.Cost)
	elapsed := time.Since(start)
	switch {
	case errors.Is(err, limiter.ErrCostExceedsCapacity):
		writeError(w, http.StatusUnprocessableEntity, err.Error())
		return
	case err != nil:
		s.log.Error("limiter decision failed", "rule", rule.Name, "err", err)
		writeError(w, http.StatusServiceUnavailable, "rate limiter unavailable")
		return
	}

	outcome := "allowed"
	status := http.StatusOK
	if !res.Allowed {
		outcome, status = "denied", http.StatusTooManyRequests
	}
	s.metrics.ObserveDecision(rule.Name, outcome, elapsed)
	res.WriteHeaders(w.Header())
	writeJSON(w, status, CheckResponse{
		Allowed:      res.Allowed,
		Limit:        res.Limit,
		Remaining:    res.Remaining,
		RetryAfterMs: ms(res.RetryAfter),
		ResetAfterMs: ms(res.ResetAfter),
	})
}

func (s *Server) handleRules(w http.ResponseWriter, _ *http.Request) {
	type ruleView struct {
		Name      string `json:"name"`
		Algorithm string `json:"algorithm"`
		Limit     int64  `json:"limit"`
		Window    string `json:"window"`
		Burst     int64  `json:"burst,omitempty"`
	}
	out := make([]ruleView, 0, len(s.rules))
	for _, r := range s.rules {
		out = append(out, ruleView{r.Name, string(r.Algorithm), r.Limit, r.Window.String(), r.Burst})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if s.ready != nil {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := s.ready(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func ms(d time.Duration) float64 { return math.Round(float64(d)/1e3) / 1e3 }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
