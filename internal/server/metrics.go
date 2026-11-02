package server

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics is a small, dependency-free Prometheus exposition. It covers the
// three signals that matter for a rate limiter: decisions by outcome,
// decision latency, and backend failures.
type Metrics struct {
	mu            sync.RWMutex
	decisions     map[[2]string]*atomic.Uint64 // {rule, outcome}
	latency       map[string]*histogram        // by rule
	backendErrors atomic.Uint64
}

var latencyBuckets = []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1}

type histogram struct {
	counts []atomic.Uint64 // len(latencyBuckets)+1, last is +Inf
	sumNs  atomic.Uint64
}

// NewMetrics returns an empty metrics registry.
func NewMetrics() *Metrics {
	return &Metrics{
		decisions: make(map[[2]string]*atomic.Uint64),
		latency:   make(map[string]*histogram),
	}
}

// ObserveDecision records one limiter decision.
func (m *Metrics) ObserveDecision(rule, outcome string, d time.Duration) {
	m.counter(rule, outcome).Add(1)
	h := m.hist(rule)
	secs := d.Seconds()
	i := sort.SearchFloat64s(latencyBuckets, secs)
	h.counts[i].Add(1)
	h.sumNs.Add(uint64(d.Nanoseconds()))
}

// IncBackendError records a primary-backend failure.
func (m *Metrics) IncBackendError() { m.backendErrors.Add(1) }

func (m *Metrics) counter(rule, outcome string) *atomic.Uint64 {
	k := [2]string{rule, outcome}
	m.mu.RLock()
	c := m.decisions[k]
	m.mu.RUnlock()
	if c != nil {
		return c
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if c = m.decisions[k]; c == nil {
		c = new(atomic.Uint64)
		m.decisions[k] = c
	}
	return c
}

func (m *Metrics) hist(rule string) *histogram {
	m.mu.RLock()
	h := m.latency[rule]
	m.mu.RUnlock()
	if h != nil {
		return h
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if h = m.latency[rule]; h == nil {
		h = &histogram{counts: make([]atomic.Uint64, len(latencyBuckets)+1)}
		m.latency[rule] = h
	}
	return h
}

// ServeHTTP writes metrics in the Prometheus text exposition format.
func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	m.Render(w)
}

// Render writes all metrics in a stable order.
func (m *Metrics) Render(w io.Writer) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fmt.Fprintln(w, "# HELP throttle_decisions_total Rate-limit decisions by rule and outcome.")
	fmt.Fprintln(w, "# TYPE throttle_decisions_total counter")
	keys := make([][2]string, 0, len(m.decisions))
	for k := range m.decisions {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i][0]+keys[i][1] < keys[j][0]+keys[j][1] })
	for _, k := range keys {
		fmt.Fprintf(w, "throttle_decisions_total{rule=%q,outcome=%q} %d\n", k[0], k[1], m.decisions[k].Load())
	}

	fmt.Fprintln(w, "# HELP throttle_decision_duration_seconds Latency of rate-limit decisions.")
	fmt.Fprintln(w, "# TYPE throttle_decision_duration_seconds histogram")
	rules := make([]string, 0, len(m.latency))
	for r := range m.latency {
		rules = append(rules, r)
	}
	sort.Strings(rules)
	for _, r := range rules {
		h := m.latency[r]
		var cum uint64
		for i, b := range latencyBuckets {
			cum += h.counts[i].Load()
			fmt.Fprintf(w, "throttle_decision_duration_seconds_bucket{rule=%q,le=\"%s\"} %d\n", r, trimFloat(b), cum)
		}
		cum += h.counts[len(latencyBuckets)].Load()
		fmt.Fprintf(w, "throttle_decision_duration_seconds_bucket{rule=%q,le=\"+Inf\"} %d\n", r, cum)
		fmt.Fprintf(w, "throttle_decision_duration_seconds_sum{rule=%q} %g\n", r, float64(h.sumNs.Load())/1e9)
		fmt.Fprintf(w, "throttle_decision_duration_seconds_count{rule=%q} %d\n", r, cum)
	}

	fmt.Fprintln(w, "# HELP throttle_backend_errors_total Primary backend failures that triggered the failure mode.")
	fmt.Fprintln(w, "# TYPE throttle_backend_errors_total counter")
	fmt.Fprintf(w, "throttle_backend_errors_total %d\n", m.backendErrors.Load())
}

func trimFloat(f float64) string {
	s := fmt.Sprintf("%f", f)
	return strings.TrimRight(strings.TrimRight(s, "0"), ".")
}
