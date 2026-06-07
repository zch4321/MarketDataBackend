// Package observability provides Prometheus-compatible metrics, a health-check
// handler and a readiness handler. It is shared by both md-control-plane and
// md-stream-runtime so every binary exposes uniform telemetry.
package observability

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Metric types
// ---------------------------------------------------------------------------

// Counter is a monotonically increasing cumulative metric.
type Counter struct{ val atomic.Int64 }

func (c *Counter) Inc()         { c.val.Add(1) }
func (c *Counter) Add(n int64)  { c.val.Add(n) }
func (c *Counter) Value() int64 { return c.val.Load() }

// Gauge is a value that can go up and down.
type Gauge struct{ val atomic.Int64 }

func (g *Gauge) Set(v int64)  { g.val.Store(v) }
func (g *Gauge) Value() int64 { return g.val.Load() }

// Histogram records observations into pre-configured buckets.
type Histogram struct {
	mu      sync.Mutex
	buckets []float64
	counts  []int64
	sum     float64
	total   int64
}

// NewHistogram creates a histogram with the given upper bounds (must be sorted).
// Typical latency buckets: 1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000 ms.
func NewHistogram(buckets []float64) *Histogram {
	return &Histogram{
		buckets: append([]float64(nil), buckets...),
		counts:  make([]int64, len(buckets)+1), // +∞ bucket is last
	}
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += v
	h.total++
	for i, upper := range h.buckets {
		if v <= upper {
			h.counts[i]++
			return
		}
	}
	h.counts[len(h.buckets)]++ // +∞ bucket
}

// ---------------------------------------------------------------------------
// Pre-defined latency buckets (milliseconds).
// ---------------------------------------------------------------------------
var DefaultLatencyBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000}

// ---------------------------------------------------------------------------
// Registry — holds named metrics and exposes /metrics.
// ---------------------------------------------------------------------------

// Registry collects counters, gauges and histograms and renders them in
// Prometheus text format on /metrics.  It is safe for concurrent use.
type Registry struct {
	mu         sync.Mutex
	counters   map[string]*Counter
	gauges     map[string]*Gauge
	histograms map[string]*Histogram
	start      time.Time
	labels     map[string]string // static labels added to every metric
}

// NewRegistry creates a Registry.  Static labels (e.g. service="md-stream-runtime")
// are added to every emitted metric line.
func NewRegistry(labels map[string]string) *Registry {
	return &Registry{
		counters:   make(map[string]*Counter),
		gauges:     make(map[string]*Gauge),
		histograms: make(map[string]*Histogram),
		start:      time.Now(),
		labels:     labels,
	}
}

// Counter returns the named Counter, creating it on first access.
func (r *Registry) Counter(name, help string) *Counter {
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c
	}
	c := &Counter{}
	r.counters[name] = c
	return c
}

// Gauge returns the named Gauge, creating it on first access.
func (r *Registry) Gauge(name, help string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	if g, ok := r.gauges[name]; ok {
		return g
	}
	g := &Gauge{}
	r.gauges[name] = g
	return g
}

// Histogram returns the named Histogram, creating it on first access.
func (r *Registry) Histogram(name, help string, buckets []float64) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h, ok := r.histograms[name]; ok {
		return h
	}
	h := NewHistogram(buckets)
	r.histograms[name] = h
	return h
}

// Handler returns an http.Handler that serves /metrics in Prometheus text format.
func (r *Registry) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", r.serveMetrics)
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (r *Registry) serveMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	r.mu.Lock()
	defer r.mu.Unlock()

	// Sort names for deterministic output.
	counters := sortedKeys(r.counters)
	gauges := sortedKeys(r.gauges)
	histograms := sortedKeys(r.histograms)

	labels := ""
	if len(r.labels) > 0 {
		parts := make([]string, 0, len(r.labels))
		for _, k := range sortedKeys(r.labels) {
			parts = append(parts, fmt.Sprintf(`%s="%s"`, k, r.labels[k]))
		}
		labels = "{" + strings.Join(parts, ",") + "}"
	}

	for _, name := range counters {
		fmt.Fprintf(w, "# HELP %s (counter)\n", name)
		fmt.Fprintf(w, "# TYPE %s counter\n", name)
		fmt.Fprintf(w, "%s%s %d\n", name, labels, r.counters[name].Value())
	}
	for _, name := range gauges {
		fmt.Fprintf(w, "# HELP %s (gauge)\n", name)
		fmt.Fprintf(w, "# TYPE %s gauge\n", name)
		fmt.Fprintf(w, "%s%s %d\n", name, labels, r.gauges[name].Value())
	}
	for _, name := range histograms {
		h := r.histograms[name]
		h.mu.Lock()
		fmt.Fprintf(w, "# HELP %s (histogram)\n", name)
		fmt.Fprintf(w, "# TYPE %s histogram\n", name)
		for i, upper := range h.buckets {
			le := fmt.Sprintf(`%s_bucket%s,le="%g"`, name, labels, upper)
			// cumulative count
			cum := int64(0)
			for j := 0; j <= i; j++ {
				cum += h.counts[j]
			}
			fmt.Fprintf(w, "%s %d\n", le, cum)
		}
		inf := fmt.Sprintf(`%s_bucket%s,le="+Inf"`, name, labels)
		fmt.Fprintf(w, "%s %d\n", inf, h.total)
		fmt.Fprintf(w, "%s_sum%s %g\n", name, labels, h.sum)
		fmt.Fprintf(w, "%s_count%s %d\n", name, labels, h.total)
		h.mu.Unlock()
	}
}

func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// ---------------------------------------------------------------------------
// Health / Readiness handlers
// ---------------------------------------------------------------------------

// HealthHandler returns 200 OK when the process is alive.
func HealthHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
}

// ReadinessHandler returns 200 OK when the given check function returns nil.
// If check returns an error, 503 Service Unavailable is returned with the
// error message in the body.
func ReadinessHandler(check func() error) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if err := check(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"status": "not ready",
				"error":  err.Error(),
			})
			return
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ready"})
	})
}

// ---------------------------------------------------------------------------
// Pre-defined metric names (convention: marketdata_<subsystem>_<name>)
// ---------------------------------------------------------------------------

const (
	// Runtime metrics.
	MetricHeartbeatAgeSec   = "marketdata_runtime_heartbeat_age_seconds"
	MetricOwnedGroups       = "marketdata_runtime_owned_groups"
	MetricProcessedTotal    = "marketdata_runtime_processed_messages_total"
	MetricWriteErrorsTotal  = "marketdata_runtime_write_errors_total"
	MetricDecodeErrorsTotal = "marketdata_runtime_decode_errors_total"
	MetricLeaseFailures     = "marketdata_runtime_lease_renew_failures_total"

	// Storage metrics.
	MetricStorageWriteLatency = "marketdata_storage_write_latency_ms"
)

// toFloat64Seconds returns seconds as a float64, clamping NaN/Inf to 0.
func toFloat64Seconds(d time.Duration) float64 {
	s := d.Seconds()
	if math.IsNaN(s) || math.IsInf(s, 0) {
		return 0
	}
	return s
}
