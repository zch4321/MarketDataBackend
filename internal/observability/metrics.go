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
	helps      map[string]string
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
		helps:      make(map[string]string),
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
	r.helps[name] = help
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
	r.helps[name] = help
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
	r.helps[name] = help
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
		help := r.helps[name]
		fmt.Fprintf(w, "# HELP %s %s\n", name, help)
		fmt.Fprintf(w, "# TYPE %s counter\n", name)
		fmt.Fprintf(w, "%s%s %d\n", name, labels, r.counters[name].Value())
	}
	for _, name := range gauges {
		help := r.helps[name]
		fmt.Fprintf(w, "# HELP %s %s\n", name, help)
		fmt.Fprintf(w, "# TYPE %s gauge\n", name)
		fmt.Fprintf(w, "%s%s %d\n", name, labels, r.gauges[name].Value())
	}
	for _, name := range histograms {
		h := r.histograms[name]
		h.mu.Lock()
		help := r.helps[name]
		fmt.Fprintf(w, "# HELP %s %s\n", name, help)
		fmt.Fprintf(w, "# TYPE %s histogram\n", name)
		for i, upper := range h.buckets {
			cum := int64(0)
			for j := 0; j <= i; j++ {
				cum += h.counts[j]
			}
			lbl := histogramLabels(labels, fmt.Sprintf("%g", upper))
			fmt.Fprintf(w, "%s_bucket%s %d\n", name, lbl, cum)
		}
		inf := histogramLabels(labels, "+Inf")
		fmt.Fprintf(w, "%s_bucket%s %d\n", name, inf, h.total)
		fmt.Fprintf(w, "%s_sum%s %g\n", name, labels, h.sum)
		fmt.Fprintf(w, "%s_count%s %d\n", name, labels, h.total)
		h.mu.Unlock()
	}
}

// histogramLabels merges the static labels (already formatted as "{k1="v1",k2="v2"}")
// with the bucket le label. When static labels are empty, returns just the le pair.
func histogramLabels(staticLabels string, le string) string {
	if staticLabels == "" {
		return fmt.Sprintf(`{le="%s"}`, le)
	}
	// Insert le before the closing brace.
	return staticLabels[:len(staticLabels)-1] + fmt.Sprintf(`,le="%s"}`, le)
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
// Used by ObservedWriteLatencyMs (B1 metrics integration) to convert durations.
func toFloat64Seconds(d time.Duration) float64 {
	s := d.Seconds()
	if math.IsNaN(s) || math.IsInf(s, 0) {
		return 0
	}
	return s
}

// ---------------------------------------------------------------------------
// Global registry — convenience methods so business code can record metrics
// without threading a *Registry through every constructor.
// ---------------------------------------------------------------------------

var globalRegistry *Registry

// SetGlobalRegistry installs the process-wide registry created in main.
// Must be called once before any metric recording.
func SetGlobalRegistry(r *Registry) { globalRegistry = r }

// IncProcessedTotal increments the total processed messages counter by n.
func IncProcessedTotal(n int64) {
	if globalRegistry == nil {
		return
	}
	globalRegistry.Counter(MetricProcessedTotal, "Total number of messages decoded and persisted").Add(n)
}

// IncWriteErrors increments the write errors counter.
func IncWriteErrors() {
	if globalRegistry == nil {
		return
	}
	globalRegistry.Counter(MetricWriteErrorsTotal, "Total number of database write failures").Inc()
}

// IncDecodeErrors increments the decode errors counter.
func IncDecodeErrors() {
	if globalRegistry == nil {
		return
	}
	globalRegistry.Counter(MetricDecodeErrorsTotal, "Total number of message decode/validation failures").Inc()
}

// IncLeaseFailures increments the lease renewal failure counter.
func IncLeaseFailures() {
	if globalRegistry == nil {
		return
	}
	globalRegistry.Counter(MetricLeaseFailures, "Total number of group lease renewal failures").Inc()
}

// ObserveWriteLatencyMs records a storage write latency observation in milliseconds.
func ObserveWriteLatencyMs(ms float64) {
	if globalRegistry == nil {
		return
	}
	globalRegistry.
		Histogram(MetricStorageWriteLatency, "Database write latency in milliseconds", DefaultLatencyBuckets).
		Observe(ms)
}

// SetOwnedGroups sets the gauge for the number of groups currently owned by this runtime.
func SetOwnedGroups(n int64) {
	if globalRegistry == nil {
		return
	}
	globalRegistry.Gauge(MetricOwnedGroups, "Number of MarketGroups currently owned by this runtime").Set(n)
}

// ReportHeartbeat sets the heartbeat age metric to zero, indicating that the
// runtime node has just successfully heartbeated.  Consumers can monitor this
// gauge; a value persistently > 0 or absent indicates a stale node.
func ReportHeartbeat() {
	if globalRegistry == nil {
		return
	}
	globalRegistry.Gauge(MetricHeartbeatAgeSec, "Seconds since last successful runtime heartbeat").Set(0)
}
