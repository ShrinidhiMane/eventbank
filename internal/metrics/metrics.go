// Package metrics keeps simple counters and latency stats for monitoring the
// event flow. Exposed as JSON at /metrics and in Prometheus text at /metrics/prom.
package metrics

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// Registry is a concurrency-safe set of named counters and timers.
type Registry struct {
	mu       sync.Mutex
	counters map[string]int64
	timers   map[string]*timer
	started  time.Time
}

type timer struct {
	count int64
	total time.Duration
	max   time.Duration
}

// New returns an empty registry.
func New() *Registry {
	return &Registry{counters: map[string]int64{}, timers: map[string]*timer{}, started: time.Now()}
}

// Inc adds 1 to a counter.
func (r *Registry) Inc(name string) { r.Add(name, 1) }

// Add adds n to a counter.
func (r *Registry) Add(name string, n int64) {
	r.mu.Lock()
	r.counters[name] += n
	r.mu.Unlock()
}

// Observe records a duration for a timer.
func (r *Registry) Observe(name string, d time.Duration) {
	r.mu.Lock()
	t := r.timers[name]
	if t == nil {
		t = &timer{}
		r.timers[name] = t
	}
	t.count++
	t.total += d
	if d > t.max {
		t.max = d
	}
	r.mu.Unlock()
}

// Counter returns a counter's value.
func (r *Registry) Counter(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counters[name]
}

// Snapshot is a JSON-friendly copy of all metrics.
type Snapshot struct {
	UptimeSeconds float64                  `json:"uptime_seconds"`
	Counters      map[string]int64         `json:"counters"`
	Timers        map[string]TimerSnapshot `json:"timers"`
}

type TimerSnapshot struct {
	Count int64   `json:"count"`
	AvgMs float64 `json:"avg_ms"`
	MaxMs float64 `json:"max_ms"`
}

// Snapshot copies current values.
func (r *Registry) Snapshot() Snapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := Snapshot{
		UptimeSeconds: time.Since(r.started).Seconds(),
		Counters:      map[string]int64{},
		Timers:        map[string]TimerSnapshot{},
	}
	for k, v := range r.counters {
		s.Counters[k] = v
	}
	for k, t := range r.timers {
		ts := TimerSnapshot{Count: t.count, MaxMs: ms(t.max)}
		if t.count > 0 {
			ts.AvgMs = ms(t.total) / float64(t.count)
		}
		s.Timers[k] = ts
	}
	return s
}

// WritePrometheus writes metrics in Prometheus text exposition format.
func (r *Registry) WritePrometheus(w io.Writer) {
	s := r.Snapshot()
	names := make([]string, 0, len(s.Counters))
	for k := range s.Counters {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		fmt.Fprintf(w, "eventbank_%s_total %d\n", k, s.Counters[k])
	}
	names = names[:0]
	for k := range s.Timers {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		t := s.Timers[k]
		fmt.Fprintf(w, "eventbank_%s_count %d\n", k, t.Count)
		fmt.Fprintf(w, "eventbank_%s_avg_ms %.3f\n", k, t.AvgMs)
		fmt.Fprintf(w, "eventbank_%s_max_ms %.3f\n", k, t.MaxMs)
	}
	fmt.Fprintf(w, "eventbank_uptime_seconds %.0f\n", s.UptimeSeconds)
}

func ms(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
