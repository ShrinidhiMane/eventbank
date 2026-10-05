// Package broker implements publish/subscribe messaging. The in-memory broker is
// used by default; a Kafka-backed broker lives in internal/kafka (build tag "kafka").
package broker

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ShrinidhiMane/eventbank/internal/events"
	"github.com/ShrinidhiMane/eventbank/internal/metrics"
)

// Handler processes one event. Returning an error triggers a retry.
type Handler func(ctx context.Context, e events.Event) error

// Broker is the pub/sub contract used by the application.
type Broker interface {
	Publish(ctx context.Context, topic string, evts ...events.Event) error
	// Subscribe registers a named consumer on a topic. Every subscriber name gets
	// its own copy of each event (fan-out), processed in publish order.
	Subscribe(topic, name string, h Handler) error
	Close() error
}

// DeadLetter is an event that exhausted its retries.
type DeadLetter struct {
	Topic      string       `json:"topic"`
	Subscriber string       `json:"subscriber"`
	Event      events.Event `json:"event"`
	Error      string       `json:"error"`
	Attempts   int          `json:"attempts"`
	FailedAt   time.Time    `json:"failed_at"`
}

// RetryPolicy controls redelivery with exponential backoff.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// DefaultRetry is 5 attempts starting at 10ms, capped at 500ms.
var DefaultRetry = RetryPolicy{MaxAttempts: 5, BaseDelay: 10 * time.Millisecond, MaxDelay: 500 * time.Millisecond}

func (p RetryPolicy) delay(attempt int) time.Duration {
	d := p.BaseDelay << (attempt - 1)
	if d > p.MaxDelay || d <= 0 {
		d = p.MaxDelay
	}
	return d
}

// ErrClosed is returned after Close.
var ErrClosed = errors.New("broker: closed")

// Memory is an in-process broker: each subscription has its own buffered queue
// and worker goroutine, giving per-subscriber ordering and isolation (a slow or
// failing subscriber doesn't block the others).
type Memory struct {
	mu      sync.RWMutex
	subs    map[string][]*subscription
	dlq     []DeadLetter
	dlqMu   sync.Mutex
	retry   RetryPolicy
	metrics *metrics.Registry
	log     *slog.Logger
	pending sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
	workers sync.WaitGroup
	closed  bool
}

type subscription struct {
	topic, name string
	handler     Handler
	queue       chan events.Event
}

// NewMemory creates an in-memory broker.
func NewMemory(retry RetryPolicy, m *metrics.Registry, log *slog.Logger) *Memory {
	if m == nil {
		m = metrics.New()
	}
	if log == nil {
		log = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Memory{subs: map[string][]*subscription{}, retry: retry, metrics: m, log: log, ctx: ctx, cancel: cancel}
}

func (b *Memory) Subscribe(topic, name string, h Handler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	s := &subscription{topic: topic, name: name, handler: h, queue: make(chan events.Event, 1024)}
	b.subs[topic] = append(b.subs[topic], s)
	b.workers.Add(1)
	go b.run(s)
	return nil
}

func (b *Memory) Publish(ctx context.Context, topic string, evts ...events.Event) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return ErrClosed
	}
	for _, e := range evts {
		b.metrics.Inc("events_published")
		for _, s := range b.subs[topic] {
			b.pending.Add(1)
			select {
			case s.queue <- e:
			case <-ctx.Done():
				b.pending.Done()
				return ctx.Err()
			}
		}
	}
	return nil
}

func (b *Memory) run(s *subscription) {
	defer b.workers.Done()
	for {
		select {
		case <-b.ctx.Done():
			return
		case e := <-s.queue:
			b.deliver(s, e)
			b.pending.Done()
		}
	}
}

func (b *Memory) deliver(s *subscription, e events.Event) {
	var err error
	for attempt := 1; attempt <= b.retry.MaxAttempts; attempt++ {
		start := time.Now()
		err = safeCall(b.ctx, s.handler, e)
		b.metrics.Observe("handler_latency_"+s.name, time.Since(start))
		if err == nil {
			b.metrics.Inc("events_handled_" + s.name)
			return
		}
		b.metrics.Inc("handler_errors_" + s.name)
		b.log.Warn("handler failed", "subscriber", s.name, "event", e.Type, "id", e.ID, "attempt", attempt, "err", err)
		if attempt < b.retry.MaxAttempts {
			select {
			case <-time.After(b.retry.delay(attempt)):
			case <-b.ctx.Done():
				return
			}
		}
	}
	b.metrics.Inc("dead_lettered")
	b.log.Error("event dead-lettered", "subscriber", s.name, "event", e.Type, "id", e.ID, "err", err)
	b.dlqMu.Lock()
	b.dlq = append(b.dlq, DeadLetter{Topic: s.topic, Subscriber: s.name, Event: e, Error: err.Error(), Attempts: b.retry.MaxAttempts, FailedAt: time.Now().UTC()})
	b.dlqMu.Unlock()
}

// safeCall turns a handler panic into an error so one bad event can't crash a worker.
func safeCall(ctx context.Context, h Handler, e events.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errors.New("panic in handler")
		}
	}()
	return h(ctx, e)
}

// DeadLetters returns a copy of the dead-letter queue.
func (b *Memory) DeadLetters() []DeadLetter {
	b.dlqMu.Lock()
	defer b.dlqMu.Unlock()
	return append([]DeadLetter(nil), b.dlq...)
}

// Replay re-publishes every dead-lettered event to its original subscriber only,
// and clears the queue. Returns how many were replayed.
func (b *Memory) Replay(ctx context.Context) (int, error) {
	b.dlqMu.Lock()
	letters := b.dlq
	b.dlq = nil
	b.dlqMu.Unlock()

	b.mu.RLock()
	defer b.mu.RUnlock()
	n := 0
	for _, dl := range letters {
		for _, s := range b.subs[dl.Topic] {
			if s.name != dl.Subscriber {
				continue
			}
			b.pending.Add(1)
			select {
			case s.queue <- dl.Event:
				n++
			case <-ctx.Done():
				b.pending.Done()
				return n, ctx.Err()
			}
		}
	}
	b.metrics.Add("dlq_replayed", int64(n))
	return n, nil
}

// Flush blocks until every published event has been handled or dead-lettered.
func (b *Memory) Flush() { b.pending.Wait() }

// Close stops all workers after draining in-flight events.
func (b *Memory) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()
	b.pending.Wait()
	b.cancel()
	b.workers.Wait()
	return nil
}
