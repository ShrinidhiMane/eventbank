package projection

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ShrinidhiMane/eventbank/internal/events"
)

// Alert is a notification produced by a downstream subscriber.
type Alert struct {
	AccountID string    `json:"account_id"`
	EventID   string    `json:"event_id"`
	Message   string    `json:"message"`
	At        time.Time `json:"at"`
}

// Alerts is an independent subscriber that flags large withdrawals. It shows
// fan-out: the same event reaches the balances projection and this service
// without either knowing about the other.
type Alerts struct {
	mu        sync.Mutex
	threshold int64
	seen      map[string]bool // event IDs already processed (idempotency)
	items     []Alert
}

// NewAlerts flags withdrawals at or above threshold cents.
func NewAlerts(threshold int64) *Alerts {
	return &Alerts{threshold: threshold, seen: map[string]bool{}}
}

func (a *Alerts) Handle(_ context.Context, e events.Event) error {
	if e.Type != events.MoneyWithdrawn {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.seen[e.ID] {
		return nil
	}
	var d events.MoneyWithdrawnData
	if err := e.Decode(&d); err != nil {
		return err
	}
	a.seen[e.ID] = true
	if d.Amount >= a.threshold {
		a.items = append(a.items, Alert{
			AccountID: e.AggregateID,
			EventID:   e.ID,
			Message:   fmt.Sprintf("large withdrawal of $%.2f", float64(d.Amount)/100),
			At:        e.Timestamp,
		})
	}
	return nil
}

// List returns alerts, newest first.
func (a *Alerts) List() []Alert {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Alert, len(a.items))
	for i, al := range a.items {
		out[len(a.items)-1-i] = al
	}
	return out
}

// Notifier simulates an unreliable external service (e.g. an email API) to
// demonstrate retries with backoff and the dead-letter queue. FailureRate is
// the probability (0..1) that a call fails.
type Notifier struct {
	mu          sync.Mutex
	FailureRate float64
	sent        map[string]bool
	rng         *rand.Rand
}

func NewNotifier(failureRate float64) *Notifier {
	return &Notifier{FailureRate: failureRate, sent: map[string]bool{}, rng: rand.New(rand.NewSource(time.Now().UnixNano()))}
}

// ErrUnavailable mimics a transient downstream outage.
var ErrUnavailable = errors.New("notification service unavailable")

func (n *Notifier) Handle(_ context.Context, e events.Event) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.sent[e.ID] {
		return nil
	}
	if n.rng.Float64() < n.FailureRate {
		return ErrUnavailable
	}
	n.sent[e.ID] = true
	return nil
}

// SetFailureRate changes the simulated failure rate at runtime.
func (n *Notifier) SetFailureRate(r float64) {
	n.mu.Lock()
	n.FailureRate = r
	n.mu.Unlock()
}

// Rate returns the current failure rate.
func (n *Notifier) Rate() float64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.FailureRate
}

// Sent returns how many notifications were delivered.
func (n *Notifier) Sent() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.sent)
}
