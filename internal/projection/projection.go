// Package projection is the query side of CQRS: denormalized read models kept
// up to date by subscribing to events. They are disposable and can always be
// rebuilt by replaying the event store.
package projection

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/example/eventbank/internal/events"
)

// AccountView is what the read API returns.
type AccountView struct {
	ID           string    `json:"id"`
	Owner        string    `json:"owner"`
	Balance      int64     `json:"balance_cents"`
	Status       string    `json:"status"`
	Deposits     int       `json:"deposits"`
	Withdrawals  int       `json:"withdrawals"`
	Version      int       `json:"version"`
	OpenedAt     time.Time `json:"opened_at"`
	LastActivity time.Time `json:"last_activity"`
}

// Accounts is the balances read model.
type Accounts struct {
	mu    sync.RWMutex
	views map[string]*AccountView
}

// NewAccounts returns an empty read model.
func NewAccounts() *Accounts { return &Accounts{views: map[string]*AccountView{}} }

// Handle applies one event. It is idempotent: events at or below the view's
// version are ignored, so redelivery (at-least-once) is safe.
func (p *Accounts) Handle(_ context.Context, e events.Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	v := p.views[e.AggregateID]
	if v != nil && e.Version <= v.Version {
		return nil // duplicate
	}
	if v == nil && e.Type != events.AccountOpened {
		return fmt.Errorf("projection: %s for unknown account %s (out of order)", e.Type, e.AggregateID)
	}
	if v != nil && e.Version != v.Version+1 {
		return fmt.Errorf("projection: gap for %s: have v%d, got v%d", e.AggregateID, v.Version, e.Version)
	}

	switch e.Type {
	case events.AccountOpened:
		var d events.AccountOpenedData
		if err := e.Decode(&d); err != nil {
			return err
		}
		v = &AccountView{ID: e.AggregateID, Owner: d.Owner, Status: "open", OpenedAt: e.Timestamp}
		p.views[e.AggregateID] = v
	case events.MoneyDeposited:
		var d events.MoneyDepositedData
		if err := e.Decode(&d); err != nil {
			return err
		}
		v.Balance += d.Amount
		v.Deposits++
	case events.MoneyWithdrawn:
		var d events.MoneyWithdrawnData
		if err := e.Decode(&d); err != nil {
			return err
		}
		v.Balance -= d.Amount
		v.Withdrawals++
	case events.AccountClosed:
		v.Status = "closed"
	}
	v.Version = e.Version
	v.LastActivity = e.Timestamp
	return nil
}

// Get returns one view.
func (p *Accounts) Get(id string) (AccountView, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	v, ok := p.views[id]
	if !ok {
		return AccountView{}, false
	}
	return *v, true
}

// List returns all views, newest first.
func (p *Accounts) List() []AccountView {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]AccountView, 0, len(p.views))
	for _, v := range p.views {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenedAt.After(out[j].OpenedAt) })
	return out
}

// Totals is a small aggregate query.
type Totals struct {
	Accounts     int   `json:"accounts"`
	OpenAccounts int   `json:"open_accounts"`
	TotalBalance int64 `json:"total_balance_cents"`
}

func (p *Accounts) Totals() Totals {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var t Totals
	for _, v := range p.views {
		t.Accounts++
		if v.Status == "open" {
			t.OpenAccounts++
		}
		t.TotalBalance += v.Balance
	}
	return t
}

// Reset clears the model before a rebuild.
func (p *Accounts) Reset() {
	p.mu.Lock()
	p.views = map[string]*AccountView{}
	p.mu.Unlock()
}

// Rebuild replays a full event log into the model.
func (p *Accounts) Rebuild(all []events.Event) error {
	p.Reset()
	for _, e := range all {
		if err := p.Handle(context.Background(), e); err != nil {
			return err
		}
	}
	return nil
}
