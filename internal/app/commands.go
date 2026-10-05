// Package app is the command side of CQRS: it loads an aggregate from the event
// store, runs a command, appends the resulting events and publishes them.
package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/example/eventbank/internal/account"
	"github.com/example/eventbank/internal/broker"
	"github.com/example/eventbank/internal/events"
	"github.com/example/eventbank/internal/eventstore"
	"github.com/example/eventbank/internal/metrics"
)

// Topic is where account events are published.
const Topic = "accounts"

// Commands handles writes.
type Commands struct {
	store   eventstore.Store
	broker  broker.Broker
	metrics *metrics.Registry
}

// NewCommands wires the command handler.
func NewCommands(s eventstore.Store, b broker.Broker, m *metrics.Registry) *Commands {
	if m == nil {
		m = metrics.New()
	}
	return &Commands{store: s, broker: b, metrics: m}
}

// OpenAccount creates a new account and returns its ID.
func (c *Commands) OpenAccount(ctx context.Context, owner string) (string, error) {
	id := events.NewID()[:12]
	_, err := c.execute(ctx, id, func(a *account.Account) ([]events.Event, error) { return a.OpenAccount(owner) })
	return id, err
}

func (c *Commands) Deposit(ctx context.Context, id string, cents int64) (*account.Account, error) {
	return c.execute(ctx, id, func(a *account.Account) ([]events.Event, error) { return a.Deposit(cents) })
}

func (c *Commands) Withdraw(ctx context.Context, id string, cents int64) (*account.Account, error) {
	return c.execute(ctx, id, func(a *account.Account) ([]events.Event, error) { return a.Withdraw(cents) })
}

func (c *Commands) Close(ctx context.Context, id, reason string) (*account.Account, error) {
	return c.execute(ctx, id, func(a *account.Account) ([]events.Event, error) { return a.Close(reason) })
}

// execute runs load → decide → append → publish, retrying on optimistic
// concurrency conflicts (another writer appended to the same stream first).
func (c *Commands) execute(ctx context.Context, id string, decide func(*account.Account) ([]events.Event, error)) (*account.Account, error) {
	const maxConflicts = 3
	for try := 0; ; try++ {
		history, err := c.store.Load(id)
		if err != nil {
			return nil, err
		}
		acc, err := account.Rehydrate(id, history)
		if err != nil {
			return nil, err
		}
		newEvents, err := decide(acc)
		if err != nil {
			c.metrics.Inc("commands_rejected")
			return nil, err
		}
		err = c.store.Append(id, acc.Version, newEvents...)
		if errors.Is(err, eventstore.ErrConcurrency) && try < maxConflicts {
			c.metrics.Inc("concurrency_retries")
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, e := range newEvents {
			if err := acc.Apply(e); err != nil {
				return nil, err
			}
		}
		c.metrics.Inc("commands_accepted")
		// The event store is the source of truth; a publish failure is reported
		// but the write already happened. Projections can be rebuilt from the
		// store (see Queries.Rebuild). In production use an outbox for this step.
		if err := c.broker.Publish(ctx, Topic, newEvents...); err != nil {
			c.metrics.Inc("publish_failures")
			return acc, fmt.Errorf("events stored but not published: %w", err)
		}
		return acc, nil
	}
}

// History returns the raw event stream for an account (the audit log).
func (c *Commands) History(id string) ([]events.Event, error) {
	h, err := c.store.Load(id)
	if err != nil {
		return nil, err
	}
	if len(h) == 0 {
		return nil, account.ErrNotFound
	}
	return h, nil
}

// StateAt rebuilds the account as it was at a given version ("time travel").
func (c *Commands) StateAt(id string, version int) (*account.Account, error) {
	h, err := c.History(id)
	if err != nil {
		return nil, err
	}
	if version > 0 && version < len(h) {
		h = h[:version]
	}
	return account.Rehydrate(id, h)
}
