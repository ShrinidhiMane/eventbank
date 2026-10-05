// Package account is the write-side domain model. State is never stored
// directly: it is rebuilt by replaying events (event sourcing). Commands check
// business rules and return new events; they never mutate state themselves.
package account

import (
	"errors"
	"fmt"

	"github.com/ShrinidhiMane/eventbank/internal/events"
)

// Business-rule errors.
var (
	ErrNotFound          = errors.New("account not found")
	ErrAlreadyExists     = errors.New("account already exists")
	ErrClosed            = errors.New("account is closed")
	ErrInvalidAmount     = errors.New("amount must be positive")
	ErrInsufficientFunds = errors.New("insufficient funds")
	ErrNonZeroBalance    = errors.New("balance must be zero to close")
	ErrOwnerRequired     = errors.New("owner is required")
)

// Account is the aggregate.
type Account struct {
	ID      string
	Owner   string
	Balance int64 // cents
	Open    bool
	Closed  bool
	Version int // number of events applied
}

// Rehydrate rebuilds an account from its event history.
func Rehydrate(id string, history []events.Event) (*Account, error) {
	a := &Account{ID: id}
	for _, e := range history {
		if err := a.Apply(e); err != nil {
			return nil, err
		}
	}
	return a, nil
}

// Apply mutates state from one event. It must never fail on business rules:
// events are facts that already happened.
func (a *Account) Apply(e events.Event) error {
	switch e.Type {
	case events.AccountOpened:
		var d events.AccountOpenedData
		if err := e.Decode(&d); err != nil {
			return err
		}
		a.Owner, a.Open = d.Owner, true
	case events.MoneyDeposited:
		var d events.MoneyDepositedData
		if err := e.Decode(&d); err != nil {
			return err
		}
		a.Balance += d.Amount
	case events.MoneyWithdrawn:
		var d events.MoneyWithdrawnData
		if err := e.Decode(&d); err != nil {
			return err
		}
		a.Balance -= d.Amount
	case events.AccountClosed:
		a.Closed = true
	default:
		return fmt.Errorf("account: unknown event type %q", e.Type)
	}
	a.Version = e.Version
	return nil
}

// --- Commands: validate, then emit events. ---

func (a *Account) OpenAccount(owner string) ([]events.Event, error) {
	if a.Open {
		return nil, ErrAlreadyExists
	}
	if owner == "" {
		return nil, ErrOwnerRequired
	}
	return a.emit(events.AccountOpened, events.AccountOpenedData{Owner: owner})
}

func (a *Account) Deposit(amount int64) ([]events.Event, error) {
	if err := a.mustBeActive(); err != nil {
		return nil, err
	}
	if amount <= 0 {
		return nil, ErrInvalidAmount
	}
	return a.emit(events.MoneyDeposited, events.MoneyDepositedData{Amount: amount})
}

func (a *Account) Withdraw(amount int64) ([]events.Event, error) {
	if err := a.mustBeActive(); err != nil {
		return nil, err
	}
	if amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if amount > a.Balance {
		return nil, ErrInsufficientFunds
	}
	return a.emit(events.MoneyWithdrawn, events.MoneyWithdrawnData{Amount: amount})
}

func (a *Account) Close(reason string) ([]events.Event, error) {
	if err := a.mustBeActive(); err != nil {
		return nil, err
	}
	if a.Balance != 0 {
		return nil, ErrNonZeroBalance
	}
	return a.emit(events.AccountClosed, events.AccountClosedData{Reason: reason})
}

func (a *Account) mustBeActive() error {
	if !a.Open {
		return ErrNotFound
	}
	if a.Closed {
		return ErrClosed
	}
	return nil
}

func (a *Account) emit(eventType string, payload any) ([]events.Event, error) {
	e, err := events.New(eventType, a.ID, a.Version+1, payload)
	if err != nil {
		return nil, err
	}
	return []events.Event{e}, nil
}
