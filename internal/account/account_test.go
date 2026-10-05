package account

import (
	"errors"
	"testing"

	"github.com/ShrinidhiMane/eventbank/internal/events"
)

// given/when/then style: given a history, when a command runs, then expect events or an error.
func given(t *testing.T, cmds ...func(*Account) ([]events.Event, error)) *Account {
	t.Helper()
	a := &Account{ID: "acc-1"}
	for _, c := range cmds {
		evts, err := c(a)
		if err != nil {
			t.Fatalf("setup command failed: %v", err)
		}
		for _, e := range evts {
			if err := a.Apply(e); err != nil {
				t.Fatal(err)
			}
		}
	}
	return a
}

func open(a *Account) ([]events.Event, error) { return a.OpenAccount("Ada") }
func dep(n int64) func(*Account) ([]events.Event, error) {
	return func(a *Account) ([]events.Event, error) { return a.Deposit(n) }
}

func TestDepositEmitsEvent(t *testing.T) {
	a := given(t, open)
	evts, err := a.Deposit(500)
	if err != nil {
		t.Fatal(err)
	}
	if len(evts) != 1 || evts[0].Type != events.MoneyDeposited || evts[0].Version != 2 {
		t.Fatalf("unexpected events: %+v", evts)
	}
	// Commands must not mutate state; only Apply does.
	if a.Balance != 0 {
		t.Fatalf("command mutated state: balance=%d", a.Balance)
	}
}

func TestBusinessRules(t *testing.T) {
	tests := []struct {
		name    string
		history []func(*Account) ([]events.Event, error)
		cmd     func(*Account) ([]events.Event, error)
		want    error
	}{
		{"deposit to missing account", nil, dep(100), ErrNotFound},
		{"open twice", []func(*Account) ([]events.Event, error){open}, open, ErrAlreadyExists},
		{"empty owner", nil, func(a *Account) ([]events.Event, error) { return a.OpenAccount("") }, ErrOwnerRequired},
		{"negative deposit", []func(*Account) ([]events.Event, error){open}, dep(-5), ErrInvalidAmount},
		{"overdraw", []func(*Account) ([]events.Event, error){open, dep(100)},
			func(a *Account) ([]events.Event, error) { return a.Withdraw(101) }, ErrInsufficientFunds},
		{"close with balance", []func(*Account) ([]events.Event, error){open, dep(100)},
			func(a *Account) ([]events.Event, error) { return a.Close("") }, ErrNonZeroBalance},
		{"deposit after close", []func(*Account) ([]events.Event, error){open,
			func(a *Account) ([]events.Event, error) { return a.Close("") }}, dep(1), ErrClosed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a := given(t, tc.history...)
			if _, err := tc.cmd(a); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func TestRehydrateReplaysHistory(t *testing.T) {
	a := given(t, open, dep(1000), dep(250))
	w, _ := a.Withdraw(300)
	_ = a.Apply(w[0])

	var history []events.Event
	b := &Account{ID: "acc-1"}
	for _, c := range []func(*Account) ([]events.Event, error){open, dep(1000), dep(250),
		func(x *Account) ([]events.Event, error) { return x.Withdraw(300) }} {
		evts, _ := c(b)
		_ = b.Apply(evts[0])
		history = append(history, evts...)
	}

	r, err := Rehydrate("acc-1", history)
	if err != nil {
		t.Fatal(err)
	}
	if r.Balance != 950 || r.Version != 4 || r.Owner != "Ada" {
		t.Fatalf("rehydrated wrong state: %+v", r)
	}
	if r.Balance != a.Balance {
		t.Fatalf("replay differs from live state")
	}
}
