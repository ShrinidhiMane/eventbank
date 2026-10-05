package projection

import (
	"context"
	"testing"

	"github.com/example/eventbank/internal/events"
)

func e(t *testing.T, typ string, v int, data any) events.Event {
	t.Helper()
	ev, err := events.New(typ, "acc", v, data)
	if err != nil {
		t.Fatal(err)
	}
	return ev
}

func TestProjectionIsIdempotent(t *testing.T) {
	p := NewAccounts()
	ctx := context.Background()
	opened := e(t, events.AccountOpened, 1, events.AccountOpenedData{Owner: "Grace"})
	dep := e(t, events.MoneyDeposited, 2, events.MoneyDepositedData{Amount: 700})

	for _, ev := range []events.Event{opened, dep, dep, opened, dep} { // duplicates from redelivery
		if err := p.Handle(ctx, ev); err != nil {
			t.Fatal(err)
		}
	}
	v, _ := p.Get("acc")
	if v.Balance != 700 || v.Deposits != 1 || v.Version != 2 {
		t.Fatalf("duplicates changed the view: %+v", v)
	}
}

func TestProjectionDetectsGaps(t *testing.T) {
	p := NewAccounts()
	ctx := context.Background()
	_ = p.Handle(ctx, e(t, events.AccountOpened, 1, events.AccountOpenedData{Owner: "Grace"}))
	if err := p.Handle(ctx, e(t, events.MoneyDeposited, 3, events.MoneyDepositedData{Amount: 1})); err == nil {
		t.Fatal("expected gap error so the broker retries")
	}
}

func TestAlertsThreshold(t *testing.T) {
	a := NewAlerts(10000)
	ctx := context.Background()
	small := e(t, events.MoneyWithdrawn, 2, events.MoneyWithdrawnData{Amount: 9999})
	big := e(t, events.MoneyWithdrawn, 3, events.MoneyWithdrawnData{Amount: 10000})
	for _, ev := range []events.Event{small, big, big} {
		_ = a.Handle(ctx, ev)
	}
	if got := a.List(); len(got) != 1 || got[0].EventID != big.ID {
		t.Fatalf("alerts: %+v", got)
	}
}
