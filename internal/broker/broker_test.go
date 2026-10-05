package broker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/example/eventbank/internal/events"
	"github.com/example/eventbank/internal/metrics"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))
var fast = RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: 2 * time.Millisecond}

func mk(id string, v int) events.Event {
	e, _ := events.New(events.MoneyDeposited, id, v, events.MoneyDepositedData{Amount: 1})
	return e
}

func TestFanOutAndOrdering(t *testing.T) {
	b := NewMemory(fast, nil, quiet)
	defer b.Close()

	var mu sync.Mutex
	got := map[string][]int{}
	for _, name := range []string{"one", "two"} {
		name := name
		b.Subscribe("t", name, func(_ context.Context, e events.Event) error {
			mu.Lock()
			got[name] = append(got[name], e.Version)
			mu.Unlock()
			return nil
		})
	}
	for v := 1; v <= 50; v++ {
		if err := b.Publish(context.Background(), "t", mk("a", v)); err != nil {
			t.Fatal(err)
		}
	}
	b.Flush()
	for _, name := range []string{"one", "two"} {
		if len(got[name]) != 50 {
			t.Fatalf("%s got %d events", name, len(got[name]))
		}
		for i, v := range got[name] {
			if v != i+1 {
				t.Fatalf("%s out of order at %d: %v", name, i, got[name][:i+1])
			}
		}
	}
}

func TestRetryThenSucceed(t *testing.T) {
	m := metrics.New()
	b := NewMemory(fast, m, quiet)
	defer b.Close()
	var calls atomic.Int32
	b.Subscribe("t", "flaky", func(context.Context, events.Event) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	})
	b.Publish(context.Background(), "t", mk("a", 1))
	b.Flush()
	if calls.Load() != 3 || len(b.DeadLetters()) != 0 || m.Counter("events_handled_flaky") != 1 {
		t.Fatalf("calls=%d dlq=%d", calls.Load(), len(b.DeadLetters()))
	}
}

func TestDeadLetterAndReplay(t *testing.T) {
	b := NewMemory(fast, nil, quiet)
	defer b.Close()
	var healthy atomic.Bool
	var handledOK, otherCalls atomic.Int32
	b.Subscribe("t", "broken", func(context.Context, events.Event) error {
		if !healthy.Load() {
			return errors.New("down")
		}
		handledOK.Add(1)
		return nil
	})
	b.Subscribe("t", "other", func(context.Context, events.Event) error { otherCalls.Add(1); return nil })

	b.Publish(context.Background(), "t", mk("a", 1), mk("a", 2))
	b.Flush()
	dl := b.DeadLetters()
	if len(dl) != 2 || dl[0].Subscriber != "broken" || dl[0].Attempts != 3 {
		t.Fatalf("unexpected DLQ: %+v", dl)
	}

	healthy.Store(true)
	n, err := b.Replay(context.Background())
	b.Flush()
	if err != nil || n != 2 || handledOK.Load() != 2 || len(b.DeadLetters()) != 0 {
		t.Fatalf("replay n=%d ok=%d err=%v", n, handledOK.Load(), err)
	}
	// Replay targets only the failed subscriber.
	if otherCalls.Load() != 2 {
		t.Fatalf("other subscriber saw %d events, want 2", otherCalls.Load())
	}
}

func TestPanicIsContained(t *testing.T) {
	b := NewMemory(fast, nil, quiet)
	defer b.Close()
	b.Subscribe("t", "boom", func(context.Context, events.Event) error { panic("bad") })
	b.Publish(context.Background(), "t", mk("a", 1))
	b.Flush()
	if len(b.DeadLetters()) != 1 {
		t.Fatal("panicking handler should dead-letter, not crash")
	}
}
