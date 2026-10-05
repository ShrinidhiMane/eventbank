package eventstore

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/example/eventbank/internal/events"
)

func ev(t *testing.T, id string, v int) events.Event {
	t.Helper()
	e, err := events.New(events.MoneyDeposited, id, v, events.MoneyDepositedData{Amount: 1})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestOptimisticConcurrency(t *testing.T) {
	s := NewMemory()
	if err := s.Append("a", 0, ev(t, "a", 1)); err != nil {
		t.Fatal(err)
	}
	// A second writer that also loaded version 0 must be rejected.
	if err := s.Append("a", 0, ev(t, "a", 1)); !errors.Is(err, ErrConcurrency) {
		t.Fatalf("want ErrConcurrency, got %v", err)
	}
	if err := s.Append("a", 1, ev(t, "a", 2)); err != nil {
		t.Fatal(err)
	}
	got, _ := s.Load("a")
	if len(got) != 2 {
		t.Fatalf("want 2 events, got %d", len(got))
	}
}

func TestFileStoreSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	s, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = s.Append("a", 0, ev(t, "a", 1), ev(t, "a", 2))
	_ = s.Append("b", 0, ev(t, "b", 1))
	s.Close()

	s2, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	all, _ := s2.All()
	a, _ := s2.Load("a")
	if len(all) != 3 || len(a) != 2 {
		t.Fatalf("after restart: all=%d a=%d", len(all), len(a))
	}
	if err := s2.Append("a", 2, ev(t, "a", 3)); err != nil {
		t.Fatalf("append after restart: %v", err)
	}
}
