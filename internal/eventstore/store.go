// Package eventstore is the append-only source of truth for the write side.
package eventstore

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/example/eventbank/internal/events"
)

// ErrConcurrency is returned when the expected version doesn't match the stream's
// current version (optimistic concurrency control).
var ErrConcurrency = errors.New("eventstore: concurrency conflict")

// Store appends and loads event streams.
type Store interface {
	// Append adds events to an aggregate stream. expectedVersion is the version the
	// caller last saw (0 for a new stream).
	Append(aggregateID string, expectedVersion int, evts ...events.Event) error
	// Load returns all events for one aggregate, in order.
	Load(aggregateID string) ([]events.Event, error)
	// All returns every event in global order (used to rebuild projections).
	All() ([]events.Event, error)
}

// Memory is an in-memory store, optionally persisted to a JSON-lines file.
type Memory struct {
	mu      sync.RWMutex
	streams map[string][]events.Event
	log     []events.Event
	file    *os.File
}

// NewMemory creates a purely in-memory store.
func NewMemory() *Memory {
	return &Memory{streams: map[string][]events.Event{}}
}

// OpenFile creates a store backed by an append-only JSON-lines file. Existing
// events in the file are loaded on start, so state survives restarts.
func OpenFile(path string) (*Memory, error) {
	s := NewMemory()
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024)
	for sc.Scan() {
		var e events.Event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			f.Close()
			return nil, fmt.Errorf("eventstore: corrupt line: %w", err)
		}
		s.streams[e.AggregateID] = append(s.streams[e.AggregateID], e)
		s.log = append(s.log, e)
	}
	if err := sc.Err(); err != nil {
		f.Close()
		return nil, err
	}
	s.file = f
	return s, nil
}

func (s *Memory) Append(aggregateID string, expectedVersion int, evts ...events.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	current := len(s.streams[aggregateID])
	if current != expectedVersion {
		return fmt.Errorf("%w: aggregate %s expected v%d, at v%d", ErrConcurrency, aggregateID, expectedVersion, current)
	}
	for i, e := range evts {
		if e.Version != expectedVersion+i+1 {
			return fmt.Errorf("eventstore: event %d has version %d, want %d", i, e.Version, expectedVersion+i+1)
		}
	}
	if s.file != nil {
		for _, e := range evts {
			line, err := json.Marshal(e)
			if err != nil {
				return err
			}
			if _, err := s.file.Write(append(line, '\n')); err != nil {
				return err
			}
		}
		if err := s.file.Sync(); err != nil {
			return err
		}
	}
	s.streams[aggregateID] = append(s.streams[aggregateID], evts...)
	s.log = append(s.log, evts...)
	return nil
}

func (s *Memory) Load(aggregateID string) ([]events.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]events.Event(nil), s.streams[aggregateID]...), nil
}

func (s *Memory) All() ([]events.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return append([]events.Event(nil), s.log...), nil
}

// Close releases the backing file, if any.
func (s *Memory) Close() error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
