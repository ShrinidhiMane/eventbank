// Package events defines the immutable facts that flow through the system.
package events

import (
	"encoding/json"
	"time"

	"crypto/rand"
	"encoding/hex"
)

// Event types. Named in the past tense: an event is something that already happened.
const (
	AccountOpened  = "AccountOpened"
	MoneyDeposited = "MoneyDeposited"
	MoneyWithdrawn = "MoneyWithdrawn"
	AccountClosed  = "AccountClosed"
)

// Event is the envelope stored in the event store and sent over the broker.
type Event struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	AggregateID string          `json:"aggregate_id"`
	Version     int             `json:"version"` // position within the aggregate's stream (1-based)
	Timestamp   time.Time       `json:"timestamp"`
	Data        json.RawMessage `json:"data"`
}

// Payloads.
type AccountOpenedData struct {
	Owner string `json:"owner"`
}

type MoneyDepositedData struct {
	Amount int64 `json:"amount"` // cents
}

type MoneyWithdrawnData struct {
	Amount int64 `json:"amount"` // cents
}

type AccountClosedData struct {
	Reason string `json:"reason,omitempty"`
}

// New builds an event with a fresh ID and timestamp.
func New(eventType, aggregateID string, version int, payload any) (Event, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return Event{}, err
	}
	return Event{
		ID:          NewID(),
		Type:        eventType,
		AggregateID: aggregateID,
		Version:     version,
		Timestamp:   time.Now().UTC(),
		Data:        raw,
	}, nil
}

// Decode unmarshals the payload into v.
func (e Event) Decode(v any) error { return json.Unmarshal(e.Data, v) }

// NewID returns a random 16-byte hex identifier.
func NewID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
