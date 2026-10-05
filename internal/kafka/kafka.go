//go:build kafka

// Package kafka is a broker.Broker backed by Apache Kafka using the official
// Confluent Go client. Build with: go build -tags kafka ./...
//
// Design:
//   - Messages are keyed by aggregate ID so every event for one account lands
//     on the same partition and is consumed in order.
//   - Each subscriber name is its own consumer group, so every subscriber
//     receives every event (fan-out), and Kafka load-balances partitions across
//     instances of the same subscriber.
//   - Offsets are committed only after the handler succeeds (at-least-once).
//   - Failures retry with exponential backoff; events that still fail are
//     produced to "<topic>.dlq" and the offset moves on.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	ck "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	"github.com/example/eventbank/internal/broker"
	"github.com/example/eventbank/internal/events"
	"github.com/example/eventbank/internal/metrics"
)

// Broker implements broker.Broker on Kafka.
type Broker struct {
	bootstrap string
	group     string
	producer  *ck.Producer
	retry     broker.RetryPolicy
	metrics   *metrics.Registry
	log       *slog.Logger

	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.Mutex
	consumers []*ck.Consumer
	dlq       []broker.DeadLetter
}

// New connects a producer. Consumers are created per Subscribe call.
func New(bootstrap, group string, m *metrics.Registry, log *slog.Logger) (*Broker, error) {
	p, err := ck.NewProducer(&ck.ConfigMap{
		"bootstrap.servers":  bootstrap,
		"acks":               "all",
		"enable.idempotence": true, // no duplicates from producer retries
		"linger.ms":          5,
	})
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{bootstrap: bootstrap, group: group, producer: p, retry: broker.DefaultRetry, metrics: m, log: log, ctx: ctx, cancel: cancel}
	go b.logDeliveryErrors()
	return b, nil
}

func (b *Broker) logDeliveryErrors() {
	for ev := range b.producer.Events() {
		if msg, ok := ev.(*ck.Message); ok && msg.TopicPartition.Error != nil {
			b.metrics.Inc("kafka_delivery_errors")
			b.log.Error("kafka delivery failed", "err", msg.TopicPartition.Error)
		}
	}
}

// Publish produces events synchronously (waits for broker acknowledgement).
func (b *Broker) Publish(ctx context.Context, topic string, evts ...events.Event) error {
	for _, e := range evts {
		if err := b.produce(ctx, topic, e); err != nil {
			return err
		}
		b.metrics.Inc("events_published")
	}
	return nil
}

func (b *Broker) produce(ctx context.Context, topic string, e events.Event) error {
	val, err := json.Marshal(e)
	if err != nil {
		return err
	}
	done := make(chan ck.Event, 1)
	err = b.producer.Produce(&ck.Message{
		TopicPartition: ck.TopicPartition{Topic: &topic, Partition: ck.PartitionAny},
		Key:            []byte(e.AggregateID),
		Value:          val,
		Headers:        []ck.Header{{Key: "event-type", Value: []byte(e.Type)}},
	}, done)
	if err != nil {
		return err
	}
	select {
	case ev := <-done:
		if m, ok := ev.(*ck.Message); ok && m.TopicPartition.Error != nil {
			return m.TopicPartition.Error
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Subscribe starts a consumer in group "<group>-<name>".
func (b *Broker) Subscribe(topic, name string, h broker.Handler) error {
	c, err := ck.NewConsumer(&ck.ConfigMap{
		"bootstrap.servers":  b.bootstrap,
		"group.id":           b.group + "-" + name,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": false,
	})
	if err != nil {
		return fmt.Errorf("kafka consumer: %w", err)
	}
	if err := c.SubscribeTopics([]string{topic}, nil); err != nil {
		c.Close()
		return err
	}
	b.mu.Lock()
	b.consumers = append(b.consumers, c)
	b.mu.Unlock()
	b.wg.Add(1)
	go b.consume(c, topic, name, h)
	return nil
}

func (b *Broker) consume(c *ck.Consumer, topic, name string, h broker.Handler) {
	defer b.wg.Done()
	for b.ctx.Err() == nil {
		msg, err := c.ReadMessage(200 * time.Millisecond)
		if err != nil {
			var kerr ck.Error
			if errors.As(err, &kerr) && kerr.Code() == ck.ErrTimedOut {
				continue
			}
			b.log.Warn("kafka read", "subscriber", name, "err", err)
			continue
		}
		var e events.Event
		if err := json.Unmarshal(msg.Value, &e); err != nil {
			// Poison message: can't even decode it. Send raw to DLQ and move on.
			b.deadLetter(topic, name, events.Event{ID: "undecodable", Data: msg.Value}, err, 1)
			_, _ = c.CommitMessage(msg)
			continue
		}
		b.handleWithRetry(topic, name, h, e)
		if _, err := c.CommitMessage(msg); err != nil {
			b.log.Warn("kafka commit", "subscriber", name, "err", err)
		}
	}
}

func (b *Broker) handleWithRetry(topic, name string, h broker.Handler, e events.Event) {
	var err error
	for attempt := 1; attempt <= b.retry.MaxAttempts; attempt++ {
		start := time.Now()
		err = h(b.ctx, e)
		b.metrics.Observe("handler_latency_"+name, time.Since(start))
		if err == nil {
			b.metrics.Inc("events_handled_" + name)
			return
		}
		b.metrics.Inc("handler_errors_" + name)
		if attempt < b.retry.MaxAttempts {
			d := b.retry.BaseDelay << (attempt - 1)
			if d > b.retry.MaxDelay {
				d = b.retry.MaxDelay
			}
			select {
			case <-time.After(d):
			case <-b.ctx.Done():
				return
			}
		}
	}
	b.deadLetter(topic, name, e, err, b.retry.MaxAttempts)
}

func (b *Broker) deadLetter(topic, name string, e events.Event, err error, attempts int) {
	dl := broker.DeadLetter{Topic: topic, Subscriber: name, Event: e, Error: err.Error(), Attempts: attempts, FailedAt: time.Now().UTC()}
	b.metrics.Inc("dead_lettered")
	b.mu.Lock()
	b.dlq = append(b.dlq, dl)
	b.mu.Unlock()
	val, _ := json.Marshal(dl)
	dlqTopic := topic + ".dlq"
	if perr := b.producer.Produce(&ck.Message{
		TopicPartition: ck.TopicPartition{Topic: &dlqTopic, Partition: ck.PartitionAny},
		Key:            []byte(e.AggregateID),
		Value:          val,
	}, nil); perr != nil {
		b.log.Error("dlq produce failed", "err", perr)
	}
}

// DeadLetters returns letters dead-lettered by this process.
func (b *Broker) DeadLetters() []broker.DeadLetter {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]broker.DeadLetter(nil), b.dlq...)
}

// Replay re-produces dead-lettered events to their original topic. Every
// subscriber will see them again, which is safe because handlers are idempotent.
func (b *Broker) Replay(ctx context.Context) (int, error) {
	b.mu.Lock()
	letters := b.dlq
	b.dlq = nil
	b.mu.Unlock()
	for i, dl := range letters {
		if err := b.produce(ctx, dl.Topic, dl.Event); err != nil {
			b.mu.Lock()
			b.dlq = append(letters[i:], b.dlq...)
			b.mu.Unlock()
			return i, err
		}
	}
	b.metrics.Add("dlq_replayed", int64(len(letters)))
	return len(letters), nil
}

// Close stops consumers and flushes the producer.
func (b *Broker) Close() error {
	b.cancel()
	b.wg.Wait()
	b.mu.Lock()
	for _, c := range b.consumers {
		_ = c.Close()
	}
	b.mu.Unlock()
	b.producer.Flush(5000)
	b.producer.Close()
	return nil
}
