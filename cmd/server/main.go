// Command server runs EventBank: an event-sourced, CQRS bank-account service.
//
//	go run ./cmd/server                      # in-memory broker, events in eventbank.jsonl
//	go run -tags kafka ./cmd/server -broker kafka -kafka localhost:9092
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/example/eventbank/internal/api"
	"github.com/example/eventbank/internal/app"
	"github.com/example/eventbank/internal/broker"
	"github.com/example/eventbank/internal/eventstore"
	"github.com/example/eventbank/internal/metrics"
	"github.com/example/eventbank/internal/projection"
)

// newKafkaBroker is set by kafka.go when built with -tags kafka.
var newKafkaBroker func(bootstrap, group string, m *metrics.Registry, log *slog.Logger) (broker.Broker, error)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	dataFile := flag.String("data", "eventbank.jsonl", "event store file (empty = memory only)")
	brokerKind := flag.String("broker", "memory", "message broker: memory | kafka")
	kafkaAddr := flag.String("kafka", "localhost:9092", "Kafka bootstrap servers")
	alertAt := flag.Float64("alert", 1000, "flag withdrawals of at least this many dollars")
	failRate := flag.Float64("fail-rate", 0, "initial failure rate (0..1) of the simulated notifier")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if err := run(*addr, *dataFile, *brokerKind, *kafkaAddr, *alertAt, *failRate, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(addr, dataFile, brokerKind, kafkaAddr string, alertAt, failRate float64, log *slog.Logger) error {
	m := metrics.New()

	// 1. Event store: the write-side source of truth.
	var store *eventstore.Memory
	var err error
	if dataFile == "" {
		store = eventstore.NewMemory()
	} else if store, err = eventstore.OpenFile(dataFile); err != nil {
		return fmt.Errorf("open event store: %w", err)
	}
	defer store.Close()

	// 2. Read models, rebuilt from the full history on start.
	accounts := projection.NewAccounts()
	alerts := projection.NewAlerts(int64(alertAt * 100))
	notifier := projection.NewNotifier(failRate)
	history, _ := store.All()
	if err := accounts.Rebuild(history); err != nil {
		return fmt.Errorf("rebuild projection: %w", err)
	}
	for _, e := range history {
		_ = alerts.Handle(context.Background(), e)
	}
	log.Info("replayed event store", "events", len(history))

	// 3. Broker and subscribers.
	var b broker.Broker
	var dlq api.DLQ
	switch brokerKind {
	case "memory":
		mb := broker.NewMemory(broker.DefaultRetry, m, log)
		b, dlq = mb, mb
	case "kafka":
		if newKafkaBroker == nil {
			return errors.New("this binary was built without Kafka support; rebuild with -tags kafka")
		}
		if b, err = newKafkaBroker(kafkaAddr, "eventbank", m, log); err != nil {
			return err
		}
		if d, ok := b.(api.DLQ); ok {
			dlq = d
		}
	default:
		return fmt.Errorf("unknown broker %q", brokerKind)
	}
	stream := api.NewStream()
	for name, h := range map[string]broker.Handler{
		"balances": accounts.Handle,
		"alerts":   alerts.Handle,
		"notifier": notifier.Handle,
		"stream":   stream.Handle,
	} {
		if err := b.Subscribe(app.Topic, name, h); err != nil {
			return fmt.Errorf("subscribe %s: %w", name, err)
		}
	}

	// 4. HTTP.
	srv := &api.Server{
		Commands: app.NewCommands(store, b, m),
		Store:    store,
		Accounts: accounts,
		Alerts:   alerts,
		Notifier: notifier,
		DLQ:      dlq,
		Metrics:  m,
		Log:      log,
		Stream:   stream,
	}
	httpSrv := &http.Server{Addr: addr, Handler: srv.Routes(), ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() {
		log.Info("EventBank listening", "addr", addr, "broker", brokerKind, "dashboard", "http://localhost"+addr)
		errc <- httpSrv.ListenAndServe()
	}()

	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutdownCtx)
	return b.Close()
}
