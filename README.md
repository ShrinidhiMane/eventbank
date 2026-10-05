# EventBank

A small but complete event-driven application in Go, built to put the ideas from
*Building Event-Driven Applications in Go* into practice. It's a bank-account
service where every change is an event, reads and writes are separated, and
independent services react to events through a message broker.

## Run it

Requires Go 1.22+.

```bash
go run ./cmd/server
# open http://localhost:8080
```

That uses the built-in in-memory broker and stores events in `eventbank.jsonl`,
so your accounts survive restarts. Useful flags:

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `:8080` | HTTP address |
| `-data` | `eventbank.jsonl` | event store file (`""` = memory only) |
| `-broker` | `memory` | `memory` or `kafka` |
| `-kafka` | `localhost:9092` | Kafka bootstrap servers |
| `-alert` | `1000` | flag withdrawals of at least this many dollars |
| `-fail-rate` | `0` | starting failure rate (0–1) of the simulated notifier |

### With Kafka

```bash
docker compose up -d                                    # single-node Kafka
go run -tags kafka ./cmd/server -broker kafka           # needs cgo (default on macOS/Linux)
```

The Kafka adapter uses the official `confluent-kafka-go` client and is behind a
build tag, so the default build has no cgo or Kafka dependency.

### Tests

```bash
go test -race ./...
```

## What each course topic maps to

| Course topic | Where it lives |
|---|---|
| Events & event handlers | `internal/events` (past-tense facts in an envelope), `broker.Handler` |
| Event store | `internal/eventstore` — append-only, JSON-lines file, optimistic concurrency (`ErrConcurrency`) |
| Event sourcing | `internal/account` — the aggregate has no stored state; `Rehydrate` replays events. Commands validate and *return* events; only `Apply` changes state |
| CQRS | Write side: `internal/app` (load → decide → append → publish). Read side: `internal/projection` (denormalized views updated by events) |
| Publisher / subscriber | `internal/broker` — every subscriber name gets its own ordered queue (fan-out) |
| Kafka | `internal/kafka` — events keyed by account ID (per-account ordering), one consumer group per subscriber, manual offset commit after success (at-least-once), idempotent producer |
| Error handling | Retries with exponential backoff, panic recovery, dead-letter queue (`<topic>.dlq` on Kafka), replay endpoint |
| Idempotency | Projections ignore events at or below the version they've seen; alerts/notifier dedupe by event ID. So redelivery and DLQ replay are safe |
| Eventual consistency | Command responses return write-side state; the read model catches up asynchronously. Gaps are detected and retried |
| Monitoring | `internal/metrics` — counters and latency per subscriber, `/metrics` (JSON) and `/metrics/prom` (Prometheus) |
| Testing | Given/when/then aggregate tests, broker retry/DLQ tests, projection idempotency tests, end-to-end HTTP tests including a concurrent-overdraw test |

## Flow of one command

```
POST /accounts/{id}/withdraw
  └─ app.Commands: load events → Rehydrate → Withdraw() checks rules → MoneyWithdrawn
       └─ eventstore.Append(expectedVersion)   ← conflict? reload and retry
            └─ broker.Publish("accounts")
                 ├─ balances  → projection.Accounts   (GET /accounts)
                 ├─ alerts    → projection.Alerts     (GET /alerts)
                 ├─ notifier  → simulated email API   (flaky on purpose)
                 └─ stream    → browsers via SSE      (GET /stream)
```

## API

Commands (writes) — amounts are in dollars:

```bash
curl -XPOST localhost:8080/accounts -d '{"owner":"Ada"}'               # → {"id":"..."}
curl -XPOST localhost:8080/accounts/ID/deposit  -d '{"amount":250}'
curl -XPOST localhost:8080/accounts/ID/withdraw -d '{"amount":40.5}'
curl -XPOST localhost:8080/accounts/ID/close    -d '{"reason":"moving"}'
```

Queries (reads):

| Endpoint | Returns |
|---|---|
| `GET /accounts`, `GET /accounts/ID` | read-model views |
| `GET /totals` | aggregate totals |
| `GET /alerts` | large-withdrawal alerts |
| `GET /accounts/ID/events` | raw event stream (audit log) |
| `GET /accounts/ID/at/N` | state replayed to version N ("time travel") |
| `GET /events?limit=100` | global event log, newest first |
| `GET /stream` | live events (Server-Sent Events) |

Operations:

| Endpoint | Does |
|---|---|
| `GET /dlq`, `POST /dlq/replay` | inspect / replay dead letters |
| `GET/POST /notifier` | read or set `{"failure_rate":0.8}` to simulate an outage |
| `POST /admin/rebuild` | throw away the read model and rebuild it from the event store |
| `GET /metrics`, `/metrics/prom`, `/healthz` | monitoring |

## Things to try

1. Open the dashboard, create two accounts, make deposits and withdrawals. Watch the live stream.
2. Withdraw more than the balance — the command is rejected and **no event is written**.
3. Click an owner's name and drag the version slider to replay state at any point in history.
4. Set the notifier failure rate to 100%, make a few transactions, and watch events land in the
   dead-letter queue while balances keep updating (subscribers are isolated). Set it back to 0
   and press **Replay DLQ**.
5. Press **Rebuild projection** — the read model is disposable; the event store is the truth.
6. Stop the server and start it again: everything is rebuilt from `eventbank.jsonl`.

## Production notes

These are deliberate simplifications worth knowing about:

- **Dual write.** The command appends to the store and then publishes. If the process dies in
  between, the event is stored but not published. The fix is the *transactional outbox* pattern
  (write events and an outbox row in one transaction; a relay publishes them). Here, startup
  rebuilds projections from the store, which covers the read model.
- **Event store.** A JSON-lines file is fine for learning; use PostgreSQL (with a unique
  `(aggregate_id, version)` constraint), EventStoreDB, or similar for real workloads.
- **Snapshots.** Rehydrating replays the whole stream. For long-lived aggregates, store periodic
  snapshots and replay only the events after them.
- **Schema evolution.** Event payloads are versioned only by type name. In practice, add a schema
  version and upcasters, or use Protobuf/Avro with a schema registry.
