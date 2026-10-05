package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/ShrinidhiMane/eventbank/internal/app"
	"github.com/ShrinidhiMane/eventbank/internal/broker"
	"github.com/ShrinidhiMane/eventbank/internal/eventstore"
	"github.com/ShrinidhiMane/eventbank/internal/metrics"
	"github.com/ShrinidhiMane/eventbank/internal/projection"
)

type harness struct {
	t      *testing.T
	srv    *httptest.Server
	broker *broker.Memory
	store  *eventstore.Memory
	notif  *projection.Notifier
}

// newHarness wires the whole system with the in-memory broker: an end-to-end
// test of command → store → broker → subscribers → read API.
func newHarness(t *testing.T) *harness {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	m := metrics.New()
	store := eventstore.NewMemory()
	b := broker.NewMemory(broker.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Millisecond, MaxDelay: time.Millisecond}, m, quiet)
	accounts := projection.NewAccounts()
	alerts := projection.NewAlerts(100_00)
	notifier := projection.NewNotifier(0)
	b.Subscribe(app.Topic, "balances", accounts.Handle)
	b.Subscribe(app.Topic, "alerts", alerts.Handle)
	b.Subscribe(app.Topic, "notifier", notifier.Handle)
	s := &Server{Commands: app.NewCommands(store, b, m), Store: store, Accounts: accounts, Alerts: alerts,
		Notifier: notifier, DLQ: b, Metrics: m, Log: quiet, Stream: NewStream()}
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(func() { ts.Close(); b.Close() })
	return &harness{t: t, srv: ts, broker: b, store: store, notif: notifier}
}

func (h *harness) do(method, path string, body any, wantStatus int) map[string]any {
	h.t.Helper()
	var buf bytes.Buffer
	if body != nil {
		json.NewEncoder(&buf).Encode(body)
	}
	req, _ := http.NewRequest(method, h.srv.URL+path, &buf)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	if res.StatusCode != wantStatus {
		h.t.Fatalf("%s %s: status %d, want %d: %s", method, path, res.StatusCode, wantStatus, raw)
	}
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out
}

func TestEndToEnd(t *testing.T) {
	h := newHarness(t)
	id := h.do("POST", "/accounts", map[string]string{"owner": "Linus"}, 201)["id"].(string)
	h.do("POST", "/accounts/"+id+"/deposit", map[string]float64{"amount": 250.75}, 200)
	h.do("POST", "/accounts/"+id+"/withdraw", map[string]float64{"amount": 150}, 200)
	h.do("POST", "/accounts/"+id+"/withdraw", map[string]float64{"amount": 1000}, 409) // insufficient funds
	h.do("POST", "/accounts/"+id+"/deposit", map[string]float64{"amount": -1}, 400)
	h.do("POST", "/accounts/nope/deposit", map[string]float64{"amount": 1}, 404)

	h.broker.Flush() // wait for eventual consistency
	view := h.do("GET", "/accounts/"+id, nil, 200)
	if view["balance_cents"].(float64) != 10075 || view["version"].(float64) != 3 {
		t.Fatalf("read model: %+v", view)
	}

	// Time travel: state as of version 2 (after the deposit).
	at := h.do("GET", "/accounts/"+id+"/at/2", nil, 200)
	if at["balance_cents"].(float64) != 25075 {
		t.Fatalf("state at v2: %+v", at)
	}

	// Large withdrawal produces an alert in an independent subscriber.
	var alerts []map[string]any
	res, _ := http.Get(h.srv.URL + "/alerts")
	json.NewDecoder(res.Body).Decode(&alerts)
	res.Body.Close()
	if len(alerts) != 1 {
		t.Fatalf("alerts: %+v", alerts)
	}
}

func TestConcurrentWithdrawalsNeverOverdraw(t *testing.T) {
	h := newHarness(t)
	id := h.do("POST", "/accounts", map[string]string{"owner": "Barbara"}, 201)["id"].(string)
	h.do("POST", "/accounts/"+id+"/deposit", map[string]float64{"amount": 100}, 200)

	var wg sync.WaitGroup
	var mu sync.Mutex
	codes := map[int]int{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, _ := json.Marshal(map[string]float64{"amount": 10})
			res, err := http.Post(h.srv.URL+"/accounts/"+id+"/withdraw", "application/json", bytes.NewReader(b))
			if err != nil {
				return
			}
			res.Body.Close()
			mu.Lock()
			codes[res.StatusCode]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	h.broker.Flush()

	if codes[200] != 10 {
		t.Fatalf("exactly 10 withdrawals of $10 should succeed from $100, got %v", codes)
	}
	view := h.do("GET", "/accounts/"+id, nil, 200)
	if view["balance_cents"].(float64) != 0 {
		t.Fatalf("balance: %v", view["balance_cents"])
	}
}

func TestDeadLetterReplayAndRebuild(t *testing.T) {
	h := newHarness(t)
	h.notif.SetFailureRate(1) // downstream outage
	id := h.do("POST", "/accounts", map[string]string{"owner": "Ken"}, 201)["id"].(string)
	h.do("POST", "/accounts/"+id+"/deposit", map[string]float64{"amount": 5}, 200)
	h.broker.Flush()

	if n := len(h.broker.DeadLetters()); n != 2 {
		t.Fatalf("want 2 dead letters, got %d", n)
	}
	h.notif.SetFailureRate(0) // outage over
	if r := h.do("POST", "/dlq/replay", nil, 200); r["replayed"].(float64) != 2 {
		t.Fatalf("replay: %+v", r)
	}
	h.broker.Flush()
	if h.notif.Sent() != 2 {
		t.Fatalf("notifier sent %d after replay", h.notif.Sent())
	}

	// The read model is disposable: rebuilding from the store gives the same answer.
	h.do("POST", "/admin/rebuild", nil, 200)
	if v := h.do("GET", "/accounts/"+id, nil, 200); v["balance_cents"].(float64) != 500 {
		t.Fatalf("after rebuild: %+v", v)
	}
}
