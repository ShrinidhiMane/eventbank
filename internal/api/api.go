// Package api exposes commands (writes) and queries (reads) over HTTP, plus
// monitoring endpoints and a live event stream (Server-Sent Events).
package api

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ShrinidhiMane/eventbank/internal/account"
	"github.com/ShrinidhiMane/eventbank/internal/app"
	"github.com/ShrinidhiMane/eventbank/internal/broker"
	"github.com/ShrinidhiMane/eventbank/internal/events"
	"github.com/ShrinidhiMane/eventbank/internal/eventstore"
	"github.com/ShrinidhiMane/eventbank/internal/metrics"
	"github.com/ShrinidhiMane/eventbank/internal/projection"
)

//go:embed static/index.html
var static embed.FS

// DLQ is implemented by brokers that keep a dead-letter queue.
type DLQ interface {
	DeadLetters() []broker.DeadLetter
	Replay(ctx context.Context) (int, error)
}

// Server holds the dependencies for the HTTP handlers.
type Server struct {
	Commands *app.Commands
	Store    eventstore.Store
	Accounts *projection.Accounts
	Alerts   *projection.Alerts
	Notifier *projection.Notifier
	DLQ      DLQ // optional
	Metrics  *metrics.Registry
	Log      *slog.Logger
	Stream   *Stream
}

// Routes builds the HTTP mux.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()

	// Commands
	mux.HandleFunc("POST /accounts", s.openAccount)
	mux.HandleFunc("POST /accounts/{id}/deposit", s.amountCmd(s.Commands.Deposit))
	mux.HandleFunc("POST /accounts/{id}/withdraw", s.amountCmd(s.Commands.Withdraw))
	mux.HandleFunc("POST /accounts/{id}/close", s.closeAccount)

	// Queries (read model)
	mux.HandleFunc("GET /accounts", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.Accounts.List()) })
	mux.HandleFunc("GET /accounts/{id}", s.getAccount)
	mux.HandleFunc("GET /totals", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.Accounts.Totals()) })
	mux.HandleFunc("GET /alerts", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.Alerts.List()) })

	// Event sourcing views (straight from the store)
	mux.HandleFunc("GET /accounts/{id}/events", s.accountEvents)
	mux.HandleFunc("GET /accounts/{id}/at/{version}", s.stateAt)
	mux.HandleFunc("GET /events", s.allEvents)
	mux.HandleFunc("POST /admin/rebuild", s.rebuild)

	// Error handling & monitoring
	mux.HandleFunc("GET /dlq", s.deadLetters)
	mux.HandleFunc("POST /dlq/replay", s.replay)
	mux.HandleFunc("GET /notifier", s.notifierStatus)
	mux.HandleFunc("POST /notifier", s.setNotifier)
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, s.Metrics.Snapshot()) })
	mux.HandleFunc("GET /metrics/prom", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		s.Metrics.WritePrometheus(w)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	if s.Stream != nil {
		mux.Handle("GET /stream", s.Stream)
	}

	// Dashboard
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		b, _ := static.ReadFile("static/index.html")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(b)
	})

	return s.logRequests(mux)
}

type openReq struct {
	Owner string `json:"owner"`
}

type amountReq struct {
	Amount float64 `json:"amount"` // dollars, e.g. 12.50
}

func (s *Server) openAccount(w http.ResponseWriter, r *http.Request) {
	var req openReq
	if !decode(w, r, &req) {
		return
	}
	id, err := s.Commands.OpenAccount(r.Context(), req.Owner)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

func (s *Server) amountCmd(cmd func(context.Context, string, int64) (*account.Account, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req amountReq
		if !decode(w, r, &req) {
			return
		}
		cents := int64(req.Amount*100 + 0.5)
		acc, err := cmd(r.Context(), r.PathValue("id"), cents)
		if err != nil {
			s.fail(w, err)
			return
		}
		writeJSON(w, 200, writeResult(acc))
	}
}

func (s *Server) closeAccount(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Reason string `json:"reason"`
	}
	if r.ContentLength > 0 && !decode(w, r, &req) {
		return
	}
	acc, err := s.Commands.Close(r.Context(), r.PathValue("id"), req.Reason)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, writeResult(acc))
}

// writeResult is the response to a command: the write-side state immediately
// after the command. The read model catches up asynchronously (eventual consistency).
func writeResult(a *account.Account) map[string]any {
	return map[string]any{"id": a.ID, "balance_cents": a.Balance, "version": a.Version, "closed": a.Closed}
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	v, ok := s.Accounts.Get(r.PathValue("id"))
	if !ok {
		writeErr(w, 404, account.ErrNotFound.Error())
		return
	}
	writeJSON(w, 200, v)
}

func (s *Server) accountEvents(w http.ResponseWriter, r *http.Request) {
	h, err := s.Commands.History(r.PathValue("id"))
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, h)
}

func (s *Server) stateAt(w http.ResponseWriter, r *http.Request) {
	v, err := strconv.Atoi(r.PathValue("version"))
	if err != nil || v < 1 {
		writeErr(w, 400, "version must be a positive integer")
		return
	}
	acc, err := s.Commands.StateAt(r.PathValue("id"), v)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"id": acc.ID, "owner": acc.Owner, "balance_cents": acc.Balance, "version": acc.Version, "closed": acc.Closed})
}

func (s *Server) allEvents(w http.ResponseWriter, r *http.Request) {
	all, err := s.Store.All()
	if err != nil {
		s.fail(w, err)
		return
	}
	limit := 100
	if l, err := strconv.Atoi(r.URL.Query().Get("limit")); err == nil && l > 0 {
		limit = l
	}
	if len(all) > limit {
		all = all[len(all)-limit:]
	}
	// newest first
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	writeJSON(w, 200, all)
}

func (s *Server) rebuild(w http.ResponseWriter, r *http.Request) {
	all, err := s.Store.All()
	if err != nil {
		s.fail(w, err)
		return
	}
	start := time.Now()
	if err := s.Accounts.Rebuild(all); err != nil {
		s.fail(w, err)
		return
	}
	s.Metrics.Inc("projection_rebuilds")
	writeJSON(w, 200, map[string]any{"replayed_events": len(all), "took_ms": time.Since(start).Milliseconds()})
}

func (s *Server) deadLetters(w http.ResponseWriter, r *http.Request) {
	if s.DLQ == nil {
		writeJSON(w, 200, []broker.DeadLetter{})
		return
	}
	dl := s.DLQ.DeadLetters()
	if dl == nil {
		dl = []broker.DeadLetter{}
	}
	writeJSON(w, 200, dl)
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	if s.DLQ == nil {
		writeErr(w, 501, "broker has no dead-letter queue")
		return
	}
	n, err := s.DLQ.Replay(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]int{"replayed": n})
}

func (s *Server) notifierStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"failure_rate": s.Notifier.Rate(), "sent": s.Notifier.Sent()})
}

func (s *Server) setNotifier(w http.ResponseWriter, r *http.Request) {
	var req struct {
		FailureRate float64 `json:"failure_rate"`
	}
	if !decode(w, r, &req) {
		return
	}
	if req.FailureRate < 0 || req.FailureRate > 1 {
		writeErr(w, 400, "failure_rate must be between 0 and 1")
		return
	}
	s.Notifier.SetFailureRate(req.FailureRate)
	s.notifierStatus(w, r)
}

// fail maps domain errors to HTTP status codes.
func (s *Server) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, account.ErrNotFound):
		writeErr(w, 404, err.Error())
	case errors.Is(err, account.ErrInvalidAmount), errors.Is(err, account.ErrOwnerRequired):
		writeErr(w, 400, err.Error())
	case errors.Is(err, account.ErrInsufficientFunds), errors.Is(err, account.ErrClosed),
		errors.Is(err, account.ErrNonZeroBalance), errors.Is(err, account.ErrAlreadyExists),
		errors.Is(err, eventstore.ErrConcurrency):
		writeErr(w, 409, err.Error())
	default:
		s.Log.Error("internal error", "err", err)
		writeErr(w, 500, err.Error())
	}
}

func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	if err := json.NewDecoder(r.Body).Decode(v); err != nil {
		writeErr(w, 400, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) { r.status = code; r.ResponseWriter.WriteHeader(code) }
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *Server) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: 200}
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/stream" {
			return
		}
		s.Metrics.Observe("http_latency", time.Since(start))
		s.Metrics.Inc(fmt.Sprintf("http_%dxx", rec.status/100))
		if r.Method != http.MethodGet {
			s.Log.Info("request", "method", r.Method, "path", r.URL.Path, "status", rec.status, "dur", time.Since(start))
		}
	})
}

// Stream fans events out to connected browsers via Server-Sent Events. It is
// itself just another subscriber on the broker.
type Stream struct {
	mu      sync.Mutex
	clients map[chan events.Event]struct{}
}

func NewStream() *Stream { return &Stream{clients: map[chan events.Event]struct{}{}} }

// Handle is the broker subscriber; it never blocks on slow clients.
func (st *Stream) Handle(_ context.Context, e events.Event) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	for c := range st.clients {
		select {
		case c <- e:
		default: // drop for slow client
		}
	}
	return nil
}

func (st *Stream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	c := make(chan events.Event, 64)
	st.mu.Lock()
	st.clients[c] = struct{}{}
	st.mu.Unlock()
	defer func() {
		st.mu.Lock()
		delete(st.clients, c)
		st.mu.Unlock()
	}()
	fmt.Fprint(w, ": connected\n\n")
	fl.Flush()
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ping.C:
			fmt.Fprint(w, ": ping\n\n")
			fl.Flush()
		case e := <-c:
			b, _ := json.Marshal(e)
			fmt.Fprintf(w, "data: %s\n\n", b)
			fl.Flush()
		}
	}
}
