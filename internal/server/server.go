// Package server implements the WebSocket service described in docs/PROTOCOL.md, connecting downstream sessions to hark.Engine.
package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/vocab"
	"github.com/coder/websocket"
)

const (
	maxMessage   = 1 << 20          // max size of one client message
	writeTimeout = 10 * time.Second // write timeout per message (downstream reading too slowly)
)

// Instance is a configured engine instance.
type Instance struct {
	Name   string
	Type   string
	Engine hark.Engine
}

type Config struct {
	Tokens        []string      // empty: no authentication
	Default       string        // default engine name
	MaxSession    time.Duration // max duration of one session
	FinishTimeout time.Duration // how long to wait for upstream to finalize after finish
	// AudioQueue is how much audio each engine track buffers; when full, backpressure applies.
	AudioQueue time.Duration
	// StallTimeout is how long to wait at most once a track's audio queue is full; beyond it the engine is considered stuck and only that track is ended.
	StallTimeout time.Duration
	// Vocab is the personal vocabulary store; nil means disabled.
	Vocab *vocab.Store
}

type Server struct {
	cfg       Config
	instances map[string]Instance
	log       *slog.Logger

	// For shutdown: after closing, no new connections or sessions are accepted. Registration and the closing check
	// happen under the same lock, so once Shutdown sets closing, the counts below only go down.
	mu       sync.Mutex
	closing  bool
	jobs     map[*job]struct{} // running sessions; removed only when run returns (all engines closed)
	jobsWG   sync.WaitGroup
	conns    map[*websocket.Conn]struct{}
	handlers sync.WaitGroup // in-flight /v1/stream handlers (both before and after upgrade)
}

func New(cfg Config, instances []Instance, log *slog.Logger) (*Server, error) {
	if cfg.MaxSession <= 0 {
		cfg.MaxSession = 10 * time.Minute
	}
	if cfg.FinishTimeout <= 0 {
		cfg.FinishTimeout = 15 * time.Second
	}
	if cfg.AudioQueue <= 0 {
		cfg.AudioQueue = 2 * time.Second
	}
	if cfg.StallTimeout <= 0 {
		cfg.StallTimeout = 10 * time.Second
	}
	m := make(map[string]Instance, len(instances))
	for _, in := range instances {
		if _, dup := m[in.Name]; dup {
			return nil, fmt.Errorf("duplicate engine name %q", in.Name)
		}
		m[in.Name] = in
	}
	if _, ok := m[cfg.Default]; !ok {
		return nil, fmt.Errorf("default engine %q not configured", cfg.Default)
	}
	return &Server{cfg: cfg, instances: m, log: log, jobs: map[*job]struct{}{}, conns: map[*websocket.Conn]struct{}{}}, nil
}

// admit registers a session about to run; it returns false if the server is shutting down.
func (s *Server) admit(j *job) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.jobs[j] = struct{}{}
	s.jobsWG.Add(1)
	return true
}

func (s *Server) release(j *job) {
	s.mu.Lock()
	delete(s.jobs, j)
	s.mu.Unlock()
	s.jobsWG.Done()
}

// Shutdown stops accepting new connections and sessions. In-flight sessions may finish normally until ctx expires;
// those still running then are aborted with shutting_down. It returns only after sessions have wrapped up (all engine
// Streams closed) and all connections are closed; after that the engine instances can be closed safely.
// ctx only sets the grace period and does not bound the return time (a hung engine is left to the process manager).
// It may be called more than once.
func (s *Server) Shutdown(ctx context.Context) {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()

	jobsDone := make(chan struct{})
	go func() {
		s.jobsWG.Wait()
		close(jobsDone)
	}()
	select {
	case <-jobsDone:
	case <-ctx.Done():
		s.mu.Lock()
		running := make([]*job, 0, len(s.jobs))
		for j := range s.jobs {
			running = append(running, j)
		}
		s.mu.Unlock()
		for _, j := range running {
			j.cancel(errShutdown) // sessions already ended and only wrapping up are unaffected (only the first cancel cause takes effect)
		}
		<-jobsDone
	}

	s.mu.Lock()
	conns := make([]*websocket.Conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	for _, c := range conns {
		go c.Close(websocket.StatusGoingAway, "server shutting down")
	}
	s.handlers.Wait()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /v1/engines", s.handleEngines)
	mux.HandleFunc("GET /v1/stream", s.handleStream)
	mux.HandleFunc("GET /v1/users/{user}/words", s.handleWords)
	mux.HandleFunc("POST /v1/users/{user}/words", s.handlePutWord)
	mux.HandleFunc("DELETE /v1/users/{user}/words", s.handleDeleteWords)
	mux.HandleFunc("POST /v1/users/{user}/corrections", s.handleCorrections)
	return mux
}

func (s *Server) authorized(r *http.Request) bool {
	if len(s.cfg.Tokens) == 0 {
		return true
	}
	tok := r.URL.Query().Get("token")
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		tok = strings.TrimPrefix(h, "Bearer ")
	}
	for _, t := range s.cfg.Tokens {
		if subtle.ConstantTimeCompare([]byte(t), []byte(tok)) == 1 {
			return true
		}
	}
	return false
}

func (s *Server) handleEngines(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	type item struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	out := struct {
		Default string `json:"default"`
		Engines []item `json:"engines"`
	}{Default: s.cfg.Default, Engines: []item{}}
	for _, in := range s.instances {
		out.Engines = append(out.Engines, item{in.Name, in.Type})
	}
	sort.Slice(out.Engines, func(i, j int) bool { return out.Engines[i].Name < out.Engines[j].Name })
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	s.handlers.Add(1)
	s.mu.Unlock()
	defer s.handlers.Done()

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	// Shutdown began during the upgrade: Shutdown can't see this connection, so close it here.
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		conn.Close(websocket.StatusGoingAway, "server shutting down")
		return
	}
	s.conns[conn] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()
	conn.SetReadLimit(maxMessage)
	c := &connection{s: s, conn: conn, log: s.log.With("remote", r.RemoteAddr)}
	c.serve()
}

// writeJSON writes with a timeout. It doesn't use the session ctx, so end can still be written after the session is cancelled.
func (s *Server) writeJSON(c *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
	defer cancel()
	return c.Write(ctx, websocket.MessageText, b)
}
