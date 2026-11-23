// Package harkcmd is a reusable hark entry point: it reads the config, registers engines, and starts the server.
//
// The official binary (cmd/hark) is just:
//
//	harkcmd.Run(harkcmd.Builtin())
//
// A private deployment writes a main in its own repository, registers its private engines, and builds its own binary:
//
//	harkcmd.Run(
//		harkcmd.Builtin(),
//		harkcmd.Engine("foo", foo.New), // foo.New(foo.Config) (*foo.Engine, error)
//	)
//
// Built-in and private engines register the same way, with no special privileges. Only this layer reads the config file:
// engine packages provide only typed constructors, and decoding a config section into a struct happens here.
package harkcmd

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/engine/batch"
	"github.com/Bowl42/hark/engine/httpasr"
	"github.com/Bowl42/hark/engine/mock"
	"github.com/Bowl42/hark/engine/openai"
	"github.com/Bowl42/hark/engine/sherpa"
	"github.com/Bowl42/hark/engine/volc"
	"github.com/Bowl42/hark/engine/wsasr"
	"github.com/Bowl42/hark/internal/server"
	"github.com/Bowl42/hark/vocab"
	"gopkg.in/yaml.v3"
)

// Option configures Run.
type Option func(*registry)

// factory turns an engine instance's config section into an engine.
type factory func(section *yaml.Node) (hark.Engine, error)

type registry struct {
	factories map[string]factory
}

func register(typ string, f factory) Option {
	return func(r *registry) {
		if _, dup := r.factories[typ]; dup {
			panic(fmt.Sprintf("harkcmd: engine type %q registered twice", typ))
		}
		r.factories[typ] = f
	}
}

// Engine registers an engine type, referenced in the config file as type: <typ>; a duplicate type name is reported at startup.
// At startup, each instance of this type decodes its config section into C (yaml tags, unknown fields ignored), then calls ctor.
// On success ctor must return a valid instance. Instances that implement io.Closer are closed when the server exits.
func Engine[C any, E hark.Engine](typ string, ctor func(C) (E, error)) Option {
	return register(typ, func(section *yaml.Node) (hark.Engine, error) {
		var c C
		if err := section.Decode(&c); err != nil {
			return nil, err
		}
		e, err := ctor(c)
		if err != nil {
			return nil, err // can't return e directly: on failure it may be a nil pointer wrapped in a non-nil interface
		}
		return e, nil
	})
}

// Batch registers an engine type capable only of batch (whole-utterance) recognition (see engine/batch): the config section
// decodes into the recognizer's C, and interval / silence / max_sentence in the same section decode into batch.Options
// (these three names belong to batch; the recognizer's own config must not use them). interval defaults to 0 when omitted:
// recognize only at sentence end, with no intermediate results (so a pay-per-call API isn't called repeatedly).
// To get text while speaking, set it explicitly, e.g. 500ms (durations in yaml need a unit).
func Batch[C any, R batch.Recognizer](typ string, ctor func(C) (R, error)) Option {
	return register(typ, func(section *yaml.Node) (hark.Engine, error) {
		var c C
		if err := section.Decode(&c); err != nil {
			return nil, err
		}
		var opts batch.Options
		if err := section.Decode(&opts); err != nil {
			return nil, err
		}
		// Create the recognizer only after all config decodes, so it isn't created and then leaked due to a config error.
		rec, err := ctor(c)
		if err != nil {
			return nil, err
		}
		return batch.New(rec, opts), nil
	})
}

// Builtin registers all built-in engines.
func Builtin() Option {
	return func(r *registry) {
		Engine("mock", mock.New)(r)
		Batch("sherpa", sherpa.New)(r)
		Batch("http", httpasr.New)(r)
		Batch("openai", openai.New)(r)
		Engine("ws", wsasr.New)(r)
		Engine("volc", volc.New)(r)
	}
}

// Run parses the command line, reads the config, and starts the server; it exits the process on error.
func Run(opts ...Option) {
	path := flag.String("config", "config.yaml", "path to the config file")
	flag.Parse()
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := Serve(ctx, *path, log, opts...); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

// Serve reads the config and runs the server until ctx is cancelled, for embedding in tests or other programs.
func Serve(ctx context.Context, configPath string, log *slog.Logger, opts ...Option) error {
	reg := &registry{factories: map[string]factory{}}
	for _, o := range opts {
		o(reg)
	}
	raw, err := os.ReadFile(configPath)
	if err != nil {
		return err
	}
	cfg, err := parseConfig(raw)
	if err != nil {
		return err
	}
	instances, err := cfg.engines(reg)
	// On exit (including a failed or partial startup), close the engines created (those implementing io.Closer), after the HTTP server stops.
	defer func() {
		for _, in := range instances {
			if c, ok := in.Engine.(io.Closer); ok {
				c.Close()
			}
		}
	}()
	if err != nil {
		return err
	}
	srv := cfg.server(instances)
	if len(cfg.Tokens) == 0 {
		log.Warn("no tokens configured, authentication disabled")
	}
	if cfg.DataDir != "" {
		store, err := vocab.Open(cfg.DataDir, log)
		if err != nil {
			return fmt.Errorf("data_dir: %w", err)
		}
		srv.Vocab = store
		defer store.Close() // closed after the HTTP server fully stops: every change has been written to disk
	}
	h, err := server.New(srv, instances, log)
	if err != nil {
		return err
	}

	hs := &http.Server{Addr: cfg.Listen, Handler: h.Handler(), ReadHeaderTimeout: 10 * time.Second}
	served := make(chan error, 1)
	go func() { served <- hs.ListenAndServe() }()
	log.Info("listening", "addr", cfg.Listen, "default_engine", srv.Default,
		"data_dir", cfg.DataDir)
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-served: // listen failed or serving errored: wrap up the same way below
	}

	// Wrap up: close the listener and wait for plain HTTP requests (the vocabulary endpoints etc.); meanwhile let in-flight
	// recognition sessions finish within the grace period, abort those still running when it expires, and wait for them
	// to really end. Only after both return do the defers above close the vocabulary store and engine instances.
	gctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	httpDone := make(chan error, 1)
	go func() { httpDone <- hs.Shutdown(gctx) }()
	h.Shutdown(gctx)
	if herr := <-httpDone; herr != nil {
		// Plain requests not done within the grace period (e.g. a request body never fully sent): force-close them, and their handlers exit.
		log.Warn("http shutdown", "err", herr)
		hs.Close()
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}

// shutdownGrace is how long in-flight recognition sessions get to end naturally on shutdown; after it they are aborted
// (the client receives shutting_down). Sessions still have to wrap up and connections close after that, so leave
// headroom in the process manager's kill deadline when deploying (docker defaults to 10s).
const shutdownGrace = 5 * time.Second

type config struct {
	Listen        string        `yaml:"listen"`
	Tokens        []string      `yaml:"tokens"`
	DefaultEngine string        `yaml:"default_engine"`
	MaxSession    time.Duration `yaml:"max_session"`
	Rollover      time.Duration `yaml:"rollover"` // 0: session.DefaultRollover
	FinishTimeout time.Duration `yaml:"finish_timeout"`
	DataDir       string        `yaml:"data_dir"`  // personal vocabulary directory; empty means disabled
	SpoolDir      string        `yaml:"spool_dir"` // where final-mode results wait for track_end; empty: the system temp dir
	Engines       yaml.Node     `yaml:"engines"`
}

func parseConfig(raw []byte) (*config, error) {
	// Supports ${ENV} references, so secrets need not be written in the config file.
	var cfg config
	if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(raw))), &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Listen == "" {
		cfg.Listen = ":8080"
	}
	if cfg.MaxSession < 0 || cfg.Rollover < 0 {
		return nil, errors.New("config: max_session and rollover must not be negative")
	}
	// ${X} expands to "" when the variable is unset; drop empty tokens so an empty token can't pass authentication.
	tokens := cfg.Tokens[:0]
	for _, t := range cfg.Tokens {
		if t != "" {
			tokens = append(tokens, t)
		}
	}
	cfg.Tokens = tokens
	return &cfg, nil
}

// engines parses engines: { <name>: { type: <type>, ...the engine's own fields } } and creates the instances.
// On error it still returns those already created, for the caller to close.
func (c *config) engines(reg *registry) (out []server.Instance, err error) {
	n := &c.Engines
	if n.Kind != yaml.MappingNode || len(n.Content) == 0 {
		return nil, errors.New("config: engines is empty")
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		name, body := n.Content[i].Value, n.Content[i+1]
		var head struct {
			Type string `yaml:"type"`
		}
		if err := body.Decode(&head); err != nil || head.Type == "" {
			return out, fmt.Errorf("config: engine %q missing type", name)
		}
		f, ok := reg.factories[head.Type]
		if !ok {
			return out, fmt.Errorf("config: engine %q: unknown type %q (registered: %v)", name, head.Type, reg.types())
		}
		e, err := f(body)
		if err != nil {
			return out, fmt.Errorf("config: engine %q: %w", name, err)
		}
		out = append(out, server.Instance{Name: name, Type: head.Type, Engine: e})
	}
	return out, nil
}

// server returns the server parameters; with only one engine configured, it is the default engine.
func (c *config) server(instances []server.Instance) server.Config {
	def := c.DefaultEngine
	if def == "" && len(instances) == 1 {
		def = instances[0].Name
	}
	return server.Config{
		Tokens:        c.Tokens,
		Default:       def,
		MaxSession:    c.MaxSession,
		FinishTimeout: c.FinishTimeout,
		Rollover:      c.Rollover,
		SpoolDir:      c.SpoolDir,
	}
}

func (r *registry) types() []string {
	out := make([]string, 0, len(r.factories))
	for k := range r.factories {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
