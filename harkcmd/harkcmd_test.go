package harkcmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/engine/mock"
	"github.com/coder/websocket"
	"gopkg.in/yaml.v3"
)

func reg(opts ...Option) *registry {
	r := &registry{factories: map[string]factory{}}
	for _, o := range opts {
		o(r)
	}
	return r
}

type privateConfig struct {
	Model string `yaml:"model"`
}

func TestConfig(t *testing.T) {
	t.Setenv("HARK_TEST_TOKEN", "abc")
	cfg, err := parseConfig([]byte(`
tokens: [ "${HARK_TEST_TOKEN}", "${HARK_TEST_UNSET}" ]
max_session: 2m
finish_timeout: 3s
engines:
  a: { type: mock, step_ms: 10 }
  b: { type: private, model: big }
`))
	if err != nil {
		t.Fatal(err)
	}
	var got privateConfig
	private := func(c privateConfig) (*mock.Engine, error) { got = c; return mock.New(mock.Config{}) }
	instances, err := cfg.engines(reg(Builtin(), Engine("private", private)))
	if err != nil {
		t.Fatal(err)
	}
	if len(instances) != 2 || instances[1].Name != "b" || instances[1].Type != "private" || got.Model != "big" {
		t.Fatalf("instances = %+v, private config = %+v", instances, got)
	}
	sc := cfg.server(instances)
	if len(sc.Tokens) != 1 || sc.Tokens[0] != "abc" {
		t.Fatalf("tokens = %q (unset env must be dropped)", sc.Tokens)
	}
	if sc.MaxSession != 2*time.Minute || sc.FinishTimeout != 3*time.Second || cfg.Listen != ":8080" {
		t.Fatalf("server config = %+v listen %q", sc, cfg.Listen)
	}
	if sc.Default != "" {
		t.Fatalf("default should stay empty with 2 engines, got %q", sc.Default)
	}
	if one := cfg.server(instances[:1]); one.Default != "a" {
		t.Fatalf("the only engine should be the default, got %q", one.Default)
	}
}

func TestConfigErrors(t *testing.T) {
	for in, want := range map[string]string{
		"engines: {}":                    "engines is empty",
		"engines: { a: { step_ms: 1 } }": "missing type",
		"engines: { a: { type: nope } }": `unknown type "nope"`,
	} {
		cfg, err := parseConfig([]byte(in))
		if err == nil {
			_, err = cfg.engines(reg(Builtin()))
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", in, err, want)
		}
	}
}

// When a later engine fails to build, the ones already built must be handed back for the caller to close.
func TestPartialFailureReturnsCreated(t *testing.T) {
	cfg, _ := parseConfig([]byte(`
engines:
  a: { type: mock }
  b: { type: broken }
`))
	broken := func(struct{}) (*mock.Engine, error) { return nil, errors.New("boom") }
	instances, err := cfg.engines(reg(Builtin(), Engine("broken", broken)))
	if err == nil || len(instances) != 1 || instances[0].Name != "a" {
		t.Fatalf("instances = %+v, err = %v", instances, err)
	}
}

func section(t *testing.T, src string) *yaml.Node {
	t.Helper()
	var n yaml.Node
	if err := yaml.Unmarshal([]byte(src), &n); err != nil {
		t.Fatal(err)
	}
	return n.Content[0]
}

type recognizer struct{ closed bool }

func (r *recognizer) Recognize(context.Context, hark.Request, []byte) (string, error) { return "", nil }
func (r *recognizer) Close() error                                                    { r.closed = true; return nil }

// A config error creates no recognizer; a built engine keeps the recognizer's Close.
func TestBatchRegistration(t *testing.T) {
	var rec *recognizer
	ctor := func(struct {
		URL string `yaml:"url"`
	}) (*recognizer, error) {
		rec = &recognizer{}
		return rec, nil
	}
	r := reg(Batch("offline", ctor))

	if _, err := r.factories["offline"](section(t, "url: x\ninterval: abc")); err == nil || rec != nil {
		t.Fatalf("bad interval: err = %v, recognizer created = %v", err, rec != nil)
	}
	e, err := r.factories["offline"](section(t, "url: x"))
	if err != nil {
		t.Fatal(err)
	}
	c, ok := e.(io.Closer)
	if !ok {
		t.Fatalf("%T does not implement io.Closer", e)
	}
	c.Close()
	if !rec.closed {
		t.Fatal("closing the engine must close the recognizer")
	}
}

func TestDuplicateTypePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("want panic")
		}
	}()
	reg(Builtin(), Engine("mock", mock.New))
}

// probe records the close order: a session's Stream.Close waits on gate, and the engine instance's Close is logged.
type probe struct {
	gate   chan struct{}
	events chan string
}

type probeConfig struct{}

func (p *probe) Open(_ context.Context, _ hark.Request, _ hark.Output) (hark.Stream, error) {
	p.events <- "open"
	return probeStream{p}, nil
}
func (p *probe) Close() error { p.events <- "engine closed"; return nil }

type probeStream struct{ p *probe }

func (probeStream) Write([]byte) error           { return nil }
func (probeStream) Finish(context.Context) error { return nil }
func (s probeStream) Close() error {
	<-s.p.gate
	s.p.events <- "stream closed"
	return nil
}

// After the stop signal: the in-flight session is aborted, and only after its engine Stream has closed does Serve close the engine instance and return.
func TestServeClosesEnginesAfterSessions(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	path := filepath.Join(t.TempDir(), "config.yaml")
	os.WriteFile(path, []byte("listen: "+addr+"\nengines:\n  p: { type: probe }\n"), 0o600)

	p := &probe{gate: make(chan struct{}), events: make(chan string, 10)}
	ctx, stop := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- Serve(ctx, path, slog.New(slog.NewTextHandler(io.Discard, nil)),
			Engine("probe", func(probeConfig) (*probe, error) { return p, nil }))
	}()

	var c *websocket.Conn
	for i := 0; ; i++ { // wait for the server to come up
		dctx, cancel := context.WithTimeout(context.Background(), time.Second)
		c, _, err = websocket.Dial(dctx, "ws://"+addr+"/v1/stream", nil)
		cancel()
		if err == nil {
			break
		}
		if i == 50 {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer c.CloseNow()
	start := `{"type":"start","request_id":"r","audio":{"encoding":"pcm_s16le","sample_rate":16000,"channels":1}}`
	c.Write(context.Background(), websocket.MessageText, []byte(start))
	go func() { // keep reading, answering the close like a real client
		for {
			if _, _, err := c.Read(context.Background()); err != nil {
				return
			}
		}
	}()
	if ev := <-p.events; ev != "open" {
		t.Fatalf("got %q", ev)
	}

	stop() // SIGTERM
	select {
	case ev := <-p.events:
		t.Fatalf("%q before the session's stream was closed", ev)
	case err := <-served:
		t.Fatalf("Serve returned (%v) while the stream was still closing", err)
	case <-time.After(shutdownGrace + time.Second): // grace period over and session aborted, but the Stream hasn't finished closing
	}
	close(p.gate)
	if a, b := <-p.events, <-p.events; a != "stream closed" || b != "engine closed" {
		t.Fatalf("order: %q, %q", a, b)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
}

// On shutdown, connections whose plain HTTP request isn't done within the grace period (the body is never fully sent) are dropped, not left behind.
func TestServeClosesStuckHTTPRequests(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	os.WriteFile(path, []byte("listen: "+addr+"\ndata_dir: "+dir+"\nengines:\n  m: { type: mock }\n"), 0o600)

	ctx, stop := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- Serve(ctx, path, slog.New(slog.NewTextHandler(io.Discard, nil)), Builtin()) }()

	var conn net.Conn
	for i := 0; ; i++ {
		if conn, err = net.Dial("tcp", addr); err == nil {
			break
		}
		if i == 50 {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer conn.Close()
	// complete headers, only half the body
	fmt.Fprintf(conn, "POST /v1/users/alice/words HTTP/1.1\r\nHost: x\r\nContent-Length: 100\r\n\r\n{\"from\":")
	time.Sleep(100 * time.Millisecond) // let the handler start reading the body

	stop()
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := io.ReadAll(conn); err != nil {
		t.Fatalf("stuck connection was not closed: %v", err)
	}
}
