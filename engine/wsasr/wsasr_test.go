package wsasr

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/coder/websocket"
)

// recorder keeps the latest full snapshot: everything handed over, then the live segments.
type recorder struct {
	mu   sync.Mutex
	done []hark.Segment
	last []hark.Segment
	n    int
}

func (r *recorder) Update(done, live []hark.Segment) {
	r.mu.Lock()
	r.done = append(r.done, done...)
	s := append(append([]hark.Segment(nil), r.done...), live...)
	r.last, r.n = s, r.n+1
	r.mu.Unlock()
}

func (r *recorder) get() ([]hark.Segment, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last, r.n
}

// plugin starts a fake service: handle gets the connection and the received start.
func plugin(t *testing.T, handle func(c *websocket.Conn, start startMsg, r *http.Request)) string {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		_, data, err := c.Read(context.Background())
		if err != nil {
			return
		}
		var start startMsg
		json.Unmarshal(data, &start)
		handle(c, start, r)
	}))
	t.Cleanup(ts.Close)
	return "ws" + strings.TrimPrefix(ts.URL, "http")
}

func send(c *websocket.Conn, v any) {
	b, _ := json.Marshal(v)
	c.Write(context.Background(), websocket.MessageText, b)
}

func open(t *testing.T, url string, req hark.Request) (hark.Stream, *recorder) {
	t.Helper()
	e, err := New(Config{URL: url, Token: "k"})
	if err != nil {
		t.Fatal(err)
	}
	out := &recorder{}
	s, err := e.Open(context.Background(), req, out)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, out
}

func ctx(t *testing.T) context.Context {
	c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return c
}

// Normal flow: start carries the session and options, audio arrives unchanged, a snapshot comes back every 3200 bytes, and after finish the final result + done.
func TestStream(t *testing.T) {
	url := plugin(t, func(c *websocket.Conn, start startMsg, r *http.Request) {
		if start.Type != "start" || start.Session != "s1" || start.Options["lang"] != "zh" ||
			r.Header.Get("Authorization") != "Bearer k" {
			send(c, reply{Type: "error", Message: "bad start"})
			return
		}
		got := 0
		for {
			typ, data, err := c.Read(context.Background())
			if err != nil {
				return
			}
			if typ == websocket.MessageBinary {
				got += len(data)
				send(c, map[string]any{"type": "result", "segments": []any{map[string]any{"text": strings.Repeat("字", got/3200)}}})
				continue
			}
			send(c, map[string]any{"type": "result", "segments": []any{
				map[string]any{"text": "今天。", "final": true, "speaker": "A", "audio_range": map[string]any{"start_sample": 0, "end_sample": got / 2}},
			}})
			send(c, map[string]any{"type": "done"})
		}
	})
	s, out := open(t, url, hark.Request{SessionID: "s1", Options: map[string]any{"lang": "zh"}})
	for i := 0; i < 3; i++ {
		if err := s.Write(make([]byte, 3200)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Finish(ctx(t)); err != nil {
		t.Fatal(err)
	}
	segs, n := out.get()
	if n < 2 || len(segs) != 1 || segs[0].Text != "今天。" || !segs[0].Final || segs[0].Speaker != "A" ||
		segs[0].Audio.End != 4800 {
		t.Fatalf("updates %d, last %+v", n, segs)
	}
}

// Service error, early done, bad message, bad audio_range: the next Write or Finish returns the reason.
func TestServerFailures(t *testing.T) {
	for name, msg := range map[string]string{
		"error":       `{"type":"error","message":"quota exceeded"}`,
		"early done":  `{"type":"done"}`,
		"not json":    `hello`,
		"unknown":     `{"type":"partial"}`,
		"bad range":   `{"type":"result","segments":[{"text":"x","audio_range":{"start_sample":10,"end_sample":5}}]}`,
		"conn closed": ``,
	} {
		t.Run(name, func(t *testing.T) {
			want := map[string]string{"error": "quota exceeded", "early done": "done before finish", "not json": "bad message",
				"unknown": "unknown message type", "bad range": "bad audio_range", "conn closed": "connection lost"}[name]
			url := plugin(t, func(c *websocket.Conn, _ startMsg, _ *http.Request) {
				if msg != "" {
					c.Write(context.Background(), websocket.MessageText, []byte(msg))
				}
				time.Sleep(50 * time.Millisecond)
			})
			s, _ := open(t, url, hark.Request{})
			var err error
			for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
				if err = s.Write(make([]byte, 320)); err != nil {
					break
				}
			}
			if err == nil {
				err = s.Finish(ctx(t))
			}
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("err = %v, want %q", err, want)
			}
		})
	}
}

// The service reports an error and closes the connection right away (after reading the audio already received, so a
// TCP reset doesn't swallow the error message): Write reports the service's error, not a send failure.
func TestErrorThenDisconnect(t *testing.T) {
	for i := 0; i < 20; i++ {
		url := plugin(t, func(c *websocket.Conn, _ startMsg, _ *http.Request) {
			c.SetReadLimit(-1) // keep draining: hitting the default 32KB limit would close with unread data, and the TCP reset could drop the error
			c.Write(context.Background(), websocket.MessageText, []byte(`{"type":"error","message":"quota exceeded"}`))
			for {
				if _, _, err := c.Read(context.Background()); err != nil {
					return
				}
			}
		})
		s, _ := open(t, url, hark.Request{})
		var err error
		for deadline := time.Now().Add(2 * time.Second); err == nil && time.Now().Before(deadline); {
			err = s.Write(make([]byte, 64<<10))
		}
		if err == nil || !strings.Contains(err.Error(), "quota exceeded") {
			t.Fatalf("run %d: err = %v", i, err)
		}
	}
}

// When the service doesn't read audio (TCP buffer full), Write blocks; it returns immediately once the session ctx is canceled.
func TestWriteUnblocksOnCancel(t *testing.T) {
	url := plugin(t, func(c *websocket.Conn, _ startMsg, _ *http.Request) { time.Sleep(3 * time.Second) })
	e, _ := New(Config{URL: url})
	sctx, cancel := context.WithCancel(context.Background())
	s, err := e.Open(sctx, hark.Request{}, &recorder{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	go func() { time.Sleep(300 * time.Millisecond); cancel() }()
	begin := time.Now()
	for err == nil && time.Since(begin) < 2*time.Second {
		err = s.Write(make([]byte, 1<<20))
	}
	if err == nil || time.Since(begin) > time.Second {
		t.Fatalf("write did not return after cancel: %v after %v", err, time.Since(begin))
	}
}

// The service never replies after receiving finish: Finish returns promptly when ctx expires or on Close.
func TestFinishStuck(t *testing.T) {
	hang := func(c *websocket.Conn, _ startMsg, _ *http.Request) {
		c.Read(context.Background())
		time.Sleep(3 * time.Second)
	}
	s, _ := open(t, plugin(t, hang), hark.Request{})
	c, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := s.Finish(c); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("finish with expired ctx: %v", err)
	}

	s, _ = open(t, plugin(t, hang), hark.Request{})
	go func() { time.Sleep(100 * time.Millisecond); s.Close() }()
	begin := time.Now()
	if err := s.Finish(ctx(t)); !errors.Is(err, errClosed) || time.Since(begin) > time.Second {
		t.Fatalf("finish during close: %v after %v", err, time.Since(begin))
	}
	if err := s.Write(make([]byte, 2)); !errors.Is(err, errClosed) {
		t.Fatalf("write after close: %v", err)
	}
}

func TestDialFailure(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("missing url should fail")
	}
	e, _ := New(Config{URL: "ws://127.0.0.1:1/none"})
	if _, err := e.Open(context.Background(), hark.Request{}, &recorder{}); err == nil || !strings.Contains(err.Error(), "dial") {
		t.Fatalf("err = %v", err)
	}
}

// The service may hand over a leading run of segments with "done"; a done after a live segment is ignored.
func TestHandover(t *testing.T) {
	url := plugin(t, func(c *websocket.Conn, start startMsg, r *http.Request) {
		for {
			typ, _, err := c.Read(context.Background())
			if err != nil {
				return
			}
			if typ == websocket.MessageBinary {
				continue
			}
			send(c, map[string]any{"type": "result", "segments": []any{
				map[string]any{"text": "甲。", "done": true}, map[string]any{"text": "乙"}, map[string]any{"text": "丙", "done": true},
			}})
			send(c, map[string]any{"type": "result", "segments": []any{map[string]any{"text": "乙。", "final": true}, map[string]any{"text": "丙"}}})
			send(c, map[string]any{"type": "done"})
		}
	})
	s, out := open(t, url, hark.Request{})
	if err := s.Finish(ctx(t)); err != nil {
		t.Fatal(err)
	}
	segs, _ := out.get()
	if len(out.done) != 1 || len(segs) != 3 || segs[0].Text != "甲。" || segs[1].Text != "乙。" || segs[2].Text != "丙" {
		t.Fatalf("done %+v, full %+v", out.done, segs)
	}
}
