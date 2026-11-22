package server_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/engine/mock"
	"github.com/Bowl42/hark/internal/server"
	"github.com/coder/websocket"
)

// shutdown calls Shutdown in the background; the returned channel is closed when it returns.
func shutdown(srv *server.Server, grace time.Duration) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), grace)
		defer cancel()
		srv.Shutdown(ctx)
		close(done)
	}()
	return done
}

// begin sends start and 1 second of audio, without finish.
func (c *client) begin(engine string) {
	c.t.Helper()
	c.send(pcmStart("r1", 1, engineTrack(engine, 0)))
	if typ, data := c.recv(); typ != api.TypeStarted {
		c.t.Fatalf("want started, got %s", data)
	}
	if err := c.c.Write(context.Background(), websocket.MessageBinary, make([]byte, 32000)); err != nil {
		c.t.Fatal(err)
	}
}

// until reads up to end and returns each track's track_end and the end.
func (c *client) until() ([]api.TrackEnd, api.End) {
	c.t.Helper()
	var ends []api.TrackEnd
	for {
		typ, data := c.recv()
		switch typ {
		case api.TypeTrackEnd:
			var e api.TrackEnd
			json.Unmarshal(data, &e)
			ends = append(ends, e)
		case api.TypeEnd:
			var e api.End
			json.Unmarshal(data, &e)
			return ends, e
		}
	}
}

// closeCode keeps reading in the background like a real client until the server closes the connection, then yields the close code.
// When closing, the server waits for the client to answer the close frame; a client that doesn't read makes Shutdown wait several extra seconds.
func (c *client) closeCode() <-chan websocket.StatusCode {
	ch := make(chan websocket.StatusCode, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for {
			if _, _, err := c.c.Read(ctx); err != nil {
				ch <- websocket.CloseStatus(err)
				return
			}
		}
	}()
	return ch
}

func waitClosed(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

// Within the grace period: an in-flight session finishes speaking and finishes normally; new sessions are rejected (retryable); afterwards the connection closes with going away.
func TestShutdownLetsSessionsFinish(t *testing.T) {
	srv, ts := startServer(t, server.Config{}, server.Instance{Name: "mock", Type: "mock", Engine: must(mock.New(mock.Config{}))})
	busy, idle := dial(t, ts, ""), dial(t, ts, "")
	busy.begin("mock")
	done := shutdown(srv, 5*time.Second)

	time.Sleep(50 * time.Millisecond) // let Shutdown set closing first
	idle.send(pcmStart("r2", 1))
	typ, data := idle.recv()
	var rj api.Rejected
	json.Unmarshal(data, &rj)
	if typ != api.TypeRejected || rj.Error.Code != api.ErrShuttingDown || !rj.Error.Retryable {
		t.Fatalf("new start during shutdown: %s", data)
	}
	select {
	case <-done:
		t.Fatal("Shutdown returned while a session was still running")
	default:
	}

	idleClosed := idle.closeCode()
	busy.send(api.Envelope{Type: api.TypeFinish})
	ends, end := busy.until()
	if len(ends) != 1 || ends[0].Reason != api.ReasonDone || end.Reason != api.ReasonDone || end.Status != api.StatusComplete {
		t.Fatalf("track_end %+v end %+v", ends, end)
	}
	busyClosed := busy.closeCode()
	waitClosed(t, done, "Shutdown")
	for name, ch := range map[string]<-chan websocket.StatusCode{"busy": busyClosed, "idle": idleClosed} {
		if code := <-ch; code != websocket.StatusGoingAway {
			t.Errorf("%s connection closed with %v, want going away", name, code)
		}
	}
}

// Grace period over: an in-flight session ends with shutting_down (retryable).
func TestShutdownAbortsAfterGrace(t *testing.T) {
	srv, ts := startServer(t, server.Config{}, server.Instance{Name: "mock", Type: "mock", Engine: must(mock.New(mock.Config{}))})
	c := dial(t, ts, "")
	c.begin("mock")
	done := shutdown(srv, 100*time.Millisecond)
	ends, end := c.until()
	if len(ends) != 1 || ends[0].Reason != api.ReasonError || ends[0].Error.Code != api.ErrShuttingDown || !ends[0].Error.Retryable {
		t.Fatalf("track_end %+v", ends)
	}
	if end.Reason != api.ReasonError || end.Error == nil || end.Error.Code != api.ErrShuttingDown || !end.Error.Retryable {
		t.Fatalf("end %+v", end)
	}
	closed := c.closeCode()
	waitClosed(t, done, "Shutdown")
	if code := <-closed; code != websocket.StatusGoingAway {
		t.Fatalf("closed with %v", code)
	}
}

// The session has ended and the connection is gone, but the engine is still closing: Shutdown must wait for it before returning (only then can engine instances be closed).
func TestShutdownWaitsForEngineClose(t *testing.T) {
	gate := make(chan struct{})
	srv, ts := startServer(t, server.Config{}, server.Instance{Name: "slow", Type: "test", Engine: &controlled{closeGate: gate}})
	c := dial(t, ts, "")
	c.begin("slow")
	c.send(api.Envelope{Type: api.TypeCancel})
	c.until()
	c.c.CloseNow()

	done := shutdown(srv, 50*time.Millisecond)
	select {
	case <-done:
		t.Fatal("Shutdown returned while an engine stream was still closing")
	case <-time.After(300 * time.Millisecond):
	}
	close(gate)
	waitClosed(t, done, "Shutdown")
}

// With no sessions it returns immediately; it may be called more than once; new connections afterwards are rejected.
func TestShutdownIdleAndRepeated(t *testing.T) {
	srv, ts := startServer(t, server.Config{}, server.Instance{Name: "mock", Type: "mock", Engine: must(mock.New(mock.Config{}))})
	c := dial(t, ts, "")
	closed := c.closeCode()
	waitClosed(t, shutdown(srv, time.Second), "Shutdown")
	if code := <-closed; code != websocket.StatusGoingAway {
		t.Fatalf("idle connection closed with %v", code)
	}
	waitClosed(t, shutdown(srv, time.Second), "second Shutdown")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, wsURL(ts), nil)
	if err == nil || resp == nil || resp.StatusCode != 503 {
		t.Fatalf("dial after shutdown: resp %v err %v", resp, err)
	}
}
