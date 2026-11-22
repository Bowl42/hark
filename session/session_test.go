package session_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/engine/mock"
	"github.com/Bowl42/hark/session"
	"github.com/Bowl42/hark/vocab"
)

func mockEngine(t *testing.T) hark.Engine {
	t.Helper()
	e, err := mock.New(mock.Config{StepMS: 100})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// reader is the sole reader: it merges all updates and checks that rev is contiguous (Doc.Apply validates it).
type reader struct {
	doc   api.Doc
	texts []string
	err   error
	done  chan struct{}
}

func read(t *testing.T, s *session.Session) *reader {
	r := &reader{done: make(chan struct{})}
	go func() {
		defer close(r.done)
		for {
			u, err := s.Next(context.Background())
			if err != nil {
				r.err = err
				return
			}
			if err := r.doc.Apply(u); err != nil {
				r.err = err
				return
			}
			r.texts = append(r.texts, r.doc.Text())
		}
	}()
	return r
}

func (r *reader) wait(t *testing.T) {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not finish")
	}
}

func speak(t *testing.T, s *session.Session, ms int) {
	t.Helper()
	chunk := make([]byte, 3200) // 100ms
	for i := 0; i < ms/100; i++ {
		if err := s.Write(chunk); err != nil {
			t.Fatal(err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

const mockFinal = "今天天气很好。我们去公园吧。"

func TestBasic(t *testing.T) {
	s, err := session.Open(context.Background(), mockEngine(t), hark.Request{}, api.Commit{})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := read(t, s)
	speak(t, s, 1000)
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	if r.err != io.EOF {
		t.Fatalf("err = %v", r.err)
	}
	if r.doc.Text() != mockFinal || r.doc.Commit != 2 || r.doc.Stable != 2 {
		t.Fatalf("doc = %+v", r.doc)
	}
	if !strings.Contains(strings.Join(r.texts, "|"), "今天天汽") {
		t.Fatalf("expected intermediate revisions: %v", r.texts)
	}
	snap := s.Snapshot()
	if len(snap.Segments) != 2 || snap.Commit != 2 {
		t.Fatalf("snapshot = %+v", snap)
	}
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal("finish must be idempotent:", err)
	}
	if err := s.Write(make([]byte, 2)); err != session.ErrFinished {
		t.Fatalf("write after finish: %v", err)
	}
	// Close doesn't affect a session that already finished normally
	s.Close()
	if _, err := s.Next(context.Background()); err != io.EOF {
		t.Fatalf("next after close: %v", err)
	}
}

func TestFinalOnly(t *testing.T) {
	s, _ := session.Open(context.Background(), mockEngine(t), hark.Request{}, api.Commit{})
	defer s.Close()
	speak(t, s, 1000)
	s.Finish(context.Background())
	if snap := s.Snapshot(); snap.Segments[0].Text+snap.Segments[1].Text != mockFinal {
		t.Fatalf("snapshot = %+v", snap)
	}
}

func TestVocabAndOptions(t *testing.T) {
	m, err := vocab.Compile(map[string]string{"公园": "花园"})
	if err != nil {
		t.Fatal(err)
	}
	e := &scripted{}
	opts := map[string]any{"hotwords": []any{"hark"}}
	s, _ := session.Open(context.Background(), vocab.Wrap(e, m), hark.Request{Options: opts, SessionID: "x"}, api.Commit{})
	defer s.Close()
	if e.req.SessionID != "x" || e.req.Options["hotwords"] == nil {
		t.Fatalf("request = %+v", e.req)
	}
	e.out.Update([]hark.Segment{{Text: "去公园", Final: true}})
	r := read(t, s)
	s.Finish(context.Background())
	r.wait(t)
	if r.doc.Text() != "去花园" {
		t.Fatalf("text = %q", r.doc.Text())
	}
}

func TestEmptySession(t *testing.T) {
	s, _ := session.Open(context.Background(), mockEngine(t), hark.Request{}, api.Commit{})
	defer s.Close()
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Next(context.Background()); err != io.EOF {
		t.Fatalf("err = %v", err)
	}
}

func TestBadOptions(t *testing.T) {
	if _, err := session.Open(context.Background(), mockEngine(t), hark.Request{}, api.Commit{Policy: "nope"}); err == nil {
		t.Fatal("expected error")
	}
	s, _ := session.Open(context.Background(), mockEngine(t), hark.Request{}, api.Commit{})
	defer s.Close()
	if err := s.Write(make([]byte, 3)); err != session.ErrOddLength {
		t.Fatalf("err = %v", err)
	}
}

// scripted is an engine whose Output the test drives directly; Write / Finish behavior is configurable.
type scripted struct {
	onOpen    func() // called in Open (e.g. to cancel the parent ctx)
	onFinish  func() // called before Finish returns
	req       hark.Request
	out       hark.Output
	ctx       context.Context
	writeErr  error
	finishErr error
	block     bool          // Write blocks until ctx is canceled or Close
	closeGate chan struct{} // when non-nil, Close blocks until it is closed
	closed    chan struct{}
	once      sync.Once
}

func (e *scripted) Open(ctx context.Context, req hark.Request, out hark.Output) (hark.Stream, error) {
	e.req, e.out, e.ctx, e.closed = req, out, ctx, make(chan struct{})
	if e.onOpen != nil {
		e.onOpen()
	}
	return e, nil
}
func (e *scripted) Write([]byte) error {
	if e.block {
		select {
		case <-e.ctx.Done():
		case <-e.closed:
		}
		return errors.New("aborted")
	}
	return e.writeErr
}
func (e *scripted) Finish(ctx context.Context) error {
	if e.onFinish != nil {
		e.onFinish()
	}
	if e.finishErr == errHang {
		<-ctx.Done()
		return ctx.Err()
	}
	return e.finishErr
}
func (e *scripted) Close() error {
	e.once.Do(func() { close(e.closed) })
	if e.closeGate != nil {
		<-e.closeGate
	}
	return nil
}

var errHang = errors.New("hang")

// The freeze result depends only on when the engine reports, not on whether or how often anyone reads.
func TestFreezeIndependentOfReader(t *testing.T) {
	run := func(readEarly bool) (string, int) {
		e := &scripted{}
		s, _ := session.Open(context.Background(), e, hark.Request{}, api.Commit{Policy: api.CommitFreeze, AfterMS: 50})
		defer s.Close()
		var r *reader
		if readEarly {
			r = read(t, s)
		}
		e.out.Update([]hark.Segment{{Text: "甲。", Final: true}})
		time.Sleep(120 * time.Millisecond)                                   // 甲 is due and should freeze
		e.out.Update([]hark.Segment{{Text: "乙。", Final: true}, {Text: "丙"}}) // rewrites frozen content: no timestamps, so the whole snapshot is discarded
		e.out.Update([]hark.Segment{{Text: "甲。", Final: true}, {Text: "丙"}})
		if !readEarly {
			r = read(t, s)
		}
		s.Finish(context.Background())
		r.wait(t)
		return r.doc.Text(), s.Dropped()
	}
	a, da := run(true)
	b, db := run(false)
	if a != "甲。丙" || a != b || da != 1 || db != 1 {
		t.Fatalf("early=%q/%d late=%q/%d", a, da, b, db)
	}
}

func TestWriteErrorEndsSession(t *testing.T) {
	e := &scripted{}
	s, _ := session.Open(context.Background(), e, hark.Request{}, api.Commit{})
	defer s.Close()
	e.out.Update([]hark.Segment{{Text: "部分"}})
	e.writeErr = errors.New("upstream gone")
	if err := s.Write(make([]byte, 2)); err == nil || !strings.Contains(err.Error(), "upstream gone") {
		t.Fatalf("write err = %v", err)
	}
	r := read(t, s)
	r.wait(t)
	// deliver existing results first, then the error; remaining text is not committed
	if r.doc.Text() != "部分" || r.doc.Commit != 0 || r.err == nil || !strings.Contains(r.err.Error(), "upstream gone") {
		t.Fatalf("doc=%+v err=%v", r.doc, r.err)
	}
	if err := s.Finish(context.Background()); err == nil {
		t.Fatal("finish after failure must fail")
	}
	select {
	case <-e.closed:
	default:
		t.Fatal("engine stream must be closed")
	}
}

func TestFinishErrorAndTimeout(t *testing.T) {
	e := &scripted{finishErr: errors.New("bad final")}
	s, _ := session.Open(context.Background(), e, hark.Request{}, api.Commit{})
	if err := s.Finish(context.Background()); err == nil {
		t.Fatal("expected error")
	}
	if _, err := s.Next(context.Background()); err == nil || err == io.EOF {
		t.Fatalf("next err = %v", err)
	}

	e = &scripted{finishErr: errHang}
	s, _ = session.Open(context.Background(), e, hark.Request{}, api.Commit{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := s.Finish(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout err = %v", err)
	}
}

// Close can interrupt a blocked Write and Finish.
func TestCloseAbortsBlockedCalls(t *testing.T) {
	e := &scripted{block: true}
	s, _ := session.Open(context.Background(), e, hark.Request{}, api.Commit{})
	errc := make(chan error, 1)
	go func() { errc <- s.Write(make([]byte, 2)) }()
	time.Sleep(20 * time.Millisecond)
	s.Close()
	select {
	case err := <-errc:
		if err != session.ErrClosed {
			t.Fatalf("write err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write not aborted")
	}
	if _, err := s.Next(context.Background()); err != session.ErrClosed {
		t.Fatalf("next err = %v", err)
	}

	e = &scripted{finishErr: errHang}
	s, _ = session.Open(context.Background(), e, hark.Request{}, api.Commit{})
	go func() { errc <- s.Finish(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	s.Close()
	select {
	case err := <-errc:
		if err != session.ErrClosed {
			t.Fatalf("finish err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("finish not aborted")
	}
}

func TestParentContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &scripted{block: true}
	s, _ := session.Open(ctx, e, hark.Request{}, api.Commit{})
	r := read(t, s)
	cancel()
	r.wait(t)
	if !errors.Is(r.err, context.Canceled) {
		t.Fatalf("err = %v", r.err)
	}
	select {
	case <-e.closed:
	case <-time.After(time.Second):
		t.Fatal("engine stream must be closed")
	}
}

// Next's ctx bounds only this one wait and never skips an update; modifying the returned object doesn't affect internal state.
func TestNextContextAndOwnership(t *testing.T) {
	e := &scripted{}
	s, _ := session.Open(context.Background(), e, hark.Request{}, api.Commit{})
	defer s.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := s.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	e.out.Update([]hark.Segment{{Text: "ab", Audio: hark.AudioRange{Start: 0, End: 10}}})
	u, err := s.Next(context.Background())
	if err != nil || u.Rev != 1 {
		t.Fatalf("u=%+v err=%v", u, err)
	}
	u.Ops[0].Insert[0].Text = "XX"
	u.Ops[0].Insert[0].AudioRange.EndSample = 999
	snap := s.Snapshot()
	snap.Segments[0].AudioRange.StartSample = 5
	e.out.Update([]hark.Segment{{Text: "abc", Audio: hark.AudioRange{Start: 0, End: 10}}})
	u2, _ := s.Next(context.Background())
	var d api.Doc
	d.Apply(api.Update{Type: api.TypeUpdate, Rev: 1, Ops: []api.Op{{Insert: []api.Segment{{Text: "ab", AudioRange: api.AudioRange{EndSample: 10}}}}}})
	if err := d.Apply(u2); err != nil || d.Text() != "abc" {
		t.Fatalf("u2 = %+v err=%v text=%q", u2, err, d.Text())
	}
	if r := s.Snapshot().Segments[0].AudioRange; r.StartSample != 0 || r.EndSample != 10 {
		t.Fatalf("internal state modified: %+v", r)
	}
}

// The parent ctx is canceled during Open: there must be no race between the cancel callback and initialization.
func TestCancelDuringOpen(t *testing.T) {
	for i := 0; i < 200; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		e := &scripted{onOpen: cancel}
		s, err := session.Open(ctx, e, hark.Request{}, api.Commit{})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Next(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
		select {
		case <-e.closed:
		case <-time.After(time.Second):
			t.Fatal("stream not closed")
		}
	}
}

// On abnormal end, freezes already due are still committed, whether or not there is a reader; the remaining text is not committed.
func TestFreezeOnAbort(t *testing.T) {
	for _, how := range []string{"close", "write-error", "finish-error"} {
		run := func(withReader bool) api.Snapshot {
			e := &scripted{finishErr: errors.New("bad")}
			s, _ := session.Open(context.Background(), e, hark.Request{}, api.Commit{Policy: api.CommitFreeze, AfterMS: 30})
			var r *reader
			if withReader {
				r = read(t, s)
			}
			e.out.Update([]hark.Segment{{Text: "甲。", Final: true}, {Text: "乙"}})
			time.Sleep(80 * time.Millisecond)
			switch how {
			case "close":
				s.Close()
			case "write-error":
				e.writeErr = errors.New("gone")
				s.Write(make([]byte, 2))
			case "finish-error":
				s.Finish(context.Background())
			}
			if r != nil {
				r.wait(t)
			}
			return s.Snapshot()
		}
		a, b := run(true), run(false)
		if a.Commit != 1 || b.Commit != 1 || len(a.Segments) != 2 || len(b.Segments) != 2 {
			t.Fatalf("%s: with reader %+v, without %+v", how, a, b)
		}
	}
}

// The parent ctx is canceled before the engine's Finish returns: cancellation wins, not success.
func TestCancelWinsOverFinish(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	e := &scripted{onFinish: cancel}
	s, _ := session.Open(ctx, e, hark.Request{}, api.Commit{})
	e.out.Update([]hark.Segment{{Text: "甲"}})
	if err := s.Finish(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("finish err = %v", err)
	}
	if snap := s.Snapshot(); snap.Commit != 0 {
		t.Fatalf("must not commit: %+v", snap)
	}
}

// Finish times out while waiting for a blocked Write: it returns only ctx's error and the session is unaffected.
func TestFinishWaitTimeout(t *testing.T) {
	e := &scripted{block: true}
	s, _ := session.Open(context.Background(), e, hark.Request{}, api.Commit{})
	go s.Write(make([]byte, 2))
	time.Sleep(20 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := s.Finish(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	if _, err := s.Next(ctx2); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("session must still be running, next err = %v", err)
	}
	s.Close()
}

// While something else (the cancel callback) is slowly closing the engine, Next sees the terminal state immediately and
// Close returns only once the close has actually completed.
func TestCloseWaitsForConcurrentClose(t *testing.T) {
	gate := make(chan struct{})
	e := &scripted{closeGate: gate}
	ctx, cancel := context.WithCancel(context.Background())
	s, err := session.Open(ctx, e, hark.Request{}, api.Commit{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	nctx, ncancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer ncancel()
	if _, err := s.Next(nctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("next = %v", err)
	}
	closed := make(chan struct{})
	go func() {
		s.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned before the stream was closed")
	case <-time.After(100 * time.Millisecond):
	}
	close(gate)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return")
	}
}
