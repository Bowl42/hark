package session_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/session"
)

// parts is an engine that records every stream it opens. Each stream reports one segment covering the audio it
// received; text, speaker and finish behavior can be customized per stream.
type parts struct {
	mu      sync.Mutex
	onClose func(n int)
	streams []*partStream
	onOpen  func(n int)
	text    func(n int) string
	speaker string
	finish  func(ctx context.Context, n int) error // called in Finish before the final snapshot
}

type partStream struct {
	e      *parts
	n      int
	out    hark.Output
	ctx    context.Context
	bytes  int
	closed bool
}

func (e *parts) Open(ctx context.Context, _ hark.Request, out hark.Output) (hark.Stream, error) {
	e.mu.Lock()
	st := &partStream{e: e, n: len(e.streams), out: out, ctx: ctx}
	e.streams = append(e.streams, st)
	e.mu.Unlock()
	if e.onOpen != nil {
		e.onOpen(st.n)
	}
	return st, nil
}

func (e *parts) all() []partStream {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]partStream, len(e.streams))
	for i, st := range e.streams {
		out[i] = *st
	}
	return out
}

func (st *partStream) report(final bool) {
	text := fmt.Sprintf("[%d]", st.n)
	if st.e.text != nil {
		text = st.e.text(st.n)
	}
	st.out.Update([]hark.Segment{{Text: text, Final: final, Speaker: st.e.speaker, Audio: hark.AudioRange{Start: 0, End: int64(st.bytes / 2)}}})
}

func (st *partStream) Write(pcm []byte) error {
	st.e.mu.Lock()
	st.bytes += len(pcm)
	st.e.mu.Unlock()
	st.report(false)
	return nil
}

func (st *partStream) Finish(ctx context.Context) error {
	if st.e.finish != nil {
		if err := st.e.finish(ctx, st.n); err != nil {
			return err
		}
	}
	st.report(true)
	return nil
}

func (st *partStream) Close() error {
	if st.e.onClose != nil {
		st.e.onClose(st.n)
	}
	st.e.mu.Lock()
	st.closed = true
	st.e.mu.Unlock()
	return nil
}

// tone and silence build test audio: a 440Hz tone counts as speech, digital silence as a pause.
func tone(ms int) []byte {
	b := make([]byte, ms*32)
	for i := 0; i < len(b)/2; i++ {
		v := int16(0.3 * 32767 * math.Sin(2*math.Pi*440*float64(i)/16000))
		binary.LittleEndian.PutUint16(b[2*i:], uint16(v))
	}
	return b
}

func silence(ms int) []byte { return make([]byte, ms*32) }

// speech is continuous talk: tone with 20ms gaps (the VAD treats a steady tone as background noise after a few
// seconds, but the gaps keep the noise floor low; they are far shorter than a pause).
func speech(ms int) []byte {
	var b []byte
	for ; ms > 0; ms -= 300 {
		b = append(append(b, tone(min(ms, 280))...), silence(min(max(ms-280, 0), 20))...)
	}
	return b
}

// write sends audio in uneven chunks, so cuts land inside Writes.
func write(t *testing.T, s *session.Session, audio ...[]byte) {
	t.Helper()
	pcm := []byte{}
	for _, a := range audio {
		pcm = append(pcm, a...)
	}
	for len(pcm) > 0 {
		n := min(len(pcm), 2*777)
		if err := s.Write(pcm[:n]); err != nil {
			t.Fatal(err)
		}
		pcm = pcm[n:]
	}
}

// waitFor polls until cond holds.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// Parts end at the first pause after Rollover; audio is split exactly at the cut, the finished part's text is
// committed (even under strict), and the reader sees one continuous document.
func TestRolloverAtPause(t *testing.T) {
	e := &parts{}
	s, err := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	r := read(t, s)
	// part 0: 1.5s speech, then the pause reaches 500ms at 2.0s → cut; part 1 likewise; part 2 is the rest
	write(t, s, tone(1500), silence(700))
	waitFor(t, "part 0 sealed", func() bool { return s.Snapshot().Commit == 1 }) // otherwise the next cut is postponed
	write(t, s, tone(1200), silence(600))
	waitFor(t, "part 1 sealed", func() bool { return s.Snapshot().Commit == 2 })
	write(t, s, tone(300))
	if snap := s.Snapshot(); snap.Stable < 2 || snap.Base+len(snap.Segments) != 3 {
		t.Fatalf("before finish: %+v", snap)
	}
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	if r.err == nil || r.err.Error() != "EOF" {
		t.Fatalf("reader: %v", r.err)
	}
	got := e.all()
	want := []int{2000, 1900, 400} // ms per part: 1500+500 | 200+1200+500 | 100+300
	if len(got) != 3 {
		t.Fatalf("%d parts", len(got))
	}
	total := 0
	for i, st := range got {
		if st.bytes != want[i]*32 || !st.closed {
			t.Errorf("part %d: %d bytes (want %d), closed %v", i, st.bytes, want[i]*32, st.closed)
		}
		total += st.bytes
	}
	if r.doc.Text() != "[0][1][2]" || r.doc.Commit != 3 {
		t.Fatalf("doc: %q commit %d", r.doc.Text(), r.doc.Commit)
	}
	// audio ranges are in session time
	if a := r.doc.Segments[1].AudioRange; a.StartSample != 2000*16 || a.EndSample != 3900*16 {
		t.Fatalf("part 1 range %+v", a)
	}
	if total != (1500+700+1200+600+300)*32 {
		t.Fatalf("total %d", total)
	}
}

// Without a pause, a part is cut one minute after Rollover regardless.
func TestRolloverForced(t *testing.T) {
	e := &parts{}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	defer s.Close()
	write(t, s, speech(62_000))
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	got := e.all()
	if len(got) != 2 || got[0].bytes != 61_000*32 || got[1].bytes != 1_000*32 {
		t.Fatalf("parts: %+v", got)
	}
}

// The next part is held back until the previous one is sealed; a rollover is postponed while a part is still
// finishing; Finish waits for it.
func TestRolloverGated(t *testing.T) {
	release := make(chan struct{})
	e := &parts{finish: func(_ context.Context, n int) error {
		if n == 0 {
			<-release
		}
		return nil
	}}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	defer s.Close()
	r := read(t, s)
	write(t, s, tone(1500), silence(600), tone(1500), silence(600), tone(1500), silence(600))
	if n := len(e.all()); n != 2 { // part 0 is still finishing: no further rollover
		t.Fatalf("%d parts while part 0 is finishing", n)
	}
	time.Sleep(50 * time.Millisecond)
	if snap := s.Snapshot(); len(snap.Segments) != 1 || strings.Contains(r.doc.Text(), "[1]") {
		t.Fatalf("part 1 visible before part 0 is sealed: %+v", snap)
	}
	finished := make(chan error, 1)
	go func() { finished <- s.Finish(context.Background()) }()
	select {
	case err := <-finished:
		t.Fatalf("Finish returned before part 0 was sealed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	if r.doc.Text() != "[0][1]" || r.doc.Commit != 2 {
		t.Fatalf("doc %q commit %d", r.doc.Text(), r.doc.Commit)
	}
}

// Joining parts: a space where English text meets, speaker labels of later parts are namespaced.
func TestRolloverJoin(t *testing.T) {
	e := &parts{text: func(n int) string { return []string{"Hello.", "World"}[n] }, speaker: "S1"}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	write(t, s, tone(1500), silence(600), tone(500))
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap := s.Snapshot()
	if len(snap.Segments) != 2 || snap.Segments[0].Text+snap.Segments[1].Text != "Hello. World" ||
		snap.Segments[0].Speaker != "S1" || snap.Segments[1].Speaker != "S1#1" {
		t.Fatalf("%+v", snap.Segments)
	}
}

// A part that fails to finalize fails the session; the text of earlier parts stays committed.
func TestRolloverFinishError(t *testing.T) {
	e := &parts{finish: func(_ context.Context, n int) error {
		if n == 1 {
			return errors.New("boom")
		}
		return nil
	}}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	r := read(t, s)
	write(t, s, tone(1500), silence(600))
	waitFor(t, "part 0 sealed", func() bool { return s.Snapshot().Commit == 1 })
	s.Write(append(tone(1500), silence(600)...)) // fails once the session has failed
	waitFor(t, "session failure", func() bool {
		select {
		case <-r.done:
			return true
		default:
			return false
		}
	})
	// part 1 was published and part 2 was waiting behind it: their text is kept but never committed
	if r.err == nil || !strings.Contains(r.err.Error(), "boom") || r.doc.Commit != 1 || !strings.HasPrefix(r.doc.Text(), "[0][1]") {
		t.Fatalf("err %v, doc %q commit %d", r.err, r.doc.Text(), r.doc.Commit)
	}
	s.Close() // the terminal state is announced before streams are closed; Close waits for that to finish
	for _, st := range e.all() {
		if !st.closed {
			t.Errorf("part %d not closed", st.n)
		}
	}
}

// Close while a rollover is opening the next stream: Write returns, and every stream ends up closed.
func TestRolloverCloseDuringOpen(t *testing.T) {
	opening, unblock := make(chan struct{}), make(chan struct{})
	e := &parts{onOpen: func(n int) {
		if n == 1 {
			close(opening)
			<-unblock
		}
	}}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	wrote := make(chan error, 1)
	go func() {
		pcm := append(tone(1500), silence(600)...)
		wrote <- s.Write(pcm)
	}()
	<-opening
	closed := make(chan struct{})
	go func() { s.Close(); close(closed) }()
	time.Sleep(20 * time.Millisecond)
	close(unblock)
	if err := <-wrote; !errors.Is(err, session.ErrClosed) {
		t.Fatalf("write: %v", err)
	}
	<-closed
	for _, st := range e.all() {
		if !st.closed {
			t.Errorf("part %d not closed", st.n)
		}
	}
}

// The freeze policy works within each part, and sealing never retracts stable or commit.
func TestRolloverFreeze(t *testing.T) {
	e := &parts{}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{
		Rollover: time.Second, Commit: api.Commit{Policy: api.CommitFreeze, AfterMS: 0}})
	r := read(t, s)
	write(t, s, tone(1500), silence(600))
	waitFor(t, "part 0 sealed", func() bool { return s.Snapshot().Commit == 1 })
	write(t, s, tone(1500), silence(600), tone(100))
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	r.wait(t)
	if r.doc.Text() != "[0][1][2]" || r.doc.Commit != 3 {
		t.Fatalf("doc %q commit %d", r.doc.Text(), r.doc.Commit)
	}
}

// Each stream gets its own ctx, canceled once the stream is retired, so engines can't pin resources on it.
func TestRolloverStreamContext(t *testing.T) {
	e := &parts{}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	defer s.Close()
	write(t, s, tone(1500), silence(600))
	waitFor(t, "part 0 sealed", func() bool { return s.Snapshot().Commit == 1 })
	got := e.all()
	waitFor(t, "part 0 ctx canceled", func() bool { return got[0].ctx.Err() != nil })
	if got[1].ctx.Err() != nil {
		t.Fatal("live part's ctx canceled")
	}
}

// One huge Write still rolls over: audio is handed on in bounded chunks, and when a part runs past Rollover+1m while
// the previous part is still finishing, Write waits for it instead of overfilling the part.
func TestRolloverHugeWrite(t *testing.T) {
	release := make(chan struct{})
	e := &parts{finish: func(_ context.Context, n int) error {
		if n == 0 {
			<-release
		}
		return nil
	}}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	defer s.Close()
	pcm := append(tone(1500), silence(600)...)
	pcm = append(pcm, speech(200_000)...)
	go func() { time.Sleep(200 * time.Millisecond); close(release) }()
	if err := s.Write(pcm); err != nil {
		t.Fatal(err)
	}
	got := e.all()
	if len(got) < 3 { // 2s, then at most 121s per part for 200s of speech without pauses
		t.Fatalf("%d parts", len(got))
	}
	for _, st := range got {
		if st.bytes > 122_000*32 { // rollover + 2 × grace + one scan chunk
			t.Errorf("part %d got %ds of audio", st.n, st.bytes/32000)
		}
	}
}

// If the session ends while a part is still finishing, the next part's text is kept (uncommitted); Finish waiting
// on the finishing part returns as soon as the session ends.
func TestRolloverAbortKeepsGatedText(t *testing.T) {
	block := make(chan struct{})
	defer close(block)
	e := &parts{finish: func(_ context.Context, n int) error {
		if n == 0 {
			<-block
		}
		return nil
	}}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	write(t, s, tone(1500), silence(600), tone(300))
	finished := make(chan error, 1)
	go func() { finished <- s.Finish(context.Background()) }()
	time.Sleep(20 * time.Millisecond)
	s.Close()
	select {
	case err := <-finished:
		if !errors.Is(err, session.ErrClosed) {
			t.Fatalf("finish: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Finish still waiting after Close")
	}
	snap := s.Snapshot()
	if len(snap.Segments) != 2 || snap.Segments[0].Text+snap.Segments[1].Text != "[0][1]" || snap.Commit != 0 {
		t.Fatalf("%+v", snap)
	}
}

// A part that doesn't finalize within PartFinishTimeout fails the session with ErrRolloverTimeout.
func TestRolloverTimeout(t *testing.T) {
	e := &parts{finish: func(ctx context.Context, n int) error {
		if n == 0 {
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second, PartFinishTimeout: 50 * time.Millisecond})
	defer s.Close()
	r := read(t, s)
	write(t, s, tone(1500), silence(600))
	r.wait(t)
	if !errors.Is(r.err, session.ErrRolloverTimeout) {
		t.Fatalf("err = %v", r.err)
	}
}

// Once a rolled-over stream is finalized and sealed, Write stops waiting for it even if closing it hangs.
func TestRolloverWaitEndsAtSeal(t *testing.T) {
	unblock := make(chan struct{})
	e := &parts{
		finish: func(context.Context, int) error { time.Sleep(500 * time.Millisecond); return nil }, // outlasts writing 61s: Write must wait
		onClose: func(n int) {
			if n == 0 {
				<-unblock
			}
		},
	}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	pcm := append(append(tone(1500), silence(600)...), speech(70_000)...)
	wrote := make(chan error, 1)
	go func() { wrote <- s.Write(pcm) }()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Write still waiting while the sealed stream's Close hangs")
	}
	if waiting, ended := s.RolloverWait(); waiting || ended.IsZero() {
		t.Fatalf("RolloverWait = %v, %v", waiting, ended)
	}
	close(unblock)
	s.Close()
}

// After waiting for a slow previous part, the cut still lands in a pause rather than mid-speech.
func TestRolloverPrefersPauseAfterWait(t *testing.T) {
	e := &parts{finish: func(_ context.Context, n int) error {
		if n == 0 {
			time.Sleep(time.Second) // outlasts writing the next 61s
		}
		return nil
	}}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	defer s.Close()
	// 1.5s speech + 600ms pause (part 0 ends at 2.0s), then 80 × (2s speech + 600ms pause)
	pcm := append(tone(1500), silence(600)...)
	for range 80 {
		pcm = append(append(pcm, speech(2000)...), silence(600)...)
	}
	write(t, s, pcm)
	if _, ended := s.RolloverWait(); ended.IsZero() {
		t.Fatal("Write never waited for part 0: the case under test didn't happen")
	}
	got := e.all()
	if len(got) < 3 {
		t.Fatalf("%d parts", len(got))
	}
	pos := 0
	for _, st := range got[:len(got)-1] {
		pos += st.bytes
		ms := pos / 32
		if ms == 2000 {
			continue
		}
		// pauses occupy [2100+2600k+2000, 2100+2600k+2600); a cut needs 500ms of it (less if the wait ended mid-pause)
		if off := (ms - 2100) % 2600; off < 2000 && off != 0 {
			t.Errorf("part %d ends at %dms (offset %d into the speech/pause cycle), not in a pause", st.n, ms, off)
		}
	}
}

// While a retired stream's Close hangs, the session doesn't roll over again (retired streams can't pile up); once it
// returns, rollovers resume.
func TestRolloverWaitsForClose(t *testing.T) {
	unblock := make(chan struct{})
	release := sync.OnceFunc(func() { close(unblock) })
	e := &parts{onClose: func(n int) {
		if n == 0 {
			<-unblock
		}
	}}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{Rollover: time.Second})
	defer s.Close()
	defer release() // runs before Close, even if the test fails early
	write(t, s, tone(1500), silence(600))
	waitFor(t, "part 0 sealed", func() bool { return s.Snapshot().Commit == 1 })
	for range 5 {
		write(t, s, tone(1500), silence(600))
	}
	if n := len(e.all()); n != 2 {
		t.Fatalf("%d streams while part 0's Close hangs", n)
	}
	release()
	waitFor(t, "close returned", func() bool { return e.all()[0].closed })
	time.Sleep(10 * time.Millisecond)
	write(t, s, tone(1500), silence(600))
	if n := len(e.all()); n != 3 {
		t.Fatalf("%d streams after Close returned", n)
	}
}

// The zero Options roll over too, at DefaultRollover: there is no endless stream.
func TestRolloverDefault(t *testing.T) {
	e := &parts{}
	s, _ := session.Open(context.Background(), e, hark.Request{}, session.Options{})
	defer s.Close()
	write(t, s, tone(int(session.DefaultRollover/time.Millisecond)+500), silence(600), tone(500))
	if err := s.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := len(e.all()); n != 2 {
		t.Fatalf("%d parts, want 2", n)
	}
}
