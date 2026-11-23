package server_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/engine/mock"
	"github.com/Bowl42/hark/internal/server"
	"github.com/coder/websocket"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// must panics if building the engine fails (only for building test fixtures).
func must[E hark.Engine](e E, err error) hark.Engine {
	if err != nil {
		panic(err)
	}
	return e
}

// ---- test engines and handlers ----

// fake fails on Open / fails on Finish / hangs in Finish.
type fake struct {
	openErr   error
	finishErr error
	hang      bool
	closeGate chan struct{} // if non-nil, Close blocks until it is closed
}

func (f fake) Open(context.Context, hark.Request, hark.Output) (hark.Stream, error) {
	if f.openErr != nil {
		return nil, f.openErr
	}
	return f, nil
}
func (fake) Write([]byte) error { return nil }
func (f fake) Finish(ctx context.Context) error {
	if f.hang {
		<-ctx.Done()
		return ctx.Err()
	}
	return f.finishErr
}
func (f fake) Close() error {
	if f.closeGate != nil {
		<-f.closeGate
	}
	return nil
}

// controlled: Write can block and Close can block; otherwise it writes the first sample value received, the sample count, and the speaker into the result.
type controlled struct {
	blockWrite bool
	closeGate  chan struct{}
}

func (e *controlled) Open(ctx context.Context, req hark.Request, out hark.Output) (hark.Stream, error) {
	return &controlledStream{e: e, ctx: ctx, out: out}, nil
}

type controlledStream struct {
	e     *controlled
	ctx   context.Context
	out   hark.Output
	n     int64
	first int16
}

func (s *controlledStream) Write(pcm []byte) error {
	if s.e.blockWrite {
		<-s.ctx.Done()
		return s.ctx.Err()
	}
	if s.n == 0 {
		s.first = int16(binary.LittleEndian.Uint16(pcm))
	}
	s.n += int64(len(pcm) / 2)
	s.out.Update(nil, []hark.Segment{{
		Text: fmt.Sprintf("first=%d", s.first), Speaker: "S1", Final: true,
		Audio: hark.AudioRange{Start: 0, End: s.n},
	}})
	return nil
}
func (s *controlledStream) Finish(context.Context) error { return nil }
func (s *controlledStream) Close() error {
	if s.e.closeGate != nil {
		<-s.e.closeGate
	}
	return nil
}

// paced consumes audio at real-time speed (one frame per 20ms), simulating a rate-limited real engine.
type paced struct{}

func (paced) Open(ctx context.Context, _ hark.Request, out hark.Output) (hark.Stream, error) {
	return &pacedStream{ctx: ctx, out: out}, nil
}

type pacedStream struct {
	ctx context.Context
	out hark.Output
	n   int
}

func (s *pacedStream) Write(pcm []byte) error {
	select {
	case <-time.After(time.Duration(len(pcm)/32) * time.Millisecond):
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
	s.n += len(pcm)
	s.out.Update(nil, []hark.Segment{{Text: fmt.Sprintf("%dms", s.n/32), Final: true}})
	return nil
}
func (*pacedStream) Finish(context.Context) error { return nil }
func (*pacedStream) Close() error                 { return nil }

// failLater's Write blocks for a while, then returns an error (simulating upstream dropping midway).
type failLater struct{ after time.Duration }

func (f failLater) Open(ctx context.Context, _ hark.Request, _ hark.Output) (hark.Stream, error) {
	return failLaterStream{f.after, ctx}, nil
}

type failLaterStream struct {
	after time.Duration
	ctx   context.Context
}

func (f failLaterStream) Write([]byte) error {
	select {
	case <-time.After(f.after):
		return errors.New("upstream disconnected")
	case <-f.ctx.Done():
		return f.ctx.Err()
	}
}
func (failLaterStream) Finish(context.Context) error { return nil }
func (failLaterStream) Close() error                 { return nil }

// ---- test server and client ----

func start(t *testing.T, cfg server.Config, insts ...server.Instance) *httptest.Server {
	_, ts := startServer(t, cfg, insts...)
	return ts
}

func startServer(t *testing.T, cfg server.Config, insts ...server.Instance) (*server.Server, *httptest.Server) {
	t.Helper()
	if cfg.Default == "" {
		cfg.Default = insts[0].Name
	}
	srv, err := server.New(cfg, insts, quiet)
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

func testServer(t *testing.T, cfg server.Config) *httptest.Server {
	return start(t, cfg,
		server.Instance{Name: "mock", Type: "mock", Engine: must(mock.New(mock.Config{}))},
		server.Instance{Name: "mock2", Type: "mock", Engine: must(mock.New(mock.Config{StepMS: 200}))},
		server.Instance{Name: "ctl", Type: "test", Engine: &controlled{}},
		server.Instance{Name: "openfail", Type: "test", Engine: fake{openErr: errors.New("no upstream")}},
		server.Instance{Name: "fail", Type: "test", Engine: fake{finishErr: errors.New("upstream exploded")}},
		server.Instance{Name: "hang", Type: "test", Engine: fake{hang: true}},
		server.Instance{Name: "selftimeout", Type: "test", Engine: fake{finishErr: context.DeadlineExceeded}},
	)
}

func wsURL(ts *httptest.Server) string {
	return "ws" + strings.TrimPrefix(ts.URL, "http") + "/v1/stream"
}

type client struct {
	t *testing.T
	c *websocket.Conn
}

func dial(t *testing.T, ts *httptest.Server, token string) *client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	opts := &websocket.DialOptions{HTTPHeader: http.Header{}}
	if token != "" {
		opts.HTTPHeader.Set("Authorization", "Bearer "+token)
	}
	c, _, err := websocket.Dial(ctx, wsURL(ts), opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.CloseNow() })
	return &client{t, c}
}

func (c *client) send(v any) {
	c.t.Helper()
	b, _ := json.Marshal(v)
	if err := c.c.Write(context.Background(), websocket.MessageText, b); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) recv() (string, []byte) {
	c.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, data, err := c.c.Read(ctx)
	if err != nil {
		c.t.Fatal(err)
	}
	var env api.Envelope
	json.Unmarshal(data, &env)
	return env.Type, data
}

func pcmStart(id string, channels int, tracks ...api.TrackSpec) api.Start {
	return api.Start{Type: api.TypeStart, RequestID: id, Tracks: tracks,
		Audio: api.Audio{Encoding: api.EncodingPCM16, SampleRate: api.SampleRate, Channels: channels}}
}

func engineTrack(engine string, ch int) api.TrackSpec {
	return api.TrackSpec{Engine: engine, Channel: ch}
}

// result is everything the client saw in one session.
type result struct {
	started   api.Started
	docs      map[int]*api.Doc
	texts     map[int][]string // full text of each track after each update
	commits   map[int][]int
	trackEnds map[int]api.TrackEnd
	end       api.End
}

// interleaved: at every sample, channel c has value c+1.
func interleaved(samples, channels int) []byte {
	b := make([]byte, samples*channels*2)
	for i := 0; i < samples; i++ {
		for c := 0; c < channels; c++ {
			binary.LittleEndian.PutUint16(b[(i*channels+c)*2:], uint16(c+1))
		}
	}
	return b
}

// speak sends start, 3 seconds of audio, and finish, merges all messages per the protocol, and checks invariants.
func (c *client) speak(st api.Start) result {
	c.t.Helper()
	c.send(st)
	r := result{docs: map[int]*api.Doc{}, texts: map[int][]string{}, commits: map[int][]int{}, trackEnds: map[int]api.TrackEnd{}}
	typ, data := c.recv()
	if typ != api.TypeStarted {
		c.t.Fatalf("want started, got %s", data)
	}
	json.Unmarshal(data, &r.started)
	for _, tr := range r.started.Tracks {
		r.docs[tr.Track] = &api.Doc{}
	}
	go func() {
		chunk := interleaved(1600, st.Audio.Channels) // 100ms
		for i := 0; i < 30; i++ {
			if c.c.Write(context.Background(), websocket.MessageBinary, chunk) != nil {
				return
			}
			time.Sleep(2 * time.Millisecond)
		}
		b, _ := json.Marshal(api.Envelope{Type: api.TypeFinish})
		c.c.Write(context.Background(), websocket.MessageText, b)
	}()
	for {
		typ, data := c.recv()
		switch typ {
		case api.TypeUpdate:
			var u api.Update
			json.Unmarshal(data, &u)
			d, ok := r.docs[u.Track]
			if !ok {
				c.t.Fatalf("update for unknown track: %s", data)
			}
			if _, ended := r.trackEnds[u.Track]; ended {
				c.t.Fatalf("update after track_end: %s", data)
			}
			if err := d.Apply(u); err != nil {
				c.t.Fatalf("apply %s: %v", data, err)
			}
			r.texts[u.Track] = append(r.texts[u.Track], d.Text())
			r.commits[u.Track] = append(r.commits[u.Track], d.Commit)
		case api.TypeTrackEnd:
			var e api.TrackEnd
			json.Unmarshal(data, &e)
			if _, dup := r.trackEnds[e.Track]; dup {
				c.t.Fatalf("duplicate track_end: %s", data)
			}
			r.trackEnds[e.Track] = e
		case api.TypeEnd:
			json.Unmarshal(data, &r.end)
			if len(r.trackEnds) != len(r.started.Tracks) {
				c.t.Fatalf("end before all track_end: %d/%d", len(r.trackEnds), len(r.started.Tracks))
			}
			return r
		default:
			c.t.Fatalf("unexpected %s", data)
		}
	}
}

const want = "今天天气很好。我们去公园吧。"

// ---- single-track behavior ----

func TestDeltaWithRevisions(t *testing.T) {
	c := dial(t, testServer(t, server.Config{}), "")
	r := c.speak(pcmStart("r1", 1))

	if len(r.started.Tracks) != 1 || r.started.Tracks[0].Engine != "mock" || r.started.Tracks[0].Channel != 0 {
		t.Fatalf("started tracks = %+v", r.started.Tracks)
	}
	if r.started.Output.Mode != api.ModeDelta || r.started.Commit.Policy != api.CommitStrict {
		t.Fatalf("started = %+v", r.started)
	}
	te := r.trackEnds[0]
	if te.Reason != api.ReasonDone || te.Snapshot != nil || te.DroppedRevisions != 0 {
		t.Fatalf("track_end = %+v", te)
	}
	if r.end.Reason != api.ReasonDone || r.end.Status != api.StatusComplete {
		t.Fatalf("end = %+v", r.end)
	}
	d := r.docs[0]
	if d.Text() != want || d.Commit != 2 || d.Stable != 2 {
		t.Fatalf("doc = %+v", d)
	}
	if !strings.Contains(strings.Join(r.texts[0], "|"), "今天天汽") {
		t.Fatalf("expected typo before correction: %v", r.texts[0])
	}
	cs := r.commits[0]
	for _, c := range cs[:len(cs)-1] {
		if c != 0 {
			t.Fatalf("strict committed early: %v", cs)
		}
	}
}

func TestFinalMode(t *testing.T) {
	c := dial(t, testServer(t, server.Config{}), "")
	st := pcmStart("r1", 1)
	st.Output = &api.Output{Mode: api.ModeFinal}
	r := c.speak(st)
	if len(r.texts) != 0 {
		t.Fatalf("final mode must not send updates: %v", r.texts)
	}
	s := r.trackEnds[0].Snapshot
	if s == nil || len(s.Segments) != 2 || s.Segments[0].Text+s.Segments[1].Text != want || s.Commit != 2 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestFreezeCommitsEarly(t *testing.T) {
	c := dial(t, testServer(t, server.Config{}), "")
	st := pcmStart("r1", 1)
	st.Commit = &api.Commit{Policy: api.CommitFreeze, AfterMS: 0}
	r := c.speak(st)
	early := false
	cs := r.commits[0]
	for _, c := range cs[:len(cs)-1] {
		early = early || c > 0
	}
	if !early || r.docs[0].Text() != want {
		t.Fatalf("freeze should commit before end: commits %v text %q", cs, r.docs[0].Text())
	}
}

// ---- multiple tracks ----

func TestMultiEngineComparison(t *testing.T) {
	c := dial(t, testServer(t, server.Config{}), "")
	r := c.speak(pcmStart("r1", 1, engineTrack("mock", 0), engineTrack("mock2", 0)))
	for i := 0; i < 2; i++ {
		if r.docs[i].Text() != want || r.trackEnds[i].Reason != api.ReasonDone {
			t.Fatalf("track %d: %+v %+v", i, r.docs[i], r.trackEnds[i])
		}
	}
	if r.started.Tracks[1].Engine != "mock2" || r.end.Status != api.StatusComplete {
		t.Fatalf("started %+v end %+v", r.started.Tracks, r.end)
	}
}

// Stereo with tracks omitted: one track per channel, each engine receives only its own channel's samples; speakers and timestamps are passed through.
func TestStereoDemux(t *testing.T) {
	c := dial(t, testServer(t, server.Config{}), "")
	st := pcmStart("r1", 2)
	st.Engine = "ctl"
	r := c.speak(st)
	if len(r.started.Tracks) != 2 {
		t.Fatalf("tracks %+v", r.started.Tracks)
	}
	for ch := 0; ch < 2; ch++ {
		segs := r.docs[ch].Segments
		want := fmt.Sprintf("first=%d", ch+1) // in the test audio channel c's samples are c+1: verifies the channels really were split correctly
		if len(segs) != 1 || segs[0].Text != want || segs[0].Speaker != "S1" {
			t.Fatalf("track %d: %+v", ch, segs)
		}
		// 3 seconds = 48000 samples (per channel, not the interleaved total)
		if segs[0].AudioRange.EndSample != 48000 {
			t.Fatalf("track %d range: %+v", ch, segs[0].AudioRange)
		}
	}
}

func TestPartialFailure(t *testing.T) {
	c := dial(t, testServer(t, server.Config{}), "")
	r := c.speak(pcmStart("r1", 1, engineTrack("mock", 0), engineTrack("openfail", 0), engineTrack("fail", 0)))
	if r.docs[0].Text() != want || r.trackEnds[0].Reason != api.ReasonDone {
		t.Fatalf("healthy track affected: %+v", r.trackEnds[0])
	}
	for _, i := range []int{1, 2} {
		if e := r.trackEnds[i]; e.Reason != api.ReasonError || e.Error.Code != api.ErrEngine {
			t.Fatalf("track %d end = %+v", i, e)
		}
	}
	if r.end.Reason != api.ReasonDone || r.end.Status != api.StatusPartial {
		t.Fatalf("end = %+v", r.end)
	}
}

// A stuck engine ends only its own track; with the stuck track first and a healthy track consuming at real-time speed, the healthy track must not be killed along with it.
func TestStallDetectionIsPerTrack(t *testing.T) {
	ts := start(t, server.Config{AudioQueue: 40 * time.Millisecond, StallTimeout: 300 * time.Millisecond},
		server.Instance{Name: "stuck", Type: "test", Engine: &controlled{blockWrite: true}},
		server.Instance{Name: "paced", Type: "test", Engine: paced{}})
	c := dial(t, ts, "")
	r := c.speak(pcmStart("r1", 1, engineTrack("stuck", 0), engineTrack("paced", 0)))
	if e := r.trackEnds[0]; e.Reason != api.ReasonError || e.Error.Code != api.ErrEngine {
		t.Fatalf("stuck track end = %+v", e)
	}
	if e := r.trackEnds[1]; e.Reason != api.ReasonDone || r.docs[1].Text() != "3000ms" {
		t.Fatalf("paced track wrongly affected: %+v %q", e, r.docs[1].Text())
	}
	if r.end.Status != api.StatusPartial {
		t.Fatalf("end = %+v", r.end)
	}
}

// A track ends with an error while delivery is waiting on it: delivery must move on to the other tracks immediately, not wait out the stall timeout.
func TestFailedTrackWakesDelivery(t *testing.T) {
	ts := start(t, server.Config{AudioQueue: 40 * time.Millisecond, StallTimeout: 5 * time.Second},
		server.Instance{Name: "flaky", Type: "test", Engine: failLater{after: 200 * time.Millisecond}},
		server.Instance{Name: "mock", Type: "mock", Engine: must(mock.New(mock.Config{}))})
	c := dial(t, ts, "")
	begin := time.Now()
	r := c.speak(pcmStart("r1", 1, engineTrack("flaky", 0), engineTrack("mock", 0)))
	if e := r.trackEnds[0]; e.Reason != api.ReasonError {
		t.Fatalf("flaky track end = %+v", e)
	}
	if r.docs[1].Text() != want || r.trackEnds[1].Reason != api.ReasonDone {
		t.Fatalf("healthy track: %+v", r.trackEnds[1])
	}
	if d := time.Since(begin); d > 2*time.Second {
		t.Fatalf("healthy track was held back for %v (stall timeout 5s)", d)
	}
}

// A long stretch of audio sent at once (far beyond the queue and stall timeout): an engine consuming at real-time speed must not be judged stuck.
func TestLongMessageIsNotStall(t *testing.T) {
	ts := start(t, server.Config{AudioQueue: 40 * time.Millisecond, StallTimeout: 300 * time.Millisecond},
		server.Instance{Name: "paced", Type: "test", Engine: paced{}})
	c := dial(t, ts, "")
	c.send(pcmStart("r1", 1))
	c.recv()                                                                      // started
	c.c.Write(context.Background(), websocket.MessageBinary, make([]byte, 32000)) // 1 second in one message
	c.send(api.Envelope{Type: api.TypeFinish})
	for {
		typ, data := c.recv()
		if typ != api.TypeTrackEnd {
			continue
		}
		var e api.TrackEnd
		json.Unmarshal(data, &e)
		if e.Reason != api.ReasonDone {
			t.Fatalf("track_end = %s", data)
		}
		return
	}
}

func TestEndReasons(t *testing.T) {
	ts := testServer(t, server.Config{FinishTimeout: 200 * time.Millisecond})
	for engine, want := range map[string]struct{ reason, code string }{
		"fail": {api.ReasonError, api.ErrEngine},
		"hang": {api.ReasonTimeout, api.ErrTimeout},
		// A DeadlineExceeded returned by the engine itself (e.g. an upstream network timeout) is not FinishTimeout.
		"selftimeout": {api.ReasonError, api.ErrEngine},
	} {
		t.Run(engine, func(t *testing.T) {
			c := dial(t, ts, "")
			r := c.speak(pcmStart("r1", 1, engineTrack(engine, 0)))
			e := r.trackEnds[0]
			if e.Reason != want.reason || e.Error == nil || e.Error.Code != want.code || r.end.Status != api.StatusFailed {
				t.Fatalf("track_end %+v end %+v", e, r.end)
			}
			if r := c.speak(pcmStart("again", 1)); r.docs[0].Text() != "今天天气很好。我们去公园吧。" {
				t.Fatalf("connection unusable after error: %+v", r.docs[0])
			}
		})
	}
}

func TestCancel(t *testing.T) {
	c := dial(t, testServer(t, server.Config{}), "")
	c.send(pcmStart("r1", 1, engineTrack("mock", 0), engineTrack("mock2", 0)))
	if typ, data := c.recv(); typ != api.TypeStarted {
		t.Fatalf("%s", data)
	}
	c.c.Write(context.Background(), websocket.MessageBinary, make([]byte, 32000))
	c.send(api.Envelope{Type: api.TypeCancel})
	ends := 0
	for {
		typ, data := c.recv()
		if typ == api.TypeUpdate {
			continue
		}
		if typ == api.TypeTrackEnd {
			var e api.TrackEnd
			json.Unmarshal(data, &e)
			if e.Reason != api.ReasonCancelled {
				t.Fatalf("track_end = %s", data)
			}
			ends++
			continue
		}
		var e api.End
		json.Unmarshal(data, &e)
		if typ != api.TypeEnd || e.Reason != api.ReasonCancelled || ends != 2 {
			t.Fatalf("want end cancelled after 2 track_end, got %s (ends=%d)", data, ends)
		}
		break
	}
	c.c.Write(context.Background(), websocket.MessageBinary, make([]byte, 3200)) // in-flight audio is ignored
	if r := c.speak(pcmStart("r2", 1)); r.docs[0].Text() != want {
		t.Fatalf("after cancel: %+v", r.docs[0])
	}
}

// start right after cancel: even if the previous session's engine cleanup (Close) is slow, it must not be treated as a protocol error.
func TestStartRightAfterEndWithSlowClose(t *testing.T) {
	gate := make(chan struct{})
	defer close(gate)
	ts := start(t, server.Config{},
		server.Instance{Name: "slow", Type: "test", Engine: &controlled{closeGate: gate}},
		server.Instance{Name: "mock", Type: "mock", Engine: must(mock.New(mock.Config{}))})
	c := dial(t, ts, "")
	c.send(pcmStart("r1", 1))
	c.recv() // started
	c.send(api.Envelope{Type: api.TypeCancel})
	for {
		if typ, _ := c.recv(); typ == api.TypeEnd {
			break
		}
	}
	if r := c.speak(pcmStart("r2", 1, engineTrack("mock", 0))); r.docs[0].Text() != want {
		t.Fatalf("second session: %+v", r.docs[0])
	}
}

// The engine fails and its Close is slow: track_end and end must not wait for Close.
func TestErrorEndWithSlowClose(t *testing.T) {
	gate := make(chan struct{})
	defer close(gate)
	ts := start(t, server.Config{},
		server.Instance{Name: "failslow", Type: "test", Engine: fake{finishErr: errors.New("boom"), closeGate: gate}},
		server.Instance{Name: "mock", Type: "mock", Engine: must(mock.New(mock.Config{}))})
	c := dial(t, ts, "")
	done := make(chan result, 1)
	go func() { done <- c.speak(pcmStart("r1", 1)) }()
	select {
	case r := <-done:
		if e := r.trackEnds[0]; e.Reason != api.ReasonError || e.Error.Code != api.ErrEngine || r.end.Status != api.StatusFailed {
			t.Fatalf("track_end %+v end %+v", e, r.end)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("track_end blocked by slow Close")
	}
	if r := c.speak(pcmStart("r2", 1, engineTrack("mock", 0))); r.docs[0].Text() != want {
		t.Fatalf("second session: %+v", r.docs[0])
	}
}

func TestRejected(t *testing.T) {
	c := dial(t, testServer(t, server.Config{}), "")
	withStart := func(f func(*api.Start)) api.Start { s := pcmStart("x", 1); f(&s); return s }
	nine := make([]api.TrackSpec, 9)
	for i := range nine {
		nine[i] = engineTrack("mock", 0)
	}
	bad := []struct {
		name  string
		start api.Start
		code  string
	}{
		{"no request_id", withStart(func(s *api.Start) { s.RequestID = "" }), api.ErrBadRequest},
		{"8kHz", withStart(func(s *api.Start) { s.Audio.SampleRate = 8000 }), api.ErrUnsupportedAudio},
		{"9 channels", pcmStart("x", 9), api.ErrUnsupportedAudio},
		{"unknown engine", pcmStart("x", 1, engineTrack("nope", 0)), api.ErrUnknownEngine},
		{"channel out of range", pcmStart("x", 1, engineTrack("mock", 1)), api.ErrBadRequest},
		{"too many tracks", pcmStart("x", 1, nine...), api.ErrBadRequest},
		{"bad commit", withStart(func(s *api.Start) { s.Commit = &api.Commit{Policy: "freeze", AfterMS: -1} }), api.ErrBadRequest},
	}
	for _, b := range bad {
		c.send(b.start)
		typ, data := c.recv()
		var rj api.Rejected
		json.Unmarshal(data, &rj)
		if typ != api.TypeRejected || rj.Error.Code != b.code {
			t.Fatalf("%s: want rejected %s, got %s", b.name, b.code, data)
		}
	}
	for name, raw := range map[string]string{
		"unknown field":  `{"type":"start","request_id":"x","audio":{"encoding":"pcm_s16le","sample_rate":16000,"channels":1},"language":"en"}`,
		"unknown nested": `{"type":"start","request_id":"x","audio":{"encoding":"pcm_s16le","sample_rate":16000,"channels":1},"tracks":[{"engine":"mock","lang":"en"}]}`,
		"empty tracks":   `{"type":"start","request_id":"x","audio":{"encoding":"pcm_s16le","sample_rate":16000,"channels":1},"tracks":[]}`,
	} {
		c.c.Write(context.Background(), websocket.MessageText, []byte(raw))
		typ, data := c.recv()
		var rj api.Rejected
		json.Unmarshal(data, &rj)
		if typ != api.TypeRejected || rj.RequestID != "x" || rj.Error.Code != api.ErrBadRequest {
			t.Fatalf("%s: want rejected bad_request, got %s", name, data)
		}
	}
	if r := c.speak(pcmStart("ok", 1)); r.docs[0].Text() != want {
		t.Fatalf("connection unusable after rejects: %+v", r.docs[0])
	}
}

func TestProtocolErrorsCloseConnection(t *testing.T) {
	ts := testServer(t, server.Config{Tokens: []string{"secret"}})
	resp, err := http.Get(ts.URL + "/v1/engines")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}

	cases := map[string]func(c *client){
		"audio without start": func(c *client) {
			c.c.Write(context.Background(), websocket.MessageBinary, make([]byte, 320))
		},
		"start while active": func(c *client) {
			c.send(pcmStart("a", 1))
			c.recv() // started
			c.send(pcmStart("b", 1))
		},
		"misaligned stereo frame": func(c *client) {
			c.send(pcmStart("a", 2))
			c.recv() // started
			c.c.Write(context.Background(), websocket.MessageBinary, make([]byte, 6))
		},
		"unknown type": func(c *client) { c.send(map[string]string{"type": "nope"}) },
	}
	for name, do := range cases {
		t.Run(name, func(t *testing.T) {
			c := dial(t, ts, "secret")
			do(c)
			for {
				typ, data := c.recv()
				if typ == api.TypeUpdate {
					continue
				}
				var e api.ErrorMsg
				json.Unmarshal(data, &e)
				if typ != api.TypeError || e.Error.Code != api.ErrProtocol {
					t.Fatalf("want protocol error, got %s", data)
				}
				break
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, _, err := c.c.Read(ctx); err == nil {
				t.Fatal("connection should be closed")
			}
		})
	}
}

// Rollover: the track moves to fresh engine streams during the session; the client sees one continuous document whose
// commit advances at each rollover even under the strict policy.
func TestRollover(t *testing.T) {
	ts := start(t, server.Config{Rollover: time.Second},
		server.Instance{Name: "mock", Type: "mock", Engine: must(mock.New(mock.Config{StepMS: 100}))})
	r := dial(t, ts, "").speak(pcmStart("r", 1))
	if r.end.Status != api.StatusComplete {
		t.Fatalf("end = %+v", r.end)
	}
	commits := r.commits[0]
	early := 0
	for _, c := range commits[:len(commits)-1] {
		early = max(early, c)
	}
	if d := r.docs[0]; early == 0 || d.Commit != len(d.Segments) || len(d.Segments) <= early {
		t.Fatalf("commits %v, doc %+v", commits, d)
	}
}

// slowFinish takes 600ms to finalize each engine stream and 50ms to open one, and consumes audio instantly.
type slowFinish struct{}

type slowStream struct{ out hark.Output }

func (slowFinish) Open(_ context.Context, _ hark.Request, out hark.Output) (hark.Stream, error) {
	time.Sleep(50 * time.Millisecond) // right after a rollover wait, before any new audio is consumed
	return slowStream{out}, nil
}
func (slowStream) Write([]byte) error { return nil }
func (s slowStream) Finish(ctx context.Context) error {
	select {
	case <-time.After(600 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	s.out.Update(nil, []hark.Segment{{Text: "x", Final: true}})
	return nil
}
func (slowStream) Close() error { return nil }

// Unthrottled audio outruns a rollover: the session waits for the old stream to finalize (600ms), which is longer
// than the stall timeout (300ms). That wait is bounded by the session, so it must not be reported as a stall.
// (Opening a stream, 50ms, does count, so the stall timeout leaves room for scheduling delays under load.)
func TestRolloverIsNotAStall(t *testing.T) {
	ts := start(t, server.Config{Rollover: time.Second, StallTimeout: 300 * time.Millisecond, FinishTimeout: 900 * time.Millisecond},
		server.Instance{Name: "slow", Type: "test", Engine: slowFinish{}})
	c := dial(t, ts, "")
	c.send(pcmStart("s", 1))
	if typ, data := c.recv(); typ != api.TypeStarted {
		t.Fatalf("want started: %s", data)
	}
	go func() {
		chunk := interleaved(16000, 1) // 1s, silent: every part is cut at its first chance
		for i := 0; i < 130; i++ {     // enough for a part to run past rollover + 1m while the previous one finalizes
			if c.c.Write(context.Background(), websocket.MessageBinary, chunk) != nil {
				return
			}
		}
		b, _ := json.Marshal(api.Envelope{Type: api.TypeFinish})
		c.c.Write(context.Background(), websocket.MessageText, b)
	}()
	for {
		typ, data := c.recv()
		switch typ {
		case api.TypeTrackEnd:
			var e api.TrackEnd
			json.Unmarshal(data, &e)
			if e.Reason != api.ReasonDone {
				t.Fatalf("track_end: %s", data)
			}
		case api.TypeEnd:
			return
		}
	}
}

// wordy emits one ~100-byte final segment per 20ms of audio, so a long session builds a large transcript quickly.
type wordy struct{}

type wordyStream struct {
	out  hark.Output
	segs []hark.Segment
}

func (wordy) Open(_ context.Context, _ hark.Request, out hark.Output) (hark.Stream, error) {
	return &wordyStream{out: out}, nil
}
func (s *wordyStream) Write(pcm []byte) error {
	n := len(s.segs)
	s.segs = append(s.segs, hark.Segment{Text: fmt.Sprintf("<%d %s \"q\">", n, strings.Repeat("w", 90)), Final: true,
		Audio: hark.AudioRange{Start: int64(n * 320), End: int64(n*320 + 320)}})
	s.out.Update(nil, append([]hark.Segment(nil), s.segs...))
	return nil
}
func (*wordyStream) Finish(context.Context) error { return nil }
func (*wordyStream) Close() error                 { return nil }

// longSession sends seconds of silent audio as fast as possible, then finish (or cancel), and returns every message
// received.
func longSession(t *testing.T, ts *httptest.Server, mode string, seconds int, last ...string) (docs *api.Doc, ends []api.TrackEnd, end api.End) {
	stop := api.TypeFinish
	if len(last) > 0 {
		stop = last[0]
	}
	c := dial(t, ts, "")
	c.c.SetReadLimit(-1)
	st := pcmStart("long", 1)
	st.Output = &api.Output{Mode: mode}
	c.send(st)
	if typ, data := c.recv(); typ != api.TypeStarted {
		t.Fatalf("want started: %s", data)
	}
	go func() {
		chunk := interleaved(16000, 1)
		for i := 0; i < seconds; i++ {
			if c.c.Write(context.Background(), websocket.MessageBinary, chunk) != nil {
				return
			}
		}
		if stop == api.TypeCancel {
			time.Sleep(200 * time.Millisecond) // let the engine take the audio
		}
		b, _ := json.Marshal(api.Envelope{Type: stop})
		c.c.Write(context.Background(), websocket.MessageText, b)
	}()
	doc := &api.Doc{}
	for {
		typ, data := c.recv()
		switch typ {
		case api.TypeUpdate:
			var u api.Update
			json.Unmarshal(data, &u)
			if err := doc.Apply(u); err != nil {
				t.Fatal(err)
			}
		case api.TypeTrackEnd:
			var e api.TrackEnd
			if err := json.Unmarshal(data, &e); err != nil {
				t.Fatalf("track_end does not decode: %v", err)
			}
			ends = append(ends, e)
		case api.TypeEnd:
			json.Unmarshal(data, &end)
			return doc, ends, end
		}
	}
}

// Final mode over a long session with rollovers: committed text goes to a spool file (the transcript is far beyond
// 64KB), track_end is streamed from it, and it holds every segment in order. No file is left behind.
func TestFinalModeSpool(t *testing.T) {
	dir := t.TempDir()
	ts := start(t, server.Config{Rollover: time.Second, SpoolDir: dir},
		server.Instance{Name: "wordy", Type: "test", Engine: wordy{}})
	_, ends, end := longSession(t, ts, api.ModeFinal, 20)
	if end.Status != api.StatusComplete || len(ends) != 1 || ends[0].Snapshot == nil {
		t.Fatalf("end %+v, track_end %+v", end, ends)
	}
	snap := ends[0].Snapshot
	size, parts := 0, 0
	for i, s := range snap.Segments {
		size += len(s.Text)
		if strings.HasPrefix(s.Text, "<0 ") {
			parts++
		}
		// one segment per 20ms frame: audio ranges (in session time) say where each belongs
		if s.AudioRange.StartSample != int64(i*320) || !strings.HasSuffix(s.Text, `"q">`) {
			t.Fatalf("segment %d out of place: %+v", i, s)
		}
	}
	if len(snap.Segments) != 1000 || size <= spoolMemory || parts < 5 ||
		snap.Stable != len(snap.Segments) || snap.Commit != len(snap.Segments) {
		t.Fatalf("%d segments (%d bytes, %d parts, stable %d commit %d)", len(snap.Segments), size, parts, snap.Stable, snap.Commit)
	}
	waitEmpty(t, dir)
}

// waitEmpty waits for the server to remove its spool files (it does so right after writing end).
func waitEmpty(t *testing.T, dir string) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(5 * time.Millisecond) {
		left, _ := os.ReadDir(dir)
		if len(left) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("spool files left: %v", left)
		}
	}
}

// Cancelled in final mode after much text was spooled: track_end still carries everything recognized, in order,
// with the watermarks the session reached (the rest is not committed).
func TestFinalModeSpoolCancel(t *testing.T) {
	dir := t.TempDir()
	ts := start(t, server.Config{Rollover: time.Second, SpoolDir: dir},
		server.Instance{Name: "wordy", Type: "test", Engine: wordy{}})
	_, ends, end := longSession(t, ts, api.ModeFinal, 20, api.TypeCancel)
	if end.Reason != api.ReasonCancelled || len(ends) != 1 || ends[0].Reason != api.ReasonCancelled || ends[0].Snapshot == nil {
		t.Fatalf("end %+v, track_end %+v", end, ends)
	}
	snap := ends[0].Snapshot
	for i, s := range snap.Segments {
		if s.AudioRange.StartSample != int64(i*320) {
			t.Fatalf("segment %d out of place: %+v", i, s)
		}
	}
	if len(snap.Segments) < 900 || snap.Commit > snap.Stable || snap.Stable > len(snap.Segments) || snap.Commit < 800 {
		t.Fatalf("%d segments, stable %d, commit %d", len(snap.Segments), snap.Stable, snap.Commit)
	}
	waitEmpty(t, dir)
}

// spoolMemory mirrors server.spoolMemory.
const spoolMemory = 64 << 10

// If the result can't be stored, the track fails instead of returning part of the text as if it were complete.
func TestFinalModeSpoolFailure(t *testing.T) {
	ts := start(t, server.Config{Rollover: time.Second, SpoolDir: filepath.Join(t.TempDir(), "missing")},
		server.Instance{Name: "wordy", Type: "test", Engine: wordy{}})
	_, ends, end := longSession(t, ts, api.ModeFinal, 20)
	if end.Status != api.StatusFailed || len(ends) != 1 || ends[0].Error == nil || ends[0].Error.Code != api.ErrInternal ||
		ends[0].Snapshot == nil || len(ends[0].Snapshot.Segments) != 0 {
		t.Fatalf("end %+v, track_end %+v", end, ends)
	}
}
