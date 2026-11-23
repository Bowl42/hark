// Package session uses hark inside your own process: instead of starting an HTTP/WebSocket server, it drives an
// engine directly, with the same semantics as the server: updates in the same format (rev, OT ops, stable, commit),
// the same normalization, and the same commit policies (strict / freeze). Each track on the server is a Session.
// Depending on how fast you read, intermediate states may be merged into different numbers of updates, but the
// resulting document is the same.
//
// Session is the bridge from "engine snapshots" to "a document with commitments", and it does only that: one engine,
// one mono audio stream, one track. For multiple channels or comparing engines, open multiple Sessions; for personal
// vocabulary replacement, wrap the engine with vocab.Wrap before passing it to Open.
//
// Long sessions stay bounded however long they run: segments the engine hands over (see hark.Output) are committed
// and leave the engine stream; an engine stream holding too much unfinalized audio is rolled over to a fresh one at a
// pause (Options.Rollover); and committed segments are dropped once Next has delivered them.
//
// Usage: Open → a goroutine loops on Next to read updates (io.EOF on normal end) → Write audio → Finish.
// If you only need the final result, you don't need to call Next: read Snapshot after Finish. With Next, the reader
// holds the document; use api.Doc.Trim to keep it flat in memory too. See examples/embedded for a complete example.
package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
	"github.com/google/uuid"
)

var (
	// ErrClosed means the session was aborted by Close.
	ErrClosed = errors.New("session: closed")
	// ErrFinished means the session has already finished normally and no more audio can be written.
	ErrFinished = errors.New("session: already finished")
	// ErrOddLength means the PCM length is odd; it must be an even number of bytes (16-bit samples).
	ErrOddLength = errors.New("session: pcm length must be even")
	// ErrRolloverTimeout means an engine stream did not finalize within PartFinishTimeout after a rollover.
	ErrRolloverTimeout = errors.New("session: engine stream did not finalize in time after rollover")
)

// Lifecycle: running → (Finish) finishing → succeeded. At any time an engine error, Close, or cancellation of
// Open's ctx can move it to failed. succeeded / failed are terminal states and never change afterwards.
type state int

const (
	running state = iota
	finishing
	succeeded
	failed
)

// DefaultRollover is how much audio one engine stream receives by default before the session moves to a fresh one.
const DefaultRollover = 2 * time.Minute

// Options configures a Session. The zero value is usable.
type Options struct {
	// Commit is the commit policy. The zero value is strict, which commits only what the engine finalizes: at the
	// end, segments it hands over, and each rollover;
	// for freeze see docs/PROTOCOL.md.
	Commit api.Commit
	// Rollover bounds how much unsettled audio one engine stream holds: once a stream has received this much audio
	// beyond what it has handed over results for (see hark.Output), the session finishes it at the next pause (500ms
	// of silence; a minute later it cuts regardless) and continues on a fresh stream. The finished stream's text is
	// committed. Engines that keep handing over finished segments are therefore rarely rolled over; those that never
	// hand over are rolled over every Rollover of audio. Zero means DefaultRollover; it can't be turned off.
	// When audio arrives faster than the previous stream finalizes, a cut is postponed, so a stream can receive up to
	// about Rollover plus two minutes (and Write waits for the previous stream rather than exceed that).
	Rollover time.Duration
	// PartFinishTimeout bounds finalizing a stream at rollover; exceeding it fails the session. Default 15s.
	PartFinishTimeout time.Duration
}

// Session is one in-process recognition. Write and Finish may be called from one goroutine while Next reads from
// another; Snapshot, Dropped, and Close may be called at any time.
type Session struct {
	engine hark.Engine
	req    hark.Request
	opts   Options
	out    *output
	ctx    context.Context // ctx given to the engine, canceled on reaching a terminal state
	cancel context.CancelCauseFunc
	stop   func() bool // stops watching Open's ctx

	op chan struct{} // serializes Write / Finish (a lock that ctx or a terminal state can interrupt)

	// Used only by Write / Finish (serialized by op).
	cur     *stream // the stream receiving audio
	cut     cutter
	written int64 // samples written so far

	mu      sync.Mutex
	state   state
	err     error         // reason for failed
	done    chan struct{} // closed on entering a terminal state
	streams []*stream     // streams not yet closed; cleanup closes them all
	old     *stream       // stream finishing in the background after a rollover; nil if none
	closing int           // retired streams whose Close hasn't returned yet

	rolling atomic.Bool  // Write is waiting for a rolled-over stream to finalize
	rolled  atomic.Int64 // when that wait last ended (UnixNano)

	nextMu sync.Mutex
	sender sender
}

// stream is one engine stream and the part of the document it produces. Each stream has its own ctx (a child of the
// session's), canceled when the stream is closed, so nothing an engine hangs on it outlives the stream.
type stream struct {
	s        hark.Stream
	part     *part
	cancel   context.CancelFunc
	once     sync.Once
	finished chan struct{} // closed when its background finish (after a rollover) is over
}

func (st *stream) close() {
	st.once.Do(func() {
		st.s.Close()
		st.cancel()
	})
}

// Open starts a recognition with req. The commit policy in opts decides when the reader is promised that text will no
// longer change.
//
// req is passed to the engine as is (on rollover, each new stream gets the same req with a part field in Log), and its zero value is usable: an empty SessionID gets a generated UUID, and a nil
// Log uses slog.Default() (both carry a session field).
//
// ctx covers the whole session: when it is canceled the session aborts with ctx's error (equivalent to Close).
// The engine instance belongs to the caller and may be shared by multiple sessions; Session never closes it.
func Open(ctx context.Context, e hark.Engine, req hark.Request, opts Options) (*Session, error) {
	commit, err := opts.Commit.Normalize()
	if err != nil {
		return nil, err
	}
	switch {
	case opts.Rollover < 0:
		return nil, errors.New("session: rollover must not be negative")
	case opts.Rollover == 0:
		opts.Rollover = DefaultRollover
	}
	opts.Commit = commit
	if opts.PartFinishTimeout <= 0 {
		opts.PartFinishTimeout = 15 * time.Second
	}
	if req.SessionID == "" {
		req.SessionID = uuid.NewString()
	}
	if req.Log == nil {
		req.Log = slog.Default()
	}
	req.Log = req.Log.With("session", req.SessionID)
	sctx, cancel := context.WithCancelCause(ctx)
	s := &Session{
		engine: e, req: req, opts: opts, out: newOutput(commit), ctx: sctx, cancel: cancel,
		op: make(chan struct{}, 1), done: make(chan struct{}), cut: newCutter(opts.Rollover),
	}
	p := s.out.newPart(0)
	pctx, pcancel := context.WithCancel(sctx)
	st, err := e.Open(pctx, req, p)
	if err != nil {
		pcancel()
		cancel(err)
		return nil, err
	}
	s.cur = &stream{s: st, part: p, cancel: pcancel}
	s.streams = []*stream{s.cur}
	// Register under mu: if ctx is already canceled the callback runs immediately on another goroutine, and terminate
	// must take mu first, so it always sees the fully initialized object.
	s.mu.Lock()
	s.stop = context.AfterFunc(ctx, func() { s.terminate(context.Cause(ctx)) })
	s.mu.Unlock()
	return s, nil
}

// acquire obtains the right to run Write/Finish and returns a release function. If the session is already in a
// terminal state it doesn't wait (another call may still be stuck in the engine); it returns a no-op release
// function right away and leaves the caller to handle the terminal state.
func (s *Session) acquire(ctx context.Context) (func(), error) {
	select {
	case s.op <- struct{}{}:
		return func() { <-s.op }, nil
	case <-s.done:
		return func() {}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// terminal returns (true, the error Write/Finish should return) in a terminal state.
func (s *Session) terminal() (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case succeeded:
		return true, nil
	case failed:
		return true, s.err
	}
	return false, nil
}

// Write sends a chunk of 16kHz / 16-bit little-endian / mono PCM of any length (an even number of bytes). pcm may be
// reused after it returns (per the hark.Stream.Write contract, an engine that needs to keep it makes its own copy).
// It blocks when the engine consumes at real-time speed (backpressure); Close or ctx cancellation makes it return
// promptly. If the engine fails, the session ends with that error, and Next returns it after delivering the
// results already available.
func (s *Session) Write(pcm []byte) error {
	if len(pcm)%2 != 0 {
		return ErrOddLength
	}
	release, _ := s.acquire(context.Background())
	defer release()
	if done, err := s.terminal(); done {
		if err == nil {
			return ErrFinished
		}
		return err
	}
	if len(pcm) == 0 {
		return s.write(pcm)
	}
	for len(pcm) > 0 {
		if s.cut.overdue(s.cur.part.settled.Load()) { // fast input outran the previous part's finish: wait for it rather than overfill this part
			if err := s.waitOld(); err != nil {
				return err
			}
		}
		n, cut := s.cut.scan(pcm[:min(len(pcm), maxScan)], s.idle(), s.cur.part.settled.Load())
		if n > 0 {
			if err := s.write(pcm[:n]); err != nil {
				return err
			}
			s.written += int64(n / 2)
		}
		pcm = pcm[n:]
		if cut {
			if err := s.rollover(); err != nil {
				return err
			}
		}
	}
	return nil
}

// maxScan bounds how much audio Write hands to the current stream before checking again whether it may roll over.
const maxScan = hark.BytesPerSecond

// waitOld waits for the stream finishing in the background, if any.
func (s *Session) waitOld() error {
	s.mu.Lock()
	old := s.old
	s.mu.Unlock()
	if old == nil {
		return nil
	}
	s.rolling.Store(true)
	select {
	case <-old.finished:
	case <-s.done:
	}
	s.rolled.Store(time.Now().UnixNano())
	s.rolling.Store(false)
	if done, err := s.terminal(); done {
		if err == nil {
			return ErrFinished
		}
		return err
	}
	return nil
}

// write sends pcm to the current stream. The part's fed count is published first, so results the engine hands over
// during this Write may settle the audio it was just given.
func (s *Session) write(pcm []byte) error {
	s.cur.part.fed.Add(int64(len(pcm) / 2))
	if err := s.cur.s.Write(pcm); err != nil {
		return s.engineErr(err)
	}
	return nil
}

// engineErr fails the session with an engine error, unless it already ended (then it reports the real cause).
func (s *Session) engineErr(err error) error {
	if done, terr := s.terminal(); done && terr != nil {
		return terr // already closed / canceled: report the real cause
	}
	err = fmt.Errorf("session: engine: %w", err)
	s.terminate(err)
	return err
}

// idle reports whether no retired stream is still finishing or closing, so a rollover may start. Waiting for the close
// too keeps retired streams from piling up when an engine's Close hangs (the session then just stays on one stream).
func (s *Session) idle() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.old == nil && s.closing == 0
}

// rollover opens a fresh stream for the audio that follows and finishes the current one in the background.
// The new stream's results are held back until the old one is sealed. Called from Write.
func (s *Session) rollover() error {
	p := s.out.newPart(s.written)
	req := s.req
	req.Log = req.Log.With("part", p.n)
	req.Log.Info("rollover: new engine stream", "at_ms", s.written*1000/hark.SampleRate)
	pctx, pcancel := context.WithCancel(s.ctx)
	st, err := s.engine.Open(pctx, req, p)
	if err != nil {
		pcancel()
		return s.engineErr(err)
	}
	next := &stream{s: st, part: p, cancel: pcancel}
	s.mu.Lock()
	if s.state == succeeded || s.state == failed { // ended while opening: the stream is ours to close
		err := s.err
		s.mu.Unlock()
		next.close()
		return err
	}
	old := s.cur
	old.finished = make(chan struct{})
	s.old = old
	s.streams = append(s.streams, next)
	s.mu.Unlock()
	s.cur = next
	go s.finishPart(old)
	return nil
}

// finishPart finalizes a stream after rollover, seals its text and closes it. Failing to finalize fails the session.
//
// old.finished is closed as soon as the outcome is known (sealed, or the session failed), before the stream is closed:
// closing has no deadline, and nothing waiting on finished should depend on it.
func (s *Session) finishPart(old *stream) {
	finished := sync.OnceFunc(func() { close(old.finished) })
	defer finished()
	ctx, cancel := context.WithTimeout(s.ctx, s.opts.PartFinishTimeout)
	defer cancel()
	err := old.s.Finish(ctx)
	if s.ctx.Err() != nil {
		return // the session ended meanwhile; cleanup closes the stream
	}
	if err != nil {
		if ctx.Err() != nil && s.ctx.Err() == nil {
			err = fmt.Errorf("%w (part %d)", ErrRolloverTimeout, old.part.n)
		} else {
			err = fmt.Errorf("session: engine: %w", err)
		}
		s.terminate(err)
		return
	}
	s.out.seal(old.part)
	s.mu.Lock()
	s.old = nil
	s.closing++
	s.mu.Unlock()
	finished()
	old.close()
	s.mu.Lock()
	s.closing--
	for i, st := range s.streams {
		if st == old {
			s.streams = append(s.streams[:i], s.streams[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
}

// Finish signals the end of audio and blocks until the engine finalizes; ctx bounds the finalization timeout. On
// success all text is stable and committed, and Next returns io.EOF after delivering the last changes. If the engine
// fails to finalize or times out, the session ends with that error and the remaining text is not committed.
// Repeated calls return the result of the first call.
//
// Two kinds of timeout: if ctx expires while still waiting for another Write to return, only ctx's error is returned
// and the session is unaffected (Finish can be called again later); if it expires while the engine is finalizing, the
// session fails. If the engine still returns successfully after ctx expires, it counts as success.
func (s *Session) Finish(ctx context.Context) error {
	release, err := s.acquire(ctx)
	if err != nil {
		return err
	}
	defer release()
	s.mu.Lock()
	switch s.state {
	case succeeded:
		s.mu.Unlock()
		return nil
	case failed:
		err := s.err
		s.mu.Unlock()
		return err
	}
	s.state = finishing
	old := s.old
	s.mu.Unlock()

	// Both the caller's timeout and the session's abort must be able to interrupt the engine's Finish.
	fctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	err = s.cur.s.Finish(fctx)

	// A stream from the last rollover may still be finishing (in parallel with the one above): its text comes first,
	// so the session can only end once it is sealed.
	if old != nil && err == nil {
		select {
		case <-old.finished:
		case <-s.done:
		case <-fctx.Done():
			err = fctx.Err()
		}
	}

	s.mu.Lock()
	if s.state == failed { // closed / canceled in the meantime
		err := s.err
		s.mu.Unlock()
		return err
	}
	if err == nil && s.ctx.Err() != nil {
		// Open's ctx is already canceled but the cancel callback hasn't run yet: cancellation wins, not success.
		err = context.Cause(s.ctx)
		s.mu.Unlock()
		s.terminate(err)
		return err
	}
	if err != nil {
		s.mu.Unlock()
		if ctx.Err() != nil {
			err = fmt.Errorf("session: finish: %w", context.Cause(ctx))
		} else {
			err = fmt.Errorf("session: engine: %w", err)
		}
		s.terminate(err)
		return err
	}
	s.out.finish() // finalize first, then announce the terminal state: when Next sees it, the last state is ready
	s.state = succeeded
	close(s.done)
	parent := s.stop
	s.mu.Unlock()
	s.cleanup(nil, parent)
	return nil
}

// terminate moves to failed with err (doing nothing if already in a terminal state) and releases the engine.
// It seals the state and announces the terminal state first (so Next sees it right away), then closes the engine.
func (s *Session) terminate(err error) {
	s.mu.Lock()
	if s.state == succeeded || s.state == failed {
		s.mu.Unlock()
		return
	}
	s.state, s.err = failed, err
	s.out.close() // keep the current result (committing due freezes first) and ignore late snapshots
	close(s.done)
	stop := s.stop
	s.mu.Unlock()
	s.cleanup(err, stop)
}

func (s *Session) cleanup(cause error, stop func() bool) {
	s.cancel(cause)
	if stop != nil {
		stop()
	}
	s.closeStreams()
}

// closeStreams closes every stream that is still open; for one already closing elsewhere, it waits until it's closed.
func (s *Session) closeStreams() {
	s.mu.Lock()
	streams := append([]*stream(nil), s.streams...)
	s.mu.Unlock()
	for _, st := range streams {
		st.close()
	}
}

// Close aborts the session and releases the engine connection. It may be called repeatedly and at any time; when it
// returns, the engine's Stream is closed (if a close is in progress elsewhere, it waits for it to complete).
// A session that already finished normally is unaffected (the result is kept and Next delivers up to io.EOF as
// usual); otherwise the session ends with ErrClosed.
func (s *Session) Close() error {
	s.terminate(ErrClosed)
	s.closeStreams()
	return nil
}

// Next blocks until the state changes and returns an update in the same format as the updates the server sends
// (Track is always 0). Merge it with api.Doc.Apply (when the user edits while speaking, go through
// draft.Changes → Draft.Rebase).
//
// There can be only one reader. When reading is slow, intermediate states are merged into one update (always diffed
// against the state last returned, so no content is lost), keeping memory bounded; but it never slows the engine.
// The timing of freeze commits does not depend on how often you read.
//
// Returns: io.EOF after a normal end once the last changes are delivered; on session failure, it first delivers the
// results already available and then returns the failure cause (ErrClosed for Close, ctx's error for cancellation of
// Open's ctx). ctx bounds only this one wait: on cancellation it returns ctx's error without affecting the session or
// skipping any update. The returned update belongs to the caller.
func (s *Session) Next(ctx context.Context) (api.Update, error) {
	s.nextMu.Lock()
	defer s.nextMu.Unlock()
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		final := closedChan(s.done) // check the terminal state before reading state: changes before it are always picked up here
		if u, ok := s.sender.next(s.out); ok {
			return u, nil
		}
		if final {
			s.mu.Lock()
			st, e := s.state, s.err
			s.mu.Unlock()
			if st == succeeded {
				return api.Update{}, io.EOF
			}
			return api.Update{}, e
		}
		var tc <-chan time.Time
		if d, ok := s.out.nextDeadline(); ok {
			if timer == nil {
				timer = time.NewTimer(time.Until(d))
			} else {
				timer.Reset(time.Until(d))
			}
			tc = timer.C
		}
		select {
		case <-s.out.notify:
		case <-tc: // freeze is due: the commit advances on the next state read
		case <-s.done:
		case <-ctx.Done():
			return api.Update{}, ctx.Err()
		}
	}
}

// RolloverWait reports whether Write is currently blocked waiting for a rolled-over engine stream to finalize (fast
// input outran it), and when such a wait last ended. The wait is bounded by PartFinishTimeout, so callers watching
// for stuck engines should not count it: measure from max(last progress, ended), and not at all while waiting.
func (s *Session) RolloverWait() (waiting bool, ended time.Time) {
	waiting = s.rolling.Load()
	if n := s.rolled.Load(); n != 0 {
		ended = time.Unix(0, n)
	}
	return waiting, ended
}

// Snapshot is a copy of the session's current state. Committed segments that Next has already delivered are no
// longer kept (that is what keeps memory flat in long sessions); Base says how many were left out. Segments[i] is
// segment Base+i; Stable and Commit are absolute.
type Snapshot struct {
	Base     int
	Segments []api.Segment
	Stable   int
	Commit   int
}

// Snapshot returns an independent copy of the current state (normalized by the commit policy) without affecting Next.
// If Next is never called, nothing is left out: Snapshot after Finish is the whole result.
func (s *Session) Snapshot() Snapshot { return s.out.snapshot() }

// Dropped returns the number of engine snapshots discarded under the freeze policy because they conflicted with
// committed content.
func (s *Session) Dropped() int { return s.out.dropped() }

func closedChan(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
