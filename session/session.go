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
// Usage: Open → a goroutine loops on Next to read updates (io.EOF on normal end) → Write audio → Finish.
// If you only need the final result, you don't need to call Next: read Snapshot after Finish. See examples/embedded
// for a complete example.
package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
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

// Session is one in-process recognition. Write and Finish may be called from one goroutine while Next reads from
// another; Snapshot, Dropped, and Close may be called at any time.
type Session struct {
	stream hark.Stream
	out    *output
	ctx    context.Context // ctx given to the engine, canceled on reaching a terminal state
	cancel context.CancelCauseFunc
	stop   func() bool // stops watching Open's ctx

	op chan struct{} // serializes Write / Finish (a lock that ctx or a terminal state can interrupt)

	mu    sync.Mutex
	state state
	err   error         // reason for failed
	done  chan struct{} // closed on entering a terminal state

	closeStream sync.Once

	nextMu sync.Mutex
	sender sender
}

// Open starts a recognition with req. The commit policy (the zero value is strict, which commits only on normal end;
// for freeze see docs/PROTOCOL.md) decides when the reader is promised that text will no longer change.
//
// req is passed to the engine as is, and its zero value is usable: an empty SessionID gets a generated UUID, and a nil
// Log uses slog.Default() (both carry a session field).
//
// ctx covers the whole session: when it is canceled the session aborts with ctx's error (equivalent to Close).
// The engine instance belongs to the caller and may be shared by multiple sessions; Session never closes it.
func Open(ctx context.Context, e hark.Engine, req hark.Request, commit api.Commit) (*Session, error) {
	commit, err := commit.Normalize()
	if err != nil {
		return nil, err
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
		out: newOutput(commit), ctx: sctx, cancel: cancel,
		op: make(chan struct{}, 1), done: make(chan struct{}),
	}
	stream, err := e.Open(sctx, req, s.out)
	if err != nil {
		cancel(err)
		return nil, err
	}
	s.stream = stream
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
	err := s.stream.Write(pcm)
	if err != nil {
		if done, terr := s.terminal(); done && terr != nil {
			return terr // already closed / canceled: report the real cause
		}
		err = fmt.Errorf("session: engine: %w", err)
		s.terminate(err)
		return err
	}
	return nil
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
	s.mu.Unlock()

	// Both the caller's timeout and the session's abort must be able to interrupt the engine's Finish.
	fctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	err = s.stream.Finish(fctx)

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
	s.closeStream.Do(func() { s.stream.Close() })
}

// Close aborts the session and releases the engine connection. It may be called repeatedly and at any time; when it
// returns, the engine's Stream is closed (if a close is in progress elsewhere, it waits for it to complete).
// A session that already finished normally is unaffected (the result is kept and Next delivers up to io.EOF as
// usual); otherwise the session ends with ErrClosed.
func (s *Session) Close() error {
	s.terminate(ErrClosed)
	s.closeStream.Do(func() { s.stream.Close() })
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

// Snapshot returns an independent copy of the current state (normalized by the commit policy) without affecting Next.
func (s *Session) Snapshot() api.Snapshot { return s.out.snapshot() }

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
