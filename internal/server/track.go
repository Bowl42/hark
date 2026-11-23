package server

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/session"
	"github.com/Bowl42/hark/vocab"
)

var errFinishTimeout = errors.New("did not finish in time")

// track is one output track: a session.Session (engine + normalization) plus the server-side parts:
// the audio queue (stall detection), the track number, sending per output mode, and deciding the final state.
type track struct {
	s    *job
	idx  int
	plan trackPlan

	ctx    context.Context // child of the session ctx: lets this track be ended on its own (e.g. a stuck engine)
	cancel context.CancelCauseFunc

	queue    chan []byte  // mono 20ms frames; closed by demux at end of audio
	consumed atomic.Int64 // when audio was last taken from the queue (UnixNano), for stall detection

	sess *session.Session                // nil if Open failed
	live atomic.Pointer[session.Session] // sess, readable from demux

	// Output mode final: the result is built from the session's updates as a client would, with committed segments
	// moved to the spool, so neither the session nor the track keeps the whole transcript in memory.
	doc   api.Doc
	spool *spool // closed by the job once its writer has exited

	reason string // final state; readable after run returns
}

func newTrack(s *job, idx int, p trackPlan) *track {
	ctx, cancel := context.WithCancelCause(s.ctx)
	t := &track{s: s, idx: idx, plan: p, ctx: ctx, cancel: cancel,
		queue: make(chan []byte, max(1, int(s.c.s.cfg.AudioQueue/(20*time.Millisecond))))}
	if s.output.Mode == api.ModeFinal {
		t.spool = &spool{dir: s.c.s.cfg.SpoolDir}
	}
	t.consumed.Store(time.Now().UnixNano())
	return t
}

func (t *track) lastConsumed() time.Time { return time.Unix(0, t.consumed.Load()) }

// rolloverWait reports the session's rollover wait (see session.RolloverWait), which stall detection must not mistake
// for a stuck engine.
func (t *track) rolloverWait() (bool, time.Time) {
	if s := t.live.Load(); s != nil {
		return s.RolloverWait()
	}
	return false, time.Time{}
}

// run runs until this track ends, then writes its track_end.
func (t *track) run() {
	reason, apiErr := t.execute()
	if t.spool != nil && reason != "" {
		// The rest of the result joins the spool; if it can't be stored, the result can't be delivered complete.
		if err := t.spool.add(t.doc.Segments); err != nil {
			reason, apiErr = t.storageFailed(err)
		}
	}
	t.reason = reason
	t.cancel(nil)        // mark this track ended: demux stops delivering audio to it
	signal(t.s.progress) // wake a delivery that may be waiting on this track so it moves on to the others
	dropped := 0
	if t.sess != nil {
		dropped = t.sess.Dropped()
	}
	if reason != "" && context.Cause(t.s.ctx) != errGone {
		t.s.send(t.endMsg(reason, apiErr, dropped))
	}
	t.s.log.Info("track end", "track", t.idx, "reason", reason, "err", apiErr, "dropped", dropped)
}

// execute opens the session, feeds audio, forwards results, and returns track_end.reason; "" means the connection is gone.
// On return the session is in its final state (sealed); the engine is closed in the background, without blocking
// track_end or the next session.
func (t *track) execute() (string, *api.Error) {
	req := hark.Request{
		SessionID: t.s.id,
		Options:   t.plan.options,
		Log:       t.s.c.log.With("track", t.idx, "channel", t.plan.channel),
	}
	sess, err := session.Open(t.ctx, vocab.Wrap(t.plan.engine, t.plan.vocab), req, session.Options{
		Commit: t.s.commit, Rollover: t.s.c.s.cfg.Rollover, PartFinishTimeout: t.s.c.s.cfg.FinishTimeout,
	})
	if err != nil {
		if r, e, ok := t.byCause(); ok {
			return r, e
		}
		return api.ReasonError, &api.Error{Code: api.ErrEngine, Message: err.Error(), Retryable: true}
	}
	t.sess = sess
	t.live.Store(sess)

	var worker sync.WaitGroup
	defer t.background(&worker)
	worker.Add(1)
	go func() {
		defer worker.Done()
		t.feed()
	}()

	// Read until the session's final state with a ctx that isn't cancelled with the track: when the track is aborted,
	// the session itself (watching t.ctx) seals and ends, so the result and Dropped are fixed by the time we return.
	// In delta mode, updates read after an abort are not sent; final mode collects every update (the result is
	// built from them).
	for {
		u, err := sess.Next(context.Background())
		if err == nil {
			if t.spool != nil {
				if err := t.collect(u); err != nil {
					return t.storageFailed(err)
				}
			} else if _, _, aborted := t.byCause(); !aborted {
				t.deliver(u)
			}
			continue
		}
		// An abort takes precedence over an engine result arriving at the same time.
		if r, e, ok := t.byCause(); ok {
			return r, e
		}
		switch {
		case err == io.EOF:
			return api.ReasonDone, nil
		case errors.Is(err, errFinishTimeout):
			return api.ReasonTimeout, &api.Error{Code: api.ErrTimeout, Message: errFinishTimeout.Error(), Retryable: true}
		case errors.Is(err, session.ErrRolloverTimeout):
			return api.ReasonTimeout, &api.Error{Code: api.ErrTimeout, Message: err.Error(), Retryable: true}
		default:
			return api.ReasonError, &api.Error{Code: api.ErrEngine, Message: err.Error(), Retryable: true}
		}
	}
}

// feed writes queued audio to the session; a closed queue means end of audio, so it calls Finish (with the finalize timeout).
// Errors need no handling here: the session ends with that error, and Next reports it.
func (t *track) feed() {
	for {
		select {
		case f, ok := <-t.queue:
			if !ok {
				// Tag the timeout with errFinishTimeout to distinguish it from a DeadlineExceeded returned by the engine itself.
				ctx, cancel := context.WithTimeoutCause(t.ctx, t.s.c.s.cfg.FinishTimeout, errFinishTimeout)
				t.sess.Finish(ctx)
				cancel()
				return
			}
			// Taking a frame counts as consumption (before Write): stall detection checks whether the engine is still taking audio.
			t.consumed.Store(time.Now().UnixNano())
			signal(t.s.progress)
			if t.sess.Write(f) != nil {
				return
			}
		case <-t.ctx.Done():
			return
		}
	}
}

// collect applies u to the final-mode result and moves what it commits to the spool.
func (t *track) collect(u api.Update) error {
	if err := t.doc.Apply(u); err != nil {
		t.spool.err = err // the result is no longer trustworthy: track_end must not present it
		return err
	}
	return t.spool.add(t.doc.Trim())
}

// deliver sends u tagged with the track number in delta mode.
func (t *track) deliver(u api.Update) {
	u.Track = t.idx
	t.s.send(u)
}

// background closes the engine and waits for the feeding goroutine in the background, without blocking the final messages or the next session.
func (t *track) background(worker *sync.WaitGroup) {
	t.s.cleanup.Add(1)
	go func() {
		defer t.s.cleanup.Done()
		t.cancel(nil)
		t.sess.Close() // on return the engine Stream is closed (if closed elsewhere, this still waits for it to finish)
		worker.Wait()
	}()
}

// byCause returns the final state when the track or session was aborted.
func (t *track) byCause() (string, *api.Error, bool) {
	switch cause := context.Cause(t.ctx); cause {
	case nil:
		return "", nil, false
	case errGone:
		return "", nil, true
	default:
		reason, err := endOf(cause)
		return reason, err, true
	}
}

// storageFailed ends a final-mode track whose result couldn't be stored.
func (t *track) storageFailed(err error) (string, *api.Error) {
	t.s.log.Error("storing final output", "track", t.idx, "err", err)
	return api.ReasonError, &api.Error{Code: api.ErrInternal, Message: "storing the transcript failed"}
}

// endMsg returns track_end. In final mode it carries the result, streamed from the spool; if the spool failed, the
// snapshot is empty (the track has failed) rather than part of the text passed off as the whole.
func (t *track) endMsg(reason string, apiErr *api.Error, dropped int) any {
	msg := api.TrackEnd{Type: api.TypeTrackEnd, Track: t.idx, Reason: reason, Error: apiErr, DroppedRevisions: dropped}
	switch {
	case t.spool == nil:
		return msg
	case t.spool.err != nil:
		msg.Snapshot = &api.Snapshot{Segments: []api.Segment{}}
		return msg
	}
	return &finalEnd{msg: msg, spool: t.spool, stable: t.doc.Stable, commit: t.doc.Commit}
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
