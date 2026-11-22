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

	sess *session.Session // nil if Open failed

	reason string // final state; readable after run returns
}

func newTrack(s *job, idx int, p trackPlan) *track {
	ctx, cancel := context.WithCancelCause(s.ctx)
	t := &track{s: s, idx: idx, plan: p, ctx: ctx, cancel: cancel,
		queue: make(chan []byte, max(1, int(s.c.s.cfg.AudioQueue/(20*time.Millisecond))))}
	t.consumed.Store(time.Now().UnixNano())
	return t
}

func (t *track) lastConsumed() time.Time { return time.Unix(0, t.consumed.Load()) }

// run runs until this track ends, then writes its track_end.
func (t *track) run() {
	reason, apiErr := t.execute()
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
	sess, err := session.Open(t.ctx, vocab.Wrap(t.plan.engine, t.plan.vocab), req, t.s.commit)
	if err != nil {
		if r, e, ok := t.byCause(); ok {
			return r, e
		}
		return api.ReasonError, &api.Error{Code: api.ErrEngine, Message: err.Error(), Retryable: true}
	}
	t.sess = sess

	var worker sync.WaitGroup
	defer t.background(&worker)
	worker.Add(1)
	go func() {
		defer worker.Done()
		t.feed()
	}()

	// Read until the session's final state with a ctx that isn't cancelled with the track: when the track is aborted,
	// the session itself (watching t.ctx) seals and ends, so Snapshot / Dropped are fixed by the time we return.
	// Updates read after an abort are discarded.
	for {
		u, err := sess.Next(context.Background())
		if err == nil {
			if _, _, aborted := t.byCause(); !aborted {
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

// deliver sends u tagged with the track number in delta mode (final mode sends only the snapshot at the end).
func (t *track) deliver(u api.Update) {
	if t.s.output.Mode != api.ModeDelta {
		return
	}
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

func (t *track) endMsg(reason string, apiErr *api.Error, dropped int) api.TrackEnd {
	msg := api.TrackEnd{Type: api.TypeTrackEnd, Track: t.idx, Reason: reason, Error: apiErr, DroppedRevisions: dropped}
	if t.s.output.Mode == api.ModeFinal {
		snap := api.Snapshot{Segments: []api.Segment{}}
		if t.sess != nil {
			snap = t.sess.Snapshot()
		}
		msg.Snapshot = &snap
	}
	return msg
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}
