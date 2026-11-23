package server

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/vocab"
)

// Reasons a session or track is aborted (context cause).
var (
	errCancelled = errors.New("cancelled by client")
	errGone      = errors.New("connection closed")
	errMaxTime   = errors.New("session exceeded max duration")
	errStalled   = errors.New("engine stopped consuming audio")
	errShutdown  = errors.New("server is shutting down")
)

// endOf maps an abort reason to a final state; nil means normal completion.
func endOf(cause error) (string, *api.Error) {
	switch cause {
	case nil:
		return api.ReasonDone, nil
	case errCancelled:
		return api.ReasonCancelled, nil
	case errMaxTime:
		return api.ReasonTimeout, &api.Error{Code: api.ErrTimeout, Message: cause.Error()}
	case errStalled:
		return api.ReasonError, &api.Error{Code: api.ErrEngine, Message: cause.Error(), Retryable: true}
	case errShutdown:
		return api.ReasonError, &api.Error{Code: api.ErrShuttingDown, Message: cause.Error(), Retryable: true}
	}
	return api.ReasonError, &api.Error{Code: api.ErrInternal, Message: cause.Error()}
}

// trackPlan is a track that passed static checks (its track number is its index in plans).
type trackPlan struct {
	engineName string
	engine     hark.Engine
	channel    int
	options    map[string]any
	vocab      *vocab.Matcher // the user's personal vocabulary; nil means no replacement
}

// jobParams holds everything needed to run a session, after start has been validated and defaults filled in.
type jobParams struct {
	id        string
	requestID string
	format    api.Audio
	plans     []trackPlan
	output    api.Output
	commit    api.Commit
	log       *slog.Logger
}

// job is one recognition session in the protocol (start → end). It has several tracks, each a session.Session.
// This layer does the composition: splitting interleaved multichannel audio across tracks, stall detection,
// aggregating the final state, and writing to the connection in order.
//
// Writing: all messages go through writes to a single writer goroutine, which writes them in order.
// Lifecycle: all tracks end (each writes its track_end) → close ending → write end → close done → clean up engines in the background.
// Once done is closed, this session will not write to the connection again, and the connection can start the next session.
type job struct {
	jobParams
	c *connection

	audio     chan []byte // connection → demux, interleaved PCM; closed by the connection on finish
	finishing bool        // read and written only by the connection goroutine

	ctx       context.Context
	cancel    context.CancelCauseFunc
	stopTimer context.CancelFunc
	ending    chan struct{} // about to write end
	done      chan struct{} // end has been written (or can't be); no more writes to the connection

	writes   chan any
	tracks   []*track
	progress chan struct{} // some engine track consumed a frame (wakes deliver)

	cleanup sync.WaitGroup // tracks close their engines in the background
}

func newJob(c *connection, p jobParams) *job {
	base, cancel := context.WithCancelCause(context.Background())
	ctx, stopTimer := context.Context(base), context.CancelFunc(func() {})
	if c.s.cfg.MaxSession > 0 {
		ctx, stopTimer = context.WithTimeoutCause(base, c.s.cfg.MaxSession, errMaxTime)
	}
	return &job{
		jobParams: p,
		c:         c,
		audio:     make(chan []byte, 16),
		ctx:       ctx,
		cancel:    cancel,
		stopTimer: stopTimer,
		ending:    make(chan struct{}),
		done:      make(chan struct{}),
		writes:    make(chan any, 32),
		progress:  make(chan struct{}, 1),
	}
}

func closed(ch chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func (s *job) ended() bool       { return closed(s.done) }
func (s *job) terminating() bool { return closed(s.ending) }

// send hands msg to the writer goroutine. The writer keeps draining messages after an error, so send never blocks for good.
func (s *job) send(msg any) { s.writes <- msg }

func (s *job) writer(exited chan<- struct{}) {
	defer close(exited)
	failed := false
	for msg := range s.writes {
		if failed {
			continue
		}
		if err := s.c.s.writeJSON(s.c.conn, msg); err != nil {
			failed = true
			s.cancel(errGone)
		}
	}
}

// discard drops a job that never ran (the server is shutting down), releasing the timer created by newJob.
func (s *job) discard() {
	s.stopTimer()
	s.cancel(nil)
}

func (s *job) run() {
	defer s.c.s.release(s) // runs last: by then all engines have been closed in cleanup
	defer s.stopTimer()
	begin := time.Now()
	writerExited := make(chan struct{})
	go s.writer(writerExited)

	accepted := make([]api.AcceptedTrack, len(s.plans))
	for i, p := range s.plans {
		accepted[i] = api.AcceptedTrack{Track: i, Engine: p.engineName, Channel: p.channel}
	}
	s.send(api.Started{
		Type: api.TypeStarted, RequestID: s.requestID, SessionID: s.id,
		Audio: s.format, Output: s.output, Commit: s.commit, Tracks: accepted,
	})
	s.log.Info("session start", "tracks", len(s.plans), "channels", s.format.Channels)

	s.tracks = make([]*track, len(s.plans))
	for i, p := range s.plans {
		s.tracks[i] = newTrack(s, i, p)
	}
	go s.demux()

	var wg sync.WaitGroup
	for _, t := range s.tracks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.run()
		}()
	}
	wg.Wait()

	// All tracks have ended (including when all ended early, before the client sent finish).
	end := s.endMsg()
	close(s.ending)
	if end != nil {
		s.send(end)
	}
	close(s.writes)
	<-writerExited
	for _, t := range s.tracks {
		t.spool.close() // written or not, track_end is done with it
	}
	close(s.done)

	s.cancel(nil) // stop demux and make engine calls still running return
	s.cleanup.Wait()
	result := ""
	if end != nil {
		result = end.Reason + "/" + end.Status
	}
	s.log.Info("session end", "result", result, "elapsed_ms", time.Since(begin).Milliseconds())
}

// endMsg returns the session's final state, or nil if the connection is gone.
func (s *job) endMsg() *api.End {
	cause := context.Cause(s.ctx)
	if cause == errGone {
		return nil
	}
	msg := &api.End{Type: api.TypeEnd}
	msg.Reason, msg.Error = endOf(cause)
	done := 0
	for _, t := range s.tracks {
		if t.reason == api.ReasonDone {
			done++
		}
	}
	switch done {
	case len(s.tracks):
		msg.Status = api.StatusComplete
	case 0:
		msg.Status = api.StatusFailed
	default:
		msg.Status = api.StatusPartial
	}
	return msg
}

// frameBytes: each mono frame is at most 20ms, so queue capacity is measured in duration.
const frameBytes = hark.BytesPerSecond / 50

// demux splits interleaved multichannel PCM into mono and distributes it to the tracks in 20ms frames.
// If a track consumes no audio for longer than StallTimeout, only that track is ended; the others continue.
func (s *job) demux() {
	for {
		select {
		case data, ok := <-s.audio:
			if !ok {
				for _, t := range s.tracks {
					close(t.queue) // tell the track's engine: end of audio
				}
				return
			}
			chans := split(data, s.format.Channels)
			pending := make([][][]byte, len(s.tracks))
			for i, t := range s.tracks {
				pending[i] = frames(chans[t.plan.channel])
			}
			if !s.deliver(pending) {
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

// deliver delivers the frames split from one message to the engine tracks, advancing fairly: each round does a
// non-blocking send to every track, and when none can accept, waits for any track to consume (the progress signal)
// or for some track's stall deadline to expire.
//
// A stall means: audio is waiting to be delivered, the queue is full, and the track has consumed nothing in the
// meantime. It is measured per track rather than by delivery time, so a track consuming at normal real-time
// speed is not misjudged because the client sent a long stretch of audio at once or because another track is stuck.
func (s *job) deliver(pending [][][]byte) bool {
	stall := s.c.s.cfg.StallTimeout
	// blocked[i] is when track i first failed to accept a frame in this delivery because its queue was full.
	// The stall clock starts at max(last consumption, start of blocking): while a track waits on other tracks
	// it has nothing to consume, so that time must not count as its stall.
	blocked := make([]time.Time, len(s.tracks))
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	for {
		remaining := false
		var earliest time.Time
		for i, t := range s.tracks {
		push:
			for len(pending[i]) > 0 {
				select {
				case t.queue <- pending[i][0]:
					pending[i] = pending[i][1:]
				case <-t.ctx.Done():
					pending[i] = nil // track has ended
				default:
					break push
				}
			}
			if len(pending[i]) == 0 {
				continue
			}
			if blocked[i].IsZero() {
				blocked[i] = time.Now()
			}
			since := t.lastConsumed()
			if blocked[i].After(since) {
				since = blocked[i]
			}
			// Waiting for a rollover to finalize is not a stall (the session bounds that wait itself): don't count it,
			// and restart the clock when it ends.
			if waiting, ended := t.rolloverWait(); waiting {
				since = time.Now()
			} else if ended.After(since) {
				since = ended
			}
			deadline := since.Add(stall)
			if !time.Now().Before(deadline) {
				t.cancel(errStalled)
				pending[i] = nil
				continue
			}
			remaining = true
			if earliest.IsZero() || deadline.Before(earliest) {
				earliest = deadline
			}
		}
		if !remaining {
			return true
		}
		if timer == nil {
			timer = time.NewTimer(time.Until(earliest))
		} else {
			timer.Reset(time.Until(earliest))
		}
		select {
		case <-s.progress:
		case <-timer.C:
		case <-s.ctx.Done():
			return false
		}
	}
}

// split turns interleaved PCM into one mono PCM buffer per channel.
func split(data []byte, channels int) [][]byte {
	if channels == 1 {
		return [][]byte{data}
	}
	n := len(data) / (2 * channels)
	out := make([][]byte, channels)
	for c := range out {
		out[c] = make([]byte, 2*n)
	}
	for i := 0; i < n; i++ {
		for c := 0; c < channels; c++ {
			src := (i*channels + c) * 2
			out[c][2*i], out[c][2*i+1] = data[src], data[src+1]
		}
	}
	return out
}

func frames(mono []byte) [][]byte {
	var out [][]byte
	for len(mono) > frameBytes {
		out = append(out, mono[:frameBytes])
		mono = mono[frameBytes:]
	}
	if len(mono) > 0 {
		out = append(out, mono)
	}
	return out
}
