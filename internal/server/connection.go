package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/vocab"
	"github.com/coder/websocket"
	"github.com/google/uuid"
)

// connection is one WebSocket connection. It is the only reader, and dispatches messages to the current session via a state machine.
//
// States: idle (cur == nil) → starting / streaming (cur set) → finishing (cur.finishing) → idle.
// Write rule: while a session exists, only the session goroutine writes; when cur is nil, only this goroutine writes.
type connection struct {
	s    *Server
	conn *websocket.Conn
	log  *slog.Logger

	cur *job
	// The previous session has ended; until the next start, ignore audio and finish still in flight from the client.
	draining bool
}

// connError is a connection-level error: send an error message, then close the connection.
type connError struct{ e api.Error }

func (e *connError) Error() string { return e.e.Message }

func protocolErr(format string, a ...any) error {
	return &connError{api.Error{Code: api.ErrProtocol, Message: fmt.Sprintf(format, a...)}}
}

func (c *connection) serve() {
	err := c.loop()
	if c.cur != nil {
		c.cur.cancel(errGone)
		<-c.cur.done
	}
	var ce *connError
	if errors.As(err, &ce) {
		c.s.writeJSON(c.conn, api.ErrorMsg{Type: api.TypeError, Error: ce.e})
		c.conn.Close(websocket.StatusPolicyViolation, ce.e.Code)
		return
	}
	c.conn.Close(websocket.StatusNormalClosure, "")
}

func (c *connection) loop() error {
	for {
		// coder/websocket closes the whole connection when Read's ctx is cancelled, so no cancellable ctx here;
		// Read returns on its own when the connection closes.
		typ, data, err := c.conn.Read(context.Background())
		if err != nil {
			return nil // client disconnected
		}
		if c.cur != nil && c.cur.ended() {
			c.cur, c.draining = nil, true
		}
		if typ == websocket.MessageBinary {
			if err := c.onAudio(data); err != nil {
				return err
			}
			continue
		}
		var env api.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			return protocolErr("invalid json")
		}
		switch env.Type {
		case api.TypeStart:
			err = c.onStart(data)
		case api.TypeFinish:
			err = c.onFinish()
		case api.TypeCancel:
			if c.cur != nil {
				c.cur.cancel(errCancelled)
			}
		default:
			err = protocolErr("unknown message type %q", env.Type)
		}
		if err != nil {
			return err
		}
	}
}

func (c *connection) onAudio(data []byte) error {
	s := c.cur
	switch {
	case s == nil && c.draining:
		return nil
	case s == nil:
		return protocolErr("audio without start")
	case s.finishing:
		return protocolErr("audio after finish")
	case len(data)%(2*s.format.Channels) != 0:
		return protocolErr("audio message length must be a multiple of %d (2 bytes x %d channels)", 2*s.format.Channels, s.format.Channels)
	}
	// When the queue is full, block reading to apply backpressure downstream. The demux goroutine's wait on
	// each track is bounded (StallTimeout) and a stuck track is ended on its own, so this blocking is bounded.
	select {
	case s.audio <- data:
	case <-s.done:
	}
	return nil
}

func (c *connection) onFinish() error {
	s := c.cur
	switch {
	case s == nil && c.draining:
		return nil
	case s == nil:
		return protocolErr("finish without start")
	case s.finishing:
		return nil // idempotent
	}
	s.finishing = true
	close(s.audio)
	return nil
}

func (c *connection) onStart(data []byte) error {
	if s := c.cur; s != nil {
		if !s.finishing && !s.terminating() {
			return protocolErr("start while a session is active")
		}
		// The client may send start right after receiving end, or early after finish:
		// wait for the previous session to finish writing its final messages.
		select {
		case <-s.done:
		case <-time.After(c.s.cfg.FinishTimeout + 5*time.Second):
			return protocolErr("start before previous session ended")
		}
		c.cur = nil
	}
	c.draining = false

	// Parse leniently first to get request_id, then strictly: reject any unknown field, so the client never assumes a feature is active when it isn't.
	var st api.Start
	if err := json.Unmarshal(data, &st); err != nil {
		return protocolErr("invalid start: %v", err)
	}
	reject := func(code, format string, a ...any) error {
		return c.s.writeJSON(c.conn, api.Rejected{Type: api.TypeRejected, RequestID: st.RequestID,
			Error: api.Error{Code: code, Message: fmt.Sprintf(format, a...), Retryable: code == api.ErrShuttingDown}})
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&api.Start{}); err != nil {
		return reject(api.ErrBadRequest, "%v", err)
	}
	if st.RequestID == "" {
		return reject(api.ErrBadRequest, "request_id is required")
	}
	if st.Audio.Encoding != api.EncodingPCM16 || st.Audio.SampleRate != api.SampleRate ||
		st.Audio.Channels < 1 || st.Audio.Channels > api.MaxChannels {
		return reject(api.ErrUnsupportedAudio, "audio must be pcm_s16le / 16000 / 1..%d channels, got %+v", api.MaxChannels, st.Audio)
	}
	var out api.Output
	if st.Output != nil {
		out = *st.Output
	}
	if out.Mode == "" {
		out.Mode = api.ModeDelta
	}
	if out.Mode != api.ModeDelta && out.Mode != api.ModeFinal {
		return reject(api.ErrBadRequest, "output.mode must be delta or final")
	}
	commit, err := st.Commit.Normalize()
	if err != nil {
		return reject(api.ErrBadRequest, "%v", err)
	}
	plans, code, err := c.plan(&st)
	if err != nil {
		return reject(code, "%v", err)
	}
	if st.User != "" {
		if c.s.cfg.Vocab == nil {
			return reject(api.ErrBadRequest, "user requires the server to enable personal vocabulary (data_dir)")
		}
		name, err := vocab.NormalizeUser(st.User)
		if err != nil {
			return reject(api.ErrBadRequest, "user: %v", err)
		}
		// Snapshot at session start: vocabulary changes during the session don't affect it, so replacements are deterministic.
		m, err := c.s.cfg.Vocab.Matcher(name)
		if err != nil {
			c.log.Error("vocab load failed", "user", name, "err", err)
			return reject(api.ErrInternal, "personal vocabulary unavailable")
		}
		for i := range plans {
			plans[i].vocab = m
		}
	}

	id := uuid.NewString()
	s := newJob(c, jobParams{
		id: id, requestID: st.RequestID, format: st.Audio, plans: plans, output: out, commit: commit,
		log: c.log.With("session", id),
	})
	if !c.s.admit(s) {
		s.discard()
		return reject(api.ErrShuttingDown, "server is shutting down")
	}
	c.cur = s
	go s.run()
	return nil
}

// plan statically checks the track layout before any engine is opened; any error results in rejected.
func (c *connection) plan(st *api.Start) ([]trackPlan, string, error) {
	specs := st.Tracks
	if specs == nil {
		for ch := 0; ch < st.Audio.Channels; ch++ {
			specs = append(specs, api.TrackSpec{Channel: ch})
		}
	}
	if len(specs) == 0 || len(specs) > api.MaxTracks {
		return nil, api.ErrBadRequest, fmt.Errorf("tracks must have 1..%d entries", api.MaxTracks)
	}
	defaultEngine := st.Engine
	if defaultEngine == "" {
		defaultEngine = c.s.cfg.Default
	}
	plans := make([]trackPlan, len(specs))
	for i, sp := range specs {
		name := sp.Engine
		if name == "" {
			name = defaultEngine
		}
		inst, ok := c.s.instances[name]
		if !ok {
			return nil, api.ErrUnknownEngine, fmt.Errorf("track %d: unknown engine %q", i, name)
		}
		ch := sp.Channel
		if ch < 0 || ch >= st.Audio.Channels {
			return nil, api.ErrBadRequest, fmt.Errorf("track %d: channel %d out of range (audio has %d)", i, ch, st.Audio.Channels)
		}
		opts := sp.Options
		if opts == nil {
			opts = st.Options
		}
		// No need to copy options here: session.Open gives each engine its own copy.
		plans[i] = trackPlan{
			engineName: name, engine: inst.Engine, channel: ch, options: opts,
		}
	}
	return plans, "", nil
}
