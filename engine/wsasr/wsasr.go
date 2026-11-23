// Package wsasr is a generic WebSocket streaming engine: it connects any service that emits text while
// listening as a hark backend, through configuration alone, with no Go code.
//
// It simply carries the hark.Engine contract over the network: hark sends audio, the service sends back
// **all current sentences** every time (except those it has handed over, see done below), and hark computes the
// diffs, rewrites, stable and commit. One
// connection per session, with hark as the client:
//
//	connect  GET <url>, with Authorization: Bearer <token> when a token is configured
//
//	hark → service
//	  text    {"type":"start","session":"<session ID>","options":{...}}   first message; options is the downstream start.options, may be absent
//	  binary  PCM s16le / 16kHz / mono                                    any length per message but always an even number of bytes
//	  text    {"type":"finish"}                                           audio has ended; no more audio follows
//
//	service → hark
//	  {"type":"result","segments":[{"text":"today's weather","final":false}, ...]}   sent whenever the result changes, always all sentences not handed over yet
//	  {"type":"done"}                                                                sent after receiving finish and sending the final result; the only sign of a normal end
//	  {"type":"error","message":"..."}                                               an error; this track ends with engine_error
//
// done and error are each the last message. Closing the connection without sending done (including a normal
// WebSocket close) counts as an error. hark simply closes the connection when the session ends or is aborted;
// closing without sending finish means the session was aborted.
//
// Segments are in the order they appear in the full text, with these fields:
//   - text: segments are concatenated directly into the full text, and hark adds no spaces; languages that need
//     spaces put them in text (e.g. "Hello", " world.")
//   - final: optional; the service considers this sentence settled, as a hint only; hark decides when to promise
//     downstream that it will no longer change
//   - done: optional; the service hands this sentence over for good: it will never change, hark commits it (right
//     away, unless it conflicts with text hark has already frozen, which takes precedence), and the service must not send it again (later results start after it), so it can forget it. Only
//     the leading run of done segments counts: a done on a segment after one that isn't done is ignored
//     completely (the segment stays live, and the service must keep sending it). Services that never set it
//     simply send everything every time; long sessions are then bounded by rollover instead
//   - speaker: optional, the speaker
//   - audio_range: optional, {"start_sample":0,"end_sample":16000}, a half-open interval [start, end), counted from
//     the session's first sample
//
// See examples/plugins for a reference implementation.
//
// Configuration:
//
//	paraformer:
//	  type: ws
//	  url: ws://10.0.0.6:9001/stream
//	  token: ${PARAFORMER_TOKEN}   # optional
package wsasr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Bowl42/hark"
	"github.com/coder/websocket"
)

type Config struct {
	URL   string `yaml:"url"`
	Token string `yaml:"token"`
}

func New(c Config) (*Engine, error) {
	if c.URL == "" {
		return nil, errors.New("ws: url is required")
	}
	return &Engine{cfg: c}, nil
}

const dialTimeout = 5 * time.Second

type Engine struct{ cfg Config }

func (e *Engine) Open(ctx context.Context, req hark.Request, out hark.Output) (hark.Stream, error) {
	start, err := json.Marshal(startMsg{Type: "start", Session: req.SessionID, Options: req.Options})
	if err != nil {
		return nil, fmt.Errorf("ws: options: %w", err)
	}
	h := http.Header{}
	if e.cfg.Token != "" {
		h.Set("Authorization", "Bearer "+e.cfg.Token)
	}
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, _, err := websocket.Dial(dctx, e.cfg.URL, &websocket.DialOptions{HTTPHeader: h})
	if err != nil {
		return nil, fmt.Errorf("ws: dial: %w", err)
	}
	conn.SetReadLimit(1 << 20)
	if err := conn.Write(dctx, websocket.MessageText, start); err != nil {
		conn.CloseNow()
		return nil, fmt.Errorf("ws: send start: %w", err)
	}
	s := &stream{ctx: ctx, conn: conn, out: out, done: make(chan struct{})}
	go s.read()
	context.AfterFunc(ctx, func() { conn.CloseNow() }) // session aborted: Read and any blocked Write return immediately
	return s, nil
}

type startMsg struct {
	Type    string         `json:"type"`
	Session string         `json:"session"`
	Options map[string]any `json:"options,omitempty"`
}

type reply struct {
	Type     string    `json:"type"`
	Segments []segment `json:"segments"`
	Message  string    `json:"message"`
}

type segment struct {
	Text       string `json:"text"`
	Final      bool   `json:"final"`
	Done       bool   `json:"done"`
	Speaker    string `json:"speaker"`
	AudioRange struct {
		StartSample int64 `json:"start_sample"`
		EndSample   int64 `json:"end_sample"`
	} `json:"audio_range"`
}

var (
	errClosed = errors.New("ws: stream closed")
	errEarly  = errors.New("ws: server sent done before finish")
	errLost   = errors.New("ws: connection lost")
)

type stream struct {
	ctx  context.Context
	conn *websocket.Conn
	out  hark.Output

	finishing atomic.Bool   // finish has been sent: a done after this is a normal end
	closed    atomic.Bool   // Close has been called
	done      chan struct{} // closed when read ends
	mu        sync.Mutex
	err       error // why read ended; nil when it ended normally on receiving done
}

// read can't read with a cancelable ctx (coder/websocket closes the connection on cancel); Read returns on its own at Close.
// On exit it records the reason before closing the connection: after an error the service may stop reading audio, and a blocked Write only returns once the connection is closed.
func (s *stream) read() {
	defer close(s.done)
	defer s.conn.CloseNow()
	for {
		typ, data, err := s.conn.Read(context.Background())
		if err != nil {
			s.fail(fmt.Errorf("%w: %w", errLost, err))
			return
		}
		var m reply
		if typ != websocket.MessageText || json.Unmarshal(data, &m) != nil {
			s.fail(fmt.Errorf("ws: bad message: %.200q", data))
			return
		}
		switch m.Type {
		case "result":
			segs, err := convert(m.Segments)
			if err != nil {
				s.fail(err)
				return
			}
			k := 0
			for k < len(m.Segments) && m.Segments[k].Done {
				k++
			}
			s.out.Update(segs[:k:k], segs[k:])
		case "done":
			if !s.finishing.Load() {
				s.fail(errEarly)
			}
			return
		case "error":
			s.fail(fmt.Errorf("ws: server error: %s", m.Message))
			return
		default:
			s.fail(fmt.Errorf("ws: unknown message type %q", m.Type))
			return
		}
	}
}

// convert creates a new slice every time: once handed to Output, it belongs to hark.
func convert(in []segment) ([]hark.Segment, error) {
	out := make([]hark.Segment, len(in))
	for i, g := range in {
		r := g.AudioRange
		if r.StartSample < 0 || r.EndSample < r.StartSample {
			return nil, fmt.Errorf("ws: segment %d: bad audio_range [%d, %d)", i, r.StartSample, r.EndSample)
		}
		out[i] = hark.Segment{Text: g.Text, Final: g.Final, Speaker: g.Speaker, Audio: hark.AudioRange{Start: r.StartSample, End: r.EndSample}}
	}
	return out, nil
}

func (s *stream) fail(err error) {
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.mu.Unlock()
}

// ended returns the reason when the session can no longer continue.
func (s *stream) ended() error {
	if s.closed.Load() {
		return errClosed
	}
	select {
	case <-s.done:
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.err != nil {
			return s.err
		}
		return errEarly
	default:
		return nil
	}
}

// sendErr, when a send fails, tries to report the service's reason (a send usually fails because the other side has
// already reported an error and disconnected): it closes the connection, waits for the read loop to process the
// messages already received, then checks the recorded reason. Best effort: if the other side disconnects with
// unread data, TCP resets the connection, and an error message not yet read may be lost.
func (s *stream) sendErr(what string, err error) error {
	s.conn.CloseNow()
	<-s.done
	if s.closed.Load() {
		return errClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil && !errors.Is(s.err, errLost) {
		return s.err
	}
	return fmt.Errorf("ws: send %s: %w", what, err)
}

func (s *stream) Write(pcm []byte) error {
	if err := s.ended(); err != nil {
		return err
	}
	if err := s.conn.Write(s.ctx, websocket.MessageBinary, pcm); err != nil {
		return s.sendErr("audio", err)
	}
	return nil
}

func (s *stream) Finish(ctx context.Context) error {
	if err := s.ended(); err != nil {
		return err
	}
	s.finishing.Store(true)
	if err := s.conn.Write(ctx, websocket.MessageText, []byte(`{"type":"finish"}`)); err != nil {
		return s.sendErr("finish", err)
	}
	select {
	case <-s.done:
		if s.closed.Load() {
			return errClosed
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *stream) Close() error {
	s.closed.Store(true)
	s.conn.CloseNow()
	return nil
}
