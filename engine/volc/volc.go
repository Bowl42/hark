// Package volc implements Volcengine (Doubao) large-model streaming speech recognition:
// wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async.
//
// It uses the "full" result mode: each time, the service returns all utterances so far, which is exactly a hark
// snapshot; an utterance's definite maps to Segment.Final, and its timestamps map to Segment.Audio. hark computes
// the diffs, rewrites, stable and commit. Audio is sent in 200ms packets, at least 200ms apart (the official
// recommendation), so never faster than real time; at the end, the remaining partial packet is sent immediately
// together with the end flag, without extra waiting.
//
// As a library:
//
//	eng, err := volc.New(volc.Config{APIKey: os.Getenv("VOLC_API_KEY")})
//
// Config file (harkcmd):
//
//	doubao:
//	  type: volc
//	  api_key: ${VOLC_API_KEY}         # new console; with the old console use app_key + access_key instead
//	  # resource_id: volc.seedasr.sauc.duration   # default: model 2.0, hourly plan; for the concurrency plan / 1.0 see the console
//	  # url: wss://...                  # default bigmodel_async; change this for BytePlus overseas, etc.
//
// The protocol follows the official docs at https://www.volcengine.com/docs/6561/1354869 and was checked against
// open-source implementations; it has not been tested with a real account.
package volc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Bowl42/hark"
	"github.com/coder/websocket"
	"github.com/google/uuid"
)

type Config struct {
	APIKey     string `yaml:"api_key"`     // new console
	AppKey     string `yaml:"app_key"`     // old console: app_key + access_key
	AccessKey  string `yaml:"access_key"`  //
	ResourceID string `yaml:"resource_id"` // default volc.seedasr.sauc.duration
	URL        string `yaml:"url"`         // default bigmodel_async endpoint
}

const (
	defaultURL      = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async"
	defaultResource = "volc.seedasr.sauc.duration"
	dialTimeout     = 5 * time.Second
	packet          = hark.BytesPerSecond / 5 // 200ms
	packetDur       = 200 * time.Millisecond
	maxMessage      = 1 << 20
)

func New(c Config) (*Engine, error) {
	switch {
	case c.APIKey != "" && (c.AppKey != "" || c.AccessKey != ""):
		return nil, errors.New("volc: use either api_key or app_key + access_key, not both")
	case c.APIKey == "" && (c.AppKey == "" || c.AccessKey == ""):
		return nil, errors.New("volc: api_key (or app_key + access_key) is required")
	}
	if c.ResourceID == "" {
		c.ResourceID = defaultResource
	}
	if c.URL == "" {
		c.URL = defaultURL
	}
	return &Engine{cfg: c}, nil
}

type Engine struct{ cfg Config }

func (e *Engine) Open(ctx context.Context, req hark.Request, out hark.Output) (hark.Stream, error) {
	h := http.Header{}
	if e.cfg.APIKey != "" {
		h.Set("X-Api-Key", e.cfg.APIKey)
	} else {
		h.Set("X-Api-App-Key", e.cfg.AppKey)
		h.Set("X-Api-Access-Key", e.cfg.AccessKey)
	}
	h.Set("X-Api-Resource-Id", e.cfg.ResourceID)
	h.Set("X-Api-Request-Id", uuid.NewString())
	h.Set("X-Api-Connect-Id", uuid.NewString())
	h.Set("X-Api-Sequence", "-1")

	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	conn, resp, err := websocket.Dial(dctx, e.cfg.URL, &websocket.DialOptions{HTTPHeader: h})
	if err != nil {
		if resp != nil && resp.Body != nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
			return nil, fmt.Errorf("volc: dial: %w: %s (logid %s)", err, strings.TrimSpace(string(body)), resp.Header.Get("X-Tt-Logid"))
		}
		return nil, fmt.Errorf("volc: dial: %w", err)
	}
	conn.SetReadLimit(maxMessage)
	logid := resp.Header.Get("X-Tt-Logid")

	start, _ := json.Marshal(map[string]any{
		"user":  map[string]any{"uid": req.SessionID},
		"audio": map[string]any{"format": "pcm", "codec": "raw", "rate": hark.SampleRate, "bits": 16, "channel": 1},
		"request": map[string]any{
			"model_name": "bigmodel", "result_type": "full", "show_utterances": true,
			"enable_itn": true, "enable_punc": true,
		},
	})
	if err := conn.Write(dctx, websocket.MessageBinary, frame(typeFullClient, 0, serJSON, start)); err != nil {
		conn.CloseNow()
		return nil, fmt.Errorf("volc: send start: %w", err)
	}
	s := &stream{ctx: ctx, conn: conn, out: out, logid: logid, done: make(chan struct{}), stop: make(chan struct{})}
	go s.read()
	context.AfterFunc(ctx, func() { conn.CloseNow() }) // session aborted: Read and any blocked Write return immediately
	return s, nil
}

// Frame: a 4-byte header [version 1 | header size 1×4, type<<4 | flags, serialization<<4 | compression, 0],
// followed by optional fields depending on type and flags, a 4-byte payload size (big-endian), and the payload.
const (
	typeFullClient = 0x1
	typeAudio      = 0x2
	typeServer     = 0x9
	typeError      = 0xF

	flagSeq       = 0x1 // with a positive sequence number
	flagLast      = 0x2 // last packet, without a sequence number
	flagLastSeq   = 0x3 // last packet, with a negative sequence number
	serJSON       = 0x1
	compressNone  = 0x0
	compressGzip  = 0x1
	maxDecompress = 4 << 20
)

// frame builds an uncompressed client frame.
func frame(typ, flags, ser byte, payload []byte) []byte {
	b := make([]byte, 8, 8+len(payload))
	b[0], b[1], b[2] = 0x11, typ<<4|flags, ser<<4|compressNone
	binary.BigEndian.PutUint32(b[4:], uint32(len(payload)))
	return append(b, payload...)
}

type serverFrame struct {
	typ, flags byte
	code       uint32 // error code of an error frame
	payload    []byte
}

func (f serverFrame) last() bool { return f.flags == flagLast || f.flags == flagLastSeq }

func parse(b []byte) (serverFrame, error) {
	var f serverFrame
	if len(b) < 4 || b[0]>>4 != 1 {
		return f, errors.New("bad header")
	}
	p := int(b[0]&0x0F) * 4
	if p < 4 || len(b) < p {
		return f, errors.New("bad header size")
	}
	f.typ, f.flags = b[1]>>4, b[1]&0x0F
	comp := b[2] & 0x0F
	u32 := func(what string) (uint32, error) {
		if len(b) < p+4 {
			return 0, fmt.Errorf("truncated %s", what)
		}
		v := binary.BigEndian.Uint32(b[p:])
		p += 4
		return v, nil
	}
	var err error
	switch f.typ {
	case typeServer:
		if f.flags == flagSeq || f.flags == flagLastSeq {
			if _, err = u32("sequence"); err != nil {
				return f, err
			}
		}
	case typeError:
		if f.code, err = u32("error code"); err != nil {
			return f, err
		}
	default:
		return f, fmt.Errorf("unexpected message type %d", f.typ)
	}
	n, err := u32("payload size")
	if err != nil {
		return f, err
	}
	if uint64(n) > uint64(len(b)-p) {
		return f, fmt.Errorf("payload size %d exceeds frame", n)
	}
	f.payload = b[p : p+int(n)]
	switch comp {
	case compressNone:
	case compressGzip:
		r, err := gzip.NewReader(bytes.NewReader(f.payload))
		if err != nil {
			return f, fmt.Errorf("gzip: %w", err)
		}
		if f.payload, err = io.ReadAll(io.LimitReader(r, maxDecompress+1)); err != nil {
			return f, fmt.Errorf("gzip: %w", err)
		}
		if len(f.payload) > maxDecompress {
			return f, errors.New("gzip: payload too large")
		}
	default:
		return f, fmt.Errorf("unsupported compression %d", comp)
	}
	return f, nil
}

type response struct {
	Code       *int   `json:"code"`
	StatusCode *int   `json:"status_code"`
	Message    string `json:"message"`
	Result     *struct {
		Text       string `json:"text"`
		Utterances []struct {
			Text      string `json:"text"`
			StartTime int64  `json:"start_time"`
			EndTime   int64  `json:"end_time"`
			Definite  bool   `json:"definite"`
		} `json:"utterances"`
	} `json:"result"`
}

// decode returns the snapshot in this frame; it returns nil (not an update) when the frame has no result.
func decode(payload []byte) ([]hark.Segment, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var r response
	if err := json.Unmarshal(payload, &r); err != nil {
		return nil, fmt.Errorf("bad response: %.200s", payload)
	}
	code := r.Code
	if code == nil {
		code = r.StatusCode
	}
	if code != nil && *code != 0 && *code != 1000 && *code != 20000000 {
		return nil, fmt.Errorf("error %d: %s", *code, r.Message)
	}
	if r.Result == nil {
		return nil, nil
	}
	segs := []hark.Segment{} // non-nil: an explicit empty snapshot
	for _, u := range r.Result.Utterances {
		if u.Text == "" {
			continue
		}
		g := hark.Segment{Text: u.Text, Final: u.Definite}
		if u.StartTime >= 0 && u.EndTime >= u.StartTime { // implausible times: give no position, but don't treat it as an error
			g.Audio = hark.AudioRange{Start: u.StartTime * hark.SampleRate / 1000, End: u.EndTime * hark.SampleRate / 1000}
		}
		segs = append(segs, g)
	}
	if len(r.Result.Utterances) == 0 && r.Result.Text != "" { // no utterance info: use the whole text
		segs = append(segs, hark.Segment{Text: r.Result.Text})
	}
	return segs, nil
}

var (
	errClosed = errors.New("volc: stream closed")
	errEarly  = errors.New("volc: server ended the session before finish")
	errLost   = errors.New("volc: connection lost")
)

type stream struct {
	ctx   context.Context
	conn  *websocket.Conn
	out   hark.Output
	logid string

	// used only in Write / Finish (never concurrently)
	buf  []byte    // audio short of a full packet
	next time.Time // earliest time the next packet may be sent

	finishing atomic.Bool   // the last packet has been sent: an end frame after this is a normal end
	closed    atomic.Bool   // Close has been called
	stop      chan struct{} // closed by Close: interrupts the wait for send pacing
	stopOnce  sync.Once
	done      chan struct{} // closed when read ends
	mu        sync.Mutex
	err       error          // why read ended; nil on a normal end
	last      []hark.Segment // the latest snapshot (kept here; Output gets a copy)
}

// read can't read with a cancelable ctx (coder/websocket closes the connection on cancel); Read returns on its own at Close.
func (s *stream) read() {
	defer close(s.done)
	defer s.conn.CloseNow()
	for {
		typ, data, err := s.conn.Read(context.Background())
		if err != nil {
			s.fail(fmt.Errorf("%w: %w", errLost, err))
			return
		}
		if typ != websocket.MessageBinary {
			s.fail(fmt.Errorf("volc: unexpected text message: %.200q", data))
			return
		}
		f, err := parse(data)
		if err != nil {
			s.fail(fmt.Errorf("volc: bad frame: %w", err))
			return
		}
		if f.typ == typeError {
			s.fail(fmt.Errorf("volc: error %d: %s (logid %s)", f.code, f.payload, s.logid))
			return
		}
		segs, err := decode(f.payload)
		if err != nil {
			s.fail(fmt.Errorf("volc: %w (logid %s)", err, s.logid))
			return
		}
		if segs != nil {
			s.last = segs
		}
		if f.last() {
			if !s.finishing.Load() {
				s.fail(errEarly)
				return
			}
			for i := range s.last {
				s.last[i].Final = true
			}
			s.publish()
			return
		}
		if segs != nil {
			s.publish()
		}
	}
}

// publish hands over a new slice every time: once handed to Output, it belongs to hark.
func (s *stream) publish() {
	s.out.Update(nil, append([]hark.Segment(nil), s.last...))
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

// sendErr, when a send fails, tries to report the server's reason (a send usually fails because the other side has already reported an error and disconnected).
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
	return fmt.Errorf("volc: send %s: %w", what, err)
}

// send sends one audio frame. With pace, it keeps pacing: consecutive packets are at least the previous packet's
// duration apart, so it is never faster than real time; after an idle period, next counts from now, with no catching
// up. The wait can be interrupted by ctx, Close, or the server ending the session.
func (s *stream) send(ctx context.Context, flags byte, pcm []byte, pace bool) error {
	if wait := time.Until(s.next); pace && wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stop:
			return errClosed
		case <-s.done:
			return s.ended()
		}
	}
	s.next = time.Now().Add(time.Duration(len(pcm)) * time.Second / hark.BytesPerSecond)
	if err := s.conn.Write(ctx, websocket.MessageBinary, frame(typeAudio, flags, 0, pcm)); err != nil {
		return s.sendErr("audio", err)
	}
	return nil
}

// Write sends a packet as soon as 200ms has accumulated; it buffers only the partial packet (copied, since pcm is reused after Write returns).
func (s *stream) Write(pcm []byte) error {
	if err := s.ended(); err != nil {
		return err
	}
	if len(s.buf) > 0 {
		n := min(packet-len(s.buf), len(pcm))
		s.buf, pcm = append(s.buf, pcm[:n]...), pcm[n:]
		if len(s.buf) < packet {
			return nil
		}
		if err := s.send(s.ctx, 0, s.buf, true); err != nil {
			return err
		}
		s.buf = s.buf[:0]
	}
	for ; len(pcm) >= packet; pcm = pcm[packet:] {
		if err := s.send(s.ctx, 0, pcm[:packet], true); err != nil {
			return err
		}
	}
	s.buf = append(s.buf, pcm...)
	return nil
}

func (s *stream) Finish(ctx context.Context) error {
	if err := s.ended(); err != nil {
		return err
	}
	s.finishing.Store(true)
	if err := s.send(ctx, flagLast, s.buf, false); err != nil { // the remaining partial packet goes out immediately with the end flag
		return err
	}
	s.buf = nil
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
	s.stopOnce.Do(func() { close(s.stop) })
	s.conn.CloseNow()
	return nil
}
