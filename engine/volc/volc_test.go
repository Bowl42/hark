package volc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/coder/websocket"
)

// recorder keeps every full snapshot: everything handed over, then the live segments.
type recorder struct {
	mu   sync.Mutex
	done []hark.Segment
	live []hark.Segment
	snap [][]hark.Segment
}

func (r *recorder) Update(live []hark.Segment) {
	r.mu.Lock()
	r.live = live
	r.snap = append(r.snap, append(append([]hark.Segment(nil), r.done...), live...))
	r.mu.Unlock()
}

// Done moves the first n live segments to done, as hark does.
func (r *recorder) Done(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	n = max(0, min(n, len(r.live)))
	r.done = append(r.done, r.live[:n]...)
	r.live = r.live[n:]
}

func (r *recorder) all() [][]hark.Segment {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]hark.Segment(nil), r.snap...)
}

// Server frame: built independently from the protocol (with a sequence number, optionally gzip).
func serverMsg(typ, flags byte, seq int32, code uint32, gz bool, payload []byte) []byte {
	comp := byte(0)
	if gz {
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write(payload)
		w.Close()
		payload, comp = b.Bytes(), 1
	}
	out := []byte{0x11, typ<<4 | flags, 1<<4 | comp, 0}
	if typ == typeServer && (flags == flagSeq || flags == flagLastSeq) {
		out = binary.BigEndian.AppendUint32(out, uint32(seq))
	}
	if typ == typeError {
		out = binary.BigEndian.AppendUint32(out, code)
	}
	out = binary.BigEndian.AppendUint32(out, uint32(len(payload)))
	return append(out, payload...)
}

// Client frame: parsed strictly by its declared sizes.
func clientMsg(t *testing.T, b []byte) (typ, flags byte, payload []byte) {
	if len(b) < 8 || b[0] != 0x11 || b[2]&0x0F != 0 || b[3] != 0 {
		t.Errorf("bad client header % x", b[:min(len(b), 8)])
		return
	}
	n := binary.BigEndian.Uint32(b[4:])
	if int(n) != len(b)-8 {
		t.Errorf("payload size %d, frame has %d", n, len(b)-8)
	}
	return b[1] >> 4, b[1] & 0x0F, b[8:]
}

func result(utts ...string) []byte { // "text|start|end|definite"
	type u struct {
		Text      string `json:"text"`
		StartTime int64  `json:"start_time"`
		EndTime   int64  `json:"end_time"`
		Definite  bool   `json:"definite"`
	}
	var us []u
	text := ""
	for _, s := range utts {
		p := strings.Split(s, "|")
		var st, en int64
		json.Unmarshal([]byte(p[1]), &st)
		json.Unmarshal([]byte(p[2]), &en)
		us = append(us, u{p[0], st, en, p[3] == "1"})
		text += p[0]
	}
	b, _ := json.Marshal(map[string]any{"audio_info": map[string]any{"duration": 1000},
		"result": map[string]any{"text": text, "utterances": us}})
	return b
}

type session struct {
	header http.Header
	start  map[string]any
	audio  [][]byte // payload of each audio frame
	flags  []byte
}

// fakeServer calls script after receiving the start frame; script gets the conn and a function that receives packets.
func fakeServer(t *testing.T, script func(c *websocket.Conn, s *session, next func() bool)) (*Engine, chan *session) {
	got := make(chan *session, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx := context.Background()
		s := &session{header: r.Header}
		defer func() {
			select { // keep only the first session's record; tests that don't read it won't block either
			case got <- s:
			default:
			}
		}()
		_, b, err := c.Read(ctx)
		if err != nil {
			return
		}
		typ, _, p := clientMsg(t, b)
		if typ != typeFullClient || json.Unmarshal(p, &s.start) != nil {
			t.Errorf("bad start frame")
			return
		}
		next := func() bool { // reads one audio frame; returns false on the last packet
			_, b, err := c.Read(ctx)
			if err != nil {
				return false
			}
			typ, flags, p := clientMsg(t, b)
			if typ != typeAudio {
				t.Errorf("want audio frame, got type %d", typ)
			}
			s.audio, s.flags = append(s.audio, p), append(s.flags, flags)
			return flags != flagLast
		}
		script(c, s, next)
	}))
	t.Cleanup(ts.Close)
	e, err := New(Config{APIKey: "k", URL: "ws" + strings.TrimPrefix(ts.URL, "http")})
	if err != nil {
		t.Fatal(err)
	}
	return e, got
}

func TestFlow(t *testing.T) {
	e, got := fakeServer(t, func(c *websocket.Conn, s *session, next func() bool) {
		ctx := context.Background()
		next()
		c.Write(ctx, websocket.MessageBinary, serverMsg(typeServer, flagSeq, 1, 0, true, []byte(`{"code":20000000}`))) // control frame: no result
		c.Write(ctx, websocket.MessageBinary, serverMsg(typeServer, flagSeq, 2, 0, true, result("今天|0|500|0")))
		for next() {
		}
		c.Write(ctx, websocket.MessageBinary, serverMsg(typeServer, flagSeq, 3, 0, false, result("今天天气很好。|0|900|1", "|900|900|0", "我们|1000|1200|0")))
		c.Write(ctx, websocket.MessageBinary, serverMsg(typeServer, flagLastSeq, -4, 0, true, result("今天天气很好。|0|900|1", "我们走吧。|1000|1300|1")))
	})
	rec := &recorder{}
	st, err := e.Open(context.Background(), hark.Request{SessionID: "s1"}, rec)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1000)
	for i := range 20 { // 20000 bytes; chunk size isn't aligned with packet size; buf is reused
		for j := range buf {
			buf[j] = byte(i)
		}
		if err := st.Write(buf); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	st.Close()
	s := <-got

	if s.header.Get("X-Api-Key") != "k" || s.header.Get("X-Api-Resource-Id") != defaultResource ||
		s.header.Get("X-Api-Request-Id") == "" || s.header.Get("X-Api-Connect-Id") == "" || s.header.Get("X-Api-Sequence") != "-1" {
		t.Errorf("headers: %v", s.header)
	}
	audio, _ := json.Marshal(s.start["audio"])
	request, _ := json.Marshal(s.start["request"])
	if string(audio) != `{"bits":16,"channel":1,"codec":"raw","format":"pcm","rate":16000}` ||
		!strings.Contains(string(request), `"result_type":"full"`) || !strings.Contains(string(request), `"show_utterances":true`) {
		t.Errorf("start: %s %s", audio, request)
	}
	var all []byte
	for i, p := range s.audio {
		if i < len(s.audio)-1 && (len(p) != packet || s.flags[i] != 0) {
			t.Errorf("packet %d: %d bytes, flags %d", i, len(p), s.flags[i])
		}
		all = append(all, p...)
	}
	if len(s.audio) != 4 || len(s.audio[3]) != 20000-3*packet || s.flags[3] != flagLast || len(all) != 20000 || all[0] != 0 || all[19999] != 19 {
		t.Errorf("audio: %d packets, %d bytes", len(s.audio), len(all))
	}

	snaps := rec.all()
	if len(snaps) != 3 {
		t.Fatalf("snapshots: %+v", snaps)
	}
	if mid := snaps[1]; len(mid) != 2 || !mid[0].Final || mid[1].Final || mid[1].Audio != (hark.AudioRange{Start: 16000, End: 19200}) {
		t.Errorf("mid snapshot: %+v", mid)
	}
	end := snaps[2]
	if len(end) != 2 || end[1].Text != "我们走吧。" || !end[0].Final || !end[1].Final || end[0].Audio != (hark.AudioRange{End: 14400}) {
		t.Errorf("final snapshot: %+v", end)
	}
}

// The last frame has no result: the previous snapshot carries over, all finalized; with no utterances, the whole text is used.
func TestFinalWithoutResult(t *testing.T) {
	e, _ := fakeServer(t, func(c *websocket.Conn, s *session, next func() bool) {
		ctx := context.Background()
		for next() {
		}
		c.Write(ctx, websocket.MessageBinary, serverMsg(typeServer, flagSeq, 1, 0, false, []byte(`{"result":{"text":"你好"}}`)))
		c.Write(ctx, websocket.MessageBinary, serverMsg(typeServer, flagLast, 0, 0, false, nil))
	})
	rec := &recorder{}
	st, _ := e.Open(context.Background(), hark.Request{}, rec)
	defer st.Close()
	if err := st.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	snaps := rec.all()
	if len(snaps) != 2 || len(snaps[1]) != 1 || snaps[1][0].Text != "你好" || !snaps[1][0].Final || snaps[0][0].Final {
		t.Fatalf("%+v", snaps)
	}
}

func TestServerErrors(t *testing.T) {
	cases := map[string]func(c *websocket.Conn){
		"error 45000081: wait timeout": func(c *websocket.Conn) {
			c.Write(context.Background(), websocket.MessageBinary, serverMsg(typeError, 0, 0, 45000081, true, []byte("wait timeout")))
		},
		"error 55000031: busy": func(c *websocket.Conn) {
			c.Write(context.Background(), websocket.MessageBinary, serverMsg(typeServer, flagSeq, 1, 0, false, []byte(`{"code":55000031,"message":"busy"}`)))
		},
		"before finish": func(c *websocket.Conn) {
			c.Write(context.Background(), websocket.MessageBinary, serverMsg(typeServer, flagLastSeq, -1, 0, false, result("a|0|1|1")))
		},
		"connection lost": func(c *websocket.Conn) { c.Close(websocket.StatusNormalClosure, "") },
		"bad frame":       func(c *websocket.Conn) { c.Write(context.Background(), websocket.MessageBinary, []byte{0x11, 0x90}) },
		"text message":    func(c *websocket.Conn) { c.Write(context.Background(), websocket.MessageText, []byte("hi")) },
	}
	for want, reply := range cases {
		e, _ := fakeServer(t, func(c *websocket.Conn, s *session, next func() bool) {
			next()
			reply(c)
			time.Sleep(100 * time.Millisecond)
		})
		st, err := e.Open(context.Background(), hark.Request{}, &recorder{})
		if err != nil {
			t.Fatal(err)
		}
		st.Write(make([]byte, packet))
		select { // wait for the read loop to process the server's messages
		case <-st.(*stream).done:
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: stream did not end", want)
		}
		err = st.Write(make([]byte, packet))
		if err == nil {
			err = st.Finish(context.Background())
		}
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v", want, err)
		}
		st.Close()
	}
}

func TestDialError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Tt-Logid", "L1")
		http.Error(w, `{"error":"invalid key"}`, http.StatusUnauthorized)
	}))
	defer ts.Close()
	e, _ := New(Config{AppKey: "a", AccessKey: "b", URL: "ws" + strings.TrimPrefix(ts.URL, "http")})
	_, err := e.Open(context.Background(), hark.Request{}, &recorder{})
	if err == nil || !strings.Contains(err.Error(), "invalid key") || !strings.Contains(err.Error(), "logid L1") {
		t.Fatalf("err = %v", err)
	}
}

// Never faster than real time: 1 second of audio written at once goes out as 5 packets 200ms apart, in about 800ms; no catching up after idle; Close and session abort interrupt the wait.
func TestPacing(t *testing.T) {
	var mu sync.Mutex
	var at []time.Duration
	e, _ := fakeServer(t, func(c *websocket.Conn, s *session, next func() bool) {
		start := time.Now()
		for next() {
			mu.Lock()
			at = append(at, time.Since(start))
			mu.Unlock()
		}
	})
	st, _ := e.Open(context.Background(), hark.Request{}, &recorder{})
	start := time.Now()
	if err := st.Write(make([]byte, 5*packet)); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 750*time.Millisecond || d > 1500*time.Millisecond {
		t.Errorf("1s of audio sent in %v", d)
	}
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	for i := 1; i < len(at); i++ {
		if gap := at[i] - at[i-1]; gap < 150*time.Millisecond {
			t.Errorf("packets %d and %d only %v apart", i-1, i, gap)
		}
	}
	mu.Unlock()
	time.Sleep(time.Second) // after idle: the first packet goes out immediately, then pacing resumes
	start = time.Now()
	st.Write(make([]byte, 3*packet))
	if d := time.Since(start); d < 350*time.Millisecond || d > 1000*time.Millisecond {
		t.Errorf("after idle: %v", d)
	}
	time.AfterFunc(100*time.Millisecond, func() { st.Close() })
	start = time.Now()
	err := st.Write(make([]byte, 20*packet))
	if !errors.Is(err, errClosed) || time.Since(start) > time.Second {
		t.Errorf("close during pacing: %v after %v", err, time.Since(start))
	}

	ctx, cancel := context.WithCancel(context.Background())
	st, _ = e.Open(ctx, hark.Request{}, &recorder{})
	defer st.Close()
	time.AfterFunc(100*time.Millisecond, cancel)
	start = time.Now()
	if err := st.Write(make([]byte, 20*packet)); err == nil || time.Since(start) > time.Second {
		t.Errorf("session canceled during pacing: %v after %v", err, time.Since(start))
	}
}

func TestFinishTimeout(t *testing.T) {
	e, _ := fakeServer(t, func(c *websocket.Conn, s *session, next func() bool) {
		for next() {
		}
		time.Sleep(time.Second)
	})
	st, _ := e.Open(context.Background(), hark.Request{}, &recorder{})
	defer st.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := st.Finish(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestParse(t *testing.T) {
	ok := serverMsg(typeServer, flagSeq, 7, 0, false, []byte("{}"))
	for name, b := range map[string][]byte{
		"short":           {0x11},
		"version":         append([]byte{0x21}, ok[1:]...),
		"header size":     {0x10, 0x91, 0x10, 0},
		"truncated seq":   ok[:6],
		"truncated size":  ok[:10],
		"size too big":    append(ok[:8:8], 0, 0, 0, 9, '{', '}'),
		"compression":     {0x11, 0x90, 0x12, 0, 0, 0, 0, 0},
		"bad gzip":        {0x11, 0x90, 0x11, 0, 0, 0, 0, 2, 'x', 'y'},
		"unknown type":    {0x11, 0x20, 0x10, 0, 0, 0, 0, 0},
		"truncated error": {0x11, 0xF0, 0x10, 0, 0, 0},
	} {
		if _, err := parse(b); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
	// extended header (header size 2×4), last frame with a negative sequence number, compressed error frame
	last := serverMsg(typeServer, flagLastSeq, -7, 0, false, []byte("{}"))
	ext := append([]byte{0x12, 0x93, 0x10, 0, 9, 9, 9, 9}, last[4:]...)
	if f, err := parse(ext); err != nil || !f.last() || string(f.payload) != "{}" {
		t.Errorf("extended header: %+v %v", f, err)
	}
	if f, err := parse(serverMsg(typeError, 0, 0, 45000001, true, []byte("bad"))); err != nil || f.code != 45000001 || string(f.payload) != "bad" {
		t.Errorf("error frame: %+v %v", f, err)
	}
	var big bytes.Buffer
	w := gzip.NewWriter(&big)
	w.Write(make([]byte, maxDecompress+1))
	w.Close()
	if _, err := parse(append(binary.BigEndian.AppendUint32([]byte{0x11, 0x90, 0x11, 0}, uint32(big.Len())), big.Bytes()...)); err == nil {
		t.Error("oversized gzip: want error")
	}
}

func TestConfig(t *testing.T) {
	for _, c := range []Config{{}, {AppKey: "a"}, {APIKey: "k", AppKey: "a", AccessKey: "b"}} {
		if _, err := New(c); err == nil {
			t.Errorf("%+v: want error", c)
		}
	}
	e, err := New(Config{AppKey: "a", AccessKey: "b"})
	if err != nil || e.cfg.URL != defaultURL || e.cfg.ResourceID != defaultResource {
		t.Fatalf("%+v %v", e, err)
	}
}
