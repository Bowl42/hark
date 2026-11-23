package batch

import (
	"context"
	"encoding/binary"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Bowl42/hark"
)

// audio builds audio from parts: positive seconds are speech (a 440Hz sine), negative seconds are silence
func audio(parts ...float64) []byte {
	var pcm []byte
	for _, p := range parts {
		n := int(math.Abs(p) * hark.SampleRate)
		b := make([]byte, 2*n)
		if p > 0 {
			for i := 0; i < n; i++ {
				v := int16(8000 * math.Sin(2*math.Pi*440*float64(i)/hark.SampleRate))
				binary.LittleEndian.PutUint16(b[2*i:], uint16(v))
			}
		}
		pcm = append(pcm, b...)
	}
	return pcm
}

// fakeRec recognizes one "字" per 100ms of audio, so the result shows how much audio was sent
type fakeRec struct {
	mu    sync.Mutex
	calls []int // sample count of each call
	err   error
	gate  chan struct{} // if non-nil, each recognition waits for it to let it through
	empty bool          // always returns empty
}

func (r *fakeRec) Recognize(ctx context.Context, _ hark.Request, pcm []byte) (string, error) {
	r.mu.Lock()
	r.calls = append(r.calls, len(pcm)/2)
	gate, empty := r.gate, r.empty
	r.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	r.mu.Lock()
	err := r.err // read after being let through: a test can switch to an error while recognition is in progress
	r.mu.Unlock()
	if err != nil {
		return "", err
	}
	if empty {
		return "", nil
	}
	return strings.Repeat("字", len(pcm)/2/1600), nil
}

// recorder keeps every full snapshot: everything handed over, then the live segments.
type recorder struct {
	mu   sync.Mutex
	done []hark.Segment
	all  [][]hark.Segment
}

func (r *recorder) Update(done, live []hark.Segment) {
	r.mu.Lock()
	r.done = append(r.done, done...)
	s := append(append([]hark.Segment(nil), r.done...), live...)
	r.all = append(r.all, s)
	r.mu.Unlock()
}

func (r *recorder) last() []hark.Segment {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.all) == 0 {
		return nil
	}
	return r.all[len(r.all)-1]
}

// run writes in 100ms chunks (pace is the gap between chunks), then calls Finish
func run(t *testing.T, rec Recognizer, opts Options, pcm []byte, pace time.Duration) (*recorder, error) {
	t.Helper()
	out := &recorder{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := New(rec, opts).Open(ctx, hark.Request{}, out)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < len(pcm); i += 3200 {
		if err := s.Write(pcm[i:min(i+3200, len(pcm))]); err != nil {
			return out, err
		}
		time.Sleep(pace)
	}
	return out, s.Finish(ctx)
}

func sec(samples int64) float64 { return float64(samples) / hark.SampleRate }

func TestSentences(t *testing.T) {
	rec := &fakeRec{}
	out, err := run(t, rec, Options{}, audio(-0.5, 1, -1, 0.8, -0.3), 0)
	if err != nil {
		t.Fatal(err)
	}
	last := out.last()
	if len(last) != 2 {
		t.Fatalf("want 2 sentences, got %+v", last)
	}
	for _, s := range last {
		if !s.Final {
			t.Fatalf("not final: %+v", last)
		}
	}
	// first sentence: speech starts at 0.5s, with 200ms of lead-in; it stops at 1.5s, keeping 200ms of trailing silence
	a := last[0].Audio
	if math.Abs(sec(a.Start)-0.3) > 0.05 || math.Abs(sec(a.End)-1.7) > 0.05 {
		t.Fatalf("sentence 1 range %.2f-%.2f", sec(a.Start), sec(a.End))
	}
	b := last[1].Audio
	// second sentence: at Finish the trailing silence is shorter than silence; likewise only 200ms is kept
	if math.Abs(sec(b.Start)-2.3) > 0.05 || math.Abs(sec(b.End)-3.5) > 0.05 {
		t.Fatalf("sentence 2 range %.2f-%.2f", sec(b.Start), sec(b.End))
	}
	// the audio sent for whole-sentence recognition is exactly this sentence's range
	if got := last[0].Text; got != strings.Repeat("字", int(a.End-a.Start)/1600) {
		t.Fatalf("sentence 1 text %q", got)
	}
}

// Interim results keep coming while speaking (growing, not final); at the end of the sentence, whole-sentence recognition finalizes it.
func TestPartialResults(t *testing.T) {
	rec := &fakeRec{}
	out, err := run(t, rec, Options{Interval: 300 * time.Millisecond}, audio(-0.3, 2, -1), 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	partials := 0
	prev := 0
	for _, snap := range out.all {
		if len(snap) == 1 && !snap[0].Final {
			partials++
			if n := len([]rune(snap[0].Text)); n < prev {
				t.Fatalf("partial shrank")
			} else {
				prev = n
			}
		}
	}
	if partials < 3 {
		t.Fatalf("want several partial results, got %d (%d updates)", partials, len(out.all))
	}
	if last := out.last(); len(last) != 1 || !last[0].Final {
		t.Fatalf("final %+v", last)
	}
}

// Still speaking at Finish: the sentence ends right away and is finalized.
func TestFinishMidSentence(t *testing.T) {
	out, err := run(t, &fakeRec{}, Options{}, audio(-0.3, 1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if last := out.last(); len(last) != 1 || !last[0].Final || last[0].Text == "" {
		t.Fatalf("final %+v", last)
	}
}

// Forced split at max_sentence: adjacent sentences join end to end, no audio is lost, and no sentence exceeds the limit.
func TestMaxSentence(t *testing.T) {
	out, err := run(t, &fakeRec{}, Options{MaxSentence: time.Second}, audio(2.5), 0)
	if err != nil {
		t.Fatal(err)
	}
	last := out.last()
	if len(last) < 3 {
		t.Fatalf("want ≥3 forced sentences, got %+v", last)
	}
	for i, seg := range last {
		if d := sec(seg.Audio.End - seg.Audio.Start); d > 1.0001 {
			t.Fatalf("sentence %d is %.2fs", i, d)
		}
		if i > 0 && seg.Audio.Start != last[i-1].Audio.End {
			t.Fatalf("gap between sentence %d and %d", i-1, i)
		}
	}
}

// A forced split cuts at the quietest point in the last 2 seconds, not exactly at the limit.
func TestMaxSentenceCutsAtQuietPoint(t *testing.T) {
	// 4 seconds of continuous speech with a brief pause at 2.9-3.0s (not enough for a normal sentence break)
	out, err := run(t, &fakeRec{}, Options{MaxSentence: 3500 * time.Millisecond}, audio(2.9, -0.1, 1.5), 0)
	if err != nil {
		t.Fatal(err)
	}
	last := out.last()
	if len(last) != 2 || math.Abs(sec(last[0].Audio.End)-2.95) > 0.02 {
		t.Fatalf("want cut at the dip (2.95s), got %+v", last)
	}
}

// Continuous speech with very short pauses (250ms): once the sentence gets long, it is split at a pause instead of dragging on to max_sentence.
func TestLongSpeechRelaxesSilence(t *testing.T) {
	var parts []float64
	for i := 0; i < 20; i++ {
		parts = append(parts, 1, -0.25)
	}
	out, err := run(t, &fakeRec{}, Options{}, audio(parts...), 0)
	if err != nil {
		t.Fatal(err)
	}
	last := out.last()
	if len(last) < 3 {
		t.Fatalf("want several sentences, got %d", len(last))
	}
	for i, seg := range last[:len(last)-1] {
		d := sec(seg.Audio.End - seg.Audio.Start)
		// shorter than 3 seconds means short pauses split it (too fragmented); longer than 10 seconds means the silence wasn't relaxed
		if d < 3 || d > 10.5 {
			t.Fatalf("sentence %d lasts %.2fs", i, d)
		}
	}
}

// Very short clicks don't count as speech.
func TestClicksIgnored(t *testing.T) {
	rec := &fakeRec{}
	out, err := run(t, rec, Options{}, audio(-0.5, 0.04, -0.5, 0.04, -0.5), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(rec.calls) != 0 || len(out.last()) != 0 {
		t.Fatalf("calls %v, result %+v", rec.calls, out.last())
	}
}

// Sentences whose recognition result is empty (for example, just noise) are omitted.
func TestEmptySentenceDropped(t *testing.T) {
	out, err := run(t, &fakeRec{empty: true}, Options{}, audio(-0.3, 1, -1), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.last()) != 0 {
		t.Fatalf("result %+v", out.last())
	}
}

func TestRecognizerError(t *testing.T) {
	_, err := run(t, &fakeRec{err: errors.New("boom")}, Options{}, audio(-0.3, 1, -1), 0)
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("want boom, got %v", err)
	}
}

// Close during whole-sentence recognition: Finish returns an error instead of success.
func TestCloseDuringFinish(t *testing.T) {
	rec := &fakeRec{gate: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, _ := New(rec, Options{}).Open(ctx, hark.Request{}, &recorder{})
	s.Write(audio(-0.3, 1))
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.Close()
	}()
	if err := s.Finish(ctx); err == nil {
		t.Fatal("finish succeeded after close")
	}
	if err := s.Write(audio(0.1)); err == nil {
		t.Fatal("write after close should fail")
	}
}

// An interim result never overrides the whole-sentence result after it.
func TestPartialNeverOverridesFinal(t *testing.T) {
	rec := &fakeRec{}
	out, err := run(t, rec, Options{Interval: 100 * time.Millisecond}, audio(-0.3, 1, -1, 1, -1), 5*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	finalText := map[int]string{}
	for _, snap := range out.all {
		for i, s := range snap {
			if f, ok := finalText[i]; ok && s.Text != f && strings.TrimSpace(s.Text) != f {
				t.Fatalf("segment %d changed after final: %q → %q", i, f, s.Text)
			}
			if s.Final {
				finalText[i] = s.Text
			}
		}
	}
}

// noise is continuous background noise (about -40dBFS).
func noise(seconds float64) []byte {
	b := make([]byte, 2*int(seconds*hark.SampleRate))
	for i := 0; i < len(b)/2; i++ {
		binary.LittleEndian.PutUint16(b[2*i:], uint16(int16(300*math.Sin(float64(i)*1.7))))
	}
	return b
}

// manySentences is 30 short utterances, enough to make Write apply backpressure when the recognizer is stuck.
func manySentences() []byte {
	var parts []float64
	for i := 0; i < 30; i++ {
		parts = append(parts, 0.1, -0.7)
	}
	return audio(parts...)
}

func TestVADAdaptsToNoise(t *testing.T) {
	// speech (about -15dBFS) within background noise
	pcm := append(append(noise(3), audio(1)...), noise(3)...)
	out, err := run(t, &fakeRec{}, Options{}, pcm, 0)
	if err != nil {
		t.Fatal(err)
	}
	last := out.last()
	if len(last) != 1 {
		t.Fatalf("want 1 sentence in noise, got %+v", last)
	}
	if a := last[0].Audio; math.Abs(sec(a.Start)-2.8) > 0.1 {
		t.Fatalf("sentence starts at %.2f", sec(a.Start))
	}
}

// Finish after the parent cancels the session: unrecognized sentences must not count as done.
func TestParentCancelFailsFinish(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	s, _ := New(&fakeRec{}, Options{}).Open(parent, hark.Request{}, &recorder{})
	defer s.Close()
	if err := s.Write(audio(-0.3, 1)); err != nil {
		t.Fatal(err)
	}
	cancelParent()
	time.Sleep(50 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Finish(ctx); err == nil {
		t.Fatal("finish succeeded after parent context was cancelled")
	}
	if err := s.Write(audio(0.1)); err == nil {
		t.Fatal("write succeeded after parent context was cancelled")
	}
}

// Writing a large chunk of audio (many sentences) at once gives the same result as small chunks, and isn't mistaken for a slow recognizer.
func TestLargeSingleWrite(t *testing.T) {
	var parts []float64
	for i := 0; i < 20; i++ {
		parts = append(parts, 0.1, -0.7)
	}
	pcm := audio(parts...)
	out := &recorder{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, _ := New(&fakeRec{}, Options{}).Open(ctx, hark.Request{}, out)
	defer s.Close()
	if err := s.Write(pcm); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(out.last()); n != 20 {
		t.Fatalf("want 20 sentences, got %d", n)
	}
}

// When the recognizer can't keep up, Write waits (backpressure), and Close makes it return immediately.
func TestBackpressureAndClose(t *testing.T) {
	rec := &fakeRec{gate: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, _ := New(rec, Options{}).Open(ctx, hark.Request{}, &recorder{})
	done := make(chan error, 1)
	go func() { done <- s.Write(manySentences()) }()
	select {
	case err := <-done:
		t.Fatalf("write should block while recognizer is stuck, returned %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	s.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("write returned nil after close")
		}
	case <-time.After(time.Second):
		t.Fatal("write still blocked after close")
	}
}

// All-zero silence followed by a steady noise floor: the noise floor is misjudged for about 3 seconds at most, not treated as speech forever.
func TestVADDigitalSilenceThenNoise(t *testing.T) {
	var pcm []byte
	pcm = append(pcm, audio(-0.5)...)
	pcm = append(pcm, noise(8)...)
	pcm = append(pcm, audio(1)...)
	pcm = append(pcm, noise(1)...)
	out, err := run(t, &fakeRec{}, Options{}, pcm, 0)
	if err != nil {
		t.Fatal(err)
	}
	speech := false
	for _, s := range out.last() {
		a := s.Audio
		switch {
		case sec(a.End) < 4.2: // noise floor misjudged at the start, of limited length
		case math.Abs(sec(a.Start)-8.3) < 0.15:
			speech = true
		default:
			t.Fatalf("unexpected sentence %.2f-%.2f", sec(a.Start), sec(a.End))
		}
	}
	if !speech {
		t.Fatalf("speech not found: %+v", out.last())
	}
}

// The recognizer fails while Write waits on backpressure: Write returns that error right away instead of waiting forever.
func TestBackpressureRecognizerError(t *testing.T) {
	rec := &fakeRec{gate: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, _ := New(rec, Options{}).Open(ctx, hark.Request{}, &recorder{})
	defer s.Close()
	done := make(chan error, 1)
	go func() { done <- s.Write(manySentences()) }()
	time.Sleep(200 * time.Millisecond)
	rec.mu.Lock()
	rec.err = errors.New("boom")
	rec.mu.Unlock()
	close(rec.gate)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "boom") {
			t.Fatalf("want boom, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("write still blocked after recognizer error")
	}
}

// Finished sentences are handed over and forgotten: over a long stream the engine keeps only the sentence in
// progress, and the full result (handed over + live) is the same as before, spaces included.
func TestHandsOverFinishedSentences(t *testing.T) {
	rec := &wordRec{}
	out := &recorder{}
	st, err := New(rec, Options{}).Open(context.Background(), hark.Request{}, out)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s := st.(*stream)
	most := 0
	const n = 60
	for i := 0; i < n; i++ {
		if err := st.Write(audio(0.6, -0.8)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // let the worker recognize
		s.mu.Lock()
		most = max(most, len(s.sents))
		s.mu.Unlock()
	}
	if err := st.Finish(context.Background()); err != nil {
		t.Fatal(err)
	}
	var text strings.Builder
	for _, seg := range out.last() {
		text.WriteString(seg.Text)
	}
	if want := strings.TrimSpace(strings.Repeat(" word.", n)); text.String() != want {
		t.Fatalf("text %q", text.String())
	}
	if most > maxPending+2 || len(s.sents) != 0 || len(out.done) != n {
		t.Fatalf("engine kept up to %d sentences (%d at the end), handed over %d", most, len(s.sents), len(out.done))
	}
}

// wordRec recognizes every sentence as "word." (English, so the engine must space sentences itself).
type wordRec struct{}

func (wordRec) Recognize(context.Context, hark.Request, []byte) (string, error) { return "word.", nil }
