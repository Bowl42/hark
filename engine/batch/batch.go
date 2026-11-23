// Package batch turns a non-streaming recognizer (whole audio in, text out) into a streaming engine.
//
// Many recognition services can only recognize a complete piece of audio (Whisper, SenseVoice, and the
// various "recorded audio file recognition" APIs) and cannot emit text while listening. batch adds the
// parts that streaming needs in hark:
//
//   - Sentence segmentation: silence is detected by volume, and silence longer than silence ends a sentence;
//     the longer the sentence, the shorter the silence needed (see endSilence). At max_sentence the sentence
//     is forcibly split at the quietest point in its last 2 seconds
//   - Live text: while speaking, every interval the sentence's audio from its start up to now is recognized
//     as a whole, and the new result replaces the old one (downstream sees the current sentence being
//     rewritten over and over)
//   - Finalization: when a sentence ends, the whole sentence is recognized once more and the result is marked Final
//   - Handover: finalized sentences (in order) are handed over to hark once and forgotten (see hark.Output), so a
//     stream only keeps the sentences still in progress and can run for hours without a rollover
//
// An engine author only implements Recognizer and wraps it into an engine with New:
//
//	eng := batch.New(whisper, batch.Options{Interval: 500 * time.Millisecond})
//
// When deploying from a config file, harkcmd.Batch registers it and reads the Options fields from the same
// config section.
//
// Cost: every re-recognition while speaking processes the whole sentence's audio, so longer sentences take
// more compute. It suits recognizers that run much faster than real time.
package batch

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/internal/join"
	"github.com/Bowl42/hark/internal/vad"
)

// Recognizer is a non-streaming recognizer. It is called concurrently by multiple sessions and must be safe for concurrent use.
type Recognizer interface {
	// Recognize recognizes a complete piece of audio (PCM s16le / 16kHz / mono) and returns the text.
	// An empty string means nothing was recognized (for example, just noise). It should return promptly when ctx is canceled.
	Recognize(ctx context.Context, req hark.Request, pcm []byte) (string, error)
}

// Options are the streaming parameters. Zero-valued fields take their defaults.
type Options struct {
	// Interval is how often the current sentence is re-recognized while speaking; 0 means no interim results, only recognition at the end of each sentence.
	Interval time.Duration `yaml:"interval"`
	// Silence is how much silence ends a sentence (gradually shortened for long sentences). Default 600ms.
	Silence time.Duration `yaml:"silence"`
	// MaxSentence is the maximum length of a sentence; longer sentences are forcibly split. Default 20s.
	MaxSentence time.Duration `yaml:"max_sentence"`
}

func (o *Options) defaults() {
	if o.Silence <= 0 {
		o.Silence = 600 * time.Millisecond
	}
	if o.MaxSentence <= 0 {
		o.MaxSentence = 20 * time.Second
	}
}

// New creates a streaming engine from a recognizer. Zero fields in Options take their defaults (an Interval of 0 means no interim results).
func New(rec Recognizer, opts Options) *Engine {
	opts.defaults()
	return &Engine{rec: rec, opts: opts}
}

// Engine is the streaming engine (a hark.Engine). It can be shared by multiple sessions.
type Engine struct {
	rec  Recognizer
	opts Options
}

// Close calls the recognizer's Close if it implements io.Closer.
func (e *Engine) Close() error {
	if c, ok := e.rec.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// Silence detection works on 20ms frames of audio.
const frame = vad.Frame

func samples(d time.Duration) int64 { return int64(d / time.Millisecond * hark.SampleRate / 1000) }

func (e *Engine) Open(ctx context.Context, req hark.Request, out hark.Output) (hark.Stream, error) {
	ctx, cancel := context.WithCancel(ctx)
	s := &stream{
		rec: e.rec, req: req, out: out, ctx: ctx, cancel: cancel,
		interval: samples(e.opts.Interval), silence: samples(e.opts.Silence), maxLen: samples(e.opts.MaxSentence),
		wake: make(chan struct{}, 1), progress: make(chan struct{}, 1), idle: make(chan struct{}),
	}
	go s.work()
	return s, nil
}

// sentence is one sentence.
type sentence struct {
	start, end int64  // position in the session audio (samples); end is 0 until the sentence ends
	pcm        []byte // audio of this sentence; released once it is finalized
	text       string
	ended      bool  // audio has ended; waiting for (or undergoing) whole-sentence recognition
	final      bool  // whole-sentence recognition is done
	asked      int64 // sentence length (samples) when an interim result was last requested
}

type stream struct {
	rec    Recognizer
	req    hark.Request
	out    hark.Output
	ctx    context.Context
	cancel context.CancelFunc

	interval, silence, maxLen int64

	mu        sync.Mutex
	vad       vad.VAD
	rest      []byte      // audio shorter than one frame
	pos       int64       // samples processed so far
	preroll   []byte      // the last 200ms of audio outside a sentence: a sentence starts a little earlier so the onset of the first word isn't clipped
	speechRun int64       // consecutive voiced samples (before a sentence has started)
	silentRun int64       // consecutive silent samples within a sentence
	sents     []*sentence // sentences not handed over yet; the last one may still be in progress
	edge      string      // text of the last sentence handed over (for spacing the next one)
	cur       *sentence   // the sentence currently being spoken
	wantPart  bool        // the current sentence needs an interim result
	pending   int         // sentences that have ended but whose whole-sentence recognition isn't done yet
	finishing bool
	err       error // recognizer error or session canceled: Write / Finish return it from then on
	closed    bool

	wake     chan struct{} // new work available
	progress chan struct{} // a whole-sentence recognition finished (wakes a Write waiting on the backlog)
	idle     chan struct{} // closed when the worker exits: everything finalized after finishing, or an error
}

var errClosed = errors.New("batch: stream closed")

func (s *stream) Write(pcm []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failed(); err != nil {
		return err
	}
	if s.finishing {
		return errors.New("batch: write after finish")
	}
	s.rest = append(s.rest, pcm...)
	off := 0
	for len(s.rest)-off >= 2*frame {
		// too many sentences waiting for whole-sentence recognition (the recognizer can't keep up): wait, so backpressure reaches downstream and no audio is dropped
		for s.pending >= maxPending {
			if err := s.waitProgress(); err != nil {
				return err
			}
		}
		s.frame(s.rest[off : off+2*frame])
		off += 2 * frame
	}
	s.rest = append(s.rest[:0], s.rest[off:]...)
	return nil
}

// waitProgress releases the lock and waits until the worker finishes a sentence or the session ends. Called with mu held.
func (s *stream) waitProgress() error {
	s.mu.Unlock()
	select {
	case <-s.progress:
	case <-s.idle: // the worker exited (for example, a recognition error): no more progress will come
	case <-s.ctx.Done():
	}
	s.mu.Lock()
	return s.failed()
}

// failed returns the reason the stream can't continue. Called with mu held.
func (s *stream) failed() error {
	if s.closed {
		return errClosed
	}
	if s.err != nil {
		return s.err
	}
	if err := s.ctx.Err(); err != nil {
		return fmt.Errorf("batch: %w", err)
	}
	return nil
}

const (
	prerollSamples = hark.SampleRate / 5        // 200ms
	minSpeech      = 3 * frame                  // 60ms of continuous sound starts a sentence; filters out clicks
	tailSamples    = int64(hark.SampleRate / 5) // keep 200ms of silence at the end of a sentence
	maxPending     = 16                         // limit on sentences waiting for whole-sentence recognition; beyond it, Write waits
)

// frame processes one frame (20ms). Called with mu held.
func (s *stream) frame(f []byte) {
	speech := s.vad.Speech(f)
	s.pos += frame
	if s.cur == nil {
		s.preroll = append(s.preroll, f...)
		if speech {
			s.speechRun += frame
		} else {
			s.speechRun = 0
		}
		if s.speechRun >= minSpeech {
			s.begin()
		} else if over := len(s.preroll) - 2*(prerollSamples+minSpeech); over > 0 {
			s.preroll = s.preroll[over:]
		}
		return
	}
	cur := s.cur
	cur.pcm = append(cur.pcm, f...)
	if speech {
		s.silentRun = 0
	} else {
		s.silentRun += frame
	}
	length := int64(len(cur.pcm) / 2)
	switch {
	case s.silentRun >= s.endSilence(length-s.silentRun):
		s.end()
	case length >= s.maxLen:
		s.split()
	case s.interval > 0 && length-cur.asked >= s.interval:
		cur.asked = length
		s.wantPart = true
		signal(s.wake)
	}
}

// endSilence returns how much silence ends a sentence once spoken samples of it have been spoken:
// silence for the first 3 seconds, then shrinking linearly to 1/4 of silence at 10 seconds (or at max_sentence, whichever is earlier).
func (s *stream) endSilence(spoken int64) int64 {
	relaxFrom, relaxTo := int64(3*hark.SampleRate), min(int64(10*hark.SampleRate), s.maxLen)
	if spoken <= relaxFrom || relaxTo <= relaxFrom {
		return s.silence
	}
	f := min(float64(spoken-relaxFrom)/float64(relaxTo-relaxFrom), 1)
	return int64(float64(s.silence) * (1 - 0.75*f))
}

// split handles a sentence that reached max_sentence: it cuts at the quietest point in the last 2 seconds, and the audio after the cut starts the next sentence. Called with mu held.
func (s *stream) split() {
	cur := s.cur
	n := len(cur.pcm) / 2
	cut := quietest(cur.pcm, max(n-2*hark.SampleRate, n/2), n)
	rest := append([]byte(nil), cur.pcm[2*cut:]...)
	cur.pcm = cur.pcm[:2*cut]
	s.silentRun = 0 // the cut point was chosen by quietest; don't trim the sentence tail
	s.end()
	s.preroll = rest
	s.begin()
}

// quietest finds the 20ms window with the lowest energy in the sample range [from, to) of pcm and returns its midpoint;
// among consecutive equally quiet windows it takes the middle one. If the range is shorter than one window, it returns to.
func quietest(pcm []byte, from, to int) int {
	const win, hop = frame, frame / 2
	if to-from < win {
		return to
	}
	runStart, runEnd, best := to, to, int64(-1)
	for p := from; p+win <= to; p += hop {
		var e int64
		for i := p; i < p+win; i++ {
			v := int64(int16(binary.LittleEndian.Uint16(pcm[2*i:])))
			e += v * v
		}
		switch {
		case best < 0 || e < best:
			runStart, runEnd, best = p, p, e
		case e == best && p == runEnd+hop:
			runEnd = p
		}
	}
	return (runStart+runEnd)/2 + win/2
}

// begin starts a sentence, including the audio just before it. Called with mu held.
func (s *stream) begin() {
	n := int64(len(s.preroll) / 2)
	cur := &sentence{start: s.pos - n, pcm: s.preroll}
	s.preroll = nil
	s.speechRun, s.silentRun = 0, 0
	s.sents = append(s.sents, cur)
	s.cur = cur
}

// end ends the current sentence and hands it to the worker for whole-sentence recognition. Called with mu held.
func (s *stream) end() {
	cur := s.cur
	// drop excess trailing silence, keeping only tailSamples
	if cut := s.silentRun - tailSamples; cut > 0 {
		cur.pcm = cur.pcm[:len(cur.pcm)-2*int(cut)]
	}
	cur.ended = true
	cur.end = cur.start + int64(len(cur.pcm)/2)
	s.cur = nil
	s.wantPart = false
	s.silentRun = 0
	s.pending++
	signal(s.wake)
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// work runs recognitions one after another: whole-sentence recognition first (in order), then the current
// sentence's interim result (only the latest one). A session recognizes one piece at a time, so recognizer
// concurrency is determined by the number of sessions.
func (s *stream) work() {
	defer close(s.idle)
	for {
		s.mu.Lock()
		var target *sentence
		partial := false
		for _, x := range s.sents {
			if x.ended && !x.final {
				target = x
				break
			}
		}
		if target == nil && s.wantPart && s.cur != nil {
			target, partial = s.cur, true
			s.wantPart = false
		}
		if target == nil && s.finishing && s.cur == nil || s.err != nil || s.closed {
			s.mu.Unlock()
			return
		}
		if target == nil {
			s.mu.Unlock()
			select {
			case <-s.wake:
			case <-s.ctx.Done():
				// session canceled: record the reason, so Finish can't treat unrecognized sentences as done
				s.mu.Lock()
				if s.err == nil && !s.closed {
					s.err = fmt.Errorf("batch: %w", s.ctx.Err())
				}
				s.mu.Unlock()
				return
			}
			continue
		}
		pcm := target.pcm
		if partial {
			pcm = append([]byte(nil), pcm...) // the sentence is still growing; take a snapshot of it as it is now
		}
		s.mu.Unlock()

		text, err := s.rec.Recognize(s.ctx, s.req, pcm)

		s.mu.Lock()
		switch {
		case s.closed:
		case err != nil:
			if s.err == nil {
				s.err = fmt.Errorf("batch: recognize: %w", err)
			}
		case partial:
			// the sentence may have ended and be waiting for whole-sentence recognition: the interim result can still be shown meanwhile
			if !target.final {
				target.text = strings.TrimSpace(text)
				s.report()
			}
		default:
			target.text = strings.TrimSpace(text)
			target.final = true
			target.pcm = nil
			s.pending--
			signal(s.progress)
			s.report()
		}
		s.mu.Unlock()
	}
}

// report hands the current results to Output. The leading run of sentences that have ended and have their
// whole-sentence result can't change any more: they are handed over and forgotten, so the stream only keeps what
// may still change, however long it runs. Called with mu held.
func (s *stream) report() {
	k := 0
	for k < len(s.sents) && s.sents[k].ended && s.sents[k].final {
		k++
	}
	done, edge := segments(s.sents[:k], s.edge)
	live, _ := segments(s.sents[k:], edge)
	s.sents = slices.Delete(s.sents, 0, k) // also clears the vacated pointers
	s.edge = edge
	s.out.Update(append(done, live...))
	s.out.Done(len(done))
}

// segments turns sentences into segments: sentences without text are omitted, and a space is added between
// adjacent sentences that meet at English letters/digits (prev is the text before the first). It also returns the
// last text, for joining what follows.
func segments(sents []*sentence, prev string) ([]hark.Segment, string) {
	var segs []hark.Segment
	for _, x := range sents {
		if x.text == "" {
			continue
		}
		end := x.end
		if !x.ended {
			end = x.start + int64(len(x.pcm)/2)
		}
		text := x.text
		if join.NeedSpace(prev, text) {
			text = " " + text
		}
		segs = append(segs, hark.Segment{Text: text, Final: x.final, Audio: hark.AudioRange{Start: x.start, End: end}})
		prev = text
	}
	return segs, prev
}

func (s *stream) Finish(ctx context.Context) error {
	s.mu.Lock()
	if err := s.failed(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.finishing = true
	if s.cur != nil {
		s.cur.pcm = append(s.cur.pcm, s.rest...)
		s.rest = nil
		s.end()
	}
	signal(s.wake)
	s.mu.Unlock()

	select {
	case <-s.idle:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.failed(); err != nil {
		return err
	}
	s.report()
	return nil
}

func (s *stream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.cancel()
	return nil
}
