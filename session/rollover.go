package session

import (
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/internal/vad"
)

const (
	// rolloverPause is the silence that marks a good place to roll over.
	rolloverPause = 500 * time.Millisecond
	// rolloverGrace is how much longer than Rollover a part may run while waiting for a pause; then it is cut anyway.
	rolloverGrace = time.Minute
)

func samplesOf(d time.Duration) int64 { return int64(d / time.Millisecond * hark.SampleRate / 1000) }

// cutter decides where to end a part. It looks at the audio as it is written (20ms frames, volume VAD) and keeps no
// audio beyond a partial frame. Once the part is at least every long, it cuts at the first pause; if none comes within
// grace of audio while cutting is allowed, it cuts wherever it is. Cuts fall on frame boundaries.
//
// While the previous part is still finishing, cutting is postponed (the grace clock doesn't run); once the part is
// every+grace long, the session waits for the previous part (overdue) rather than overfilling this one. So a part
// holds at most every+2*grace of audio.
type cutter struct {
	every        int64 // part length in samples (> 0)
	pause, grace int64
	vad          vad.VAD
	frame        []byte // partial frame carried over from the previous Write
	length       int64  // samples analysed in the current part
	silent       int64  // current run of silence, in samples
	due          int64  // samples analysed since a cut became due and allowed
}

func newCutter(every time.Duration) cutter {
	return cutter{every: samplesOf(every), pause: samplesOf(rolloverPause), grace: samplesOf(rolloverGrace)}
}

// overdue reports whether the part has run past every+grace (a cut is due as soon as it's allowed).
func (c *cutter) overdue() bool { return c.length >= c.every+c.grace }

// scan analyses pcm and returns how many bytes of it belong to the current part, and whether the part should end
// right after them. allow=false postpones cutting (the previous part is still finishing).
func (c *cutter) scan(pcm []byte, allow bool) (int, bool) {
	for i := 0; ; {
		need := 2*vad.Frame - len(c.frame)
		if len(pcm)-i < need {
			c.frame = append(c.frame, pcm[i:]...)
			return len(pcm), false
		}
		c.frame = append(c.frame, pcm[i:i+need]...)
		i += need
		if c.vad.Speech(c.frame) {
			c.silent = 0
		} else {
			c.silent += vad.Frame
		}
		c.frame = c.frame[:0]
		c.length += vad.Frame
		if !allow || c.length < c.every {
			continue
		}
		if c.silent >= c.pause || c.due >= c.grace {
			c.length, c.silent, c.due = 0, 0, 0
			return i, true
		}
		c.due += vad.Frame
	}
}
