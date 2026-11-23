// Package vad detects speech by volume, frame by frame. It is shared by engine/batch (sentence segmentation) and
// session (choosing where to roll over to a new engine stream).
package vad

import (
	"math"

	"github.com/Bowl42/hark"
)

// Frame is the frame length in samples (20ms).
const Frame = hark.SampleRate / 50

// VAD decides by volume whether a frame is voiced: at least 10dB above the background noise, and no lower than -55dBFS.
//
// Background noise level = the quietest moment in the last 3 seconds (one minimum recorded per 100ms). Speech always
// has pauses between words, so while someone is speaking the level stays near the background noise; when the
// environment gets louder, it catches up within 3 seconds.
//   - Frames below -60dBFS (including all-zero digital silence) count as -60: otherwise, when digital silence is
//     followed by a noise floor, the noise level takes a long time to rise and the noise floor keeps being treated as speech
//   - During the first 3 seconds, the noise level is capped at -50dBFS: when speech starts right away, the quietest
//     moment is the speech itself
type VAD struct {
	mins   [vadBuckets]float64 // ring buffer
	filled int
	next   int
	low    float64 // minimum within the current 100ms
	frames int     // frames so far in the current 100ms
}

const (
	vadBuckets      = 30 // × 100ms = 3s
	framesPerBucket = 5
	quietDB         = -60.0
	warmupFloorDB   = -50.0
)

// Speech reports whether frame f (Frame samples of PCM s16le) is voiced.
func (v *VAD) Speech(f []byte) bool {
	var sum float64
	for i := 0; i+1 < len(f); i += 2 {
		x := float64(int16(uint16(f[i]) | uint16(f[i+1])<<8))
		sum += x * x
	}
	db := 10 * math.Log10(sum/float64(len(f)/2)/(32768*32768)+1e-10)
	level := max(db, quietDB)
	if v.frames == 0 || level < v.low {
		v.low = level
	}
	floor := v.low
	for _, m := range v.mins[:v.filled] {
		floor = min(floor, m)
	}
	if v.filled < vadBuckets {
		floor = min(floor, warmupFloorDB)
	}
	v.frames++
	if v.frames == framesPerBucket {
		v.mins[v.next] = v.low
		v.next = (v.next + 1) % vadBuckets
		v.filled = min(v.filled+1, vadBuckets)
		v.frames = 0
	}
	return db > floor+10 && db > -55
}
