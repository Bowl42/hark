package session

import (
	"strings"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
)

type freezePolicy struct {
	Freeze bool
	After  time.Duration
}

// transcript normalizes the snapshots reported by the engine into protocol state: segments, stable, commit.
//
// Commit policies:
//   - strict: commit stays 0 until the session ends normally.
//   - freeze: once a segment in the stable prefix has stayed unchanged for After, the gateway freezes it (commit
//     advances). Freezing is the gateway's promise, not a guarantee from upstream: later upstream changes to frozen
//     content are ignored.
type transcript struct {
	policy  freezePolicy
	segs    []api.Segment
	finals  []bool      // upstream soft-final flags, aligned with segs
	since   []time.Time // time each segment's content or stability last changed
	stable  int
	commit  int
	dropped int // number of snapshots partly or wholly discarded due to freeze conflicts
}

func newTranscript(p freezePolicy) *transcript { return &transcript{policy: p} }

// ingest accepts a snapshot from the engine and reports whether the normalized state changed, and whether the
// snapshot was accepted index-aligned: normalized segment i is snapshot segment i (no frozen content, or the
// frozen segments are kept as is). Only then may segments the engine handed over be retired by count.
func (t *transcript) ingest(snap []hark.Segment, now time.Time) (changed, aligned bool) {
	segs := make([]api.Segment, len(snap))
	finals := make([]bool, len(snap))
	for i, s := range snap {
		segs[i] = api.Segment{Text: s.Text, Speaker: s.Speaker, AudioRange: api.AudioRange{StartSample: s.Audio.Start, EndSample: s.Audio.End}}
		finals[i] = s.Final
	}
	aligned = true
	if t.commit > 0 {
		var ok bool
		if segs, finals, aligned, ok = t.keepFrozen(segs, finals); !ok {
			t.dropped++
			return false, false
		}
	}

	// Restart the timer from the first changed position (changes to text, metadata, segmentation, or soft-final state all count).
	d := t.commit
	for d < len(segs) && d < len(t.segs) && segs[d] == t.segs[d] && finals[d] == t.finals[d] {
		d++
	}
	since := make([]time.Time, len(segs))
	copy(since, t.since[:min(d, len(t.since))])
	for i := d; i < len(segs); i++ {
		since[i] = now
	}

	stable := t.commit
	for stable < len(segs) && finals[stable] {
		stable++
	}
	changed = d < len(segs) || len(segs) != len(t.segs) || stable != t.stable
	t.segs, t.finals, t.since, t.stable = segs, finals, since, stable
	return changed, aligned
}

// retire removes the first k segments, which the engine has handed over, and returns them: they are committed for
// good and leave the transcript. Watermarks shift down by k (stable then extends over final segments that only the
// retired prefix held back); the remaining segments keep their timers. The
// remainder gets fresh storage (readers may still hold the old slices, which are never modified).
func (t *transcript) retire(k int) []api.Segment {
	out := t.segs[:k:k]
	t.segs = append([]api.Segment(nil), t.segs[k:]...)
	t.finals = append([]bool(nil), t.finals[k:]...)
	t.since = append([]time.Time(nil), t.since[k:]...)
	t.stable, t.commit = max(t.stable, k)-k, max(t.commit, k)-k
	for t.stable < len(t.segs) && t.finals[t.stable] { // the retired prefix may have been what kept the rest unstable
		t.stable++
	}
	return out
}

// keepFrozen ensures the frozen prefix is not rewritten, returning the frozen segments plus the part of the new
// snapshot after the frozen content, and whether the result is index-aligned with segs (strategy 1). If the new
// content can't be located reliably it returns ok=false and the whole snapshot is discarded.
//
// Locating strategies are tried in order of reliability:
//  1. The frozen segments are kept as is: use them directly.
//  2. The frozen text is unchanged but upstream re-segmented it: split at the freeze boundary, losslessly.
//  3. The frozen text was rewritten, but segments carry audio positions: segments starting no earlier than the end of
//     the frozen content are new content; the rest (revisions of frozen content, segments crossing the freeze
//     boundary) are discarded and counted in dropped.
//  4. None of the above: the content can't be located reliably; return ok=false.
//
// It never guesses by character length: ITN and the like change text length, and aligning by length would duplicate
// or lose content.
func (t *transcript) keepFrozen(segs []api.Segment, finals []bool) (_ []api.Segment, _ []bool, aligned, ok bool) {
	frozen := t.segs[:t.commit]
	if len(segs) >= len(frozen) {
		sameText, sameMeta := true, true
		for i := range frozen {
			sameText = sameText && segs[i].Text == frozen[i].Text
			sameMeta = sameMeta && segs[i] == frozen[i]
		}
		if sameText {
			if !sameMeta {
				t.dropped++ // upstream changed the speaker or timestamps of frozen segments: ignore
			}
			return t.withFrozen(segs[len(frozen):]), finalsWithFrozen(len(frozen), finals[len(frozen):]), true, true
		}
	}

	var fj, nj strings.Builder
	for _, s := range frozen {
		fj.WriteString(s.Text)
	}
	for _, s := range segs {
		nj.WriteString(s.Text)
	}
	if frozenText := fj.String(); strings.HasPrefix(nj.String(), frozenText) {
		k, off := locate(segs, len(frozenText))
		tail := append([]api.Segment(nil), segs[k:]...)
		tailFinals := finals[k:]
		if len(tail) > 0 && off > 0 {
			tail[0].Text = tail[0].Text[off:]
			tail[0].AudioRange = api.AudioRange{} // the segment was split, so its original time range is no longer accurate
		}
		return t.withFrozen(tail), finalsWithFrozen(len(frozen), tailFinals), false, true
	}

	if end, ok := frozenEnd(frozen); ok && allTimed(segs) {
		// Filter segment by segment, without assuming segments are sorted by time.
		t.dropped++
		var tail []api.Segment
		var tailFinals []bool
		for i, seg := range segs {
			if seg.AudioRange.StartSample >= end {
				tail = append(tail, seg)
				tailFinals = append(tailFinals, finals[i])
			}
		}
		return t.withFrozen(tail), finalsWithFrozen(len(frozen), tailFinals), false, true
	}
	return nil, nil, false, false
}

func (t *transcript) withFrozen(tail []api.Segment) []api.Segment {
	out := make([]api.Segment, 0, t.commit+len(tail))
	out = append(out, t.segs[:t.commit]...)
	return append(out, tail...)
}

func finalsWithFrozen(n int, tail []bool) []bool {
	out := make([]bool, n, n+len(tail))
	for i := range out {
		out[i] = true
	}
	return append(out, tail...)
}

func frozenEnd(frozen []api.Segment) (int64, bool) {
	if !allTimed(frozen) {
		return 0, false
	}
	var end int64
	for _, s := range frozen {
		end = max(end, s.AudioRange.EndSample)
	}
	return end, true
}

// allTimed reports whether every segment carries a valid time range.
func allTimed(segs []api.Segment) bool {
	for _, s := range segs {
		if r := s.AudioRange; r == (api.AudioRange{}) || r.StartSample > r.EndSample {
			return false
		}
	}
	return true
}

// locate finds the segment containing full-text byte offset n and the offset within that segment.
func locate(segs []api.Segment, n int) (k, off int) {
	for k = range segs {
		if n < len(segs[k].Text) {
			return k, n
		}
		n -= len(segs[k].Text)
	}
	return len(segs), 0
}

// tick advances commit according to the freeze policy and reports whether it changed.
func (t *transcript) tick(now time.Time) bool {
	if !t.policy.Freeze {
		return false
	}
	before := t.commit
	for t.commit < t.stable && now.Sub(t.since[t.commit]) >= t.policy.After {
		t.commit++
	}
	return t.commit != before
}

// nextDeadline returns the next time commit may advance.
func (t *transcript) nextDeadline() (time.Time, bool) {
	if !t.policy.Freeze || t.commit >= t.stable {
		return time.Time{}, false
	}
	return t.since[t.commit].Add(t.policy.After), true
}

// finish ends normally: all segments become stable and committed.
func (t *transcript) finish() {
	t.stable, t.commit = len(t.segs), len(t.segs)
}

// state returns the current normalized state. The returned slice is never modified afterwards.
func (t *transcript) state() (segs []api.Segment, stable, commit int) {
	return t.segs, t.stable, t.commit
}
