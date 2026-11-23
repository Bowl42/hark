package session

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/internal/join"
)

func policyOf(c api.Commit) freezePolicy {
	if c.Policy == api.CommitFreeze {
		return freezePolicy{Freeze: true, After: time.Duration(c.AfterMS) * time.Millisecond}
	}
	return freezePolicy{}
}

// output collects the snapshots of a session's engine streams (parts) into one document. Snapshots are normalized on
// entry (commit policy, freeze conflict count), only the latest state is kept, readers are woken through notify, and
// the engine is never blocked.
//
// A session normally has one part. On rollover (see Options.Rollover) a new part is opened while the old one is still
// finishing; the new part is gated: its latest snapshot is held back until the old part is sealed, so at most one part
// is published at a time. Sealing appends the old part's normalized segments to sealed, all committed, and publishes
// the next part after them. Each part is normalized by its own transcript, so the freeze policy never spans parts.
//
// Engines may also hand over finished segments (Done): when the part's normalized state lines
// up with them segment by segment, they are retired at once: committed and moved to sealed, so neither the engine
// nor the part keeps them. When it doesn't (a freeze conflict re-segmented or dropped content), the part holds them
// and keeps presenting the full snapshot, exactly as for engines that never hand over, until it lines up again or
// the part is sealed.
//
// Sealed segments the reader has received (release) are dropped: they never change, so the output only keeps what
// the reader doesn't have yet plus the live and gated parts. gone counts them, and boundary keeps the last character
// of their text for joining.
//
// Freeze commits advance by the current time (tick) before every state read or write, so the result depends only
// on when snapshots arrive, not on how often readers read. Readers also read on a timer set by nextDeadline, but only
// to see watermark changes promptly.
type output struct {
	mu     sync.Mutex
	policy freezePolicy
	sealed []api.Segment // segments of sealed parts not yet released: committed, never change
	gone   int           // sealed segments released and dropped; sealed[i] is segment gone+i
	edge   string        // last character of the released text (for joining the next text to it)
	live   *part         // the published part
	next   *part         // a part waiting for live to be sealed; nil if none
	parts  int           // parts created so far
	drops  int           // freeze conflicts of sealed parts
	tail   []api.Segment // on abort: the gated part's last text, shown after live but never committed
	closed bool          // late snapshots and timers after the terminal state are all ignored
	notify chan struct{}
}

// part is one engine stream's view of the document. It implements hark.Output.
type part struct {
	o       *output
	n       int   // part number, from 0
	offset  int64 // position of the part's first sample in the session audio
	tr      *transcript
	lead    string         // sealed text the part's text follows (for joining); "" once the part has retired text
	held    []hark.Segment // handed-over segments not yet retired (placed); normally empty
	cur     []hark.Segment // the engine's latest snapshot minus what it has handed over since (placed)
	aligned bool           // the last ingest was accepted index-aligned (see transcript.ingest)
	gated   bool           // waiting for the previous part to be sealed
	queued  bool           // while gated: something arrived (rawDone / raw hold it, unplaced)
	raw     []hark.Segment // while gated: the latest snapshot minus what was handed over since
	// rawDone accumulates what is handed over while gated. Its size is bounded in time (the previous part finishes
	// within PartFinishTimeout), not by the rollover length.
	rawDone []hark.Segment
	done    bool // sealed: later snapshots are ignored

	fed     atomic.Int64 // samples given to the engine so far (part-relative), published before each Write
	settled atomic.Int64 // samples the engine has handed over everything for (part-relative; see cutter)
	// handed is how far the handovers received but not yet retired (gated or held) reach: measured when each is
	// received, so audio given to the engine later doesn't count as settled when they finally retire.
	handed int64
}

// newOutput creates an output. c must already be normalized.
func newOutput(c api.Commit) *output {
	return &output{policy: policyOf(c), notify: make(chan struct{}, 1)}
}

// newPart creates the output for an engine stream starting at sample offset. The first part is published right away;
// later ones are gated until the current part is sealed. There is at most one gated part.
func (o *output) newPart(offset int64) *part {
	o.mu.Lock()
	defer o.mu.Unlock()
	p := &part{o: o, n: o.parts, offset: offset, tr: newTranscript(o.policy)}
	o.parts++
	if o.live == nil {
		o.live, p.lead = p, o.sealedText()
	} else {
		p.gated = true
		o.next = p
	}
	return p
}

// changed must be called with mu held.
func (o *output) changed() {
	select {
	case o.notify <- struct{}{}:
	default:
	}
}

// tick must be called with mu held.
func (o *output) tick(now time.Time) {
	if !o.closed && o.live != nil && o.live.tr.tick(now) {
		o.changed()
	}
}

// Update implements hark.Output.
func (p *part) Update(segs []hark.Segment) {
	o := p.o
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || p.done {
		return
	}
	if p.gated {
		p.raw, p.queued = segs, true
		return
	}
	p.cur = o.place(p, segs)
	o.ingest(p)
}

// Done implements hark.Output: the first n segments of the outstanding snapshot are handed over. If the last
// snapshot was accepted aligned they retire right away (without normalizing it again); otherwise they are held, and
// the next snapshot is normalized with them in front.
func (p *part) Done(n int) {
	o := p.o
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || p.done {
		return
	}
	if p.gated {
		n = min(n, len(p.raw))
		if n <= 0 {
			return
		}
		p.hand(p.raw[:n], 0)
		p.rawDone = append(p.rawDone, p.raw[:n]...)
		p.raw, p.queued = p.raw[n:], true
		return
	}
	n = min(n, len(p.cur))
	if n <= 0 {
		return
	}
	p.hand(p.cur[:n], p.offset)
	p.held = append(p.held, p.cur[:n]...)
	p.cur = append([]hark.Segment(nil), p.cur[n:]...)
	if p.aligned {
		o.retire(p, p.tr.retire(len(p.held)))
		p.held = nil
		o.changed()
	}
}

// hand marks segments as handed over (final) and advances p.handed to the end of the audio they cover, measured
// now: offset is what was added to their ranges when placed. Called with mu held.
func (p *part) hand(segs []hark.Segment, offset int64) {
	fed := p.fed.Load()
	for i := range segs {
		segs[i].Final = true
		end := fed // no timing: everything given to the engine so far
		if a := segs[i].Audio; a != (hark.AudioRange{}) {
			end = min(a.End-offset, fed) // never credit audio the engine hasn't been given
		}
		p.handed = max(p.handed, end)
	}
}

// ingest feeds the live part's snapshot (held, then cur; both placed) into its transcript. If it is accepted aligned,
// the held segments retire. Called with mu held.
func (o *output) ingest(p *part) {
	snap := make([]hark.Segment, 0, len(p.held)+len(p.cur))
	snap = append(append(snap, p.held...), p.cur...)
	joinTo(snap, p.lead)
	now := time.Now()
	o.tick(now) // advance due commits first: content whose freeze is already due must not be rewritten by this snapshot
	changed, aligned := p.tr.ingest(snap, now)
	p.aligned = aligned
	if aligned && len(p.held) > 0 {
		o.retire(p, p.tr.retire(len(p.held)))
		p.held, changed = nil, true
	}
	if changed {
		o.changed()
	}
}

// retire moves the retired segments of the live part p to sealed, and settles the audio they were handed over for.
// Called with mu held.
func (o *output) retire(p *part, segs []api.Segment) {
	o.sealed = append(o.sealed, segs...)
	if lastText(segs) != "" {
		p.lead = "" // from here on the engine joins its own text
	}
	p.settled.Store(max(p.settled.Load(), p.handed))
}

// joinTo adds a space where the first text of segs meets prev at English letters/digits (segs is modified in place).
// It only matters at part boundaries: within a stream the engine owns spacing. Already-spaced text is left alone.
func joinTo(segs []hark.Segment, prev string) {
	for i := range segs {
		if segs[i].Text != "" {
			if join.NeedSpace(prev, segs[i].Text) {
				segs[i].Text = " " + segs[i].Text
			}
			return
		}
	}
}

// place maps segments of a part into the session (segs belongs to hark and is modified in place): audio ranges are
// shifted by the part's offset (missing ranges stay missing), and speaker labels of later parts get a part suffix (a
// restarted engine may reuse a label for someone else). Each segment is placed exactly once. Called with mu held.
func (o *output) place(p *part, segs []hark.Segment) []hark.Segment {
	for i := range segs {
		if a := &segs[i].Audio; *a != (hark.AudioRange{}) {
			a.Start += p.offset
			a.End += p.offset
		}
		if spk := segs[i].Speaker; spk != "" {
			spk = speakerEscaper.Replace(spk) // so a label can't collide with another part's suffixed label
			if p.n > 0 {
				spk += "#" + strconv.Itoa(p.n)
			}
			segs[i].Speaker = spk
		}
	}
	return segs
}

// speakerEscaper escapes the part delimiter in engine speaker labels; labels without '#' or '\' are unchanged.
var speakerEscaper = strings.NewReplacer(`\`, `\\`, "#", `\#`)

// sealedText returns the sealed text the next part's text follows (only its end matters).
func (o *output) sealedText() string {
	if t := lastText(o.sealed); t != "" {
		return t
	}
	return o.edge
}

// release drops the sealed segments before position base (absolute), which the reader has received. Only sealed
// segments are dropped: the live part keeps its own, its transcript needs them.
func (o *output) release(base int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	k := min(base-o.gone, len(o.sealed))
	if k <= 0 {
		return
	}
	if t := lastText(o.sealed[:k]); t != "" {
		_, size := utf8.DecodeLastRuneInString(t)
		o.edge = strings.Clone(t[len(t)-size:])
	}
	o.sealed = append([]api.Segment(nil), o.sealed[k:]...) // fresh storage: readers may still hold the old slice
	o.gone += k
}

// lastText returns the text of the last segment that has any.
func lastText(segs []api.Segment) string {
	for i := len(segs) - 1; i >= 0; i-- {
		if segs[i].Text != "" {
			return segs[i].Text
		}
	}
	return ""
}

// seal ends part p (its engine stream finished): all of its text is committed and appended to sealed, and the next
// part, if any, is published with its latest snapshot. p must be the live part.
func (o *output) seal(p *part) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || p != o.live {
		return
	}
	p.tr.finish()
	o.sealed = append(o.sealed, p.tr.segs...)
	o.drops += p.tr.dropped
	p.done, p.held, p.cur = true, nil, nil // held segments are covered by the sealed state (as a dropped snapshot would be)
	o.live, o.next = o.next, nil
	if n := o.live; n != nil {
		n.gated, n.lead = false, o.sealedText()
		if n.queued {
			n.held, n.cur = o.place(n, n.rawDone), o.place(n, n.raw)
			n.queued, n.rawDone, n.raw = false, nil, nil
			o.ingest(n)
		}
	}
	o.changed()
}

// nextDeadline returns the next time a commit may advance.
func (o *output) nextDeadline() (time.Time, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed || o.live == nil {
		return time.Time{}, false
	}
	return o.live.tr.nextDeadline()
}

// stateFrom returns the current normalized state (after advancing due commits) from segment from on (absolute; it
// must lie between the released prefix and commit): the cost depends only on what follows, so a reader that has the
// committed prefix doesn't pay for it again. stable and commit count from the start. The returned slice is
// read-only; do not modify it.
func (o *output) stateFrom(from int) ([]api.Segment, int, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.view(from)
}

// view is stateFrom with mu held.
func (o *output) view(from int) ([]api.Segment, int, int) {
	o.tick(time.Now())
	from -= o.gone
	n := len(o.sealed)
	var segs []api.Segment
	stable, commit := n, n
	if o.live != nil && !o.live.done {
		var s, c int
		segs, s, c = o.live.tr.state()
		stable, commit = n+s, n+c
	}
	stable, commit = o.gone+stable, o.gone+commit
	if from >= n && o.tail == nil {
		return segs[from-n:], stable, commit
	}
	out := make([]api.Segment, 0, max(n-from, 0)+len(segs)+len(o.tail))
	if from < n {
		out = append(out, o.sealed[from:]...)
		out = append(out, segs...)
	} else {
		out = append(out, segs[from-n:]...)
	}
	return append(out, o.tail...), stable, commit
}

// snapshot returns an independent copy of what the output holds: everything after the released prefix.
func (o *output) snapshot() Snapshot {
	o.mu.Lock()
	defer o.mu.Unlock()
	segs, stable, commit := o.view(o.gone)
	return Snapshot{Base: o.gone, Segments: cloneSegments(segs), Stable: stable, Commit: commit}
}

// finish ends normally: the live part is finalized (everything becomes stable and committed), and late snapshots are
// ignored afterwards. There must be no gated part.
func (o *output) finish() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.live != nil {
		o.live.tr.finish()
		o.live.held, o.live.cur = nil, nil
	}
	o.closed = true
	o.changed()
}

// close ends abnormally: it first commits freezes that are already due (whether or not anyone is reading), after which
// the state no longer changes and late snapshots and timers are ignored. The remaining text is not committed.
//
// Text of a gated part (the session ended while the previous part was still finishing) is kept after the live part's
// text, uncommitted, so recognized speech isn't lost from the final snapshot.
func (o *output) close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.tick(time.Now())
	if n := o.next; n != nil && n.queued && o.live != nil {
		prev, _, _ := o.live.tr.state()
		prevText := lastText(prev)
		if prevText == "" {
			prevText = o.sealedText()
		}
		segs := append(o.place(n, n.rawDone), o.place(n, n.raw)...)
		joinTo(segs, prevText)
		for _, g := range segs {
			o.tail = append(o.tail, api.Segment{Text: g.Text, Speaker: g.Speaker,
				AudioRange: api.AudioRange{StartSample: g.Audio.Start, EndSample: g.Audio.End}})
		}
		n.queued, n.rawDone, n.raw = false, nil, nil
	}
	if o.live != nil {
		o.live.held, o.live.cur = nil, nil // still shown (the transcript has them), but nothing will retire them now
	}
	o.closed = true
}

// dropped returns the number of freeze conflicts.
func (o *output) dropped() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := o.drops
	if o.live != nil && !o.live.done {
		n += o.live.tr.dropped
	}
	return n
}
