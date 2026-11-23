package session

import (
	"strconv"
	"strings"
	"sync"
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
	o      *output
	n      int   // part number, from 0
	offset int64 // position of the part's first sample in the session audio
	tr     *transcript
	gated  bool           // waiting for the previous part to be sealed
	raw    []hark.Segment // latest snapshot while gated
	done   bool           // sealed: later snapshots are ignored
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
		o.live = p
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
		p.raw = segs
		return
	}
	o.ingest(p, segs)
}

// ingest feeds a snapshot of the live part p into its transcript. Called with mu held.
func (o *output) ingest(p *part, segs []hark.Segment) {
	now := time.Now()
	o.tick(now) // advance due commits first: content whose freeze is already due must not be rewritten by this snapshot
	if p.tr.ingest(o.place(p, segs, o.sealedText()), now) {
		o.changed()
	}
}

// place maps a part's snapshot into the session (segs belongs to hark and is modified in place): audio ranges are
// shifted by the part's offset (missing ranges stay missing), speaker labels of later parts get a part suffix (a
// restarted engine may reuse a label for someone else), and a space is added where the part's text meets the sealed
// text before it (prev) at English letters/digits. Called with mu held.
func (o *output) place(p *part, segs []hark.Segment, prev string) []hark.Segment {
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
	for i := range segs {
		if segs[i].Text != "" {
			if join.NeedSpace(prev, segs[i].Text) {
				segs[i].Text = " " + segs[i].Text
			}
			break
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
	p.done = true
	o.live, o.next = o.next, nil
	if n := o.live; n != nil {
		n.gated = false
		if n.raw != nil {
			o.ingest(n, n.raw)
			n.raw = nil
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
	if n := o.next; n != nil && n.raw != nil && o.live != nil {
		prev, _, _ := o.live.tr.state()
		prevText := lastText(prev)
		if prevText == "" {
			prevText = o.sealedText()
		}
		for _, g := range o.place(n, n.raw, prevText) {
			o.tail = append(o.tail, api.Segment{Text: g.Text, Speaker: g.Speaker,
				AudioRange: api.AudioRange{StartSample: g.Audio.Start, EndSample: g.Audio.End}})
		}
		n.raw = nil
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
