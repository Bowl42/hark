package session

import (
	"sync"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
)

func policyOf(c api.Commit) freezePolicy {
	if c.Policy == api.CommitFreeze {
		return freezePolicy{Freeze: true, After: time.Duration(c.AfterMS) * time.Millisecond}
	}
	return freezePolicy{}
}

// output implements hark.Output. Snapshots are normalized on entry (commit policy, freeze conflict count),
// only the latest state is kept, readers are woken through notify, and the engine is never blocked.
//
// Freeze commits advance by the current time (tick) before every state read or write, so the result depends only
// on when snapshots arrive, not on how often readers read. Readers also read on a timer set by nextDeadline, but only
// to see watermark changes promptly.
type output struct {
	mu     sync.Mutex
	tr     *transcript
	closed bool // late snapshots and timers after the terminal state are all ignored
	notify chan struct{}
}

// newOutput creates an output. c must already be normalized.
func newOutput(c api.Commit) *output {
	return &output{tr: newTranscript(policyOf(c)), notify: make(chan struct{}, 1)}
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
	if !o.closed && o.tr.tick(now) {
		o.changed()
	}
}

// Update implements hark.Output.
func (o *output) Update(segs []hark.Segment) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	now := time.Now()
	o.tick(now) // advance due commits first: content whose freeze is already due must not be rewritten by this snapshot
	if o.tr.ingest(segs, now) {
		o.changed()
	}
}

// nextDeadline returns the next time a commit may advance.
func (o *output) nextDeadline() (time.Time, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return time.Time{}, false
	}
	return o.tr.nextDeadline()
}

// state returns the current normalized state (after advancing due commits). The returned slice is read-only; do not modify it.
func (o *output) state() ([]api.Segment, int, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.tick(time.Now())
	return o.tr.state()
}

// snapshot returns an independent copy of the current state.
func (o *output) snapshot() api.Snapshot {
	segs, stable, commit := o.state()
	return api.Snapshot{Segments: cloneSegments(segs), Stable: stable, Commit: commit}
}

// finish ends normally: everything becomes stable and committed, and late snapshots are ignored afterwards.
func (o *output) finish() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.tr.finish()
	o.closed = true
	o.changed()
}

// close ends abnormally: it first commits freezes that are already due (whether or not anyone is reading), after which
// the state no longer changes and late snapshots and timers are ignored. The remaining text is not committed.
func (o *output) close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.tick(time.Now())
	o.closed = true
}

// dropped returns the number of freeze conflicts.
func (o *output) dropped() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.tr.dropped
}
