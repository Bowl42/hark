package session

import "github.com/Bowl42/hark/api"

// sender remembers the state already delivered to the reader and diffs the output's new state into protocol updates.
// It is stateful and single-reader: when next returns true the baseline advances (rev increments by 1), and the caller
// must deliver that update.
//
// Committed segments never change, so once delivered the sender forgets them (base counts them) and only reads and
// diffs what follows, and tells the output to drop them too: neither the cost of an update nor the memory held grows
// with the length of the session.
type sender struct {
	rev            int
	base           int           // committed segments already delivered and no longer kept
	segs           []api.Segment // segments last delivered, from base on (independent copy)
	stable, commit int
}

// next returns an update when the state (text, segments, watermarks) differs from what was last delivered. The update
// shares no data with the session state (Diff allocates anew each time), so the caller may modify it freely.
// The revision number is assigned only when the delta to deliver is actually built, so no update is numbered and then dropped.
func (s *sender) next(o *output) (api.Update, bool) {
	segs, stable, commit := o.stateFrom(s.base)
	ops := api.Diff(s.segs, segs, s.commit-s.base)
	if ops == nil && stable == s.stable && commit == s.commit {
		o.release(s.base) // a seal may add segments the reader already has (frozen and delivered before it)
		return api.Update{}, false
	}
	if ops != nil && s.base > 0 {
		if ops[0].Retain > 0 {
			ops[0].Retain += s.base
		} else {
			ops = append([]api.Op{{Retain: s.base}}, ops...)
		}
	}
	s.rev++
	s.segs, s.stable, s.commit = cloneSegments(segs[commit-s.base:]), stable, commit
	s.base = commit
	o.release(commit) // the reader now has every committed segment: the output can drop the sealed ones
	return api.Update{Type: api.TypeUpdate, Rev: s.rev, Ops: ops, Stable: stable, Commit: commit}, true
}

// cloneSegments returns a copy and never returns nil (so JSON gets [] rather than null).
func cloneSegments(segs []api.Segment) []api.Segment {
	return append([]api.Segment{}, segs...)
}
