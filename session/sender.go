package session

import "github.com/Bowl42/hark/api"

// sender remembers the state already delivered to the reader and diffs the output's new state into protocol updates.
// It is stateful and single-reader: when next returns true the baseline advances (rev increments by 1), and the caller
// must deliver that update.
type sender struct {
	rev            int
	segs           []api.Segment // segments last delivered (independent copy)
	stable, commit int
}

// next returns an update when the state (text, segments, watermarks) differs from what was last delivered. The update
// shares no data with the session state (Diff allocates anew each time), so the caller may modify it freely.
// The revision number is assigned only when the delta to deliver is actually built, so no update is numbered and then dropped.
func (s *sender) next(o *output) (api.Update, bool) {
	segs, stable, commit := o.state()
	ops := api.Diff(s.segs, segs, s.commit)
	if ops == nil && stable == s.stable && commit == s.commit {
		return api.Update{}, false
	}
	s.rev++
	s.segs, s.stable, s.commit = cloneSegments(segs), stable, commit
	return api.Update{Type: api.TypeUpdate, Rev: s.rev, Ops: ops, Stable: stable, Commit: commit}, true
}

// cloneSegments returns a copy and never returns nil (so JSON gets [] rather than null).
func cloneSegments(segs []api.Segment) []api.Segment {
	return append([]api.Segment{}, segs...)
}
