package api

import (
	"fmt"
	"strings"
)

// Doc is the result state built by accumulating updates per the protocol, shared by client and server.
//
// For long sessions, Trim hands over the committed segments (they never change again) so the Doc holds only what
// may still change: Base counts the segments trimmed off the front, and Segments[i] is segment Base+i. Stable and
// Commit are always absolute, as on the wire.
type Doc struct {
	Rev      int
	Base     int
	Segments []Segment
	Stable   int
	Commit   int
}

// Trim removes the committed segments the Doc still holds and returns them; they belong to the caller. Afterwards
// Base == Commit. It returns nil when there is nothing to trim.
func (d *Doc) Trim() []Segment {
	n := d.Commit - d.Base
	if n == 0 {
		return nil
	}
	out := d.Segments[:n:n]
	d.Segments = append([]Segment{}, d.Segments[n:]...) // fresh storage, so the trimmed segments aren't kept reachable
	d.Base = d.Commit
	return out
}

// Text returns the text held (all of it unless Trim was used): the segments concatenated as is.
func (d *Doc) Text() string {
	var b strings.Builder
	for _, s := range d.Segments {
		b.WriteString(s.Text)
	}
	return b.String()
}

// Apply applies one update and validates the protocol constraints (see ops.go for the ops rules). On error, Doc is unchanged.
func (d *Doc) Apply(u Update) error {
	if u.Rev != d.Rev+1 {
		return fmt.Errorf("rev %d after %d", u.Rev, d.Rev)
	}
	segs := d.Segments
	if len(u.Ops) > 0 {
		var err error
		if segs, err = applyOps(d.Segments, u.Ops, d.Commit, d.Base); err != nil {
			return err
		}
	}
	if u.Commit < d.Commit || u.Commit > u.Stable || u.Stable > d.Base+len(segs) {
		return fmt.Errorf("bad watermarks commit %d stable %d (prev commit %d, %d segments)", u.Commit, u.Stable, d.Commit, d.Base+len(segs))
	}
	d.Rev, d.Segments, d.Stable, d.Commit = u.Rev, segs, u.Stable, u.Commit
	return nil
}
