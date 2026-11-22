package api

import (
	"fmt"
	"strings"
)

// Doc is the result state built by accumulating updates per the protocol, shared by client and server.
type Doc struct {
	Rev      int
	Segments []Segment
	Stable   int
	Commit   int
}

// Text returns the full text: the segments concatenated as is.
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
		if segs, err = applyOps(d.Segments, u.Ops, d.Commit); err != nil {
			return err
		}
	}
	if u.Commit < d.Commit || u.Commit > u.Stable || u.Stable > len(segs) {
		return fmt.Errorf("bad watermarks commit %d stable %d (prev commit %d, %d segments)", u.Commit, u.Stable, d.Commit, len(segs))
	}
	d.Rev, d.Segments, d.Stable, d.Commit = u.Rev, segs, u.Stable, u.Commit
	return nil
}
