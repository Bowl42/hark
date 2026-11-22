// Package draft merges client-side edits: the user can edit text while recognition is still running, and edited
// places are not overwritten by recognition results.
//
// It is editor behavior built on top of the protocol (api), not part of the protocol itself, and needs no cooperation
// from the server.
package draft

import (
	"fmt"
	"slices"
	"strings"

	"github.com/Bowl42/hark/api"
)

// Draft is a document the user can edit: a recognition mirror plus the ranges the user actually changed (Edit). For
// each incoming update, the recognizer's changes are applied to the mirror and the user's corrections are carried
// over; for the merge rules see "Editing while speaking" in docs/PROTOCOL.md. On conflict the user's text stays
// unchanged and the recognizer's version becomes a suggestion (Suggestion) that the user can accept or dismiss.
//
// Usage:
//
//	d := draft.New("")
//	...
//	old := doc.Segments                          // doc is an api.Doc
//	doc.Apply(u)
//	visible, err := d.Rebase(draft.Changes(old, u.Ops)) // visible: visible changes to the user document, usable for moving the cursor
//	d.Replace(a, b, text)                                // user edits text (user document coordinates)
//	d.Text()                                             // the full text shown to the user
//
// All positions are in code points.
type Draft struct {
	mirror []rune
	edits  []Edit // sorted by Start, non-overlapping
}

// Edit is one user correction: the mirror range [Start, End) is shown as Text in the user document.
type Edit struct {
	Start, End int    // mirror coordinates
	Text       string // the text of this range in the user document
	Conflict   bool   // the recognizer changed this range after the user did; see Draft.Suggestion for its version
}

// Change is one change to the mirror's full text: [Start, End) of the old full text is replaced with Text.
type Change struct {
	Start, End int
	Text       string
}

func New(mirror string) *Draft { return &Draft{mirror: []rune(mirror)} }

// Mirror returns the full text of the current recognition mirror.
func (d *Draft) Mirror() string { return string(d.mirror) }

// Edits returns the user's corrections, sorted by position.
func (d *Draft) Edits() []Edit { return slices.Clone(d.edits) }

// Suggestion returns the recognizer's current version under correction i.
func (d *Draft) Suggestion(i int) string { e := d.edits[i]; return string(d.mirror[e.Start:e.End]) }

// Text returns the full text the user sees.
func (d *Draft) Text() string {
	var out []rune
	m := 0
	for _, e := range d.edits {
		out = append(out, d.mirror[m:e.Start]...)
		out = append(out, []rune(e.Text)...)
		m = e.End
	}
	return string(append(out, d.mirror[m:]...))
}

// Accept accepts the recognizer's version by removing correction i.
func (d *Draft) Accept(i int) { d.edits = slices.Delete(d.edits, i, i+1) }

// Dismiss dismisses the recognizer's suggestion and keeps the user's text.
func (d *Draft) Dismiss(i int) { d.edits[i].Conflict = false }

// trim shrinks a correction to the part that actually differs from the mirror, removing the common prefix and suffix.
// On conflict the range is conservatively widened to the recognizer's whole change; when the recognizer's result later
// matches the user's text again, the range shrinks back accordingly.
// It returns false when the correction is identical to the mirror (the correction disappears).
func (d *Draft) trim(e *Edit) bool {
	t, m := []rune(e.Text), d.mirror[e.Start:e.End]
	p := 0
	for p < len(t) && p < len(m) && t[p] == m[p] {
		p++
	}
	q := 0
	for q < len(t)-p && q < len(m)-p && t[len(t)-1-q] == m[len(m)-1-q] {
		q++
	}
	if p == len(t) && p == len(m) {
		return false
	}
	e.Start, e.End, e.Text = e.Start+p, e.End-q, string(t[p:len(t)-q])
	return true
}

// span is a piece of the user document: either taken verbatim from the mirror or one user correction.
type span struct {
	m0, m1 int // mirror coordinates
	u0, u1 int // user document coordinates
	edit   int // index of the user correction, or -1 if taken verbatim from the mirror
}

func (d *Draft) spans() []span {
	var out []span
	m, u := 0, 0
	add := func(m1 int, n int, edit int) {
		out = append(out, span{m0: m, m1: m1, u0: u, u1: u + n, edit: edit})
		m, u = m1, u+n
	}
	for i, e := range d.edits {
		if e.Start > m {
			add(e.Start, e.Start-m, -1)
		}
		add(e.End, len([]rune(e.Text)), i)
	}
	if len(d.mirror) > m {
		add(len(d.mirror), len(d.mirror)-m, -1)
	}
	return out
}

// Replace is the user replacing [a, b) of the user document with text.
// Existing corrections it touches (including adjacent ones) are merged into one; if the result matches the mirror, the
// correction disappears.
func (d *Draft) Replace(a, b int, text string) error {
	doc := []rune(d.Text())
	if a < 0 || b < a || b > len(doc) {
		return fmt.Errorf("draft: range [%d, %d) out of %d", a, b, len(doc))
	}
	if a == b && text == "" {
		return nil // no change: must not clear the conflict flag of an adjacent correction as a side effect
	}
	m0, m1 := -1, -1
	u0, u1 := a, b
	var drop []int
	for _, s := range d.spans() {
		var lo, hi int
		if s.edit >= 0 {
			if s.u1 < a || s.u0 > b { // correction: adjacent counts as touching
				continue
			}
			lo, hi = s.m0, s.m1
			u0, u1 = min(u0, s.u0), max(u1, s.u1)
			drop = append(drop, s.edit)
		} else {
			if s.u1 <= a || s.u0 >= b { // verbatim mirror text: must actually cover characters
				continue
			}
			lo, hi = s.m0+max(0, a-s.u0), s.m0+min(s.u1-s.u0, b-s.u0)
		}
		if m0 < 0 {
			m0 = lo
		}
		m1 = hi
	}
	if m0 < 0 { // just an insertion within verbatim mirror text
		m0 = d.mirrorPos(a)
		m1 = m0
	}
	e := Edit{Start: m0, End: m1, Text: string(doc[u0:a]) + text + string(doc[b:u1])}
	for i := len(drop) - 1; i >= 0; i-- {
		d.edits = slices.Delete(d.edits, drop[i], drop[i]+1)
	}
	if d.trim(&e) {
		d.insert(e)
	}
	return nil
}

// insert inserts in (Start, End) order: at the same position, empty-range corrections (pure insertions) come first.
func (d *Draft) insert(e Edit) {
	at, _ := slices.BinarySearchFunc(d.edits, e, func(x, y Edit) int {
		if x.Start != y.Start {
			return x.Start - y.Start
		}
		return x.End - y.End
	})
	d.edits = slices.Insert(d.edits, at, e)
}

// mirrorPos returns the mirror position for user document position a (which is not on or next to any correction).
func (d *Draft) mirrorPos(a int) int {
	for _, s := range d.spans() {
		if s.edit < 0 && a >= s.u0 && a <= s.u1 {
			return s.m0 + a - s.u0
		}
	}
	return len(d.mirror)
}

// Rebase applies the recognizer's changes to the mirror (old mirror coordinates, sorted by position, non-overlapping)
// and carries the user's corrections along.
//
// It returns the changes that actually happened to the user document (user document coordinates); applying them
// **in the returned order** to the old Text() yields the new Text(). A UI can use them for partial refreshes and to
// move the cursor; at conflicts the user's text is unchanged and does not appear in them.
func (d *Draft) Rebase(changes []Change) ([]Change, error) {
	end := len(d.mirror)
	for i := len(changes) - 1; i >= 0; i-- {
		c := changes[i]
		if c.Start < 0 || c.End < c.Start || c.End > end {
			return nil, fmt.Errorf("draft: change [%d, %d) out of order or range", c.Start, c.End)
		}
		end = c.Start
	}
	var visible []Change
	for i := len(changes) - 1; i >= 0; i-- { // back to front, so earlier coordinates are unaffected
		if v, ok := d.apply(changes[i]); ok {
			visible = append(visible, v)
		}
	}
	return visible, nil
}

// userAt returns the user document position of mirror position m: corrections ending at m (including pure
// insertions at m) count as before it.
func (d *Draft) userAt(m int) int {
	u := m
	for _, e := range d.edits {
		if e.End > m {
			break
		}
		u += len([]rune(e.Text)) - (e.End - e.Start)
	}
	return u
}

func (d *Draft) apply(c Change) (visible Change, changed bool) {
	absorbed := 0
	if c.Start == c.End {
		// The recognizer inserted, at the same position as a user insertion, content starting with the same text: the
		// user's inserted text has now been recognized, so don't duplicate it. Only look at the correction immediately
		// before the insertion point (the recognizer's insertion always lands after all corrections ending there),
		// otherwise the order would be scrambled.
		last := -1
		for i, e := range d.edits {
			if e.End == c.Start {
				last = i
			}
		}
		if last >= 0 {
			if e := d.edits[last]; e.Start == c.Start && strings.HasPrefix(c.Text, e.Text) {
				d.edits = slices.Delete(d.edits, last, last+1)
				absorbed = len([]rune(e.Text))
			}
		}
	}
	ins := []rune(c.Text)
	delta := len(ins) - (c.End - c.Start)
	// Corrections overlapping the change: those with e.End <= c.Start are before it and unaffected (including when the
	// change is inserted at a correction's end); those with e.Start >= c.End are after it and only shift.
	var hit []int
	for i, e := range d.edits {
		if e.End > c.Start && e.Start < c.End || e.Start < c.Start && c.Start < e.End {
			hit = append(hit, i)
		}
	}
	if len(hit) == 0 { // no conflict: the corresponding range in the user document changes along
		u := d.userAt(c.Start)
		visible = Change{Start: u, End: u + c.End - c.Start, Text: c.Text}
		if absorbed > 0 { // the user's inserted text stays as is; only the rest of the recognizer's text appears after it
			visible = Change{Start: u + absorbed, End: u + absorbed, Text: string(ins[absorbed:])}
		}
		changed = visible.End > visible.Start || visible.Text != ""
	}
	var merged *Edit
	at := 0 // the merged correction goes back in place: corrections before the change precede it, those after follow it
	if len(hit) > 0 {
		at = hit[0]
		// Conflict: the whole stretch the user sees stays unchanged, and the range widens to cover the entire change.
		lo, hi := min(c.Start, d.edits[hit[0]].Start), max(c.End, d.edits[hit[len(hit)-1]].End)
		var text []rune
		m := lo
		for _, i := range hit {
			e := d.edits[i]
			text = append(append(text, d.mirror[m:e.Start]...), []rune(e.Text)...)
			m = e.End
		}
		text = append(text, d.mirror[m:hi]...)
		merged = &Edit{Start: lo, End: hi + delta, Text: string(text), Conflict: true}
		d.edits = slices.Delete(d.edits, hit[0], hit[len(hit)-1]+1)
	}
	for i := range d.edits {
		if d.edits[i].Start >= c.End && !(d.edits[i].End <= c.Start) {
			d.edits[i].Start += delta
			d.edits[i].End += delta
		}
	}
	d.mirror = slices.Concat(d.mirror[:c.Start], ins, d.mirror[c.End:])
	if merged != nil && d.trim(merged) { // the recognizer now matches the user: the correction just disappears
		// Can't re-sort by position: after shrinking it may land on the same point as an adjacent pure insertion, and
		// only this code knows their order.
		d.edits = slices.Insert(d.edits, at, *merged)
	}
	return visible, changed
}

// Changes converts an update's ops into changes to the mirror's full text (old full-text coordinates, sorted by
// position, adjacent ones merged).
func Changes(old []api.Segment, ops []api.Op) []Change {
	var out []Change
	off, i := 0, 0
	add := func(n int, text string) {
		if l := len(out) - 1; l >= 0 && out[l].End == off {
			out[l].End += n
			out[l].Text += text
		} else if n > 0 || text != "" {
			out = append(out, Change{Start: off, End: off + n, Text: text})
		}
		off += n
	}
	for _, op := range ops {
		switch {
		case op.Retain > 0:
			for _, s := range old[i : i+op.Retain] {
				off += len([]rune(s.Text))
			}
			i += op.Retain
		case op.Delete > 0:
			var n int
			for _, s := range old[i : i+op.Delete] {
				n += len([]rune(s.Text))
			}
			add(n, "")
			i += op.Delete
		case len(op.Insert) > 0:
			var text string
			for _, s := range op.Insert {
				text += s.Text
			}
			add(0, text)
		case op.Update != nil:
			n := len([]rune(old[i].Text))
			if op.Update.Text == nil {
				off += n
			} else {
				start := off
				for _, t := range op.Update.Text {
					switch {
					case t.Retain > 0:
						off += t.Retain
					case t.Delete > 0:
						add(t.Delete, "")
					default:
						add(0, t.Insert)
					}
				}
				off = start + n
			}
			i++
		}
	}
	return out
}
