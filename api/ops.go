package api

import "fmt"

// The ops of an update: a two-level OT-style op sequence (segment-level Op, in-segment TextOp). See docs/PROTOCOL.md for semantics and validation rules.

// Op is a segment-level operation. Exactly one field is set.
type Op struct {
	Retain int        `json:"retain,omitempty"`
	Delete int        `json:"delete,omitempty"`
	Insert []Segment  `json:"insert,omitempty"`
	Update *SegUpdate `json:"update,omitempty"`
}

// SegUpdate modifies one segment: the text is edited by Text, and the metadata is replaced wholesale (an omitted field means none).
type SegUpdate struct {
	Text       []TextOp   `json:"text,omitempty"` // omitted: text unchanged
	Speaker    string     `json:"speaker,omitempty"`
	AudioRange AudioRange `json:"audio_range,omitzero"`
}

// TextOp is an in-segment text operation. Exactly one field is set; lengths are in code points.
type TextOp struct {
	Retain int    `json:"retain,omitempty"`
	Delete int    `json:"delete,omitempty"`
	Insert string `json:"insert,omitempty"`
}

// count validates a retain / delete length: > 0 and not past avail (written to avoid integer overflow).
func count(n, avail int) error {
	if n <= 0 || n > avail {
		return fmt.Errorf("length %d out of range (0, %d]", n, avail)
	}
	return nil
}

func applyOps(old []Segment, ops []Op, commit int) ([]Segment, error) {
	out := make([]Segment, 0, len(old))
	i := 0
	touch := func() error {
		if i < commit {
			return fmt.Errorf("op at segment %d touches committed prefix %d", i, commit)
		}
		return nil
	}
	for k, op := range ops {
		set := 0
		for _, b := range []bool{op.Retain != 0, op.Delete != 0, op.Insert != nil, op.Update != nil} {
			if b {
				set++
			}
		}
		if set != 1 {
			return nil, fmt.Errorf("op %d: must set exactly one of retain/delete/insert/update", k)
		}
		var err error
		switch {
		case op.Retain != 0:
			if err = count(op.Retain, len(old)-i); err == nil {
				out = append(out, old[i:i+op.Retain]...)
				i += op.Retain
			}
		case op.Delete != 0:
			if err = touch(); err == nil {
				if err = count(op.Delete, len(old)-i); err == nil {
					i += op.Delete
				}
			}
		case op.Insert != nil:
			if len(op.Insert) == 0 {
				err = fmt.Errorf("empty insert")
			} else if err = touch(); err == nil {
				out = append(out, op.Insert...)
			}
		default:
			if i >= len(old) {
				err = fmt.Errorf("update past last segment %d", len(old))
			} else if err = touch(); err == nil {
				u := op.Update
				seg := Segment{Text: old[i].Text, Speaker: u.Speaker, AudioRange: u.AudioRange}
				if u.Text != nil {
					seg.Text, err = applyText(seg.Text, u.Text)
				}
				out = append(out, seg)
				i++
			}
		}
		if err != nil {
			return nil, fmt.Errorf("op %d: %w", k, err)
		}
	}
	return append(out, old[i:]...), nil
}

func applyText(s string, ops []TextOp) (string, error) {
	if len(ops) == 0 {
		return "", fmt.Errorf("empty text ops")
	}
	r := []rune(s)
	out := make([]rune, 0, len(r))
	i := 0
	for k, op := range ops {
		set := 0
		for _, b := range []bool{op.Retain != 0, op.Delete != 0, op.Insert != ""} {
			if b {
				set++
			}
		}
		if set != 1 {
			return "", fmt.Errorf("text op %d: must set exactly one of retain/delete/insert", k)
		}
		switch {
		case op.Insert != "":
			out = append(out, []rune(op.Insert)...)
			continue
		case op.Retain != 0:
			if err := count(op.Retain, len(r)-i); err != nil {
				return "", fmt.Errorf("text op %d: %w", k, err)
			}
			out = append(out, r[i:i+op.Retain]...)
			i += op.Retain
		default:
			if err := count(op.Delete, len(r)-i); err != nil {
				return "", fmt.Errorf("text op %d: %w", k, err)
			}
			i += op.Delete
		}
	}
	return string(append(out, r[i:]...)), nil
}

// Diff computes the ops that turn old into next; it returns nil when they are equal.
// from is the number of leading segments known to be equal (usually commit), used to skip comparison.
//
// Segments are matched heuristically: equal leading and trailing segments are retained as is, the middle
// ones are paired in order into updates, and extra old segments are deleted and extra new ones inserted.
// See diffText for the text within a segment.
// The result is always correct (applying it yields next) and canonical: no adjacent ops of the same kind,
// no trailing retain. But it is not guaranteed to recover the "real editing intent", nor to be minimal.
func Diff(old, next []Segment, from int) []Op {
	p := from
	for p < len(old) && p < len(next) && old[p] == next[p] {
		p++
	}
	s := 0
	for s < len(old)-p && s < len(next)-p && old[len(old)-1-s] == next[len(next)-1-s] {
		s++
	}
	om, nm := old[p:len(old)-s], next[p:len(next)-s]
	if len(om) == 0 && len(nm) == 0 {
		return nil
	}
	var ops []Op
	retain := func(n int) {
		if l := len(ops) - 1; l >= 0 && ops[l].Retain > 0 {
			ops[l].Retain += n
		} else {
			ops = append(ops, Op{Retain: n})
		}
	}
	if p > 0 {
		retain(p)
	}
	k := min(len(om), len(nm))
	for j := 0; j < k; j++ {
		if om[j] == nm[j] {
			retain(1)
			continue
		}
		u := &SegUpdate{Speaker: nm[j].Speaker, AudioRange: nm[j].AudioRange}
		if om[j].Text != nm[j].Text {
			u.Text = diffText(om[j].Text, nm[j].Text)
		}
		ops = append(ops, Op{Update: u})
	}
	if len(om) > k {
		ops = append(ops, Op{Delete: len(om) - k})
	}
	if len(nm) > k {
		ops = append(ops, Op{Insert: append([]Segment(nil), nm[k:]...)})
	}
	return ops
}

// maxDiffCells caps the size of the per-character in-segment comparison (product of the two lengths); beyond it, the diff degrades to one whole replacement.
const maxDiffCells = 1 << 18

// diffText diffs the text within a segment (a != b): after stripping the common prefix and suffix, it computes
// the longest common subsequence of the middle by code point, then merges into the changes any equal piece that
// sits between two changes and is no longer than either of them (so that single-character coincidences such as
// "的" or "了" in Chinese don't chop a change into fragments).
// Each change is canonicalized as delete then insert.
func diffText(a, b string) []TextOp {
	ra, rb := []rune(a), []rune(b)
	p := 0
	for p < len(ra) && p < len(rb) && ra[p] == rb[p] {
		p++
	}
	s := 0
	for s < len(ra)-p && s < len(rb)-p && ra[len(ra)-1-s] == rb[len(rb)-1-s] {
		s++
	}
	ma, mb := ra[p:len(ra)-s], rb[p:len(rb)-s]

	// Pieces of the middle part: equal pieces alternate with changes, and both ends are changes.
	type piece struct {
		eq       bool
		del, ins []rune
		same     []rune
	}
	var pieces []piece
	if len(ma) == 0 || len(mb) == 0 || len(ma)*len(mb) > maxDiffCells {
		pieces = []piece{{del: ma, ins: mb}}
	} else {
		pieces = lcsPieces(ma, mb, func(eq bool, x, y []rune) piece {
			if eq {
				return piece{eq: true, same: x}
			}
			return piece{del: x, ins: y}
		})
		// Merge short equal pieces until nothing changes.
		for changed := true; changed; {
			changed = false
			for i := 1; i+1 < len(pieces); i++ {
				l, m, r := pieces[i-1], pieces[i], pieces[i+1]
				if !m.eq || l.eq || r.eq {
					continue
				}
				n := len(m.same)
				if n <= max(len(l.del), len(l.ins)) && n <= max(len(r.del), len(r.ins)) {
					merged := piece{
						del: concat(l.del, m.same, r.del),
						ins: concat(l.ins, m.same, r.ins),
					}
					pieces = append(pieces[:i-1], append([]piece{merged}, pieces[i+2:]...)...)
					changed = true
					break
				}
			}
		}
	}

	var ops []TextOp
	if p > 0 {
		ops = append(ops, TextOp{Retain: p})
	}
	for _, x := range pieces {
		if x.eq {
			ops = append(ops, TextOp{Retain: len(x.same)})
			continue
		}
		if len(x.del) > 0 {
			ops = append(ops, TextOp{Delete: len(x.del)})
		}
		if len(x.ins) > 0 {
			ops = append(ops, TextOp{Insert: string(x.ins)})
		}
	}
	return ops
}

func concat(xs ...[]rune) []rune {
	var out []rune
	for _, x := range xs {
		out = append(out, x...)
	}
	return out
}

// lcsPieces splits a and b by longest common subsequence into alternating equal pieces and change pieces.
func lcsPieces[T any](a, b []rune, mk func(eq bool, x, y []rune) T) []T {
	n, m := len(a), len(b)
	// l[i*(m+1)+j] = LCS length of a[i:] and b[j:]
	l := make([]int32, (n+1)*(m+1))
	at := func(i, j int) int32 { return l[i*(m+1)+j] }
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				l[i*(m+1)+j] = at(i+1, j+1) + 1
			} else {
				l[i*(m+1)+j] = max(at(i+1, j), at(i, j+1))
			}
		}
	}
	var out []T
	var same, del, ins []rune
	flush := func(eq bool) {
		if eq && len(same) > 0 {
			out = append(out, mk(true, same, nil))
			same = nil
		}
		if !eq && (len(del) > 0 || len(ins) > 0) {
			out = append(out, mk(false, del, ins))
			del, ins = nil, nil
		}
	}
	i, j := 0, 0
	for i < n || j < m {
		switch {
		case i < n && j < m && a[i] == b[j]:
			flush(false)
			same = append(same, a[i])
			i, j = i+1, j+1
		case j < m && (i == n || at(i, j+1) >= at(i+1, j)):
			flush(true)
			ins = append(ins, b[j])
			j++
		default:
			flush(true)
			del = append(del, a[i])
			i++
		}
	}
	flush(true)
	flush(false)
	return out
}
