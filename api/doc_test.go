package api

import (
	"encoding/json"
	"math/rand/v2"
	"reflect"
	"strings"
	"testing"
)

func segs(texts ...string) []Segment {
	out := make([]Segment, len(texts))
	for i, t := range texts {
		out[i] = Segment{Text: t}
	}
	return out
}

// tx is shorthand for in-segment ops: a positive int is retain, a negative int is delete, a string is insert.
func tx(parts ...any) Op {
	var ops []TextOp
	for _, p := range parts {
		switch v := p.(type) {
		case int:
			if v > 0 {
				ops = append(ops, TextOp{Retain: v})
			} else {
				ops = append(ops, TextOp{Delete: -v})
			}
		case string:
			ops = append(ops, TextOp{Insert: v})
		}
	}
	return Op{Update: &SegUpdate{Text: ops}}
}

func TestDiffShapes(t *testing.T) {
	r160 := AudioRange{0, 160}
	cases := []struct {
		name      string
		old, next []Segment
		want      []Op
	}{
		{"equal", segs("今天"), segs("今天"), nil},
		{"append chars", segs("今天"), segs("今天天汽"), []Op{tx(2, "天汽")}},
		{"correct tail", segs("今天天汽"), segs("今天天气很好。"), []Op{tx(3, -1, "气很好。")}},
		{"insert before period", segs("今天。"), segs("今天下午。"), []Op{tx(2, "下午")}},
		{"middle word", segs("今天下午三点开会"), segs("今天下午3点开会"), []Op{tx(4, -1, "3")}},
		{"first char and tail", segs("大家好，我是小李。主要"), segs("家好，我是小李。主要讨论"),
			[]Op{tx(-1, 10, "讨论")}},
		{"two separate fixes", segs("今天下午三点在二号会议室开会"), segs("今天下午3点在2号会议室开会"),
			[]Op{tx(4, -1, "3", 2, -1, "2")}},
		{"coincidence absorbed", segs("abcdXef"), segs("abcdYZXWVef"), []Op{tx(4, -1, "YZXWV")}},
		{"long match kept", segs("abcdXXXef"), segs("abcdYXXXWef"), []Op{tx(4, "Y", 3, "W")}},
		{"single char not split", segs("今天去了公园"), segs("明天的公园"), []Op{tx(-4, "明天的")}},
		{"new segment", segs("今天天气。"), segs("今天天气。", "我们"), []Op{{Retain: 1}, {Insert: segs("我们")}}},
		{"rewrite earlier, tail grows", segs("a。", "我们"), segs("A。", "我们去"), []Op{tx(-1, "A"), tx(2, "去")}},
		{"rewrite earlier, tail same", segs("a。", "b", "c"), segs("A。", "b", "c"), []Op{tx(-1, "A")}},
		{"merge", segs("今天。", "天气好"), segs("今天天气好。"), []Op{tx(2, "天气好"), {Delete: 1}}},
		{"split", segs("今天天气好。"), segs("今天。", "天气好。"), []Op{tx(2, -3), {Insert: segs("天气好。")}}},
		{"drop segments", segs("a", "b", "c"), segs("a"), []Op{{Retain: 1}, {Delete: 2}}},
		{"drop middle", segs("a", "b", "c"), segs("a", "c"), []Op{{Retain: 1}, {Delete: 1}}},
		{"clear", segs("a"), segs(), []Op{{Delete: 1}}},
		{"from empty", segs(), segs("a", "b"), []Op{{Insert: segs("a", "b")}}},
		{"emoji", segs("好👍🏻了"), segs("好👍🏽了"), []Op{tx(2, -1, "🏽")}},
		{"repeat chars", segs("哈哈"), segs("哈哈哈"), []Op{tx(2, "哈")}},
		{"meta only", segs("a"), []Segment{{Text: "a", AudioRange: r160}}, []Op{{Update: &SegUpdate{AudioRange: r160}}}},
		{"meta cleared", []Segment{{Text: "a", Speaker: "S1", AudioRange: r160}}, segs("ab"), []Op{tx(1, "b")}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Diff(c.old, c.next, 0)
			if !reflect.DeepEqual(got, c.want) {
				b1, _ := json.Marshal(got)
				b2, _ := json.Marshal(c.want)
				t.Fatalf("Diff = %s, want %s", b1, b2)
			}
			d := Doc{Segments: c.old}
			if err := d.Apply(Update{Rev: 1, Ops: got}); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(norm(d.Segments), norm(c.next)) {
				t.Fatalf("applied %+v, want %+v", d.Segments, c.next)
			}
		})
	}
}

// Wire format.
func TestUpdateJSON(t *testing.T) {
	u := Update{Type: TypeUpdate, Rev: 2, Ops: Diff(segs("今天。", "x"), segs("今天下午。", "x", "我们"), 0), Stable: 1}
	b, _ := json.Marshal(u)
	want := `{"type":"update","track":0,"rev":2,"ops":[{"update":{"text":[{"retain":2},{"insert":"下午"}]}},{"retain":1},{"insert":[{"text":"我们"}]}],"stable":1,"commit":0}`
	if string(b) != want {
		t.Fatalf("got  %s\nwant %s", b, want)
	}
	var back Update
	if err := json.Unmarshal(b, &back); err != nil || !reflect.DeepEqual(back, u) {
		t.Fatalf("round trip: %v %+v", err, back)
	}
}

func norm(s []Segment) []Segment {
	if len(s) == 0 {
		return nil
	}
	return s
}

func TestApplyRejects(t *testing.T) {
	base := func() Doc { return Doc{Rev: 3, Segments: segs("ab", "cd"), Stable: 2, Commit: 1} }
	w := func(ops ...Op) Update { return Update{Rev: 4, Ops: ops, Stable: 2, Commit: 1} }
	const huge = int(^uint(0) >> 1)
	bad := map[string]Update{
		"rev gap":            {Rev: 5, Stable: 2, Commit: 1},
		"update committed":   w(tx(1, "x")),
		"delete committed":   w(Op{Delete: 1}, Op{Insert: segs("ab")}),
		"insert in commit":   w(Op{Insert: segs("x")}),
		"text beyond":        w(Op{Retain: 1}, tx(1, -2, "x")),
		"text retain beyond": w(Op{Retain: 1}, tx(3)),
		"text negative":      w(Op{Retain: 1}, Op{Update: &SegUpdate{Text: []TextOp{{Retain: -1}}}}),
		"text empty op":      w(Op{Retain: 1}, Op{Update: &SegUpdate{Text: []TextOp{{}}}}),
		"text two fields":    w(Op{Retain: 1}, Op{Update: &SegUpdate{Text: []TextOp{{Retain: 1, Insert: "x"}}}}),
		"text empty list":    w(Op{Retain: 1}, Op{Update: &SegUpdate{Text: []TextOp{}}}),
		"text overflow":      w(Op{Retain: 1}, tx(1, -huge)),
		"retain beyond":      w(Op{Retain: 3}),
		"retain overflow":    w(Op{Retain: 1}, Op{Retain: huge}),
		"delete beyond":      w(Op{Retain: 1}, Op{Delete: 2}),
		"delete overflow":    w(Op{Retain: 1}, Op{Delete: huge}),
		"update past end":    w(Op{Retain: 2}, tx("x")),
		"negative retain":    w(Op{Retain: -1}),
		"empty op":           w(Op{}),
		"two fields":         w(Op{Retain: 1, Delete: 1}),
		"empty insert":       w(Op{Retain: 1}, Op{Insert: []Segment{}}),
		"commit backwards":   {Rev: 4, Stable: 2, Commit: 0},
		"commit > stable":    {Rev: 4, Stable: 1, Commit: 2},
		"stable > len":       {Rev: 4, Stable: 3, Commit: 1},
		"stable > len after": w(Op{Retain: 1}, Op{Delete: 1}),
	}
	for name, u := range bad {
		d := base()
		if err := d.Apply(u); err == nil {
			t.Errorf("%s: want error", name)
		}
		if !reflect.DeepEqual(d, base()) {
			t.Errorf("%s: doc mutated on error", name)
		}
	}
	// The insertion point may equal commit; remaining segments are implicitly retained.
	d := base()
	if err := d.Apply(w(Op{Retain: 1}, Op{Insert: segs("x")})); err != nil || d.Text() != "abxcd" {
		t.Fatalf("insert at commit: %v %q", err, d.Text())
	}
}

// Random snapshot sequences: Diff + Apply must reconstruct them exactly.
func TestDiffApplyRandom(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	alphabet := []string{"今", "天", "气", "。", "a", "b", " ", "👍", "🏽", "é", "é"}
	word := func() string {
		var b strings.Builder
		for n := r.IntN(6); n >= 0; n-- {
			b.WriteString(alphabet[r.IntN(len(alphabet))])
		}
		return b.String()
	}
	for trial := 0; trial < 300; trial++ {
		var d Doc
		var cur []Segment
		for step := 0; step < 40; step++ {
			next := append([]Segment(nil), cur...)
			switch r.IntN(7) {
			case 0: // last segment grows
				if len(next) > 0 {
					next[len(next)-1].Text += word()
				} else {
					next = append(next, Segment{Text: word()})
				}
			case 1: // new segment
				next = append(next, Segment{Text: word()})
			case 2: // rewrite a random segment
				if len(next) > 0 {
					next[r.IntN(len(next))] = Segment{Text: word()}
				}
			case 3: // merge the last two segments
				if len(next) > 1 {
					n := len(next)
					next = append(next[:n-2], Segment{Text: next[n-2].Text + next[n-1].Text})
				}
			case 5: // change metadata only
				if len(next) > 0 {
					i := r.IntN(len(next))
					next[i].AudioRange = AudioRange{int64(r.IntN(100)), int64(100 + r.IntN(100))}
				}
			case 6: // delete a middle segment
				if len(next) > 2 {
					i := 1 + r.IntN(len(next)-2)
					next = append(next[:i:i], next[i+1:]...)
				}
			case 4: // truncate
				if len(next) > 0 {
					next = next[:r.IntN(len(next))]
				}
			}
			ops := Diff(cur, next, 0)
			checkCanonical(t, ops)
			if err := d.Apply(Update{Rev: d.Rev + 1, Ops: ops}); err != nil {
				t.Fatalf("trial %d step %d: %v", trial, step, err)
			}
			if !reflect.DeepEqual(norm(d.Segments), norm(next)) {
				t.Fatalf("trial %d step %d: got %+v want %+v", trial, step, d.Segments, next)
			}
			cur = next
		}
	}
}

// Track options: nil is omitted (inherit the default); an empty map must serialize as {} (explicitly empty).
func TestTrackOptionsExplicitEmpty(t *testing.T) {
	b, _ := json.Marshal(TrackSpec{Options: map[string]any{}})
	if !strings.Contains(string(b), `"options":{}`) {
		t.Fatalf("explicit empty options lost: %s", b)
	}
	b, _ = json.Marshal(TrackSpec{})
	if strings.Contains(string(b), "options") {
		t.Fatalf("nil options should be omitted: %s", b)
	}
}

// checkCanonical checks canonical form: no zero values, no adjacent ops of the same kind, no trailing retain; each in-segment change is delete then insert.
func checkCanonical(t *testing.T, ops []Op) {
	t.Helper()
	kind := func(op Op) int {
		switch {
		case op.Retain > 0:
			return 0
		case op.Delete > 0:
			return 1
		case len(op.Insert) > 0:
			return 2
		case op.Update != nil:
			return 3
		}
		t.Fatalf("empty op in %+v", ops)
		return -1
	}
	for i, op := range ops {
		k := kind(op)
		if i > 0 && k != 3 && kind(ops[i-1]) == k {
			t.Fatalf("adjacent ops of kind %d: %+v", k, ops)
		}
		if i == len(ops)-1 && k == 0 {
			t.Fatalf("trailing retain: %+v", ops)
		}
		if u := op.Update; u != nil && u.Text != nil {
			tk := func(x TextOp) int {
				switch {
				case x.Retain > 0:
					return 0
				case x.Delete > 0:
					return 1
				case x.Insert != "":
					return 2
				}
				t.Fatalf("empty text op in %+v", u.Text)
				return -1
			}
			for j, x := range u.Text {
				if j > 0 && (tk(u.Text[j-1]) == tk(x) || tk(u.Text[j-1]) == 2 && tk(x) == 1) {
					t.Fatalf("non-canonical text ops: %+v", u.Text)
				}
			}
			if tk(u.Text[len(u.Text)-1]) == 0 {
				t.Fatalf("trailing text retain: %+v", u.Text)
			}
		}
	}
}

// A Doc trimmed at random points applies the same updates as a full one: what it holds is always the full
// document from Base on, and the trimmed pieces add up to the prefix before it.
func TestTrimRandom(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	for trial := 0; trial < 200; trial++ {
		var full, trimmed Doc
		var gone []Segment
		var cur []Segment
		commit := 0
		for step := 0; step < 60; step++ {
			next := append([]Segment(nil), cur[:commit]...)
			for n := r.IntN(4); n > 0; n-- {
				next = append(next, Segment{Text: string(rune('a' + r.IntN(26)))})
			}
			if len(cur) > commit && r.IntN(2) == 0 { // keep some of the old tail so Diff emits retains and updates
				next = append(next[:commit], cur[commit:]...)
				next[len(next)-1].Text += "x"
			}
			newCommit := commit + r.IntN(len(next)-commit+1)
			u := Update{Rev: full.Rev + 1, Ops: Diff(cur, next, commit), Stable: newCommit, Commit: newCommit}
			if ops := u.Ops; len(ops) > 0 && ops[0].Retain > 1 && r.IntN(2) == 0 { // valid but not canonical: split the retain
				u.Ops = append([]Op{{Retain: 1}, {Retain: ops[0].Retain - 1}}, ops[1:]...)
			}
			if err := full.Apply(u); err != nil {
				t.Fatalf("trial %d step %d full: %v", trial, step, err)
			}
			if err := trimmed.Apply(u); err != nil {
				t.Fatalf("trial %d step %d trimmed (base %d): %v %+v", trial, step, trimmed.Base, err, u.Ops)
			}
			if r.IntN(3) == 0 {
				gone = append(gone, trimmed.Trim()...)
			}
			if trimmed.Base != len(gone) || !reflect.DeepEqual(norm(gone), norm(full.Segments[:trimmed.Base])) ||
				!reflect.DeepEqual(norm(trimmed.Segments), norm(full.Segments[trimmed.Base:])) {
				t.Fatalf("trial %d step %d: trimmed %+v (base %d) vs full %+v", trial, step, trimmed.Segments, trimmed.Base, full.Segments)
			}
			cur, commit = next, newCommit
		}
	}
}

// Ops can't reach into the trimmed prefix (it is committed), and a failed Apply leaves the Doc as it was.
func TestTrimmedRejects(t *testing.T) {
	d := Doc{Rev: 1, Segments: segs("a", "b", "c"), Stable: 3, Commit: 2}
	d.Trim()
	if d.Base != 2 || d.Text() != "c" {
		t.Fatalf("trim: base %d text %q", d.Base, d.Text())
	}
	for name, ops := range map[string][]Op{
		"delete trimmed": {{Retain: 1}, {Delete: 1}},
		"insert before":  {{Insert: segs("x")}},
		"mixed op":       {{Retain: 2, Delete: 1}},
		"retain past":    {{Retain: 4}},
	} {
		if err := d.Apply(Update{Rev: 2, Ops: ops, Stable: 3, Commit: 2}); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if d.Rev != 1 || d.Base != 2 || d.Text() != "c" {
		t.Fatalf("doc changed on error: %+v", d)
	}
	if err := d.Apply(Update{Rev: 2, Ops: []Op{{Retain: 1}, {Retain: 1}, {Update: &SegUpdate{Text: []TextOp{{Retain: 1}, {Insert: "d"}}}}}, Stable: 3, Commit: 3}); err != nil || d.Text() != "cd" {
		t.Fatalf("split retains: %v %q", err, d.Text())
	}
	if got := d.Trim(); len(got) != 1 || got[0].Text != "cd" || d.Trim() != nil || len(d.Segments) != 0 {
		t.Fatalf("trim all: %+v, held %+v", got, d.Segments)
	}
}
