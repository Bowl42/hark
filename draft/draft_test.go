package draft

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/Bowl42/hark/api"
)

// recog simulates the recognizer: the mirror goes from old to next through the protocol path Diff → Apply → Changes → Rebase.
func recog(t *testing.T, doc *api.Doc, d *Draft, next ...string) {
	t.Helper()
	old := doc.Segments
	ops := api.Diff(old, segs(next...), 0)
	if err := doc.Apply(api.Update{Rev: doc.Rev + 1, Ops: ops}); err != nil {
		t.Fatal(err)
	}
	before := d.Text()
	vis, err := d.Rebase(Changes(old, ops))
	if err != nil {
		t.Fatal(err)
	}
	// applying the returned visible changes in order to the old document must yield the new document
	u := []rune(before)
	for _, v := range vis {
		if v.Start < 0 || v.End < v.Start || v.End > len(u) {
			t.Fatalf("visible change %+v out of range %d", v, len(u))
		}
		u = slices.Concat(u[:v.Start], []rune(v.Text), u[v.End:])
	}
	if string(u) != d.Text() {
		t.Fatalf("visible changes %+v: %q → %q, want %q\nchanges %+v\nedits now %+v mirror %q", vis, before, string(u), d.Text(), Changes(old, ops), d.Edits(), d.Mirror())
	}
	if d.Mirror() != doc.Text() {
		t.Fatalf("mirror %q != doc %q", d.Mirror(), doc.Text())
	}
}

func start(text ...string) (*api.Doc, *Draft) {
	doc := &api.Doc{Segments: segs(text...)}
	return doc, New(doc.Text())
}

func replace(t *testing.T, d *Draft, old, new string) {
	t.Helper()
	r := []rune(d.Text())
	i := strings.Index(d.Text(), old)
	if i < 0 {
		t.Fatalf("%q not in %q", old, d.Text())
	}
	a := len([]rune(d.Text()[:i]))
	if err := d.Replace(a, a+len([]rune(old)), new); err != nil {
		t.Fatal(err)
	}
	_ = r
}

func TestDraftNoEdits(t *testing.T) {
	doc, d := start("今天")
	recog(t, doc, d, "今天下午。", "我们")
	if d.Text() != "今天下午。我们" || len(d.Edits()) != 0 {
		t.Fatalf("%q %+v", d.Text(), d.Edits())
	}
}

func TestDraftKeepsUserEditAndRecognizerElsewhere(t *testing.T) {
	doc, d := start("今天下午三点开会")
	replace(t, d, "三", "3")
	recog(t, doc, d, "今天下午三点半开会")          // places the user didn't touch: correction continues
	recog(t, doc, d, "明天下午三点半开会")          // earlier text changes as usual too
	recog(t, doc, d, "明天下午三点半开会。", "在二号楼") // newly spoken text is appended as usual
	if got := d.Text(); got != "明天下午3点半开会。在二号楼" {
		t.Fatalf("got %q", got)
	}
	if e := d.Edits(); len(e) != 1 || e[0].Conflict {
		t.Fatalf("edits %+v", e)
	}
}

func TestDraftSameFixDisappears(t *testing.T) {
	doc, d := start("下午三点开会")
	replace(t, d, "三", "3")
	recog(t, doc, d, "下午3点开会") // the recognizer also changed it to 3
	if d.Text() != "下午3点开会" || len(d.Edits()) != 0 {
		t.Fatalf("%q %+v", d.Text(), d.Edits())
	}
}

func TestDraftConflictUserWins(t *testing.T) {
	doc, d := start("我们去公园散步")
	replace(t, d, "公园", "商场")
	recog(t, doc, d, "我们去工业园散步") // the recognizer also changed the same place
	if got := d.Text(); got != "我们去商场散步" {
		t.Fatalf("user text must win, got %q", got)
	}
	e := d.Edits()
	if len(e) != 1 || !e[0].Conflict || d.Suggestion(0) != "工业园" {
		t.Fatalf("edits %+v suggestion %q", e, d.Suggestion(0))
	}
	d.Accept(0)
	if d.Text() != "我们去工业园散步" {
		t.Fatalf("accept: %q", d.Text())
	}
}

func TestDraftUserTypesAtEndWhileSpeaking(t *testing.T) {
	doc, d := start("今天开会")
	if err := d.Replace(4, 4, "（重要）"); err != nil { // type at the end
		t.Fatal(err)
	}
	recog(t, doc, d, "今天开会，请准时") // keep speaking
	if got := d.Text(); got != "今天开会（重要），请准时" {
		t.Fatalf("got %q", got)
	}
}

func TestDraftInsertBeforeEdit(t *testing.T) {
	doc, d := start("下午三点")
	replace(t, d, "三点", "3点")
	recog(t, doc, d, "明天下午三点") // text inserted before the correction
	if got := d.Text(); got != "明天下午3点" {
		t.Fatalf("got %q", got)
	}
}

func TestDraftReplaceMergesTouchingEdits(t *testing.T) {
	_, d := start("abcdef")
	d.Replace(1, 2, "B")
	d.Replace(2, 3, "C") // right next to the previous one
	if e := d.Edits(); len(e) != 1 || e[0].Start != 1 || e[0].End != 3 || e[0].Text != "BC" {
		t.Fatalf("edits %+v", e)
	}
	d.Replace(1, 3, "bc") // changed back to the original: the correction disappears
	if len(d.Edits()) != 0 || d.Text() != "abcdef" {
		t.Fatalf("%q %+v", d.Text(), d.Edits())
	}
	if err := d.Replace(3, 7, "x"); err == nil {
		t.Fatal("out of range must fail")
	}
}

func TestTextChanges(t *testing.T) {
	old := segs("今天。", "天气", "x")
	got := Changes(old, api.Diff(old, segs("今天下午。", "天气很好", "x", "我们"), 0))
	want := []Change{{2, 2, "下午"}, {5, 5, "很好"}, {6, 6, "我们"}}
	if len(got) != len(want) {
		t.Fatalf("got %+v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %+v want %+v", got, want)
		}
	}
	// adjacent segment deletion + insertion: merged into one replacement
	old = segs("ab", "cd")
	got = Changes(old, []api.Op{{Retain: 1}, {Delete: 1}, {Insert: segs("XY")}})
	if len(got) != 1 || got[0] != (Change{2, 4, "XY"}) {
		t.Fatalf("got %+v", got)
	}
}

// Random: the recognizer and the user both change things at will. Text the user typed (★) can only be deleted by the
// user; the mirror always matches the protocol state.
func TestDraftRandom(t *testing.T) {
	r := rand.New(rand.NewPCG(3, 4))
	alphabet := []string{"今", "天", "气", "。", "a", "b", "，"}
	word := func() string {
		var b strings.Builder
		for n := r.IntN(5); n >= 0; n-- {
			b.WriteString(alphabet[r.IntN(len(alphabet))])
		}
		return b.String()
	}
	for trial := 0; trial < 300; trial++ {
		doc, d := start()
		var cur []string
		for step := 0; step < 60; step++ {
			if r.IntN(3) == 0 { // user edits
				n := len([]rune(d.Text()))
				a := r.IntN(n + 1)
				b := a + r.IntN(n-a+1)
				ins := strings.Repeat("★", r.IntN(3))
				before := strings.Count(d.Text(), "★") - strings.Count(string([]rune(d.Text())[a:b]), "★")
				if err := d.Replace(a, b, ins); err != nil {
					t.Fatal(err)
				}
				if got := strings.Count(d.Text(), "★"); got != before+len([]rune(ins)) {
					t.Fatalf("trial %d step %d: replace lost user text: %q", trial, step, d.Text())
				}
				continue
			}
			next := append([]string(nil), cur...)
			switch r.IntN(4) {
			case 0:
				if len(next) > 0 {
					next[len(next)-1] += word()
				} else {
					next = append(next, word())
				}
			case 1:
				next = append(next, word())
			case 2:
				if len(next) > 0 {
					next[r.IntN(len(next))] = word()
				}
			case 3:
				if len(next) > 0 {
					next = next[:r.IntN(len(next))]
				}
			}
			stars := strings.Count(d.Text(), "★")
			recog(t, doc, d, next...)
			cur = next
			if got := strings.Count(d.Text(), "★"); got != stars {
				t.Fatalf("trial %d step %d: recognizer changed user text (%d → %d): %q", trial, step, stars, got, d.Text())
			}
			// corrections are sorted, non-overlapping, and in range
			m := 0
			for _, e := range d.Edits() {
				if e.Start < m || e.End < e.Start || e.End > len([]rune(d.Mirror())) {
					t.Fatalf("trial %d step %d: bad edits %+v (mirror %q)", trial, step, d.Edits(), d.Mirror())
				}
				m = e.End
			}
		}
	}
}

// Regression: inserting the same text at the same position is not duplicated.
func TestDraftSamePointInsertDedup(t *testing.T) {
	doc, d := start("ab")
	if err := d.Replace(1, 1, "X"); err != nil {
		t.Fatal(err)
	}
	recog(t, doc, d, "aXb")
	if d.Text() != "aXb" || len(d.Edits()) != 0 {
		t.Fatalf("%q %+v", d.Text(), d.Edits())
	}
	// more is recognized: the user's text is absorbed and the rest follows as usual
	doc, d = start("今天开会")
	d.Replace(4, 4, "，")
	recog(t, doc, d, "今天开会，请准时")
	if d.Text() != "今天开会，请准时" || len(d.Edits()) != 0 {
		t.Fatalf("%q %+v", d.Text(), d.Edits())
	}
}

// Regression: the conflict range shrinks back when the recognition result matches again, instead of growing forever.
func TestDraftConflictShrinks(t *testing.T) {
	doc, d := start("abc")
	replace(t, d, "b", "B")
	recog(t, doc, d, "xyc") // a change spanning the user's correction: conservatively widened into a conflict
	if e := d.Edits(); len(e) != 1 || !e[0].Conflict || d.Text() != "aBc" {
		t.Fatalf("%q %+v", d.Text(), e)
	}
	recog(t, doc, d, "ayc") // the recognizer changes a back: the range shrinks to just B
	if e := d.Edits(); len(e) != 1 || e[0].Start != 1 || e[0].End != 2 || e[0].Text != "B" || d.Text() != "aBc" {
		t.Fatalf("%q %+v", d.Text(), e)
	}
	recog(t, doc, d, "aycd") // afterwards, places the user didn't touch update as usual
	if d.Text() != "aBcd" {
		t.Fatalf("%q", d.Text())
	}
}

// Regression: an empty replace has no side effects and doesn't clear the conflict flag.
func TestDraftNoopReplaceKeepsConflict(t *testing.T) {
	doc, d := start("abc")
	replace(t, d, "b", "B")
	recog(t, doc, d, "xyz")
	before := d.Edits()
	if err := d.Replace(3, 3, ""); err != nil {
		t.Fatal(err)
	}
	if e := d.Edits(); len(e) != 1 || !e[0].Conflict || e[0] != before[0] {
		t.Fatalf("noop replace changed edits: %+v → %+v", before, e)
	}
}

// The cursor moves with the visible changes: two cases found in review.
func TestDraftVisibleChangesForCursor(t *testing.T) {
	move := func(u int, vis []Change) int {
		for _, v := range vis {
			switch {
			case u <= v.Start:
			case u >= v.End:
				u += len([]rune(v.Text)) - (v.End - v.Start)
			default:
				u = v.Start + len([]rune(v.Text))
			}
		}
		return u
	}
	d := New("abc")
	d.Replace(1, 2, "XYZ") // aX|YZc, cursor at 2
	vis, _ := d.Rebase([]Change{{1, 1, "q"}})
	if d.Text() != "aqXYZc" || move(2, vis) != 3 {
		t.Fatalf("%q cursor %d", d.Text(), move(2, vis))
	}
	d = New("abc")
	d.Replace(1, 2, "XYZ")
	vis, _ = d.Rebase([]Change{{0, 2, "uv"}, {2, 3, "d"}})
	if d.Text() != "aXYZd" || move(2, vis) != 2 {
		t.Fatalf("%q cursor %d vis %+v", d.Text(), move(2, vis), vis)
	}
}

// Regression: when the recognizer absorbs a user insertion, the visible changes report only the new part, and the
// cursor stays within the user's text.
func TestDraftAbsorbVisibleIsSuffix(t *testing.T) {
	d := New("ab")
	d.Replace(1, 1, "XYZ")
	vis, _ := d.Rebase([]Change{{1, 1, "XYZtail"}})
	if d.Text() != "aXYZtailb" || len(vis) != 1 || vis[0] != (Change{4, 4, "tail"}) {
		t.Fatalf("%q %+v", d.Text(), vis)
	}
	d = New("ab")
	d.Replace(1, 1, "XYZ")
	if vis, _ := d.Rebase([]Change{{1, 1, "XYZ"}}); len(vis) != 0 || d.Text() != "aXYZb" {
		t.Fatalf("identical insert: %q %+v", d.Text(), vis)
	}
}

func segs(texts ...string) []api.Segment {
	out := make([]api.Segment, len(texts))
	for i, t := range texts {
		out[i] = api.Segment{Text: t}
	}
	return out
}

// Corrections: pieces fit the limit, are cut after sentence ends away from corrections, together reproduce the whole
// transcript, and leave out pieces with unresolved conflicts.
func TestCorrections(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&b, "第%d句话哈克很好。", i)
	}
	d := New(b.String())
	// correct 哈克 → hark in a few sentences (user-document positions equal mirror positions before the first edit;
	// edit from the end so earlier positions stay valid)
	mirror := []rune(d.Mirror())
	var at []int
	for i := 0; i+1 < len(mirror); i++ {
		if mirror[i] == '哈' && mirror[i+1] == '克' {
			at = append(at, i)
		}
	}
	for _, k := range []int{150, 90, 3} {
		if err := d.Replace(at[k], at[k]+2, "hark"); err != nil {
			t.Fatal(err)
		}
	}
	pieces := d.Corrections(300)
	var before, after string
	for i, p := range pieces {
		if len(p.Before) > 300 || len(p.After) > 300 {
			t.Errorf("piece %d too large: %d / %d bytes", i, len(p.Before), len(p.After))
		}
		if i < len(pieces)-1 && !strings.HasSuffix(p.Before, "。") {
			t.Errorf("piece %d not cut at a sentence end: %q", i, p.Before)
		}
		before += p.Before
		after += p.After
	}
	if before != d.Mirror() || after != d.Text() || len(pieces) < 10 {
		t.Fatalf("%d pieces don't reproduce the transcript", len(pieces))
	}

	// a conflict leaves its piece out
	d.edits[1].Conflict = true
	conflicted := d.Corrections(300)
	if len(conflicted) != len(pieces)-1 {
		t.Fatalf("%d pieces with a conflict, want %d", len(conflicted), len(pieces)-1)
	}
	for _, p := range conflicted {
		if strings.Contains(p.Before, "第90句") {
			t.Fatal("conflicted piece kept")
		}
	}

	// no sentence ends: cut anywhere outside corrections; a correction never straddles a cut
	d = New(strings.Repeat("哈克", 300))
	d.Replace(100, 102, "hark")
	all := ""
	for _, p := range d.Corrections(100) {
		if len(p.Before) > 100 || len(p.After) > 100 {
			t.Fatalf("piece too large: %+v", p)
		}
		if strings.Contains(p.After, "har") && !strings.Contains(p.After, "hark") {
			t.Fatalf("correction split: %+v", p)
		}
		all += p.After
	}
	if all != d.Text() {
		t.Fatal("pieces don't reproduce the text")
	}

	// corrections ending at the end of the text count toward the last piece
	// (a correction keeps a neighboring character, so with CJK the smallest piece around one is two characters)
	for _, c := range []struct {
		mirror string
		a, b   int
		text   string
		limit  int
	}{{"abc", 3, 3, "xy", 4}, {"abc", 2, 3, "xyz", 4}, {"abcd", 2, 4, "", 4}, {"一二三", 3, 3, "四", 6}} {
		d := New(c.mirror)
		d.Replace(c.a, c.b, c.text)
		var b, a string
		for _, p := range d.Corrections(c.limit) {
			if len(p.After) > c.limit || len(p.Before) > c.limit {
				t.Errorf("%+v: oversized piece %+v", c, p)
			}
			b, a = b+p.Before, a+p.After
		}
		if b != d.Mirror() || a != d.Text() {
			t.Errorf("%+v: pieces %q / %q", c, b, a)
		}
	}

	// empty mirror with typed text; a short transcript is one piece
	d = New("")
	d.Replace(0, 0, "hello")
	if got := d.Corrections(100); len(got) != 1 || got[0].After != "hello" || got[0].Before != "" {
		t.Fatalf("%+v", got)
	}
	if got := New("短句。").Corrections(api.MaxCorrectionBytes); len(got) != 1 || got[0].Before != "短句。" {
		t.Fatalf("%+v", got)
	}
}
