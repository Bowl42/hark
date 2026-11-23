package session

import (
	"strings"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/internal/vad"
)

// Parts are joined at their nearest non-empty texts: a space where English meets, none for CJK.
func TestPartJoin(t *testing.T) {
	for _, c := range []struct {
		a, b []string
		want string
	}{
		{[]string{"Hello", ""}, []string{"", "World"}, "Hello World"},
		{[]string{"你好。"}, []string{"世界"}, "你好。世界"},
		{[]string{"Done."}, []string{"", "", "Next"}, "Done. Next"},
	} {
		o := newOutput(api.Commit{})
		p0 := o.newPart(0)
		p0.Update(segs(c.a))
		p1 := o.newPart(1000)
		p1.Update(segs(c.b))
		o.seal(p0)
		o.finish()
		got := ""
		for _, g := range o.snapshot().Segments {
			got += g.Text
		}
		if got != c.want {
			t.Errorf("%q + %q = %q, want %q", c.a, c.b, got, c.want)
		}
	}
}

func segs(texts []string) []hark.Segment {
	out := make([]hark.Segment, len(texts))
	for i, t := range texts {
		out[i] = hark.Segment{Text: t}
	}
	return out
}

// Speaker labels are escaped before later parts get a suffix, so they can't collide.
func TestSpeakerLabels(t *testing.T) {
	o := newOutput(api.Commit{})
	p0 := o.newPart(0)
	p0.Update([]hark.Segment{{Text: "a", Speaker: "S1#1"}, {Text: "b", Speaker: "S1"}})
	p1 := o.newPart(1000)
	p1.Update([]hark.Segment{{Text: "c", Speaker: "S1"}})
	o.seal(p0)
	var got []string
	for _, g := range o.snapshot().Segments {
		got = append(got, g.Speaker)
	}
	if want := []string{`S1\#1`, "S1", "S1#1"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("speakers %q, want %q", got, want)
	}
}

// drain applies every pending update to doc.
func drain(t *testing.T, o *output, s *sender, doc *api.Doc) {
	t.Helper()
	for {
		u, ok := s.next(o)
		if !ok {
			return
		}
		if err := doc.Apply(u); err != nil {
			t.Fatal(err)
		}
	}
}

// Once the reader has a sealed part, the output drops it but still joins the next text to it correctly, also when
// the session aborts with the next text gated behind an empty live part.
func TestReleaseKeepsJoin(t *testing.T) {
	for _, abort := range []bool{false, true} {
		o := newOutput(api.Commit{})
		var s sender
		var doc api.Doc
		p0 := o.newPart(0)
		p0.Update(segs([]string{"Hello."}))
		p1 := o.newPart(1000)
		o.seal(p0)
		drain(t, o, &s, &doc)
		if len(o.sealed) != 0 || o.gone != 1 || o.edge != "." {
			t.Fatalf("after release: sealed %d gone %d edge %q", len(o.sealed), o.gone, o.edge)
		}
		if abort { // p1 stays empty; p2's text is gated when the session aborts
			p2 := o.newPart(2000)
			p2.Update(segs([]string{"World"}))
			o.close()
		} else {
			p1.Update(segs([]string{"World"}))
			o.finish()
		}
		drain(t, o, &s, &doc)
		if got := doc.Text(); got != "Hello. World" {
			t.Errorf("abort %v: %q", abort, got)
		}
	}
}

// Freeze commits within the live part are delivered before the part is sealed; sealing then adds nothing the reader
// sees, and the output must still drop the part.
func TestReleaseAfterFrozenSeal(t *testing.T) {
	o := newOutput(api.Commit{Policy: api.CommitFreeze})
	var s sender
	var doc api.Doc
	p0 := o.newPart(0)
	p0.Update([]hark.Segment{{Text: "甲。", Final: true}})
	time.Sleep(time.Millisecond)
	drain(t, o, &s, &doc)
	if doc.Commit != 1 {
		t.Fatalf("not frozen: commit %d", doc.Commit)
	}
	o.newPart(1000) // empty successor
	o.seal(p0)
	drain(t, o, &s, &doc)
	if len(o.sealed) != 0 || o.gone != 1 {
		t.Fatalf("sealed %d gone %d", len(o.sealed), o.gone)
	}
	o.close()
	drain(t, o, &s, &doc)
	if snap := o.snapshot(); snap.Base != 1 || len(snap.Segments) != 0 || snap.Commit != 1 || doc.Text() != "甲。" {
		t.Fatalf("snapshot %+v doc %+v", snap, doc)
	}
}

func seg(text string, final bool) hark.Segment { return hark.Segment{Text: text, Final: final} }

func joined(segs []api.Segment) string {
	var b strings.Builder
	for _, s := range segs {
		b.WriteString(s.Text + "|")
	}
	return b.String()
}

// Handed-over segments are committed and leave the part at once, even when the update changes nothing else.
func TestHandoverRetires(t *testing.T) {
	o := newOutput(api.Commit{})
	var s sender
	var doc api.Doc
	p := o.newPart(0)
	p.Update([]hark.Segment{seg("a", true), seg("b", false)})
	drain(t, o, &s, &doc)
	report(p, []hark.Segment{seg("a", true)}, []hark.Segment{seg("b", false)}) // same text: only the handover is new
	drain(t, o, &s, &doc)
	if doc.Commit != 1 || joined(doc.Segments) != "a|b|" || len(p.tr.segs) != 1 || len(o.sealed) != 0 {
		t.Fatalf("doc %+v, part holds %d, sealed %d", doc, len(p.tr.segs), len(o.sealed))
	}
	p.Update([]hark.Segment{seg("b2", false)})
	o.finish()
	drain(t, o, &s, &doc)
	if joined(doc.Segments) != "a|b2|" || doc.Commit != 2 {
		t.Fatalf("doc %+v", doc)
	}
}

// Within a stream the engine owns spacing: retired text is not a part boundary.
func TestHandoverNoJoinWithinStream(t *testing.T) {
	o := newOutput(api.Commit{})
	var s sender
	var doc api.Doc
	p := o.newPart(0)
	report(p, []hark.Segment{seg("hel", true)}, nil)
	p.Update([]hark.Segment{seg("lo", false)})
	o.finish()
	drain(t, o, &s, &doc)
	if doc.Text() != "hello" {
		t.Fatalf("%q", doc.Text())
	}
}

// Under freeze, frozen content past the handover stays frozen in the part and lines up with the next snapshot.
func TestHandoverBehindFrozen(t *testing.T) {
	o := newOutput(api.Commit{Policy: api.CommitFreeze})
	var s sender
	var doc api.Doc
	p := o.newPart(0)
	p.Update([]hark.Segment{seg("a", true), seg("b", true)})
	time.Sleep(time.Millisecond)
	drain(t, o, &s, &doc)
	if doc.Commit != 2 {
		t.Fatalf("not frozen: %+v", doc)
	}
	report(p, []hark.Segment{seg("a", true)}, []hark.Segment{seg("b", true), seg("c", false)})
	drain(t, o, &s, &doc)
	if len(p.tr.segs) != 2 || p.tr.commit != 1 || joined(doc.Segments) != "a|b|c|" || doc.Commit != 2 {
		t.Fatalf("part %+v commit %d, doc %+v", p.tr.segs, p.tr.commit, doc)
	}
	p.Update([]hark.Segment{seg("b", true), seg("cd", false)})
	o.finish()
	drain(t, o, &s, &doc)
	if joined(doc.Segments) != "a|b|cd|" {
		t.Fatalf("doc %+v", doc)
	}
}

// When freeze re-segmented content, the handover can't be matched segment by segment: the part keeps the segments
// and goes on as with a full snapshot, without duplicating or losing text.
func TestHandoverMisaligned(t *testing.T) {
	for _, c := range []struct {
		frozen, done []string
	}{
		{[]string{"甲乙"}, []string{"甲", "乙"}},
		{[]string{"甲", "乙"}, []string{"甲乙"}},
	} {
		o := newOutput(api.Commit{Policy: api.CommitFreeze})
		var s sender
		var doc api.Doc
		p := o.newPart(0)
		var frozen, done []hark.Segment
		for _, x := range c.frozen {
			frozen = append(frozen, seg(x, true))
		}
		for _, x := range c.done {
			done = append(done, seg(x, true))
		}
		p.Update(frozen)
		time.Sleep(time.Millisecond)
		drain(t, o, &s, &doc)
		report(p, done, []hark.Segment{seg("丙", false)})
		drain(t, o, &s, &doc)
		if doc.Text() != "甲乙丙" || len(p.held) != len(c.done) {
			t.Fatalf("%v: doc %q, held %d", c, doc.Text(), len(p.held))
		}
		p.Update([]hark.Segment{seg("丙丁", false)})
		o.finish()
		drain(t, o, &s, &doc)
		if doc.Text() != "甲乙丙丁" || doc.Commit != len(doc.Segments) {
			t.Fatalf("%v: doc %+v", c, doc)
		}
	}
}

// While gated, handovers accumulate (even with no live snapshot) and are published once; on abort they are kept
// after the live part's text, uncommitted, exactly once.
func TestHandoverGated(t *testing.T) {
	for _, abort := range []bool{false, true} {
		o := newOutput(api.Commit{})
		var s sender
		var doc api.Doc
		p0 := o.newPart(0)
		p0.Update([]hark.Segment{seg("甲", false)})
		p1 := o.newPart(1000)
		report(p1, []hark.Segment{{Text: "乙", Audio: hark.AudioRange{Start: 0, End: 10}}}, nil)
		report(p1, []hark.Segment{seg("丙", true)}, nil)
		if abort {
			o.close()
		} else {
			o.seal(p0)
			o.finish()
		}
		drain(t, o, &s, &doc)
		if joined(doc.Segments) != "甲|乙|丙|" {
			t.Fatalf("abort %v: %+v", abort, doc)
		}
		if want := map[bool]int{false: 3, true: 0}[abort]; doc.Commit != want {
			t.Fatalf("abort %v: commit %d", abort, doc.Commit)
		}
		if a := doc.Segments[1].AudioRange; a != (api.AudioRange{StartSample: 1000, EndSample: 1010}) {
			t.Fatalf("abort %v: placed %+v, want once", abort, a)
		}
	}
}

// settled: a timed handover settles up to its end, but never beyond the audio given to the engine; an untimed one
// settles what was given so far, and later audio is unsettled again.
func TestHandoverSettled(t *testing.T) {
	o := newOutput(api.Commit{})
	p := o.newPart(16000)
	p.fed.Store(8000)
	// engines report audio ranges relative to their own stream
	report(p, []hark.Segment{{Text: "a", Audio: hark.AudioRange{Start: 0, End: 4000}}}, nil)
	if got := p.settled.Load(); got != 4000 {
		t.Fatalf("timed: settled %d", got)
	}
	report(p, []hark.Segment{{Text: "b", Audio: hark.AudioRange{Start: 4000, End: 99999}}}, nil)
	if got := p.settled.Load(); got != 8000 {
		t.Fatalf("future end: settled %d, want clamped to 8000", got)
	}
	p.fed.Store(12000)
	report(p, []hark.Segment{{Text: "c"}}, nil)
	if got := p.settled.Load(); got != 12000 {
		t.Fatalf("untimed: settled %d", got)
	}
	c := newCutter(time.Second)
	c.length = 12000 + 32000
	if c.unsettled(p.settled.Load()) != 32000 {
		t.Fatalf("unsettled %d", c.unsettled(p.settled.Load()))
	}
}

// A handover received while gated is credited with the audio given to the engine when it arrived, not when the part
// is finally published and retires it.
func TestHandoverSettledWhenReceived(t *testing.T) {
	o := newOutput(api.Commit{})
	p0 := o.newPart(0)
	p1 := o.newPart(1000)
	p1.fed.Store(16000)
	report(p1, []hark.Segment{{Text: "甲"}}, nil) // untimed: settles what the engine had then
	p1.fed.Store(160000)
	p1.Update([]hark.Segment{{Text: "乙"}})
	o.seal(p0)
	if got := p1.settled.Load(); got != 16000 {
		t.Fatalf("settled %d, want 16000", got)
	}
}

// Grace earned before the engine caught up doesn't carry over: once unsettled audio dropped below every, the next
// frame that crosses it again starts a fresh grace period.
func TestCutterGraceResets(t *testing.T) {
	c := newCutter(time.Second)
	c.length = 100 * c.every
	c.due = c.grace                     // a cut was overdue...
	settled := c.length - c.every + 160 // ...but the engine has just settled all but 10ms less than every
	if _, cut := c.scan(make([]byte, 2*320), true, settled); cut {
		t.Fatal("cut on stale grace")
	}
	if c.due != vad.Frame {
		t.Fatalf("due %d, want a fresh period", c.due)
	}
}

// report is an engine's Update of done followed by live, then Done for the done part.
func report(out hark.Output, done, live []hark.Segment) {
	out.Update(append(append([]hark.Segment(nil), done...), live...))
	out.Done(len(done))
}

// Done counts from what is outstanding: repeated calls hand over one segment after another; it is clamped, and
// does nothing before any Update or for n <= 0.
func TestDoneOutstanding(t *testing.T) {
	o := newOutput(api.Commit{})
	var s sender
	var doc api.Doc
	p := o.newPart(0)
	p.Done(1)
	p.Done(-1)
	p.Update([]hark.Segment{seg("a", false), seg("b", false), seg("c", false)})
	p.Done(1)
	p.Done(1)
	drain(t, o, &s, &doc)
	if doc.Commit != 2 || joined(doc.Segments) != "a|b|c|" || len(p.cur) != 1 {
		t.Fatalf("doc %+v, outstanding %d", doc, len(p.cur))
	}
	p.Done(9)
	drain(t, o, &s, &doc)
	if doc.Commit != 3 || len(p.cur) != 0 || len(p.tr.segs) != 0 {
		t.Fatalf("clamped: doc %+v, part %d", doc, len(p.tr.segs))
	}
}

// A Done after a snapshot freeze rejected doesn't normalize it again: the conflict is counted once, and the
// handover is held for the next snapshot.
func TestDoneAfterConflictCountsOnce(t *testing.T) {
	o := newOutput(api.Commit{Policy: api.CommitFreeze})
	p := o.newPart(0)
	p.Update([]hark.Segment{seg("甲。", true)})
	time.Sleep(time.Millisecond)
	o.snapshot()                             // freeze it
	p.Update([]hark.Segment{seg("乙", true)}) // rewrites frozen text without timing: rejected
	before := o.dropped()
	p.Done(1)
	if o.dropped() != before || len(p.held) != 1 {
		t.Fatalf("dropped %d → %d, held %d", before, o.dropped(), len(p.held))
	}
}

// When a gated part is published, what it hasn't handed over yet becomes its outstanding snapshot, so a later Done
// refers to it.
func TestDoneAfterPublish(t *testing.T) {
	o := newOutput(api.Commit{})
	var s sender
	var doc api.Doc
	p0 := o.newPart(0)
	p1 := o.newPart(1000)
	p1.Update([]hark.Segment{seg("x", false), seg("y", false)})
	p1.Done(1)
	o.seal(p0)
	p1.Done(1)
	drain(t, o, &s, &doc)
	if doc.Commit != 2 || joined(doc.Segments) != "x|y|" {
		t.Fatalf("doc %+v", doc)
	}
}

// Handing over a segment that wasn't final makes the final ones after it stable at once, and freeze can then
// commit them without another update.
func TestDoneUnblocksStable(t *testing.T) {
	for _, freeze := range []bool{false, true} {
		c := api.Commit{}
		if freeze {
			c = api.Commit{Policy: api.CommitFreeze}
		}
		o := newOutput(c)
		var s sender
		var doc api.Doc
		p := o.newPart(0)
		p.Update([]hark.Segment{seg("a", false), seg("b", true)})
		drain(t, o, &s, &doc)
		p.Done(1)
		time.Sleep(time.Millisecond)
		drain(t, o, &s, &doc)
		want := map[bool]int{false: 1, true: 2}[freeze]
		if doc.Stable != 2 || doc.Commit != want {
			t.Fatalf("freeze %v: stable %d commit %d, want 2 and %d", freeze, doc.Stable, doc.Commit, want)
		}
	}
}
