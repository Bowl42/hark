package session

import (
	"strings"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
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
