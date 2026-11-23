package session

import (
	"math/rand/v2"
	"reflect"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
)

// The runtime no longer self-checks with Doc.Apply; this test guarantees it instead: applying every update from sender
// in order always yields a document equal to the output's state at that moment (covering both strict and freeze, and
// rollovers: a gated part receiving snapshots while the live one is still open, then sealing). It also checks that
// the output drops sealed segments once the reader has them. Updates randomly hand over a leading run.
func TestSenderUpdatesReproduceState(t *testing.T) {
	words := []string{"今天", "天气", "很好", "。", "我们", "去", "公园", "吧", "hark", " API"}
	for _, c := range []api.Commit{{Policy: api.CommitStrict}, {Policy: api.CommitFreeze, AfterMS: 0}} {
		rng := rand.New(rand.NewPCG(1, 2))
		o := newOutput(c)
		p := o.newPart(0)
		var next *part
		var s sender
		var doc api.Doc
		check := func() {
			t.Helper()
			for {
				u, ok := s.next(o)
				if !ok {
					break
				}
				if err := doc.Apply(u); err != nil {
					t.Fatalf("apply %+v: %v", u, err)
				}
			}
			snap := o.snapshot()
			if !reflect.DeepEqual(cloneSegments(doc.Segments[snap.Base:]), snap.Segments) ||
				doc.Stable != snap.Stable || doc.Commit != snap.Commit {
				t.Fatalf("doc %+v, state %+v", doc, snap)
			}
			if o.gone < min(s.base, o.gone+len(o.sealed)) { // every sealed segment the reader has must be dropped
				t.Fatalf("delivered up to %d, output still holds %d sealed from %d", s.base, len(o.sealed), snap.Base)
			}
		}
		for step := 0; step < 2000; step++ {
			n := rng.IntN(5)
			snap := make([]hark.Segment, n)
			for i := range snap {
				text := ""
				for k := rng.IntN(4); k >= 0; k-- {
					text += words[rng.IntN(len(words))]
				}
				snap[i] = hark.Segment{Text: text, Final: rng.IntN(3) > 0, Speaker: []string{"", "A", "B"}[rng.IntN(3)]}
				if rng.IntN(2) == 0 {
					snap[i].Audio = hark.AudioRange{Start: int64(i * 1000), End: int64(i*1000 + 900)}
				}
			}
			switch r := rng.IntN(10); {
			case r == 0 && next == nil:
				next = o.newPart(int64(step) * 100)
			case r == 1 && next != nil:
				o.seal(p)
				p, next = next, nil
			case r < 4 && next != nil:
				next.Update(handover(rng, snap))
			default:
				p.Update(handover(rng, snap))
			}
			if rng.IntN(4) == 0 {
				time.Sleep(time.Microsecond) // give freeze (after 0) a chance to advance
			}
			check()
		}
		if next != nil {
			o.seal(p)
			check()
		}
		o.finish()
		check()
	}
}

// handover splits a snapshot into a randomly long handed-over prefix and the live rest (often nothing is handed over).
func handover(rng *rand.Rand, snap []hark.Segment) ([]hark.Segment, []hark.Segment) {
	k := 0
	if rng.IntN(3) == 0 {
		k = rng.IntN(len(snap) + 1)
	}
	return snap[:k:k], snap[k:]
}

// An independent model under strict: the engine's document is everything it handed over followed by its latest live
// snapshot, and the reader's document must equal it after every update (one part, so nothing is placed or joined).
func TestHandoverMatchesModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	words := []string{"甲", "乙", "丙", "丁", "。"}
	for trial := 0; trial < 50; trial++ {
		o := newOutput(api.Commit{})
		p := o.newPart(0)
		var s sender
		var doc api.Doc
		var model []hark.Segment // what the engine has handed over
		for step := 0; step < 100; step++ {
			live := make([]hark.Segment, rng.IntN(4))
			for i := range live {
				live[i] = hark.Segment{Text: words[rng.IntN(len(words))] + words[rng.IntN(len(words))], Final: rng.IntN(2) == 0}
			}
			done, rest := handover(rng, live)
			model = append(model, done...)
			want := append(append([]hark.Segment(nil), model...), rest...)
			p.Update(append([]hark.Segment(nil), done...), append([]hark.Segment(nil), rest...))
			drain(t, o, &s, &doc)
			if doc.Commit != len(model) || len(doc.Segments) != len(want) {
				t.Fatalf("trial %d step %d: doc %+v, model %d + %d", trial, step, doc, len(model), len(rest))
			}
			for i := range want {
				if doc.Segments[i].Text != want[i].Text {
					t.Fatalf("trial %d step %d: segment %d %q, want %q", trial, step, i, doc.Segments[i].Text, want[i].Text)
				}
			}
			if len(o.sealed) != 0 || len(p.tr.segs) != len(rest) { // nothing kept that the engine handed over
				t.Fatalf("trial %d step %d: output holds %d sealed, part %d", trial, step, len(o.sealed), len(p.tr.segs))
			}
		}
	}
}
