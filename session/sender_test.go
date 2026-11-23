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
// the output drops sealed segments once the reader has them.
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
				next.Update(snap)
			default:
				p.Update(snap)
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
