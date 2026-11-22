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
// in order always yields a document equal to the output's state at that moment (covering both strict and freeze).
func TestSenderUpdatesReproduceState(t *testing.T) {
	words := []string{"今天", "天气", "很好", "。", "我们", "去", "公园", "吧", "hark", " API"}
	for _, c := range []api.Commit{{Policy: api.CommitStrict}, {Policy: api.CommitFreeze, AfterMS: 0}} {
		rng := rand.New(rand.NewPCG(1, 2))
		o := newOutput(c)
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
			segs, stable, commit := o.state()
			if !reflect.DeepEqual(doc.Segments, cloneSegments(segs)) && !(len(segs) == 0 && len(doc.Segments) == 0) ||
				doc.Stable != stable || doc.Commit != commit {
				t.Fatalf("doc %+v, state %+v %d %d", doc, segs, stable, commit)
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
			o.Update(snap)
			if rng.IntN(4) == 0 {
				time.Sleep(time.Microsecond) // give freeze (after 0) a chance to advance
			}
			check()
		}
		o.finish()
		check()
	}
}
