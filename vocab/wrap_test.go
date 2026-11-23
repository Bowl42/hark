package vocab

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Bowl42/hark"
)

func mustCompile(t testing.TB, rules map[string]string) *Matcher {
	m, err := Compile(rules)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestRewriter(t *testing.T) {
	w := &rewriter{m: mustCompile(t, map[string]string{"哈克": "hark", "hack": "hark"})}
	segs := []hark.Segment{{Text: "用哈克", Final: true}, {Text: "hack", Speaker: "a"}, {Text: "er"}}
	w.apply(segs)
	// The second segment is immediately followed by "er": not a whole word.
	if segs[0].Text != "用hark" || segs[1].Text != "hack" || segs[2].Text != "er" || !segs[0].Final || segs[1].Speaker != "a" {
		t.Fatalf("got %+v", segs)
	}
	// On a cache hit, metadata still comes from the current snapshot; a boundary change forces recomputation.
	segs = []hark.Segment{{Text: "用哈克"}, {Text: "hack", Speaker: "b"}, {Text: " ok"}}
	w.apply(segs)
	if segs[0].Text != "用hark" || segs[0].Final || segs[1].Text != "hark" || segs[1].Speaker != "b" {
		t.Fatalf("got %+v", segs)
	}
	segs = []hark.Segment{{Text: "哈克"}}
	w.apply(segs)
	if segs[0].Text != "hark" || len(w.cache) != 1 {
		t.Fatalf("got %+v cache=%d", segs, len(w.cache))
	}
}

type fakeEngine struct {
	out hark.Output
}

func (e *fakeEngine) Open(_ context.Context, _ hark.Request, out hark.Output) (hark.Stream, error) {
	e.out = out
	return nil, nil
}

// recorder keeps the latest snapshot and how many segments were handed over.
type recorder struct {
	mu   sync.Mutex
	last []hark.Segment
	done int
	n    int
}

func (r *recorder) Update(segs []hark.Segment) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last, r.n = segs, r.n+1
}

func (r *recorder) Done(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.done += n
}

// report is an engine's Update of done followed by live, then Done for the done part.
func report(out hark.Output, done, live []hark.Segment) {
	out.Update(append(append([]hark.Segment(nil), done...), live...))
	out.Done(len(done))
}

func TestWrap(t *testing.T) {
	inner := &fakeEngine{}
	if Wrap(inner, nil) != hark.Engine(inner) {
		t.Fatal("empty matcher should return the engine itself")
	}
	e := Wrap(inner, mustCompile(t, map[string]string{"哈克": "hark"}))
	var rec recorder
	if _, err := e.Open(context.Background(), hark.Request{}, &rec); err != nil {
		t.Fatal(err)
	}
	inner.out.Update([]hark.Segment{{Text: "用哈克", Final: true}})
	if rec.last[0].Text != "用hark" || !rec.last[0].Final {
		t.Fatalf("got=%+v", rec.last)
	}
	// Each Open has its own cache; concurrent Update is safe (-race).
	var rec2 recorder
	e.Open(context.Background(), hark.Request{}, &rec2)
	out2 := inner.out
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				out2.Update([]hark.Segment{{Text: fmt.Sprintf("哈克%d", j)}})
			}
		}()
	}
	wg.Wait()
	if rec2.n != 800 || rec.n != 1 {
		t.Fatalf("n=%d %d", rec2.n, rec.n)
	}
}

// Hot path: 2000 rules (many sharing a first character), a long session, only the last sentence changing.
func BenchmarkRewriter(b *testing.B) {
	rules := map[string]string{}
	for i := 0; i < 2000; i++ {
		rules[fmt.Sprintf("哈%d克", i)] = fmt.Sprintf("hark%d", i)
	}
	rules["哈克"] = "hark"
	w := &rewriter{m: mustCompile(b, rules)}
	segs := make([]hark.Segment, 200)
	for i := range segs {
		segs[i].Text = "我们今天用哈克这个网关做了一次语音识别的测试，效果不错。"
	}
	w.apply(append([]hark.Segment(nil), segs...))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := append([]hark.Segment(nil), segs...)
		s[len(s)-1].Text = fmt.Sprintf("最后一句哈克在变%d", i)
		w.apply(s)
	}
}

// After a handover the next text still has a left neighbour: the last raw character handed over. A rule for an
// English word must not fire inside a longer word that spans the handover.
func TestWrapHandover(t *testing.T) {
	inner := &fakeEngine{}
	e := Wrap(inner, mustCompile(t, map[string]string{"bar": "X"}))
	var rec recorder
	if _, err := e.Open(context.Background(), hark.Request{}, &rec); err != nil {
		t.Fatal(err)
	}
	report(inner.out, []hark.Segment{{Text: "foo"}}, []hark.Segment{{Text: "bar"}})
	if rec.done != 1 || rec.last[0].Text != "foo" || rec.last[1].Text != "bar" {
		t.Fatalf("handover: done %d, snapshot %+v", rec.done, rec.last)
	}
	inner.out.Update([]hark.Segment{{Text: "bar"}, {Text: " bar"}})
	if rec.last[0].Text != "bar" || rec.last[1].Text != " X" {
		t.Fatalf("after handover: %+v", rec.last)
	}
}

// Done with n <= 0 or beyond the snapshot doesn't break the rewriter.
func TestWrapDoneBounds(t *testing.T) {
	inner := &fakeEngine{}
	e := Wrap(inner, mustCompile(t, map[string]string{"哈克": "hark"}))
	var rec recorder
	e.Open(context.Background(), hark.Request{}, &rec)
	inner.out.Done(1)
	inner.out.Done(-1)
	inner.out.Update([]hark.Segment{{Text: "哈克"}})
	inner.out.Done(5)
	inner.out.Update([]hark.Segment{{Text: "用哈克"}})
	if rec.last[0].Text != "用hark" {
		t.Fatalf("%+v", rec.last)
	}
}
