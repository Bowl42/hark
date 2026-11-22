package vocab

import (
	"context"
	"sync"
	"unicode/utf8"

	"github.com/Bowl42/hark"
)

// Wrap returns an engine that applies m to the text of every snapshot e reports before passing it on,
// leaving other fields untouched. If m is nil (an empty vocabulary compiles to nil), it returns e as is.
// The wrapper can be shared by multiple sessions; the caller still owns closing e.
// The commit policy (including freeze timing) sees the replaced text.
func Wrap(e hark.Engine, m *Matcher) hark.Engine {
	if m == nil {
		return e
	}
	return &wrapped{e: e, m: m}
}

type wrapped struct {
	e hark.Engine
	m *Matcher
}

func (w *wrapped) Open(ctx context.Context, req hark.Request, out hark.Output) (hark.Stream, error) {
	return w.e.Open(ctx, req, &rewriteOutput{out: out, rw: rewriter{m: w.m}})
}

// rewriteOutput: Update may be called concurrently, so replacing and handing downstream happen under one
// lock and snapshots never get reordered.
type rewriteOutput struct {
	mu  sync.Mutex
	out hark.Output
	rw  rewriter
}

func (o *rewriteOutput) Update(segs []hark.Segment) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.rw.apply(segs) // per the hark.Output contract, a snapshot handed over belongs downstream, so it can be rewritten in place
	o.out.Update(segs)
}

// rewriter applies the vocabulary to one stream of snapshots. The engine reports a full snapshot every time,
// but most segments are unchanged, so it caches the previous result by segment index and reuses it when the
// raw text and both boundary characters are unchanged. Not safe for concurrent use.
type rewriter struct {
	m     *Matcher
	cache []cached
}

type cached struct {
	raw         string
	left, right rune
	out         string
}

// apply replaces each segment's Text in place, leaving other fields untouched.
func (w *rewriter) apply(segs []hark.Segment) {
	if len(w.cache) > len(segs) {
		w.cache = w.cache[:len(segs)]
	}
	for len(w.cache) < len(segs) {
		w.cache = append(w.cache, cached{})
	}
	// Boundary characters come from the raw text of adjacent non-empty segments; compute them before rewriting.
	lefts := make([]rune, len(segs))
	var prev rune
	for i, s := range segs {
		lefts[i] = prev
		if s.Text != "" {
			prev, _ = utf8.DecodeLastRuneInString(s.Text)
		}
	}
	var next rune
	for i := len(segs) - 1; i >= 0; i-- {
		raw := segs[i].Text
		c := &w.cache[i]
		if c.raw != raw || c.left != lefts[i] || c.right != next {
			*c = cached{raw: raw, left: lefts[i], right: next, out: w.m.replace(raw, lefts[i], next)}
		}
		if raw != "" {
			next, _ = utf8.DecodeRuneInString(raw)
		}
		segs[i].Text = c.out
	}
}
