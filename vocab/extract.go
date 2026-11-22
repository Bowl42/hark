package vocab

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// pair is the key of a correction candidate or an entry: the recognizer produced from, the user wants to.
type pair struct{ from, to string }

// Size limits for learning.
const (
	maxPairTokens = 6       // word-level correction: at most this many tokens per side (spaces excluded)
	maxPairRunes  = 16      // at most this many characters per side
	minFromRunes  = 2       // From needs at least 2 characters, so single-character rules don't apply globally
	maxSentTokens = 256     // at most this many tokens per side in a changed sentence block; larger blocks aren't learned
	diffBudget    = 1 << 20 // cap on the total cells of all LCS tables for one submission
)

// token is the smallest unit of learning: a run of ASCII letters and digits (an English word or a number),
// one whitespace character, one punctuation mark, or any other single character (a Chinese character, etc.).
type token struct {
	s     string
	sep   bool // whitespace or punctuation: rules can't extend across them
	space bool
	word  bool // ASCII word
}

func isWord(r rune) bool {
	return r < utf8.RuneSelf && (r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9')
}

func tokenize(s string) []token {
	var out []token
	for i := 0; i < len(s); {
		r, n := utf8.DecodeRuneInString(s[i:])
		if isWord(r) {
			j := i
			for j < len(s) && isWord(rune(s[j])) {
				j++
			}
			out = append(out, token{s: s[i:j], word: true})
			i = j
			continue
		}
		t := token{s: s[i : i+n]}
		switch {
		case unicode.IsSpace(r):
			t.sep, t.space = true, true
		case unicode.IsPunct(r) || unicode.IsSymbol(r):
			t.sep = true
		}
		out = append(out, t)
		i += n
	}
	return out
}

func isSentenceEnd(t token) bool {
	switch t.s {
	case "。", "！", "？", "!", "?", ".", "\n", "；", ";":
		return true
	}
	return false
}

// sentences splits at sentence-ending punctuation; each sentence includes its ending punctuation.
func sentences(toks []token) [][]token {
	var out [][]token
	start := 0
	for i, t := range toks {
		if isSentenceEnd(t) {
			out = append(out, toks[start:i+1])
			start = i + 1
		}
	}
	if start < len(toks) {
		out = append(out, toks[start:])
	}
	return out
}

func join(toks []token) string {
	var b strings.Builder
	for _, t := range toks {
		b.WriteString(t.s)
	}
	return b.String()
}

// span is one piece of an alignment: a[a0:a1] against b[b0:b1]; eq means both sides are equal.
type span struct {
	a0, a1, b0, b1 int
	eq             bool
}

// align aligns two sequences by longest common subsequence. The result covers every element of both sides
// end to end, alternating between equal and changed spans. It returns false if the table, after stripping
// the common prefix and suffix, exceeds the remaining budget.
func align(n, m int, eq func(i, j int) bool, budget *int) ([]span, bool) {
	p := 0
	for p < n && p < m && eq(p, p) {
		p++
	}
	s := 0
	for s < n-p && s < m-p && eq(n-1-s, m-1-s) {
		s++
	}
	var out []span
	add := func(a0, a1, b0, b1 int, same bool) {
		if a0 == a1 && b0 == b1 {
			return
		}
		if k := len(out) - 1; k >= 0 && out[k].eq == same {
			out[k].a1, out[k].b1 = a1, b1
			return
		}
		out = append(out, span{a0, a1, b0, b1, same})
	}
	add(0, p, 0, p, true)
	an, bm := n-s-p, m-s-p
	if an > 0 && bm > 0 {
		cells := (an + 1) * (bm + 1)
		if cells > *budget {
			return nil, false
		}
		*budget -= cells
		w := bm + 1
		l := make([]int32, cells) // l[i*w+j] = LCS length of a[p+i:] and b[p+j:] (middle part only)
		for i := an - 1; i >= 0; i-- {
			for j := bm - 1; j >= 0; j-- {
				if eq(p+i, p+j) {
					l[i*w+j] = l[(i+1)*w+j+1] + 1
				} else {
					l[i*w+j] = max(l[(i+1)*w+j], l[i*w+j+1])
				}
			}
		}
		i, j := 0, 0
		for i < an || j < bm {
			switch {
			case i < an && j < bm && eq(p+i, p+j):
				add(p+i, p+i+1, p+j, p+j+1, true)
				i, j = i+1, j+1
			case j < bm && (i == an || l[i*w+j+1] >= l[(i+1)*w+j]):
				add(p+i, p+i, p+j, p+j+1, false)
				j++
			default:
				add(p+i, p+i+1, p+j, p+j, false)
				i++
			}
		}
	} else {
		add(p, n-s, p, m-s, false)
	}
	add(n-s, n, m-s, m, true)
	return out, true
}

// alignAnchored aligns by sentence: sentences that occur exactly once on each side serve as anchors (taking
// the longest increasing subsequence, O(n log n)), and the gaps between anchors are aligned with LCS. A long
// text edited at both its start and its end doesn't put the whole text into one table.
func alignAnchored(a, b []string, budget *int) ([]span, bool) {
	ca, cb := map[string]int{}, map[string]int{}
	for _, x := range a {
		ca[x]++
	}
	for _, x := range b {
		cb[x]++
	}
	posB := map[string]int{}
	for j, x := range b {
		if ca[x] == 1 && cb[x] == 1 {
			posB[x] = j
		}
	}
	type pt struct{ i, j int }
	var cand []pt
	for i, x := range a {
		if j, ok := posB[x]; ok {
			cand = append(cand, pt{i, j})
		}
	}
	// Longest increasing subsequence (by j).
	tails, prev := []int{}, make([]int, len(cand))
	for k, c := range cand {
		lo, hi := 0, len(tails)
		for lo < hi {
			mid := (lo + hi) / 2
			if cand[tails[mid]].j < c.j {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		prev[k] = -1
		if lo > 0 {
			prev[k] = tails[lo-1]
		}
		if lo == len(tails) {
			tails = append(tails, k)
		} else {
			tails[lo] = k
		}
	}
	var anchors []pt
	if len(tails) > 0 {
		anchors = make([]pt, 0, len(tails)+1)
		for k := tails[len(tails)-1]; k >= 0; k = prev[k] {
			anchors = append(anchors, cand[k])
		}
		slices.Reverse(anchors)
	}
	anchors = append(anchors, pt{len(a), len(b)}) // sentinel

	var out []span
	push := func(sp span) {
		if sp.a0 == sp.a1 && sp.b0 == sp.b1 {
			return
		}
		if k := len(out) - 1; k >= 0 && out[k].eq == sp.eq {
			out[k].a1, out[k].b1 = sp.a1, sp.b1
			return
		}
		out = append(out, sp)
	}
	i0, j0 := 0, 0
	for _, an := range anchors {
		ga, gb := a[i0:an.i], b[j0:an.j]
		sub, ok := align(len(ga), len(gb), func(i, j int) bool { return ga[i] == gb[j] }, budget)
		if !ok {
			return nil, false
		}
		for _, sp := range sub {
			push(span{sp.a0 + i0, sp.a1 + i0, sp.b0 + j0, sp.b1 + j0, sp.eq})
		}
		if an.i < len(a) {
			push(span{an.i, an.i + 1, an.j, an.j + 1, true})
		}
		i0, j0 = an.i+1, an.j+1
	}
	return out, true
}

// cleanup merges an equal span sandwiched between two changes, and no longer than either of them, into the
// changes (so characters like 的 or 了 that happen to match don't chop a rewrite into pieces).
// It doesn't merge when both changes are identical (the same word corrected twice in one sentence).
func cleanup(spans []span, ta, tb []token) []span {
	size := func(sp span) int { return max(sp.a1-sp.a0, sp.b1-sp.b0) }
	same := func(x, y span) bool {
		return join(ta[x.a0:x.a1]) == join(ta[y.a0:y.a1]) && join(tb[x.b0:x.b1]) == join(tb[y.b0:y.b1])
	}
	for changed := true; changed; {
		changed = false
		for k := 1; k+1 < len(spans); k++ {
			l, m, r := spans[k-1], spans[k], spans[k+1]
			if !m.eq || l.eq || r.eq {
				continue
			}
			if n := m.a1 - m.a0; n <= size(l) && n <= size(r) && !same(l, r) {
				merged := span{l.a0, r.a1, l.b0, r.b1, false}
				spans = append(spans[:k-1], append([]span{merged}, spans[k+2:]...)...)
				changed = true
				break
			}
		}
	}
	return spans
}

// extract pulls word-level correction candidates out of one correction (recognized before → the user's
// corrected after). skipped is the number of changed blocks not analyzed because they exceeded the size limits.
//
// It aligns by sentence first, then by token within sentence blocks that don't match. Each change is padded
// with context from adjacent unchanged tokens, never across punctuation, whitespace or other changes.
// Long sentence rewrites, sentences with changed punctuation, and pure English word insertions or deletions
// aren't learned.
func extract(before, after string) (pairs []pair, skipped int) {
	sa, sb := sentences(tokenize(before)), sentences(tokenize(after))
	ka, kb := make([]string, len(sa)), make([]string, len(sb))
	for i, x := range sa {
		ka[i] = join(x)
	}
	for i, x := range sb {
		kb[i] = join(x)
	}
	budget := diffBudget
	blocks, ok := alignAnchored(ka, kb, &budget)
	if !ok {
		return nil, 1
	}
	seen := map[pair]bool{}
	for _, bl := range blocks {
		if bl.eq {
			continue
		}
		var ta, tb []token
		for _, x := range sa[bl.a0:bl.a1] {
			ta = append(ta, x...)
		}
		for _, x := range sb[bl.b0:bl.b1] {
			tb = append(tb, x...)
		}
		if len(ta) > maxSentTokens || len(tb) > maxSentTokens {
			skipped++
			continue
		}
		spans, ok := align(len(ta), len(tb), func(i, j int) bool { return ta[i].s == tb[j].s }, &budget)
		if !ok {
			skipped++
			continue
		}
		spans = cleanup(spans, ta, tb)
		var got []pair
		rewrite := false
		for k, sp := range spans {
			if sp.eq {
				continue
			}
			if hasPunct(ta[sp.a0:sp.a1]) || hasPunct(tb[sp.b0:sp.b1]) {
				rewrite = true // punctuation changed: this is a sentence rewrite, so learn nothing from this block
				break
			}
			var left, right []token
			if k > 0 {
				left = ta[spans[k-1].a0:spans[k-1].a1]
			}
			if k+1 < len(spans) {
				right = ta[spans[k+1].a0:spans[k+1].a1]
			}
			if p, ok := candidate(ta[sp.a0:sp.a1], tb[sp.b0:sp.b1], left, right); ok {
				got = append(got, p)
			}
		}
		if rewrite {
			continue
		}
		for _, p := range got {
			if !seen[p] {
				seen[p] = true
				pairs = append(pairs, p)
			}
		}
	}
	return pairs, skipped
}

func hasPunct(toks []token) bool {
	for _, t := range toks {
		if t.sep && !t.space {
			return true
		}
	}
	return false
}

// candidate turns one change into a candidate; left and right are the unchanged tokens on either side of it.
func candidate(from, to, left, right []token) (pair, bool) {
	onlySep := true
	for _, t := range append(append([]token(nil), from...), to...) {
		if !t.sep {
			onlySep = false
		}
		if t.space && t.s != " " {
			return pair{}, false // newline, tab, etc.: not a space inside a phrase
		}
	}
	if onlySep {
		return pair{}, false // only whitespace changed
	}
	if len(from) == 0 || len(to) == 0 {
		// Pure insertion or deletion: learn only missing or extra characters in languages like Chinese
		// ("哈克"→"哈尔克"); inserting or deleting whole English words is usually a rewrite.
		for _, t := range append(append([]token(nil), from...), to...) {
			if t.word || t.sep {
				return pair{}, false
			}
		}
	}
	// Pad with unchanged tokens from both sides: a pure insertion or deletion must become a replacement, and a
	// From that is too short must reach minFromRunes. When padding is needed, first add one token on each side
	// (we don't know which side the word boundary is on, so taking both is safest), then keep adding if still short.
	l, r := len(left), 0
	short := func() bool { return len(from) == 0 || len(to) == 0 || runes(from) < minFromRunes }
	growL := func() bool {
		if l == 0 || left[l-1].sep {
			return false
		}
		l--
		from = append([]token{left[l]}, from...)
		to = append([]token{left[l]}, to...)
		return true
	}
	growR := func() bool {
		if r == len(right) || right[r].sep {
			return false
		}
		from = append(from, right[r])
		to = append(to, right[r])
		r++
		return true
	}
	if short() {
		growL()
		growR()
	}
	for short() {
		if !growL() && !growR() {
			return pair{}, false
		}
	}
	if from[0].space || to[0].space || from[len(from)-1].space || to[len(to)-1].space {
		return pair{}, false
	}
	if words(from) > maxPairTokens || words(to) > maxPairTokens ||
		runes(from) > maxPairRunes || runes(to) > maxPairRunes {
		return pair{}, false
	}
	p := pair{join(from), join(to)}
	if p.from == p.to {
		return pair{}, false
	}
	return p, true
}

func runes(toks []token) int {
	n := 0
	for _, t := range toks {
		if !t.space {
			n += utf8.RuneCountInString(t.s)
		}
	}
	return n
}

func words(toks []token) int {
	n := 0
	for _, t := range toks {
		if !t.space {
			n++
		}
	}
	return n
}
