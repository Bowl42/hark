package vocab

import (
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

type rule struct {
	from, to  string
	wordStart bool // from starts with an ASCII letter or digit: the left neighbor must not be one
	wordEnd   bool
}

// Matcher is a compiled set of replacement rules. It is immutable and safe for concurrent use.
type Matcher struct {
	idx map[rune][]rule // grouped by first character, each group sorted by from length descending (longest match first)
}

// Compile compiles a set of from→to replacement rules, for cases that don't use a Store and supply a static
// vocabulary directly (for example, in-process sessions). An empty set returns nil (a nil Matcher replaces nothing).
//
// Rules: from is non-empty, from≠to, and both must be valid UTF-8; to may be empty (deletion).
// Replacement happens only within a segment, left to right, longest match first, non-overlapping and
// non-recursive, and is case-sensitive. When from starts (ends) with an ASCII letter or digit, the character
// to the left (right) of the match must not be an ASCII letter or digit, i.e. English matches whole words
// ("hack" doesn't match "hacker").
func Compile(rules map[string]string) (*Matcher, error) {
	for from, to := range rules {
		if from == "" || from == to || !utf8.ValidString(from) || !utf8.ValidString(to) {
			return nil, fmt.Errorf("vocab: invalid rule %q → %q", from, to)
		}
	}
	return compile(rules), nil
}

// compile compiles from→to (the caller guarantees the rules are valid). An empty set returns nil.
func compile(rules map[string]string) *Matcher {
	if len(rules) == 0 {
		return nil
	}
	m := &Matcher{idx: map[rune][]rule{}}
	for from, to := range rules {
		first, _ := utf8.DecodeRuneInString(from)
		last, _ := utf8.DecodeLastRuneInString(from)
		m.idx[first] = append(m.idx[first], rule{from: from, to: to, wordStart: isWord(first), wordEnd: isWord(last)})
	}
	for _, g := range m.idx {
		sort.Slice(g, func(i, j int) bool {
			if len(g[i].from) != len(g[j].from) {
				return len(g[i].from) > len(g[j].from)
			}
			return g[i].from < g[j].from
		})
	}
	return m
}

// at tries matches at s[i:] in order (longest first) and calls fn for each rule that satisfies the boundary
// conditions; it stops when fn returns false.
func (m *Matcher) at(s string, i int, left, right rune, fn func(r *rule) bool) {
	c, _ := utf8.DecodeRuneInString(s[i:])
	g := m.idx[c]
	if len(g) == 0 {
		return
	}
	prev := left
	if i > 0 {
		prev, _ = utf8.DecodeLastRuneInString(s[:i])
	}
	for k := range g {
		r := &g[k]
		if !strings.HasPrefix(s[i:], r.from) {
			continue
		}
		if r.wordStart && isWord(prev) {
			continue
		}
		if r.wordEnd {
			next := right
			if j := i + len(r.from); j < len(s) {
				next, _ = utf8.DecodeRuneInString(s[j:])
			}
			if isWord(next) {
				continue
			}
		}
		if !fn(r) {
			return
		}
	}
}

// replace substitutes left to right, longest first, without overlap. left and right are the characters
// immediately before and after s (from adjacent segments), used for English word boundaries; pass 0 if there
// are none. It returns s unchanged if nothing matches.
func (m *Matcher) replace(s string, left, right rune) string {
	if m == nil {
		return s
	}
	var b strings.Builder
	last := 0 // s[last:] not yet written to b
	for i := 0; i < len(s); {
		var hit *rule
		m.at(s, i, left, right, func(r *rule) bool { hit = r; return false })
		if hit == nil {
			_, w := utf8.DecodeRuneInString(s[i:])
			i += w
			continue
		}
		if b.Len() == 0 && last == 0 {
			b.Grow(len(s) + 16)
		}
		b.WriteString(s[last:i])
		b.WriteString(hit.to)
		i += len(hit.from)
		last = i
	}
	if last == 0 {
		return s
	}
	b.WriteString(s[last:])
	return b.String()
}

// occurring returns the froms that occur in s (overlaps allowed, same boundary rules as replace).
func (m *Matcher) occurring(s string) map[string]bool {
	found := map[string]bool{}
	if m == nil {
		return found
	}
	for i := 0; i < len(s); {
		m.at(s, i, 0, 0, func(r *rule) bool { found[r.from] = true; return true })
		_, w := utf8.DecodeRuneInString(s[i:])
		i += w
	}
	return found
}
