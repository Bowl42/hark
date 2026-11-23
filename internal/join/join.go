// Package join decides how adjacent pieces of recognized text are joined.
package join

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// NeedSpace reports whether a space is needed between prev and next: when they meet at English letters/digits
// (or next starts with one after sentence punctuation). CJK text is joined directly.
func NeedSpace(prev, next string) bool {
	a, _ := utf8.DecodeLastRuneInString(prev)
	b, _ := utf8.DecodeRuneInString(next)
	alnum := func(r rune) bool { return r < utf8.RuneSelf && (unicode.IsLetter(r) || unicode.IsDigit(r)) }
	return (alnum(a) || strings.ContainsRune(".,!?;:", a)) && alnum(b)
}
