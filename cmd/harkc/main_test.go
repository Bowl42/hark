package main

import "testing"

func TestRevises(t *testing.T) {
	for _, c := range []struct {
		before, after string
		want          bool
	}{
		{"今天。", "今天下午。", false},
		{"今天", "今天我们", false},
		{"ab", "ab", false},
		{"今天三点", "今天3点", true},
		{"ab", "a", true},
		{"", "x", false},
	} {
		if got := revises(c.before, c.after); got != c.want {
			t.Errorf("%q → %q: revises = %v", c.before, c.after, got)
		}
	}
}
