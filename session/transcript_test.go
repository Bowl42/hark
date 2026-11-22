package session

import (
	"reflect"
	"testing"
	"time"

	"github.com/Bowl42/hark"
)

func p(t string) hark.Segment { return hark.Segment{Text: t} }
func f(t string) hark.Segment { return hark.Segment{Text: t, Final: true} }

var t0 = time.Unix(1000, 0)

func at(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }

func texts(t *transcript) []string {
	segs, _, _ := t.state()
	out := []string{}
	for _, s := range segs {
		out = append(out, s.Text)
	}
	return out
}

func check(t *testing.T, tr *transcript, want []string, stable, commit int) {
	t.Helper()
	_, s, c := tr.state()
	if got := texts(tr); !reflect.DeepEqual(got, want) || s != stable || c != commit {
		t.Fatalf("got %q stable %d commit %d, want %q %d %d", got, s, c, want, stable, commit)
	}
}

func TestStrictNeverCommitsEarly(t *testing.T) {
	tr := newTranscript(freezePolicy{})
	tr.ingest([]hark.Segment{f("今天。"), p("我们")}, at(0))
	tr.tick(at(60_000))
	check(t, tr, []string{"今天。", "我们"}, 1, 0)
	if _, ok := tr.nextDeadline(); ok {
		t.Fatal("strict should have no deadline")
	}
	tr.finish()
	check(t, tr, []string{"今天。", "我们"}, 2, 2)
}

func TestFreezeTiming(t *testing.T) {
	tr := newTranscript(freezePolicy{Freeze: true, After: time.Second})
	tr.ingest([]hark.Segment{p("今天")}, at(0))
	tr.ingest([]hark.Segment{f("今天。")}, at(100)) // becomes stable; the timer starts at 100
	if d, _ := tr.nextDeadline(); !d.Equal(at(1100)) {
		t.Fatalf("deadline %v", d)
	}
	tr.ingest([]hark.Segment{f("今天。"), p("我")}, at(500)) // only later segments change, so segment 0 is not reset
	if tr.tick(at(1099)) {
		t.Fatal("too early")
	}
	if !tr.tick(at(1100)) {
		t.Fatal("should freeze")
	}
	check(t, tr, []string{"今天。", "我"}, 1, 1)

	// rewriting segment 1 resets its own timer
	tr.ingest([]hark.Segment{f("今天。"), f("我们。")}, at(1200))
	tr.tick(at(2100))
	check(t, tr, []string{"今天。", "我们。"}, 2, 1)
	tr.tick(at(2200))
	check(t, tr, []string{"今天。", "我们。"}, 2, 2)
}

func TestFreezeResplitWithoutTextChange(t *testing.T) {
	tr := newTranscript(freezePolicy{Freeze: true, After: 0})
	tr.ingest([]hark.Segment{f("今天。")}, at(0))
	tr.tick(at(0))
	// upstream merged the frozen segment and new content into one segment, but the frozen text is unchanged: split at the boundary, losslessly
	tr.ingest([]hark.Segment{f("今天。天气好")}, at(10))
	check(t, tr, []string{"今天。", "天气好"}, 2, 1)
	if tr.dropped != 0 {
		t.Fatal("no content should be dropped")
	}
}

// Without timing information the content can't be located reliably: the whole snapshot is discarded and the previous state kept.
func TestFreezeConflictWithoutTimingDropsSnapshot(t *testing.T) {
	tr := newTranscript(freezePolicy{Freeze: true, After: 0})
	tr.ingest([]hark.Segment{f("今天。")}, at(0))
	tr.tick(at(0))
	tr.ingest([]hark.Segment{f("今天天气好。"), p("我们走")}, at(10))
	check(t, tr, []string{"今天。"}, 1, 1)
	if tr.dropped != 1 {
		t.Fatalf("dropped = %d", tr.dropped)
	}
	// recovers once upstream no longer conflicts
	tr.ingest([]hark.Segment{f("今天。"), p("我们走")}, at(20))
	check(t, tr, []string{"今天。", "我们走"}, 1, 1)
}

func ts(text string, final bool, start, end int64) hark.Segment {
	return hark.Segment{Text: text, Final: final, Audio: hark.AudioRange{Start: start, End: end}}
}

// With timing information, new content is located by audio time: even when ITN changes the length, new segments are
// neither duplicated nor lost.
func TestFreezeConflictAlignsByTime(t *testing.T) {
	cases := []struct {
		name   string
		frozen hark.Segment
		next   []hark.Segment
		want   []string
	}{
		{"ITN lengthens", ts("123元。", true, 0, 1000),
			[]hark.Segment{ts("一百二十三", true, 0, 700), ts("元。", true, 700, 1000), ts("新句", false, 1000, 1500)},
			[]string{"123元。", "新句"}},
		{"ITN shortens", ts("一百二十三元。", true, 0, 1000),
			[]hark.Segment{ts("123元。", true, 0, 1000), ts("甲", true, 1000, 1200), ts("乙", true, 1200, 1400), ts("丙", false, 1400, 1600)},
			[]string{"一百二十三元。", "甲", "乙", "丙"}},
		{"merge across boundary: drop that segment, keep later ones", ts("今天。", true, 0, 500),
			[]hark.Segment{ts("今天天气好。", true, 0, 1200), ts("我们走", false, 1200, 1800)},
			[]string{"今天。", "我们走"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			tr := newTranscript(freezePolicy{Freeze: true, After: 0})
			tr.ingest([]hark.Segment{c.frozen}, at(0))
			tr.tick(at(0))
			tr.ingest(c.next, at(10))
			if got := texts(tr); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("got %q want %q", got, c.want)
			}
			if tr.dropped != 1 {
				t.Fatalf("dropped = %d", tr.dropped)
			}
		})
	}
}

// On re-segmentation, segments that weren't split keep their timestamps, so later conflicts can still be aligned by time.
func TestResplitKeepsUncutTimestamps(t *testing.T) {
	tr := newTranscript(freezePolicy{Freeze: true, After: 0})
	tr.ingest([]hark.Segment{ts("甲乙", true, 0, 100)}, at(0))
	tr.tick(at(0))
	tr.ingest([]hark.Segment{ts("甲", true, 0, 50), ts("乙", true, 50, 100), ts("丙", true, 100, 200)}, at(10))
	segs, _, _ := tr.state()
	if len(segs) != 2 || segs[1].AudioRange.StartSample != 100 {
		t.Fatalf("segs = %+v", segs)
	}
	tr.tick(at(10))                                                                       // freeze "丙"
	tr.ingest([]hark.Segment{ts("甲乙并。", true, 0, 200), ts("丁", false, 200, 300)}, at(20)) // rewrites frozen text; must align by time
	if got := texts(tr); !reflect.DeepEqual(got, []string{"甲乙", "丙", "丁"}) {
		t.Fatalf("got %q", got)
	}
}

// When segments aren't sorted by time, they are filtered one by one.
func TestTimeAlignUnordered(t *testing.T) {
	tr := newTranscript(freezePolicy{Freeze: true, After: 0})
	tr.ingest([]hark.Segment{ts("旧", true, 0, 100)}, at(0))
	tr.tick(at(0))
	tr.ingest([]hark.Segment{ts("改", true, 0, 80), ts("新", false, 100, 150), ts("旧尾", false, 80, 100)}, at(10))
	if got := texts(tr); !reflect.DeepEqual(got, []string{"旧", "新"}) {
		t.Fatalf("got %q", got)
	}
}

// commit covers metadata: a changed speaker on a frozen segment is ignored and counted.
func TestFrozenSpeakerIsImmutable(t *testing.T) {
	tr := newTranscript(freezePolicy{Freeze: true, After: 0})
	tr.ingest([]hark.Segment{{Text: "你好。", Final: true, Speaker: "A"}}, at(0))
	tr.tick(at(0))
	tr.ingest([]hark.Segment{{Text: "你好。", Final: true, Speaker: "B"}, {Text: "嗯", Speaker: "B"}}, at(10))
	segs, _, _ := tr.state()
	if segs[0].Speaker != "A" || segs[1].Speaker != "B" || tr.dropped != 1 {
		t.Fatalf("segs %+v dropped %d", segs, tr.dropped)
	}
	// a changed speaker on an unfrozen segment resets the freeze timer
	tr.ingest([]hark.Segment{{Text: "你好。", Final: true, Speaker: "A"}, {Text: "嗯", Final: true, Speaker: "C"}}, at(20))
	if d, _ := tr.nextDeadline(); !d.Equal(at(20)) {
		t.Fatalf("deadline %v", d)
	}
}
