package vocab

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestExtract(t *testing.T) {
	cases := []struct {
		before, after string
		want          []pair
	}{
		{"我们用哈克做网关。", "我们用hark做网关。", []pair{{"哈克", "hark"}}},
		{"I like hark a lot.", "I like hack a lot.", []pair{{"hark", "hack"}}},
		// A single-character change is padded with context.
		{"他们来了。", "她们来了。", []pair{{"他们", "她们"}}},
		// A missing Chinese character: a pure insertion is padded into a replacement.
		{"哈克来了", "哈尔克来了", []pair{{"哈克", "哈尔克"}}},
		// Inserting or deleting whole English words isn't learned.
		{"hello world", "hello big world", nil},
		// Don't cross newlines.
		{"哈克\n来了", "hark\n来了", []pair{{"哈克", "hark"}}},
		// Changing only punctuation or whitespace isn't learned.
		{"你好，世界", "你好。世界", nil},
		{"hello world", "hello  world", nil},
		// A change involving punctuation is a sentence rewrite.
		{"今天开会", "今天，不开会了", nil},
		// A long sentence rewrite exceeds the word-level limits.
		{"我们明天下午三点在三楼会议室开会", "下周一上午大家一起去楼下咖啡馆讨论方案", nil},
		// Merging multiple English words.
		{"push to Git Hub now", "push to GitHub now", []pair{{"Git Hub", "GitHub"}}},
		// Two independent changes.
		{"哈克很好。我用瓦斯。", "hark很好。我用wasm。", []pair{{"哈克", "hark"}, {"瓦斯", "wasm"}}},
		// Identical changes count once.
		{"哈克和哈克", "hark和hark", []pair{{"哈克", "hark"}}},
		{"一样", "一样", nil},
	}
	for _, c := range cases {
		got, skipped := extract(c.before, c.after)
		if !reflect.DeepEqual(got, c.want) || skipped != 0 {
			t.Errorf("extract(%q, %q) = %v skipped=%d, want %v", c.before, c.after, got, skipped, c.want)
		}
	}
}

// Sparse corrections in a long text are still learned (sentence alignment doesn't degrade into one big replacement).
func TestExtractLongSparse(t *testing.T) {
	var a, b strings.Builder
	for i := 0; i < 2000; i++ {
		s := fmt.Sprintf("这是第%d句话。", i)
		a.WriteString(s)
		b.WriteString(s)
	}
	before := "哈克开头。" + a.String() + "结尾用瓦斯。"
	after := "hark开头。" + b.String() + "结尾用wasm。"
	got, skipped := extract(before, after)
	want := []pair{{"哈克", "hark"}, {"瓦斯", "wasm"}}
	if !reflect.DeepEqual(got, want) || skipped != 0 {
		t.Fatalf("got %v skipped=%d", got, skipped)
	}
}

// Many distinct short sentences: exceeding the budget skips instead of exhausting memory.
func TestExtractBudget(t *testing.T) {
	var a, b strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&a, "a%d.", i)
		fmt.Fprintf(&b, "b%d.", i)
	}
	_, skipped := extract(a.String(), b.String())
	if skipped == 0 {
		t.Fatal("expected skipped")
	}
}

func TestReplace(t *testing.T) {
	m := compile(map[string]string{"哈克": "hark", "哈克斯": "hacks", "hack": "hark", "他们": "她们"})
	cases := []struct {
		in          string
		left, right rune
		want        string
	}{
		{"用哈克和哈克斯", 0, 0, "用hark和hacks"}, // longest first
		{"hack it", 0, 0, "hark it"},
		{"hacker", 0, 0, "hacker"}, // English word boundary
		{"ihack", 0, 0, "ihack"},
		{"hack", 'x', 0, "hack"}, // the adjacent segment on the left ends in a letter
		{"hack", 0, 'e', "hack"},
		{"中文hack中文", 0, 0, "中文hark中文"},
		{"他们他们", 0, 0, "她们她们"},
		{"没有命中", 0, 0, "没有命中"},
		{"", 0, 0, ""},
	}
	for _, c := range cases {
		if got := m.replace(c.in, c.left, c.right); got != c.want {
			t.Errorf("Replace(%q) = %q, want %q", c.in, got, c.want)
		}
	}
	var nilm *Matcher
	if nilm.replace("哈克", 0, 0) != "哈克" {
		t.Fatal("nil matcher must be identity")
	}
}

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func newStore(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := Open(dir, quiet)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func correct(t *testing.T, s *Store, id, before, after string) Result {
	t.Helper()
	r, err := s.Correct(context.Background(), "alice", id, before, after)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func active(t *testing.T, s *Store) map[string]string {
	t.Helper()
	m, err := s.Matcher("alice")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	if m != nil {
		for _, g := range m.idx {
			for _, r := range g {
				out[r.from] = r.to
			}
		}
	}
	return out
}

func TestLearnFlow(t *testing.T) {
	dir := t.TempDir()
	s := newStore(t, dir)

	if r := correct(t, s, "1", "我们用哈克。", "我们用hark。"); len(r.Activated) != 0 {
		t.Fatalf("one fix must not activate: %+v", r)
	}
	// Retrying with the same id doesn't count twice.
	if r := correct(t, s, "1", "我们用哈克。", "我们用hark。"); !r.Duplicate {
		t.Fatal("expected duplicate")
	}
	if len(active(t, s)) != 0 {
		t.Fatal("must not be active yet")
	}
	r := correct(t, s, "2", "哈克真好用。", "hark真好用。")
	if len(r.Activated) != 1 || r.Activated[0].From != "哈克" || r.Activated[0].To != "hark" {
		t.Fatalf("expected activation: %+v", r)
	}
	if got := active(t, s); got["哈克"] != "hark" {
		t.Fatalf("active = %v", got)
	}

	// The user changes the replacement back to the original → deactivated, and never learned back automatically.
	r = correct(t, s, "3", "我说的是hark。", "我说的是哈克。")
	if len(r.Deactivated) != 1 || len(active(t, s)) != 0 {
		t.Fatalf("expected deactivation: %+v %v", r, active(t, s))
	}
	correct(t, s, "4", "哈克。", "hark。")
	correct(t, s, "5", "哈克！", "hark！")
	if len(active(t, s)) != 0 {
		t.Fatal("rejected word must not be relearned")
	}
	// A manual add restores it.
	if _, err := s.Put("alice", "哈克", "hark"); err != nil {
		t.Fatal(err)
	}
	if active(t, s)["哈克"] != "hark" {
		t.Fatal("manual put must activate")
	}

	// Persistence: after closing and reopening, the state is identical.
	before, _ := s.Words("alice")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Correct(context.Background(), "alice", "6", "a", "b"); err != ErrClosed {
		t.Fatalf("after close: %v", err)
	}
	s2 := newStore(t, dir)
	defer s2.Close()
	after, _ := s2.Words("alice")
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("reload mismatch:\n%+v\n%+v", before, after)
	}
	if r := correct(t, s2, "1", "x", "y"); !r.Duplicate {
		t.Fatal("recent ids must persist")
	}
}

// A rewrite isn't a correction: the user kept From in other transcripts, so the ratio is too low to activate.
func TestRewriteNotLearned(t *testing.T) {
	s := newStore(t, t.TempDir())
	defer s.Close()
	correct(t, s, "1", "明天开会。", "后天开会。")
	correct(t, s, "2", "明天见。", "明天见。") // appears but not corrected
	correct(t, s, "3", "明天吃饭。", "明天吃饭。")
	correct(t, s, "4", "明天开会。", "后天开会。")
	if got := active(t, s); len(got) != 0 {
		t.Fatalf("rewrite learned: %v", got)
	}
}

// An auto rule is replaced only by one with strictly more evidence; a manual rule is never replaced by an auto rule.
func TestCompetition(t *testing.T) {
	s := newStore(t, t.TempDir())
	defer s.Close()
	for i := 0; i < 2; i++ {
		correct(t, s, fmt.Sprint("a", i), "用瓦斯。", "用wasm。")
	}
	for i := 0; i < 2; i++ {
		correct(t, s, fmt.Sprint("b", i), "用瓦斯。", "用WASM。")
	}
	if got := active(t, s)["瓦斯"]; got != "wasm" {
		t.Fatalf("tie must keep incumbent, got %q", got)
	}
	if _, err := s.Put("alice", "瓦斯", "Wasm"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		correct(t, s, fmt.Sprint("c", i), "用瓦斯。", "用WASM。")
	}
	if got := active(t, s)["瓦斯"]; got != "Wasm" {
		t.Fatalf("manual must win, got %q", got)
	}
	if n, _ := s.Delete("alice", "瓦斯", ""); n != 3 {
		t.Fatalf("delete n = %d", n)
	}
	if len(active(t, s)) != 0 {
		t.Fatal("delete must deactivate")
	}
}

func TestOpenRejects(t *testing.T) {
	garbage := t.TempDir()
	os.WriteFile(filepath.Join(garbage, "vocab.db"), []byte("not sqlite at all, definitely not sqlite"), 0o644)
	if _, err := Open(garbage, quiet); err == nil {
		t.Error("garbage file accepted")
	}
	for _, setup := range []string{"CREATE TABLE other (x)", "PRAGMA user_version = 2"} {
		dir := t.TempDir()
		db, err := sql.Open("sqlite", filepath.Join(dir, "vocab.db"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.Exec(setup); err != nil {
			t.Fatal(err)
		}
		db.Close()
		if _, err := Open(dir, quiet); err == nil {
			t.Errorf("%s: accepted", setup)
		}
	}
}

// Two Stores (standing in for two processes) open one database for the first time at once and share it:
// each sees the other's changes, and concurrent writes aren't lost.
func TestTwoStores(t *testing.T) {
	dir := t.TempDir()
	stores := make([]*Store, 2)
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for i := range stores {
		wg.Add(1)
		go func() { defer wg.Done(); stores[i], errs[i] = Open(dir, quiet) }()
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		t.Fatal(err)
	}
	a, b := stores[0], stores[1]
	defer a.Close()
	defer b.Close()
	if _, err := a.Put("alice", "瓦斯", "wasm"); err != nil {
		t.Fatal(err)
	}
	if active(t, b)["瓦斯"] != "wasm" {
		t.Fatal("b does not see a's write")
	}
	// Concurrent submissions with different ids: none lost; the same id submitted on both sides at once: counted once.
	ids := []string{"1", "2", "3", "4", "dup", "dup"}
	dups := make([]bool, len(ids))
	for i, id := range ids {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := stores[i%2].Correct(context.Background(), "alice", id, "我们用哈克。", "我们用hark。")
			if err != nil {
				t.Error(err)
			}
			dups[i] = r.Duplicate
		}()
	}
	wg.Wait()
	if dups[4] == dups[5] {
		t.Fatalf("same id: duplicate = %v, %v", dups[4], dups[5])
	}
	words, err := a.Words("alice")
	if err != nil {
		t.Fatal(err)
	}
	fixed := -1
	for _, w := range words {
		if w.From == "哈克" && w.To == "hark" {
			fixed = w.Fixed
		}
	}
	if fixed != 5 {
		t.Fatalf("fixed = %d, want 5", fixed)
	}
}

// A write fails midway (demoting the old rule succeeds, promoting the new one fails): the whole transaction
// rolls back, leaving entries and rules unchanged.
func TestFailedWriteRollsBack(t *testing.T) {
	s := newStore(t, t.TempDir())
	defer s.Close()
	ctx := context.Background()
	if _, err := s.Put("alice", "瓦斯", "wasm"); err != nil {
		t.Fatal(err)
	}
	correct(t, s, "1", "我们用哈克。", "我们用hark。")
	before, _ := s.Words("alice")
	exec := func(q string) {
		t.Helper()
		if _, err := s.db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	exec(`CREATE TRIGGER boom BEFORE INSERT ON words WHEN NEW."to" = 'boom' BEGIN SELECT RAISE(ABORT, 'boom'); END`)
	// Correction: fails writing the entry (fixed 1→2) after the dedup record was inserted successfully.
	exec(`CREATE TRIGGER boom2 BEFORE UPDATE ON words WHEN NEW.fixed = 2 BEGIN SELECT RAISE(ABORT, 'boom'); END`)

	if w, err := s.Put("alice", "瓦斯", "boom"); err == nil || w != (Word{}) {
		t.Fatalf("put: %v %+v", err, w)
	}
	if _, err := s.Correct(ctx, "alice", "2", "哈克真好用。", "hark真好用。"); err == nil {
		t.Fatal("correct must fail")
	}
	after, _ := s.Words("alice")
	if !reflect.DeepEqual(before, after) || active(t, s)["瓦斯"] != "wasm" || len(active(t, s)) != 1 {
		t.Fatalf("db changed:\n%+v\n%+v", before, after)
	}
	exec("DROP TRIGGER boom2")
	// The dedup record rolled back together with the entries: retrying the same id isn't a duplicate.
	if r := correct(t, s, "2", "哈克真好用。", "hark真好用。"); r.Duplicate || len(r.Activated) != 1 {
		t.Fatalf("retry: %+v", r)
	}
}

// Only the most recent maxRecent dedup ids are kept, and this still holds after reopening.
func TestRecentOverflow(t *testing.T) {
	dir := t.TempDir()
	s := newStore(t, dir)
	s.db.SetMaxOpenConns(1)
	s.db.Exec("PRAGMA synchronous = OFF") // only to speed up the test
	for i := range maxRecent + 10 {
		correct(t, s, fmt.Sprint(i), "a", "b")
	}
	s.Close()
	s2 := newStore(t, dir)
	defer s2.Close()
	if r := correct(t, s2, fmt.Sprint(maxRecent+9), "a", "b"); !r.Duplicate {
		t.Fatal("newest id forgotten")
	}
	if r := correct(t, s2, "0", "a", "b"); r.Duplicate {
		t.Fatal("oldest id must be pruned")
	}
}

// Data is on disk when the call returns: the process exits without Close and the data survives.
func TestDurableWithoutClose(t *testing.T) {
	if dir := os.Getenv("VOCAB_CRASH_DIR"); dir != "" {
		s, err := Open(dir, quiet)
		if err == nil {
			_, err = s.Put("alice", "哈克", "hark")
		}
		if err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestDurableWithoutClose$")
	cmd.Env = append(os.Environ(), "VOCAB_CRASH_DIR="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child: %v %s", err, out)
	}
	s := newStore(t, dir)
	defer s.Close()
	if active(t, s)["哈克"] != "hark" {
		t.Fatal("write lost")
	}
}

// Concurrent reads and writes (with -race), finally racing Close: overlapping calls either succeed or return ErrClosed.
func TestConcurrent(t *testing.T) {
	s := newStore(t, t.TempDir())
	run := func(u string, j int) error {
		if _, err := s.Correct(context.Background(), u, fmt.Sprint(j), "我们用哈克。", "我们用hark。"); err != nil {
			return err
		}
		if _, err := s.Matcher(u); err != nil {
			return err
		}
		_, err := s.Words(u)
		return err
	}
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 20 {
				if err := run(fmt.Sprint("u", i), j); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for i := range 4 {
		if m, _ := s.Matcher(fmt.Sprint("u", i)); m.replace("哈克", 0, 0) != "hark" {
			t.Errorf("u%d: rule not learned", i)
		}
	}
	for i := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range 20 {
				if err := run(fmt.Sprint("v", i), j); err != nil && err != ErrClosed {
					t.Error(err)
					return
				}
			}
		}()
	}
	s.Close()
	wg.Wait()
	if _, err := s.Matcher("u0"); err != ErrClosed {
		t.Fatalf("after close: %v", err)
	}
}

func TestUnknownUserNotCreated(t *testing.T) {
	s := newStore(t, t.TempDir())
	defer s.Close()
	m, err := s.Matcher("nobody")
	w, _ := s.Words("nobody")
	if m != nil || err != nil || len(w) != 0 {
		t.Fatal("unknown user")
	}
	var n int
	s.db.QueryRow("SELECT count(*) FROM words").Scan(&n)
	if n != 0 {
		t.Fatal("must not write")
	}
}

func TestNormalizeUser(t *testing.T) {
	for in, want := range map[string]string{"Alice": "alice", "a.b-c_1": "a.b-c_1"} {
		if got, err := NormalizeUser(in); err != nil || got != want {
			t.Errorf("%q → %q %v", in, got, err)
		}
	}
	for _, in := range []string{"", "a/b", "中文", strings.Repeat("a", 65)} {
		if _, err := NormalizeUser(in); err == nil {
			t.Errorf("%q must be rejected", in)
		}
	}
}

// A long text with many shared unique short sentences and a single change: the anchor path.
func BenchmarkExtractManyAnchors(b *testing.B) {
	var a strings.Builder
	for i := 0; i < 5000; i++ {
		fmt.Fprintf(&a, "第%d句。", i)
	}
	before := a.String() + "用哈克。"
	after := a.String() + "用hark。"
	if len(before) > maxTextBytes {
		b.Fatalf("input too large: %d", len(before))
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if p, _ := extract(before, after); len(p) != 1 {
			b.Fatal(p)
		}
	}
}
