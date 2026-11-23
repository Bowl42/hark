package server

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/Bowl42/hark/api"
)

func TestSpool(t *testing.T) {
	dir := t.TempDir()
	s := &spool{dir: dir}
	defer s.close()
	var want []api.Segment
	add := func(n int) {
		t.Helper()
		var segs []api.Segment
		for range n {
			segs = append(segs, api.Segment{Text: strings.Repeat("字", 30) + `"\` + "\n", Speaker: "S1",
				AudioRange: api.AudioRange{StartSample: int64(len(want)), EndSample: int64(len(want) + 1)}})
			want = append(want, segs[len(segs)-1])
		}
		if err := s.add(segs); err != nil {
			t.Fatal(err)
		}
	}
	read := func() []api.Segment {
		t.Helper()
		var b bytes.Buffer
		b.WriteString("[")
		if err := s.writeTo(&b); err != nil {
			t.Fatal(err)
		}
		b.WriteString("]")
		var got []api.Segment
		if err := json.Unmarshal(b.Bytes(), &got); err != nil {
			t.Fatalf("not a JSON array body: %v", err)
		}
		return got
	}
	add(0)
	if got := read(); len(got) != 0 || s.f != nil {
		t.Fatalf("empty: %+v, file %v", got, s.f)
	}
	add(10)
	if got := read(); len(got) != 10 || s.f != nil {
		t.Fatalf("small spool: %d segments, file %v", len(got), s.f)
	}
	add(2000) // past spoolMemory in one batch: flushed as it goes, the buffer never grows far beyond the threshold
	add(3)
	if s.f == nil || len(s.buf) >= spoolMemory || cap(s.buf) > 2*spoolMemory {
		t.Fatalf("file %v, %d bytes buffered (cap %d)", s.f, len(s.buf), cap(s.buf))
	}
	got := read()
	if len(got) != len(want) {
		t.Fatalf("%d segments, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("segment %d: %+v, want %+v", i, got[i], want[i])
		}
	}
	if left, _ := os.ReadDir(dir); runtime.GOOS != "windows" && len(left) != 0 {
		t.Fatalf("spool file still has a name: %v", left)
	}
	s.close()
	s.close()
}

func TestSpoolError(t *testing.T) {
	s := &spool{dir: filepath.Join(t.TempDir(), "missing")}
	defer s.close()
	big := []api.Segment{{Text: strings.Repeat("x", spoolMemory)}}
	if err := s.add(big); err == nil {
		t.Fatal("add to an unusable directory succeeded")
	}
	if s.add(nil) == nil || s.writeTo(new(bytes.Buffer)) == nil {
		t.Fatal("spool usable after an error")
	}
}
