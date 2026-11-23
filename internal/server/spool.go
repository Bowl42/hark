package server

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/Bowl42/hark/api"
)

// spoolMemory is how much committed text (as JSON) a final-mode track keeps in memory before moving it to a file.
// Most sessions stay below it and never touch the disk.
const spoolMemory = 64 << 10

// spool holds a final-mode track's committed segments until track_end, as comma-separated JSON: up to spoolMemory
// in memory, the rest appended to a temporary file. So memory doesn't grow with the length of the session, and
// track_end is streamed from it rather than built in memory.
//
// Storage errors are not papered over: the first one is returned by add (the track then fails) and every later
// call returns it again. The file is unlinked right after it is created where the OS allows, so a crash leaves
// nothing behind; otherwise close removes it.
type spool struct {
	dir  string // "" = os.TempDir()
	buf  []byte
	f    *os.File
	path string // the file's name while it still has one (unlink failed); removed by close
	n    int    // segments added
	err  error
}

func (s *spool) add(segs []api.Segment) error {
	if s.err != nil {
		return s.err
	}
	for _, seg := range segs {
		b, err := json.Marshal(seg)
		if err != nil {
			s.err = err
			return err
		}
		if s.n > 0 {
			s.buf = append(s.buf, ',')
		}
		s.buf = append(s.buf, b...)
		s.n++
		if len(s.buf) >= spoolMemory {
			if s.err = s.flush(); s.err != nil {
				return s.err
			}
		}
	}
	return nil
}

// flush moves the buffer to the file. A failed (possibly partial) write leaves the spool unusable.
func (s *spool) flush() error {
	if s.f == nil {
		f, err := os.CreateTemp(s.dir, "hark-*.spool")
		if err != nil {
			return fmt.Errorf("spool: %w", err)
		}
		s.f, s.path = f, f.Name()
		if os.Remove(s.path) == nil {
			s.path = ""
		}
	}
	if _, err := s.f.Write(s.buf); err != nil {
		return fmt.Errorf("spool: %w", err)
	}
	if cap(s.buf) > 2*spoolMemory { // one huge segment: don't keep its allocation
		s.buf = nil
	} else {
		s.buf = s.buf[:0]
	}
	return nil
}

// writeTo writes every segment added, comma-separated (the body of a JSON array).
func (s *spool) writeTo(w io.Writer) error {
	if s.err != nil {
		return s.err
	}
	if s.f != nil {
		if _, err := s.f.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("spool: %w", err)
		}
		if _, err := io.Copy(w, s.f); err != nil {
			return fmt.Errorf("spool: %w", err)
		}
	}
	_, err := w.Write(s.buf)
	return err
}

// close releases the file (safe on a nil spool and more than once).
func (s *spool) close() {
	if s == nil {
		return
	}
	if s.f != nil {
		s.f.Close()
		if s.path != "" {
			os.Remove(s.path)
		}
	}
	s.f, s.path, s.buf = nil, "", nil
}

// finalEnd is a final-mode track_end whose snapshot segments come from a spool. It is written as a stream, so the
// whole transcript is never in memory at once. The spool stays owned by the track (the job closes it after the
// writer has exited), whether or not this message is ever written.
type finalEnd struct {
	msg            api.TrackEnd // Snapshot unset
	spool          *spool
	stable, commit int
}

func (m *finalEnd) writeTo(w io.Writer) error {
	head, err := json.Marshal(m.msg)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(w)
	bw.Write(head[:len(head)-1]) // drop the closing brace: the snapshot follows
	bw.WriteString(`,"snapshot":{"segments":[`)
	if err := m.spool.writeTo(bw); err != nil {
		return err
	}
	fmt.Fprintf(bw, `],"stable":%d,"commit":%d}}`, m.stable, m.commit)
	return bw.Flush()
}
