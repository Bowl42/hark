package wav

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestEncode(t *testing.T) {
	pcm := []byte{1, 2, 3, 4, 5, 6}
	b := Encode(pcm)
	le := binary.LittleEndian
	if len(b) != 44+len(pcm) || string(b[:4]) != "RIFF" || le.Uint32(b[4:]) != uint32(len(b)-8) ||
		string(b[8:16]) != "WAVEfmt " || le.Uint32(b[16:]) != 16 || le.Uint16(b[20:]) != 1 || le.Uint16(b[22:]) != 1 ||
		le.Uint32(b[24:]) != 16000 || le.Uint32(b[28:]) != 32000 || le.Uint16(b[32:]) != 2 || le.Uint16(b[34:]) != 16 ||
		string(b[36:40]) != "data" || le.Uint32(b[40:]) != uint32(len(pcm)) {
		t.Fatalf("bad header % x", b[:44])
	}
	path := filepath.Join(t.TempDir(), "a.wav")
	os.WriteFile(path, b, 0o644)
	got, ch, err := Read(path)
	if err != nil || ch != 1 || !bytes.Equal(got, pcm) {
		t.Fatalf("round trip: %v %d %v", got, ch, err)
	}
}
