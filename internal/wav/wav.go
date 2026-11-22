// Package wav reads 16kHz / 16-bit PCM wav files (used by the command-line client and examples).
package wav

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

// Read returns the interleaved s16le samples and the channel count.
func Read(path string) (pcm []byte, channels int, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	if len(b) < 12 || string(b[:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		return nil, 0, errors.New("not a wav file")
	}
	for p := 12; p+8 <= len(b); {
		id, size := string(b[p:p+4]), int(binary.LittleEndian.Uint32(b[p+4:]))
		body := b[p+8 : min(p+8+size, len(b))]
		switch id {
		case "fmt ":
			if len(body) < 16 {
				return nil, 0, errors.New("wav: bad fmt chunk")
			}
			format, rate, bits := binary.LittleEndian.Uint16(body), binary.LittleEndian.Uint32(body[4:]), binary.LittleEndian.Uint16(body[14:])
			channels = int(binary.LittleEndian.Uint16(body[2:]))
			if format != 1 || rate != 16000 || bits != 16 || channels == 0 {
				return nil, 0, fmt.Errorf("need 16kHz 16bit PCM wav, got format %d, %d Hz, %d bit (try: ffmpeg -i in -ar 16000 -sample_fmt s16 out.wav)", format, rate, bits)
			}
		case "data":
			if channels == 0 {
				return nil, 0, errors.New("wav: data before fmt")
			}
			return body[:len(body)/(2*channels)*(2*channels)], channels, nil
		}
		p += 8 + size + size%2
	}
	return nil, 0, errors.New("wav: no data chunk")
}

// Encode prepends a 44-byte wav header to s16le / 16kHz / mono PCM.
func Encode(pcm []byte) []byte {
	const rate, channels, bits = 16000, 1, 16
	b := make([]byte, 44, 44+len(pcm))
	le := binary.LittleEndian
	copy(b[0:], "RIFF")
	le.PutUint32(b[4:], uint32(36+len(pcm)))
	copy(b[8:], "WAVEfmt ")
	le.PutUint32(b[16:], 16)
	le.PutUint16(b[20:], 1) // PCM
	le.PutUint16(b[22:], channels)
	le.PutUint32(b[24:], rate)
	le.PutUint32(b[28:], rate*channels*bits/8) // byte rate
	le.PutUint16(b[32:], channels*bits/8)      // block align
	le.PutUint16(b[34:], bits)
	copy(b[36:], "data")
	le.PutUint32(b[40:], uint32(len(pcm)))
	return append(b, pcm...)
}
