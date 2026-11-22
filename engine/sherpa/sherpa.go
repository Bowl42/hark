// Package sherpa supports local models: it connects to a sherpa-onnx non-streaming recognition server
// (sherpa-onnx-offline-websocket-server, e.g. SenseVoice).
//
// It only does "whole audio in, text out" and satisfies batch.Recognizer; only with batch.New does it become a
// streaming engine that shows text while speaking. The model runs in a separate process, so hark itself stays pure Go.
// See deploy/sherpa for setup.
//
//	sensevoice:
//	  type: sherpa
//	  url: ws://127.0.0.1:6006
//	  interval: 500ms   # re-recognize while speaking, to show text while speaking
//	  # plus the common batch fields: interval / silence / max_sentence, see harkcmd.Batch
package sherpa

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/Bowl42/hark"
	"github.com/coder/websocket"
)

type Config struct {
	URL string `yaml:"url"`
}

// New creates a recognizer. It is safe for concurrent use.
func New(c Config) (*Recognizer, error) {
	if c.URL == "" {
		return nil, errors.New("sherpa: url is required")
	}
	return &Recognizer{url: c.URL}, nil
}

// Recognizer uses a new connection for each recognition: the server's replies carry no ID, so one connection can recognize only one piece at a time.
type Recognizer struct{ url string }

// maxSamples is the per-piece limit, matching --max-utterance-length=60 in deploy/sherpa/Dockerfile (with a margin).
const maxSamples = 55 * hark.SampleRate

// Recognize protocol: send one binary message = int32 sample rate + int32 byte count + float32 samples (little-endian); the reply is one JSON message {text, ...}.
func (r *Recognizer) Recognize(ctx context.Context, _ hark.Request, pcm []byte) (string, error) {
	n := len(pcm) / 2
	if n == 0 {
		return "", nil
	}
	if n > maxSamples {
		return "", errors.New("sherpa: audio too long")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, r.url, nil)
	if err != nil {
		return "", fmt.Errorf("sherpa: dial: %w", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(8 << 20)

	buf := make([]byte, 8+4*n)
	binary.LittleEndian.PutUint32(buf, hark.SampleRate)
	binary.LittleEndian.PutUint32(buf[4:], uint32(4*n))
	for i := range n {
		v := int16(binary.LittleEndian.Uint16(pcm[2*i:]))
		binary.LittleEndian.PutUint32(buf[8+4*i:], math.Float32bits(float32(v)/32768))
	}
	if err := conn.Write(ctx, websocket.MessageBinary, buf); err != nil {
		return "", fmt.Errorf("sherpa: %w", err)
	}
	_, data, err := conn.Read(ctx)
	if err != nil {
		return "", fmt.Errorf("sherpa: %w", err)
	}
	var reply struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		return "", fmt.Errorf("sherpa: bad reply: %w", err)
	}
	return reply.Text, nil
}
