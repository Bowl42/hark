// Package httpasr is a generic HTTP recognizer: it connects any "whole audio in, text out" HTTP service as a
// hark backend, through configuration alone, with no Go code.
//
// It satisfies batch.Recognizer; batch handles sentence segmentation, re-recognition while speaking, and
// finalization. The external service only needs to implement one endpoint:
//
//	POST <url>
//	Content-Type: application/octet-stream           body: PCM s16le (little-endian) / 16kHz / mono
//	Authorization: Bearer <token>                    sent only when a token is configured
//	X-Hark-Session: <session ID>                     for matching up logs
//
//	200 {"text": "recognized text"}                  an empty string means no content (e.g. just noise)
//	any other status code counts as a failure; this track ends with engine_error
//
// With interval set, the whole sentence is resent from the start every interval while speaking, so the service
// must be much faster than real time; without it, each sentence is recognized only once.
// See examples/plugins for a reference implementation.
//
// Configuration:
//
//	whisper:
//	  type: http
//	  url: http://10.0.0.5:9000/recognize
//	  token: ${WHISPER_TOKEN}   # optional
//	  # plus the common batch fields: interval / silence / max_sentence, see harkcmd.Batch
package httpasr

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Bowl42/hark"
)

type Config struct {
	URL   string `yaml:"url"`
	Token string `yaml:"token"`
}

// New creates a recognizer, which batch.New turns into a streaming engine. It is safe for concurrent use and reuses connections.
func New(c Config) (*Recognizer, error) {
	if c.URL == "" {
		return nil, errors.New("http: url is required")
	}
	return &Recognizer{cfg: c}, nil
}

type Recognizer struct{ cfg Config }

// timeout is the limit for a single request.
const timeout = 30 * time.Second

func (r *Recognizer) Recognize(ctx context.Context, req hark.Request, pcm []byte) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, r.cfg.URL, bytes.NewReader(pcm))
	if err != nil {
		return "", fmt.Errorf("http: %w", err)
	}
	hr.Header.Set("Content-Type", "application/octet-stream") // not audio/L16: that type is defined as big-endian
	if r.cfg.Token != "" {
		hr.Header.Set("Authorization", "Bearer "+r.cfg.Token)
	}
	if req.SessionID != "" {
		hr.Header.Set("X-Hark-Session", req.SessionID)
	}
	resp, err := http.DefaultClient.Do(hr)
	if err != nil {
		return "", fmt.Errorf("http: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("http: read reply: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("http: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body[:min(len(body), 200)])))
	}
	var out struct {
		Text *string `json:"text"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.Text == nil {
		return "", fmt.Errorf("http: bad reply: %.200s", body)
	}
	return *out.Text, nil
}
