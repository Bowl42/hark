// Package openai connects to OpenAI-compatible transcription APIs (POST {base_url}/audio/transcriptions).
//
// This API is a de facto standard: cloud services such as OpenAI, Groq, and SiliconFlow, as well as self-hosted
// speaches (faster-whisper), vLLM, LocalAI, and whisper.cpp server (--inference-path /v1/audio/transcriptions)
// all implement it. Only the part common to all of them is used: a multipart upload of a wav, the fields
// model / response_format=json / language, and a top-level {"text": "..."} in the reply.
// If a particular model has extra requirements (for example, language must be specified), set them in the config.
//
// It satisfies batch.Recognizer: batch segments sentences and each sentence is recognized once. Don't set interval
// for services billed per request or per duration, or the whole sentence is resent from the start every interval
// while speaking, multiplying the cost; a self-hosted service that is fast enough can use 500ms to show text while
// speaking.
//
// As a library:
//
//	rec, err := openai.New(openai.Config{APIKey: os.Getenv("OPENAI_API_KEY"), Model: "gpt-4o-transcribe"})
//	eng := batch.New(rec, batch.Options{})
//
// Config file (harkcmd):
//
//	openai:
//	  type: openai
//	  api_key: ${OPENAI_API_KEY}
//	  model: gpt-4o-transcribe
//	  # base_url: http://10.0.0.5:8000/v1   # other compatible services; written the same way as the OpenAI SDK's base_url
//	  # language: zh                        # optional, ISO-639-1; noticeably reduces wrong-language detection on short sentences
//	  # plus the common batch fields: interval / silence / max_sentence, see harkcmd.Batch
package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/internal/wav"
)

type Config struct {
	BaseURL  string `yaml:"base_url"` // default https://api.openai.com/v1
	APIKey   string `yaml:"api_key"`  // optional; self-hosted services often don't need it
	Model    string `yaml:"model"`    // required
	Language string `yaml:"language"` // optional
}

const (
	defaultBaseURL = "https://api.openai.com/v1"
	timeout        = 30 * time.Second // limit for a single request
	maxReply       = 1 << 20
)

// New creates a recognizer, which batch.New turns into a streaming engine. It is safe for concurrent use and reuses connections.
func New(c Config) (*Recognizer, error) {
	if c.Model == "" {
		return nil, errors.New("openai: model is required")
	}
	if c.BaseURL == "" {
		c.BaseURL = defaultBaseURL
	}
	u, err := url.Parse(c.BaseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Opaque != "" || u.Hostname() == "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return nil, fmt.Errorf("openai: base_url must be an http(s) URL without query, got %q", c.BaseURL)
	}
	return &Recognizer{cfg: c, url: u.JoinPath("audio", "transcriptions").String()}, nil
}

type Recognizer struct {
	cfg Config
	url string
}

func (r *Recognizer) Recognize(ctx context.Context, _ hark.Request, pcm []byte) (string, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	h := textproto.MIMEHeader{}
	h.Set("Content-Disposition", `form-data; name="file"; filename="audio.wav"`)
	h.Set("Content-Type", "audio/wav")
	fw, _ := mw.CreatePart(h) // writing to a bytes.Buffer can't fail
	fw.Write(wav.Encode(pcm))
	mw.WriteField("model", r.cfg.Model)
	mw.WriteField("response_format", "json")
	if r.cfg.Language != "" {
		mw.WriteField("language", r.cfg.Language)
	}
	mw.Close()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, r.url, &body)
	if err != nil {
		return "", fmt.Errorf("openai: %w", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if r.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+r.cfg.APIKey)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("openai: %w", err)
	}
	defer resp.Body.Close()
	reply, err := io.ReadAll(io.LimitReader(resp.Body, maxReply+1))
	if resp.StatusCode != http.StatusOK { // status code first: report it even if the reply couldn't be read completely
		return "", fmt.Errorf("openai: status %d: %s", resp.StatusCode, strings.TrimSpace(string(reply[:min(len(reply), 200)])))
	}
	if err != nil {
		return "", fmt.Errorf("openai: read reply: %w", err)
	}
	if len(reply) > maxReply {
		return "", errors.New("openai: reply too large")
	}
	var out struct {
		Text *string `json:"text"`
	}
	if err := json.Unmarshal(reply, &out); err != nil || out.Text == nil {
		return "", fmt.Errorf("openai: bad reply: %.200s", reply)
	}
	return *out.Text, nil
}
