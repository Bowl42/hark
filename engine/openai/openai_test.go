package openai

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/internal/wav"
)

func TestRecognize(t *testing.T) {
	pcm := []byte{1, 2, 3, 4}
	for _, c := range []Config{
		{Model: "m1", APIKey: "k", Language: "zh"},
		{Model: "m2"},
	} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost || r.URL.Path != "/v1/audio/transcriptions" {
				http.Error(w, "bad path "+r.URL.Path, 404)
				return
			}
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			f, fh, err := r.FormFile("file")
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			audio, _ := io.ReadAll(f)
			wantAuth := ""
			if c.APIKey != "" {
				wantAuth = "Bearer " + c.APIKey
			}
			_, hasLang := r.MultipartForm.Value["language"]
			if r.Header.Get("Authorization") != wantAuth || r.FormValue("model") != c.Model ||
				r.FormValue("response_format") != "json" || hasLang != (c.Language != "") || r.FormValue("language") != c.Language ||
				fh.Filename != "audio.wav" || fh.Header.Get("Content-Type") != "audio/wav" || !bytes.Equal(audio, wav.Encode(pcm)) {
				http.Error(w, "bad form", 400)
				return
			}
			w.Write([]byte(`{"text":" 你好","usage":{"type":"duration","seconds":1}}`))
		}))
		c.BaseURL = ts.URL + "/v1/"
		r, err := New(c)
		if err != nil {
			t.Fatal(err)
		}
		text, err := r.Recognize(context.Background(), hark.Request{}, pcm)
		if err != nil || text != " 你好" { // whitespace is handled uniformly by batch
			t.Errorf("%+v: %q %v", c, text, err)
		}
		ts.Close()
	}
}

func TestReplies(t *testing.T) {
	big := `{"text":"` + strings.Repeat("a", maxReply) + `"}`
	for _, c := range []struct {
		code            int
		body, text, err string
	}{
		{200, `{"text":""}`, "", ""}, // just noise
		{500, `{"error":{"message":"boom"}}`, "", `status 500: {"error":{"message":"boom"}}`},
		{200, `{}`, "", "bad reply"},
		{200, `{"text":null}`, "", "bad reply"},
		{200, `{"text":1}`, "", "bad reply"},
		{200, `hello`, "", "bad reply"},
		{200, big, "", "reply too large"},
		{502, big, "", "status 502: {"}, // an oversized error reply still keeps the status code
	} {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(c.code)
			w.Write([]byte(c.body))
		}))
		r, _ := New(Config{BaseURL: ts.URL, Model: "m"})
		text, err := r.Recognize(context.Background(), hark.Request{}, make([]byte, 320))
		if c.err == "" && (err != nil || text != c.text) || c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)) {
			t.Errorf("%d %.40s: %q %v, want %q %q", c.code, c.body, text, err, c.text, c.err)
		}
		ts.Close()
	}
}

func TestConfig(t *testing.T) {
	r, err := New(Config{Model: "m"})
	if err != nil || r.url != "https://api.openai.com/v1/audio/transcriptions" {
		t.Fatalf("default: %v %v", r, err)
	}
	for base, want := range map[string]string{
		"http://h:8000/v1":   "http://h:8000/v1/audio/transcriptions",
		"http://h:8000/v1//": "http://h:8000/v1/audio/transcriptions",
		"http://h":           "http://h/audio/transcriptions",
		"https://h/a/b/":     "https://h/a/b/audio/transcriptions",
	} {
		if r, err := New(Config{Model: "m", BaseURL: base}); err != nil || r.url != want {
			t.Errorf("%s: %v %v, want %s", base, r, err, want)
		}
	}
	for _, base := range []string{"h:8000/v1", "ftp://h/v1", "http:foo", "https:///v1", "http://h/v1?x=1", "http://h/v1?", "http://h/v1#x", "://"} {
		if _, err := New(Config{Model: "m", BaseURL: base}); err == nil {
			t.Errorf("%s: want error", base)
		}
	}
	if _, err := New(Config{}); err == nil {
		t.Error("missing model should fail")
	}
}

// ctx is canceled after the request has reached the server, halfway through reading the reply: return at once without waiting for the service.
func TestCancel(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write([]byte(`{"text":`))
		w.(http.Flusher).Flush()
		close(entered)
		<-release
	}))
	defer ts.Close()
	defer close(release)
	r, _ := New(Config{BaseURL: ts.URL, Model: "m"})
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-entered; cancel() }()
	start := time.Now()
	_, err := r.Recognize(ctx, hark.Request{}, make([]byte, 320))
	if !errors.Is(err, context.Canceled) || time.Since(start) > 2*time.Second {
		t.Fatalf("err = %v after %v", err, time.Since(start))
	}
}
