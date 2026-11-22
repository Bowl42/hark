package httpasr

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Bowl42/hark"
)

func TestRecognize(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || len(body) != 3200 ||
			r.Header.Get("Content-Type") != "application/octet-stream" ||
			r.Header.Get("Authorization") != "Bearer k" || r.Header.Get("X-Hark-Session") != "s1" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Write([]byte(`{"text":"你好"}`))
	}))
	defer ts.Close()
	r, _ := New(Config{URL: ts.URL, Token: "k"})
	text, err := r.Recognize(context.Background(), hark.Request{SessionID: "s1"}, make([]byte, 3200))
	if err != nil || text != "你好" {
		t.Fatalf("%q %v", text, err)
	}
}

func TestErrors(t *testing.T) {
	for reply, want := range map[string]string{
		"500 boom":  "status 500: boom",
		"200 {}":    "bad reply",
		"200 hello": "bad reply",
	} {
		code, body, _ := strings.Cut(reply, " ")
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if code == "500" {
				w.WriteHeader(http.StatusInternalServerError)
			}
			w.Write([]byte(body))
		}))
		r, _ := New(Config{URL: ts.URL})
		_, err := r.Recognize(context.Background(), hark.Request{}, make([]byte, 320))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", reply, err, want)
		}
		ts.Close()
	}
	if _, err := New(Config{}); err == nil {
		t.Error("missing url should fail")
	}
}

// Empty text is a valid result (just noise).
func TestEmptyText(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"text":""}`))
	}))
	defer ts.Close()
	r, _ := New(Config{URL: ts.URL})
	if text, err := r.Recognize(context.Background(), hark.Request{}, make([]byte, 320)); err != nil || text != "" {
		t.Fatalf("%q %v", text, err)
	}
}
