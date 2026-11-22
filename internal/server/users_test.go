package server_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/engine/mock"
	"github.com/Bowl42/hark/internal/server"
	"github.com/Bowl42/hark/vocab"
)

func vocabServer(t *testing.T, tokens ...string) (*httptest.Server, *vocab.Store) {
	t.Helper()
	store, err := vocab.Open(t.TempDir(), quiet)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	ts := start(t, server.Config{Vocab: store, Tokens: tokens},
		server.Instance{Name: "mock", Type: "mock", Engine: must(mock.New(mock.Config{}))})
	return ts, store
}

func call(t *testing.T, method, url, token, body string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	b.ReadFrom(resp.Body)
	return resp.StatusCode, b.String()
}

// Full loop: the user corrects twice → the next recognition replaces automatically → undo → back to the original.
func TestPersonalVocabulary(t *testing.T) {
	ts, _ := vocabServer(t)
	const final = "今天天气很好。我们去公园吧。"
	post := func(id string) vocab.Result {
		body, _ := json.Marshal(map[string]string{"id": id, "before": final, "after": strings.Replace(final, "公园", "花园", 1)})
		code, out := call(t, "POST", ts.URL+"/v1/users/Alice/corrections", "", string(body))
		if code != 200 {
			t.Fatalf("corrections: %d %s", code, out)
		}
		var r vocab.Result
		json.Unmarshal([]byte(out), &r)
		return r
	}
	speak := func() string {
		st := pcmStart("v", 1)
		st.User = "alice"
		r := dial(t, ts, "").speak(st)
		if r.end.Status != api.StatusComplete {
			t.Fatalf("end = %+v", r.end)
		}
		return r.docs[0].Text()
	}

	if got := speak(); got != final {
		t.Fatalf("no vocabulary yet: %q", got)
	}
	if r := post("s1:0"); len(r.Activated) != 0 {
		t.Fatalf("first: %+v", r)
	}
	if r := post("s1:0"); !r.Duplicate {
		t.Fatalf("retry: %+v", r)
	}
	r := post("s2:0")
	if len(r.Activated) != 1 || r.Activated[0].From != "去公园" {
		t.Fatalf("second: %+v", r)
	}
	if got := speak(); got != strings.Replace(final, "公园", "花园", 1) {
		t.Fatalf("vocabulary not applied: %q", got)
	}

	code, out := call(t, "GET", ts.URL+"/v1/users/alice/words", "", "")
	if code != 200 || !strings.Contains(out, `"state":"active"`) {
		t.Fatalf("words: %d %s", code, out)
	}
	from := r.Activated[0].From
	code, out = call(t, "DELETE", ts.URL+"/v1/users/alice/words?from="+from, "", "")
	if code != 200 || !strings.Contains(out, `"rejected":1`) {
		t.Fatalf("delete: %d %s", code, out)
	}
	if got := speak(); got != final {
		t.Fatalf("after undo: %q", got)
	}

	code, out = call(t, "POST", ts.URL+"/v1/users/alice/words", "", `{"from":"天气","to":"天氣"}`)
	if code != 200 {
		t.Fatalf("put: %d %s", code, out)
	}
	if got := speak(); got != strings.Replace(final, "天气", "天氣", 1) {
		t.Fatalf("manual word: %q", got)
	}
}

func TestVocabularyHTTPErrors(t *testing.T) {
	ts, _ := vocabServer(t, "secret")
	cases := []struct {
		method, path, token, body string
		code                      int
	}{
		{"GET", "/v1/users/alice/words", "", "", 401},
		{"GET", "/v1/users/alice/words", "secret", "", 200},
		{"GET", "/v1/users/a%2Fb/words", "secret", "", 400},
		{"POST", "/v1/users/alice/corrections", "secret", `{"before":"a","after":"b","x":1}`, 400},
		{"POST", "/v1/users/alice/corrections", "secret", `{"before":"a","after":"b"} {}`, 400},
		{"POST", "/v1/users/alice/corrections", "secret", `{"before":"` + strings.Repeat("a", 70<<10) + `","after":""}`, 413},
		{"POST", "/v1/users/alice/corrections", "secret", `{"before":"` + strings.Repeat("a", 300<<10) + `"}`, 413},
		{"POST", "/v1/users/alice/words", "secret", `{"from":"a","to":"a"}`, 400},
		{"DELETE", "/v1/users/alice/words", "secret", "", 400},
	}
	for _, c := range cases {
		if code, out := call(t, c.method, ts.URL+c.path, c.token, c.body); code != c.code {
			t.Errorf("%s %s %.40s: %d %s, want %d", c.method, c.path, c.body, code, out, c.code)
		}
	}

	// when disabled
	plain := testServer(t, server.Config{})
	if code, _ := call(t, "GET", plain.URL+"/v1/users/alice/words", "", ""); code != 404 {
		t.Fatalf("disabled: %d", code)
	}
	c := dial(t, plain, "")
	st := pcmStart("u", 1)
	st.User = "alice"
	c.send(st)
	if typ, data := c.recv(); typ != api.TypeRejected || !strings.Contains(string(data), "data_dir") {
		t.Fatalf("want rejected: %s", data)
	}
	c2 := dial(t, ts, "secret")
	st.User = "a/b"
	c2.send(st)
	if typ, data := c2.recv(); typ != api.TypeRejected {
		t.Fatalf("want rejected: %s", data)
	}
}
