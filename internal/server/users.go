package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/Bowl42/hark/vocab"
)

const maxUserBody = 256 << 10

// userReq does the common checks for the personal vocabulary endpoints: authentication, whether it is enabled, and the user name.
// On failure it has already written the response and returns "".
func (s *Server) userReq(w http.ResponseWriter, r *http.Request) string {
	if !s.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return ""
	}
	if s.cfg.Vocab == nil {
		http.Error(w, "personal vocabulary is disabled (set data_dir)", http.StatusNotFound)
		return ""
	}
	name, err := vocab.NormalizeUser(r.PathValue("user"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return ""
	}
	return name
}

// decodeBody parses strictly: it limits the size and rejects unknown fields and trailing data.
func decodeBody(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxUserBody))
	dec.DisallowUnknownFields()
	err := dec.Decode(v)
	if err == nil && dec.Decode(&struct{}{}) != io.EOF {
		err = errors.New("unexpected data after JSON object")
	}
	if err != nil {
		code := http.StatusBadRequest
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			code = http.StatusRequestEntityTooLarge
		}
		http.Error(w, fmt.Sprintf("invalid body: %v", err), code)
		return false
	}
	return true
}

func writeResult(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (s *Server) vocabError(w http.ResponseWriter, user string, err error) {
	switch {
	case errors.Is(err, vocab.ErrTooLarge):
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)
	case errors.Is(err, vocab.ErrClosed):
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
	default:
		s.log.Error("vocab", "user", user, "err", err)
		http.Error(w, "personal vocabulary unavailable", http.StatusInternalServerError)
	}
}

func (s *Server) handleWords(w http.ResponseWriter, r *http.Request) {
	name := s.userReq(w, r)
	if name == "" {
		return
	}
	words, err := s.cfg.Vocab.Words(name)
	if err != nil {
		s.vocabError(w, name, err)
		return
	}
	writeResult(w, map[string]any{"words": words})
}

func (s *Server) handlePutWord(w http.ResponseWriter, r *http.Request) {
	name := s.userReq(w, r)
	if name == "" {
		return
	}
	var req struct {
		From string `json:"from"`
		To   string `json:"to"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	word, err := s.cfg.Vocab.Put(name, req.From, req.To)
	if err != nil {
		if errors.Is(err, vocab.ErrBadWord) || errors.Is(err, vocab.ErrTooManyWords) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			s.vocabError(w, name, err)
		}
		return
	}
	writeResult(w, word)
}

func (s *Server) handleDeleteWords(w http.ResponseWriter, r *http.Request) {
	name := s.userReq(w, r)
	if name == "" {
		return
	}
	from := r.URL.Query().Get("from")
	if from == "" {
		http.Error(w, "from is required", http.StatusBadRequest)
		return
	}
	n, err := s.cfg.Vocab.Delete(name, from, r.URL.Query().Get("to"))
	if err != nil {
		s.vocabError(w, name, err)
		return
	}
	writeResult(w, map[string]int{"rejected": n})
}

func (s *Server) handleCorrections(w http.ResponseWriter, r *http.Request) {
	name := s.userReq(w, r)
	if name == "" {
		return
	}
	var req struct {
		ID     string `json:"id"`
		Before string `json:"before"`
		After  string `json:"after"`
	}
	if !decodeBody(w, r, &req) {
		return
	}
	if len(req.ID) > 128 {
		http.Error(w, "id must be at most 128 bytes", http.StatusBadRequest)
		return
	}
	res, err := s.cfg.Vocab.Correct(r.Context(), name, req.ID, req.Before, req.After)
	if err != nil {
		s.vocabError(w, name, err)
		return
	}
	writeResult(w, res)
}
