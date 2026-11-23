// Package vocab implements the personal vocabulary: it learns, from the user's corrections, the words the
// recognizer keeps getting wrong, and replaces them automatically from then on.
//
// Data lives in data_dir/vocab.db (SQLite) with no in-memory cache: every operation reads and writes the
// database directly, each change happens in one transaction, and it is durable by the time the call returns.
// Several hark processes can share one database (on the same machine; don't put it on a network file system).
package vocab

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/Bowl42/hark/api"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

// Entry states.
const (
	StateCandidate = "candidate" // not enough evidence yet
	StateActive    = "active"    // active: replaced automatically in recognized text
	StateRejected  = "rejected"  // rejected by the user: no longer learned automatically, can only be added manually
)

// Entry sources.
const (
	SourceAuto   = "auto"
	SourceManual = "manual"
)

// Limits and thresholds.
const (
	maxActive    = 1000  // active rules per user: when full, no new rules become active and manual adds fail
	maxWords     = 10000 // total entries per user (counted by from→to): when full, the oldest candidate or rejected entry is evicted
	maxRecent    = 512   // how many recent submissions to remember per user, for deduplication
	minFixed     = 2     // corrected this way in at least two submissions
	maxTextBytes = api.MaxCorrectionBytes
	maxManual    = 32 // max characters per side for manual entries
	learnWorkers = 4  // concurrent learning computations
)

var (
	ErrClosed       = errors.New("vocab: store closed")
	ErrBadUser      = errors.New("user must match [a-z0-9._-]{1,64}")
	ErrBadWord      = fmt.Errorf("from/to must be 1..%d characters without newlines or surrounding spaces, and differ", maxManual)
	ErrTooManyWords = fmt.Errorf("too many active words (max %d)", maxActive)
	ErrTooLarge     = fmt.Errorf("before/after must be at most %d bytes", maxTextBytes)
)

var userRe = regexp.MustCompile(`^[a-z0-9._-]{1,64}$`)

// NormalizeUser lowercases a user name: names differing only in case are the same user.
func NormalizeUser(name string) (string, error) {
	n := strings.ToLower(name)
	if !userRe.MatchString(n) {
		return "", ErrBadUser
	}
	return n, nil
}

// Word is one entry.
type Word struct {
	From    string    `json:"from"`
	To      string    `json:"to"`
	State   string    `json:"state"`
	Source  string    `json:"source"`
	Fixed   int       `json:"fixed"` // in how many submissions the user corrected From to To
	Seen    int       `json:"seen"`  // while a candidate, in how many submissions From appeared in the recognized text
	Updated time.Time `json:"updated"`
}

// now is truncated to milliseconds, matching the stored precision, so it reads back identical.
func now() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

// user holds all of one user's entries during a modification (read from the database in a transaction;
// the difference is written back afterwards).
type user struct {
	name  string
	words map[pair]*Word
}

// Store holds every user's vocabulary. It is safe for concurrent use.
type Store struct {
	log   *slog.Logger
	db    *sql.DB
	learn chan struct{} // concurrency limit for learning computations

	// life is read-locked for the whole of each operation and write-locked by Close, so closing never
	// interrupts an operation halfway.
	life   sync.RWMutex
	closed bool
}

// enter starts an operation: it checks the store isn't closed (returning with the life read lock held,
// released by exit) and normalizes the user name.
func (s *Store) enter(name string) (string, error) {
	s.life.RLock()
	if s.closed {
		return "", ErrClosed
	}
	return NormalizeUser(name)
}

func (s *Store) exit() { s.life.RUnlock() }

// Matcher returns a snapshot of the user's currently active rules (later changes don't affect a Matcher
// already obtained), or nil if the user has no vocabulary. User names are case-insensitive; an invalid
// name returns ErrBadUser.
func (s *Store) Matcher(name string) (*Matcher, error) {
	name, err := s.enter(name)
	defer s.exit()
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT "from", "to" FROM words WHERE user = ? AND state = 'active' LIMIT ?`, name, maxActive+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	rules := map[string]string{}
	for rows.Next() {
		var from, to string
		if err := rows.Scan(&from, &to); err != nil {
			return nil, err
		}
		rules[from] = to
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(rules) > maxActive {
		return nil, fmt.Errorf("vocab: user %s: too many active words", name)
	}
	return compile(rules), nil
}

// Words lists all of the user's entries: active ones first, then sorted by from.
func (s *Store) Words(name string) ([]Word, error) {
	name, err := s.enter(name)
	defer s.exit()
	if err != nil {
		return []Word{}, err
	}
	words, err := load(s.db, name, `ORDER BY CASE state WHEN 'active' THEN 0 WHEN 'candidate' THEN 1 ELSE 2 END, "from", "to"`)
	if err != nil {
		return []Word{}, err
	}
	return words, nil
}

// modify reads all of the user's entries in one transaction, lets fn modify them in memory, then writes
// the difference back. If fn or the write fails, the whole transaction is rolled back.
// fn may use tx for other reads and writes (such as dedup records), committed in the same transaction as the entries.
func (s *Store) modify(name string, fn func(tx *sql.Tx, u *user) error) error {
	name, err := s.enter(name)
	defer s.exit()
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(context.Background(), nil) // BEGIN IMMEDIATE: take the write lock before reading, so nobody can interleave with the read-modify-write
	if err != nil {
		return err
	}
	defer tx.Rollback()
	list, err := load(tx, name, "")
	if err != nil {
		return err
	}
	u := &user{name: name, words: make(map[pair]*Word, len(list))}
	old := make(map[pair]Word, len(list))
	for i := range list {
		w := &list[i]
		u.words[pair{w.From, w.To}] = w
		old[pair{w.From, w.To}] = *w
	}
	if err := fn(tx, u); err != nil {
		return err
	}
	if err := save(tx, u, old); err != nil {
		return err
	}
	return tx.Commit()
}

func validManual(s string) bool {
	return s != "" && utf8.RuneCountInString(s) <= maxManual && !strings.ContainsAny(s, "\r\n") &&
		strings.TrimSpace(s) == s
}

// Put manually adds a rule. It becomes active immediately, replacing any other active rule for the same From.
func (s *Store) Put(name, from, to string) (Word, error) {
	if !validManual(from) || !validManual(to) || from == to {
		return Word{}, ErrBadWord
	}
	var out Word
	err := s.modify(name, func(_ *sql.Tx, u *user) error {
		t := now()
		replaced := false
		for _, o := range u.words {
			if o.From == from && o.State == StateActive {
				replaced = true
				if o.To != to {
					o.State, o.Updated = StateCandidate, t
				}
			}
		}
		if !replaced && u.active() >= maxActive {
			return ErrTooManyWords
		}
		w := u.words[pair{from, to}]
		if w == nil {
			w = u.addCandidate(pair{from, to}, t)
		}
		w.State, w.Source, w.Updated = StateActive, SourceManual, t
		out = *w
		return nil
	})
	if err != nil {
		return Word{}, err
	}
	return out, nil
}

// Delete rejects rules: if to is empty, it rejects all entries for from. While the rejected entry is kept,
// it won't be learned back automatically (a manual Put restores it). When entries are full, the oldest
// rejected entries are evicted just like candidates.
func (s *Store) Delete(name, from, to string) (int, error) {
	name, err := s.enter(name)
	defer s.exit()
	if err != nil {
		return 0, err
	}
	r, err := s.db.Exec(`UPDATE words SET state = 'rejected', fixed = 0, seen = 0, updated = ?
WHERE user = ? AND "from" = ? AND (? = '' OR "to" = ?) AND state <> 'rejected'`, now().UnixMilli(), name, from, to, to)
	if err != nil {
		return 0, err
	}
	n, err := r.RowsAffected()
	return int(n), err
}

// active returns the number of active rules.
func (u *user) active() int {
	n := 0
	for _, w := range u.words {
		if w.State == StateActive {
			n++
		}
	}
	return n
}
