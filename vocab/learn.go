package vocab

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"time"
)

// Result is the learning outcome of one submission.
type Result struct {
	Activated   []Word `json:"activated"`   // became active in this submission
	Deactivated []Word `json:"deactivated"` // deactivated in this submission (the user changed a replacement back to the original)
	Skipped     int    `json:"skipped"`     // changed blocks not analyzed because they exceeded the size limits
	Duplicate   bool   `json:"duplicate"`   // this submission was already processed
}

// Correct learns from one correction: before is the recognized text (as the client received it, i.e. after
// replacement), after is the user's corrected text. id is used for deduplication (session_id:track is
// suggested); if empty, deduplication is by content. The dedup record and the entries are committed in the
// same transaction, so retrying with the same id after an error is always safe: if it wasn't committed it
// is learned again, and if it was it is reported as a duplicate.
func (s *Store) Correct(ctx context.Context, name, id, before, after string) (Result, error) {
	res := Result{Activated: []Word{}, Deactivated: []Word{}}
	if len(before) > maxTextBytes || len(after) > maxTextBytes {
		return res, ErrTooLarge
	}
	if id == "" {
		h := sha256.Sum256([]byte(before + "\x00" + after))
		id = "sha256:" + hex.EncodeToString(h[:16])
	}
	select {
	case s.learn <- struct{}{}:
	case <-ctx.Done():
		return res, ctx.Err()
	}
	pairs, skipped := extract(before, after) // expensive, so done outside the transaction
	<-s.learn
	res.Skipped = skipped

	err := s.modify(name, func(tx *sql.Tx, u *user) error {
		r, err := tx.Exec(`INSERT INTO recent (user, id) VALUES (?, ?) ON CONFLICT DO NOTHING`, u.name, id)
		if err != nil {
			return err
		}
		if n, err := r.RowsAffected(); err != nil || n == 0 {
			res.Duplicate = true
			return err
		}
		// Keep only the most recent maxRecent.
		if _, err := tx.Exec(`DELETE FROM recent WHERE user = ? AND seq <= (
  SELECT seq FROM recent WHERE user = ? ORDER BY seq DESC LIMIT 1 OFFSET ?)`, u.name, u.name, maxRecent); err != nil {
			return err
		}
		t := now()

		// Reverse correction: the user changed an auto rule's replacement back to the original → deactivate
		// and block it, and don't learn the reverse rule this time either.
		var learn []pair
		for _, p := range pairs {
			if w := u.words[pair{p.to, p.from}]; w != nil && w.State == StateActive {
				if w.Source == SourceAuto {
					w.State, w.Fixed, w.Seen, w.Updated = StateRejected, 0, 0, t
					res.Deactivated = append(res.Deactivated, *w)
				}
				continue
			}
			learn = append(learn, p)
		}

		// Create the new candidates first, then count occurrences for all (new candidates count toward seen this time too).
		for _, p := range learn {
			if u.words[pair{p.from, p.to}] == nil {
				u.addCandidate(p, t)
			}
		}
		froms := map[string]string{}
		for _, w := range u.words {
			if w.State != StateRejected {
				froms[w.From] = w.From
			}
		}
		found := compile(froms).occurring(before)
		for _, w := range u.words {
			if w.State != StateRejected && found[w.From] {
				w.Seen++
			}
		}
		for _, p := range learn {
			w := u.words[pair{p.from, p.to}]
			if w == nil || w.State == StateRejected {
				continue // evicted by a candidate created in this submission, or rejected by the user
			}
			w.Fixed++
			w.Seen = max(w.Seen, w.Fixed)
			w.Updated = t
			if u.promote(w) {
				res.Activated = append(res.Activated, *w)
			}
		}
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	return res, nil
}

// addCandidate creates a candidate. If entries are full, it first evicts the oldest candidate or rejected
// entry (active rules never exceed maxActive, so there is always room).
func (u *user) addCandidate(p pair, t time.Time) *Word {
	if len(u.words) >= maxWords {
		var oldest *Word
		for _, w := range u.words {
			if w.State != StateActive && (oldest == nil || w.Updated.Before(oldest.Updated)) {
				oldest = w
			}
		}
		delete(u.words, pair{oldest.From, oldest.To})
	}
	w := &Word{From: p.from, To: p.to, State: StateCandidate, Source: SourceAuto, Updated: t}
	u.words[p] = w
	return w
}

// promote activates a candidate that meets the conditions and reports whether it did.
// Conditions: the same correction was made in at least minFixed submissions, and at least 80% of the times
// From appeared in the recognized text it was corrected this way.
// Only one rule per From is active: a manual rule is never replaced by an auto rule; an auto rule is replaced
// only by one with strictly more evidence. When active rules are full, a rule can only replace, not be added.
func (u *user) promote(w *Word) bool {
	if w.State != StateCandidate || w.Source != SourceAuto || w.Fixed < minFixed || w.Fixed*5 < w.Seen*4 {
		return false
	}
	var incumbent *Word
	for _, o := range u.words {
		if o != w && o.From == w.From && o.State == StateActive {
			incumbent = o
		}
	}
	switch {
	case incumbent == nil && u.active() >= maxActive:
		return false
	case incumbent != nil && (incumbent.Source == SourceManual || w.Fixed <= incumbent.Fixed):
		return false
	case incumbent != nil:
		incumbent.State = StateCandidate
	}
	w.State = StateActive
	return true
}
