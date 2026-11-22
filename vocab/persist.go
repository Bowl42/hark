package vocab

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

const schemaVersion = 1

// The constraints guard against data that would break replacement: an empty from (an infinite replacement
// loop), from = to, invalid states, and multiple active rules for the same from.
const schema = `
CREATE TABLE words (
  user    TEXT    NOT NULL,
  "from"  TEXT    NOT NULL CHECK ("from" <> ''),
  "to"    TEXT    NOT NULL CHECK ("to" <> "from"),
  state   TEXT    NOT NULL CHECK (state IN ('candidate', 'active', 'rejected')),
  source  TEXT    NOT NULL CHECK (source IN ('auto', 'manual')),
  fixed   INTEGER NOT NULL CHECK (fixed >= 0),
  seen    INTEGER NOT NULL CHECK (seen >= 0),
  updated INTEGER NOT NULL, -- unix milliseconds
  PRIMARY KEY (user, "from", "to")
) WITHOUT ROWID;
CREATE UNIQUE INDEX words_one_active ON words (user, "from") WHERE state = 'active';
CREATE TABLE recent (
  seq  INTEGER PRIMARY KEY, -- insertion order
  user TEXT NOT NULL,
  id   TEXT NOT NULL,
  UNIQUE (user, id)
);`

// Open opens (creating if needed) dir/vocab.db. If log is nil, slog.Default() is used. Call Close when done.
func Open(dir string, log *slog.Logger) (*Store, error) {
	if log == nil {
		log = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path, err := filepath.Abs(filepath.Join(dir, "vocab.db"))
	if err != nil {
		return nil, err
	}
	// Set on every connection: WAL (readers and writers don't block each other), fsync on every commit, and
	// waiting on write-lock conflicts. All transactions use BEGIN IMMEDIATE.
	q := url.Values{"_txlock": {"immediate"}, "_pragma": {
		"busy_timeout(5000)", "journal_mode(WAL)", "synchronous(FULL)", "fullfsync(1)"}}
	db, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: path, RawQuery: q.Encode()}).String())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8) // writes are serialized anyway, so few connections are needed
	// When several processes open the database for the first time at once, switching to WAL contends for a lock
	// and bypasses busy_timeout: on BUSY, retry a little later.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		err = initSchema(db)
		var e interface{ Code() int }
		if err == nil || !errors.As(err, &e) || e.Code()&0xff != 5 || time.Now().After(deadline) { // 5 = SQLITE_BUSY
			break
		}
	}
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("vocab: %s: %w", path, err)
	}
	return &Store{log: log, db: db, learn: make(chan struct{}, learnWorkers)}, nil
}

// initSchema creates the tables in an empty database; otherwise it checks the version.
func initSchema(db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var v, n int
	if err := tx.QueryRow("PRAGMA user_version").Scan(&v); err != nil {
		return err
	}
	switch v {
	case schemaVersion:
		return nil
	case 0:
		if err := tx.QueryRow("SELECT count(*) FROM sqlite_master").Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return errors.New("not a hark vocabulary database")
		}
		if _, err := tx.Exec(schema); err != nil {
			return err
		}
		if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
			return err
		}
		return tx.Commit()
	default:
		return fmt.Errorf("unsupported schema version %d", v)
	}
}

type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// load reads all of a user's entries. Exceeding the limit is treated as corruption (it reads at most the limit plus one).
func load(q querier, name, order string) ([]Word, error) {
	rows, err := q.Query(`SELECT "from", "to", state, source, fixed, seen, updated FROM words WHERE user = ? `+order+` LIMIT ?`,
		name, maxWords+1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Word{}
	for rows.Next() {
		var w Word
		var ms int64
		if err := rows.Scan(&w.From, &w.To, &w.State, &w.Source, &w.Fixed, &w.Seen, &ms); err != nil {
			return nil, err
		}
		w.Updated = time.UnixMilli(ms).UTC()
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > maxWords {
		return nil, fmt.Errorf("vocab: user %s: too many words", name)
	}
	return out, nil
}

// save writes the difference between u and old into tx, in this order: deletions, then inactive entries,
// then active ones. Old rules are thus demoted before new ones are promoted, so the transaction never
// violates "only one active rule per from" midway.
func save(tx *sql.Tx, u *user, old map[pair]Word) error {
	for k := range old {
		if u.words[k] == nil {
			if _, err := tx.Exec(`DELETE FROM words WHERE user = ? AND "from" = ? AND "to" = ?`, u.name, k.from, k.to); err != nil {
				return err
			}
		}
	}
	var inactive, active []*Word
	for k, w := range u.words {
		if o, ok := old[k]; !ok || !sameWord(o, *w) {
			if w.State == StateActive {
				active = append(active, w)
			} else {
				inactive = append(inactive, w)
			}
		}
	}
	for _, w := range append(inactive, active...) {
		if _, err := tx.Exec(`
INSERT INTO words (user, "from", "to", state, source, fixed, seen, updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (user, "from", "to") DO UPDATE SET
  state = excluded.state, source = excluded.source, fixed = excluded.fixed, seen = excluded.seen, updated = excluded.updated`,
			u.name, w.From, w.To, w.State, w.Source, w.Fixed, w.Seen, w.Updated.UnixMilli()); err != nil {
			return err
		}
	}
	return nil
}

func sameWord(a, b Word) bool {
	return a.From == b.From && a.To == b.To && a.State == b.State && a.Source == b.Source &&
		a.Fixed == b.Fixed && a.Seen == b.Seen && a.Updated.Equal(b.Updated)
}

// Close closes the database after in-progress operations finish. Later calls return ErrClosed.
// It is safe to call more than once.
func (s *Store) Close() error {
	s.life.Lock()
	defer s.life.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.db.Close()
}
