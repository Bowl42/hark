// Package api is hark's downstream wire protocol (see docs/PROTOCOL.md).
//
// Downstream Go clients can import this package directly: message structs plus Doc (client state that
// merges updates per the protocol). This package depends on no other hark package.
package api

import "fmt"

// Message types.
const (
	TypeStart    = "start"
	TypeFinish   = "finish"
	TypeCancel   = "cancel"
	TypeStarted  = "started"
	TypeRejected = "rejected"
	TypeUpdate   = "update"
	TypeTrackEnd = "track_end"
	TypeEnd      = "end"
	TypeError    = "error"
)

// end.status: whether the tracks' results are complete.
const (
	StatusComplete = "complete" // all tracks done
	StatusPartial  = "partial"  // some tracks done
	StatusFailed   = "failed"   // no track done (partial text may still exist)
)

// Limits.
const (
	MaxChannels = 8
	MaxTracks   = 8
)

// Output modes.
const (
	ModeDelta = "delta" // push intermediate results
	ModeFinal = "final" // no intermediate results; the final result is in each track's track_end.snapshot
)

// Commit policies.
const (
	CommitStrict = "strict" // commit only on normal completion
	CommitFreeze = "freeze" // the gateway freezes content after it has been stable for a while
)

// end.reason values.
const (
	ReasonDone      = "done"
	ReasonError     = "error"
	ReasonTimeout   = "timeout"
	ReasonCancelled = "cancelled"
)

// Error codes.
const (
	ErrBadRequest       = "bad_request"
	ErrUnsupportedAudio = "unsupported_audio"
	ErrUnknownEngine    = "unknown_engine"
	ErrEngine           = "engine_error"
	ErrTimeout          = "timeout"
	ErrProtocol         = "protocol_error"
	ErrShuttingDown     = "shutting_down" // the server is stopping: retry on another instance (or later)
	ErrInternal         = "internal"
)

// The only supported audio format.
const (
	EncodingPCM16 = "pcm_s16le"
	SampleRate    = 16000
)

// Envelope is a message with only a type: parse the type first, then dispatch. Also used to send finish / cancel.
type Envelope struct {
	Type string `json:"type"`
}

type Audio struct {
	Encoding   string `json:"encoding"`
	SampleRate int    `json:"sample_rate"`
	Channels   int    `json:"channels"`
}

type Output struct {
	Mode string `json:"mode"`
}

type Commit struct {
	Policy  string `json:"policy"`
	AfterMS int    `json:"after_ms,omitempty"` // freeze only
}

// MaxFreezeMS is the upper bound of after_ms for the freeze policy.
const MaxFreezeMS = 600_000

// Normalize validates the commit policy and fills in defaults: nil or an empty policy means strict (strict clears AfterMS).
func (c *Commit) Normalize() (Commit, error) {
	var out Commit
	if c != nil {
		out = *c
	}
	switch {
	case out.Policy == "" || out.Policy == CommitStrict:
		return Commit{Policy: CommitStrict}, nil
	case out.Policy == CommitFreeze && out.AfterMS >= 0 && out.AfterMS <= MaxFreezeMS:
		return out, nil
	}
	return Commit{}, fmt.Errorf("commit must be {policy: strict} or {policy: freeze, after_ms: 0..%d}", MaxFreezeMS)
}

// ---- client → server ----

type Start struct {
	Type      string         `json:"type"`
	RequestID string         `json:"request_id"`
	Engine    string         `json:"engine,omitempty"` // default engine for engine tracks
	Audio     Audio          `json:"audio"`
	Tracks    []TrackSpec    `json:"tracks,omitempty"` // omitted: one engine track per channel
	Output    *Output        `json:"output,omitempty"`
	Commit    *Commit        `json:"commit,omitempty"`
	Options   map[string]any `json:"options,omitempty"` // default options for engine tracks
	// User is optional: a user name that enables that user's personal vocabulary (the server must have data_dir configured). See docs/PROTOCOL.md.
	User string `json:"user,omitempty"`
}

// TrackSpec is one requested track: it sends one channel's audio to one engine. The track number is its index in the tracks array.
type TrackSpec struct {
	Engine  string `json:"engine,omitempty"`  // omitted: use start.engine or the server default
	Channel int    `json:"channel,omitempty"` // zero-based
	// Options: when omitted, an engine track inherits start.options; an explicit {} means empty.
	// omitzero rather than omitempty: only nil is omitted, and an empty map serializes as {}, preserving "explicitly empty".
	Options map[string]any `json:"options,omitzero"`
}

// AcceptedTrack is the track layout echoed back in started.
type AcceptedTrack struct {
	Track   int    `json:"track"`
	Engine  string `json:"engine"`
	Channel int    `json:"channel"`
}

// ---- server → client ----

type Started struct {
	Type      string          `json:"type"`
	RequestID string          `json:"request_id"`
	SessionID string          `json:"session_id"`
	Audio     Audio           `json:"audio"`
	Output    Output          `json:"output"`
	Commit    Commit          `json:"commit"`
	Tracks    []AcceptedTrack `json:"tracks"`
}

type Rejected struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
	Error     Error  `json:"error"`
}

// Segment is one segment of the result. The full text is the segments' Text concatenated as is; spaces and punctuation are in Text.
type Segment struct {
	Text string `json:"text"`
	// Speaker is a speaker label scoped to this track: the same label on different tracks does not mean the same person.
	Speaker    string     `json:"speaker,omitempty"`
	AudioRange AudioRange `json:"audio_range,omitzero"` // zero value means none
}

// AudioRange is the segment's position in the input audio: a half-open interval in samples. The zero value means none.
type AudioRange struct {
	StartSample int64 `json:"start_sample"`
	EndSample   int64 `json:"end_sample"`
}

type Update struct {
	Type   string `json:"type"`
	Track  int    `json:"track"`
	Rev    int    `json:"rev"`
	Ops    []Op   `json:"ops,omitempty"` // omitted: neither text nor segments changed, only the watermarks. See ops.go
	Stable int    `json:"stable"`
	Commit int    `json:"commit"`
}

type Snapshot struct {
	Segments []Segment `json:"segments"`
	Stable   int       `json:"stable"`
	Commit   int       `json:"commit"`
}

// TrackEnd is a track's final state. Exactly one per track is sent before the session's end.
type TrackEnd struct {
	Type             string    `json:"type"`
	Track            int       `json:"track"`
	Reason           string    `json:"reason"`
	Error            *Error    `json:"error,omitempty"`
	DroppedRevisions int       `json:"dropped_revisions"`
	Snapshot         *Snapshot `json:"snapshot,omitempty"`
}

// End is the session's final state, sent after all track_end messages.
type End struct {
	Type   string `json:"type"`
	Reason string `json:"reason"` // why the session stopped
	Status string `json:"status"` // whether the tracks' results are complete
	Error  *Error `json:"error,omitempty"`
}

// ErrorMsg is a connection-level error; the server closes the connection after sending it.
type ErrorMsg struct {
	Type  string `json:"type"`
	Error Error  `json:"error"`
}

type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}
