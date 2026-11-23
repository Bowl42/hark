// Package hark defines the plugin contract for recognition engines (Engine).
//
// Downstream, hark offers a single streaming recognition protocol (see package api and docs/PROTOCOL.md);
// upstream, it is backed by individual Engine implementations. Built-in and private engines follow the same
// contract: implement Engine / Stream, then either pass it straight to session.Open or register it with
// harkcmd.Engine as an engine type usable in the config file.
//
// This package holds only the engine contract and the most basic types. It depends on no implementation
// and knows nothing about the config file.
package hark

import (
	"context"
	"log/slog"
)

// Audio format: v1 uses PCM s16le / 16kHz / mono throughout, and engines always receive this format.
const (
	SampleRate     = 16000
	BytesPerSecond = SampleRate * 2
)

// Segment is one piece of recognition result, usually a sentence. A segment's index is its position in the snapshot.
type Segment struct {
	// Text: the full text is the segments' Text concatenated as is; hark never inserts spaces.
	// For languages that need spaces, such as English, the engine puts the spaces in Text (e.g. "Hello", " world.").
	Text string
	// Final means upstream considers this segment finalized (soft-final). It is only a hint; the segment may still be rewritten.
	// Whether and when to promise downstream that it will not change is decided by hark, per the session's commit policy.
	Final bool
	// Speaker is optional: a speaker label. A segment can have at most one speaker label; the engine should start a new
	// segment when the speaker changes. The label is scoped to this track of this session.
	Speaker string
	// Audio is optional: where this segment lies in the input audio. Upstream should provide it whenever it can:
	// besides being passed through to downstream, the freeze policy relies on it to reliably locate new content
	// when upstream rewrites frozen content.
	Audio AudioRange
}

// AudioRange is a half-open interval [Start, End) on the input audio, in samples (at 16kHz, 1ms = 16 samples),
// counted from the session's first sample. The zero value means none.
type AudioRange struct {
	Start, End int64
}

// Output is where an engine reports results. hark provides it.
type Output interface {
	// Update reports the engine's results: done are segments handed over for good, live is a **full snapshot of
	// every segment after them** (not a delta). hark works out what changed and pushes that downstream.
	//
	// Handing over is optional but keeps long streams cheap: a done segment will never change and is never
	// reported again (later updates start after it), so the engine can forget it, and hark commits it right away
	// under any commit policy. (Under freeze, text hark has already frozen takes precedence: if a handover doesn't
	// line up with it segment by segment, hark keeps the segments as part of the full snapshot and applies the
	// usual freeze normalization, as for an engine that never hands over; retiring them is deferred.) Done segments are appended after those handed over before, in order; done implies
	// Final. An engine that can't tell when a segment is final for good just reports Update(nil, everything);
	// such streams are still bounded, by rollover (see session.Options.Rollover).
	//
	// It may be called from any goroutine and never blocks. Every call's done segments are kept; of the live
	// snapshots, hark only guarantees to push the latest (snapshots in quick succession may be coalesced).
	// Calls must be ordered: a call's live snapshot must follow everything handed over up to that call.
	//
	// Ownership: after the call, both slices belong to hark, which may modify segments in place (e.g. personal
	// vocabulary replacement). The engine must not read or write them afterwards, and should build new slices
	// for every report.
	Update(done, live []Segment)
}

// Request holds the parameters of one session.
type Request struct {
	SessionID string
	// Options is downstream's start.options, passed through as is; the engine interprets the fields.
	// Read-only: the same map may be handed to several tracks at once.
	Options map[string]any
	// Log is a logger already carrying fields such as session.
	Log *slog.Logger
}

// Engine is a configured recognition engine instance (e.g. one set of credentials, one model).
// The same instance is used by many sessions concurrently, so Open must be safe for concurrent use.
type Engine interface {
	// Open starts one streaming recognition. ctx is cancelled when the session ends or is aborted.
	Open(ctx context.Context, req Request, out Output) (Stream, error)
}

// Stream is one streaming recognition session.
//
// Calling conventions:
//   - Write and Finish are never called concurrently; Finish is called at most once, and Write is not called after it
//   - Close may be called at any time from another goroutine (to abort the session), and may be called more than once
//   - After Close, there may still be one Write or Finish call; it should return an error rather than panic
//   - If upstream fails midway, the error is returned from the next Write or Finish
type Stream interface {
	// Write pushes a chunk of PCM audio of any length (an even number of bytes).
	// It may block to feed upstream at real-time speed; blocking propagates to downstream as TCP backpressure.
	// But it must return promptly when ctx is cancelled or Close is called.
	//
	// Ownership: pcm is valid only for the duration of the call; the caller may reuse the memory after it returns.
	// An engine that needs to keep it must copy it.
	Write(pcm []byte) error
	// Finish signals the end of audio and blocks until upstream finalizes.
	// The final snapshot must have been reported through Output.Update before it returns.
	// It should return promptly when ctx expires (finalize timeout).
	Finish(ctx context.Context) error
	// Close releases resources and aborts the session if it has not finished.
	Close() error
}
