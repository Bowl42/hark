// Command harkc is a command-line client for integration testing: it streams a wav file to hark
// at real-time speed and prints each track's live results.
//
//	go run ./cmd/harkc -engines sensevoice,mock test.wav
//
// The wav must be 16kHz / 16-bit PCM, mono or multichannel (with multiple channels, each channel
// gets its own track, using the first engine).
//
// Each line shows the committed text followed by the pending text in [ ]; a leading * marks an
// update that rewrote text already shown.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/internal/wav"
	"github.com/coder/websocket"
)

func main() {
	url := flag.String("url", "ws://127.0.0.1:8080/v1/stream", "hark address")
	token := flag.String("token", os.Getenv("HARK_TOKEN"), "authentication token (defaults to HARK_TOKEN)")
	engines := flag.String("engines", "", "comma-separated engine names, one track each for comparison; empty uses the server's default engine")
	freeze := flag.Int("freeze-ms", 0, "if >0, use the freeze commit policy: commit once text has been stable this long")
	speed := flag.Float64("speed", 1, "send speed multiplier: 1 is real time, 0 is unthrottled")
	quiet := flag.Bool("quiet", false, "print only the final result")
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: harkc [flags] file.wav")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(2)
	}
	if err := run(*url, *token, *engines, *freeze, *speed, *quiet, flag.Arg(0)); err != nil {
		fmt.Fprintln(os.Stderr, "harkc:", err)
		os.Exit(1)
	}
}

func run(url, token, engines string, freeze int, speed float64, quiet bool, path string) error {
	pcm, channels, err := wav.Read(path)
	if err != nil {
		return err
	}
	ctx := context.Background()
	h := http.Header{}
	if token != "" {
		h.Set("Authorization", "Bearer "+token)
	}
	dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	conn, _, err := websocket.Dial(dctx, url, &websocket.DialOptions{HTTPHeader: h})
	cancel()
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(16 << 20)

	start := api.Start{
		Type: api.TypeStart, RequestID: fmt.Sprintf("harkc-%d", time.Now().UnixNano()),
		Audio: api.Audio{Encoding: api.EncodingPCM16, SampleRate: api.SampleRate, Channels: channels},
	}
	if freeze > 0 {
		start.Commit = &api.Commit{Policy: api.CommitFreeze, AfterMS: freeze}
	}
	if engines != "" {
		names := strings.Split(engines, ",")
		if channels > 1 {
			names = names[:1]
		}
		for _, name := range names {
			for ch := 0; ch < channels; ch++ {
				start.Tracks = append(start.Tracks, api.TrackSpec{Engine: strings.TrimSpace(name), Channel: ch})
			}
		}
	}
	if err := send(ctx, conn, start); err != nil {
		return err
	}

	begin := time.Now()
	go func() {
		chunk := 3200 * channels // 100ms
		for i := 0; i < len(pcm); i += chunk {
			if conn.Write(ctx, websocket.MessageBinary, pcm[i:min(i+chunk, len(pcm))]) != nil {
				return
			}
			if speed > 0 {
				// Pace against audio time to avoid accumulating drift.
				due := time.Duration(float64(i+chunk) / float64(2*channels) / api.SampleRate / speed * float64(time.Second))
				time.Sleep(time.Until(begin.Add(due)))
			}
		}
		send(ctx, conn, api.Envelope{Type: api.TypeFinish})
	}()

	names := map[int]string{}
	docs := map[int]*api.Doc{}
	revisions := map[int]int{}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
		}
		var env api.Envelope
		json.Unmarshal(data, &env)
		elapsed := time.Since(begin).Seconds()
		switch env.Type {
		case api.TypeStarted:
			var st api.Started
			json.Unmarshal(data, &st)
			for _, tr := range st.Tracks {
				names[tr.Track] = tr.Engine
				if channels > 1 {
					names[tr.Track] += fmt.Sprintf("/ch%d", tr.Channel)
				}
				docs[tr.Track] = &api.Doc{}
			}
			fmt.Printf("session %s, %d track(s)\n", st.SessionID, len(st.Tracks))
		case api.TypeRejected:
			var r api.Rejected
			json.Unmarshal(data, &r)
			return fmt.Errorf("rejected: %s: %s", r.Error.Code, r.Error.Message)
		case api.TypeUpdate:
			var u api.Update
			json.Unmarshal(data, &u)
			d := docs[u.Track]
			before := d.Text()
			if err := d.Apply(u); err != nil {
				return fmt.Errorf("protocol violation on track %d: %w", u.Track, err)
			}
			mark := "  "
			if revises(before, d.Text()) {
				mark = "* "
				revisions[u.Track]++
			}
			if !quiet {
				fmt.Printf("%6.1fs [%s] %s %s%s\n", elapsed, names[u.Track], mark, committed(d), pending(d))
			}
		case api.TypeTrackEnd:
			var e api.TrackEnd
			json.Unmarshal(data, &e)
			if e.Reason != api.ReasonDone {
				msg := e.Reason
				if e.Error != nil {
					msg += ": " + e.Error.Message
				}
				fmt.Printf("%6.1fs [%s] ended: %s\n", elapsed, names[e.Track], msg)
			}
		case api.TypeEnd:
			var e api.End
			json.Unmarshal(data, &e)
			fmt.Printf("\n%.1fs ended: %s/%s\n", elapsed, e.Reason, e.Status)
			for i := 0; i < len(docs); i++ {
				fmt.Printf("\n[%s] %d sentences, %d rewrites\n", names[i], len(docs[i].Segments), revisions[i])
				for _, s := range docs[i].Segments {
					r := ""
					if s.AudioRange != (api.AudioRange{}) {
						r = fmt.Sprintf("%5.1f-%5.1fs  ", float64(s.AudioRange.StartSample)/api.SampleRate, float64(s.AudioRange.EndSample)/api.SampleRate)
					}
					fmt.Printf("  %s%s\n", r, s.Text)
				}
			}
			if e.Status != api.StatusComplete {
				return errors.New("session not complete")
			}
			return nil
		case api.TypeError:
			var e api.ErrorMsg
			json.Unmarshal(data, &e)
			return fmt.Errorf("%s: %s", e.Error.Code, e.Error.Message)
		}
	}
}

// committed returns the committed part (which will not change again); pending returns the rest, wrapped in [ ].
func committed(d *api.Doc) string {
	var b strings.Builder
	for _, s := range d.Segments[:d.Commit-d.Base] {
		b.WriteString(s.Text)
	}
	return b.String()
}

func pending(d *api.Doc) string {
	held := d.Commit - d.Base
	if held == len(d.Segments) {
		return ""
	}
	var b strings.Builder
	for _, s := range d.Segments[held:] {
		b.WriteString(s.Text)
	}
	return "[" + b.String() + "]"
}

func send(ctx context.Context, c *websocket.Conn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.Write(ctx, websocket.MessageText, b)
}

// revises reports whether going from before to after deleted or replaced existing text.
// Only inserting new characters (before is a subsequence of after) does not count; neither do
// changes to segmentation, metadata or the watermark alone.
func revises(before, after string) bool {
	rb := []rune(before)
	i := 0
	for _, c := range after {
		if i < len(rb) && rb[i] == c {
			i++
		}
	}
	return i < len(rb)
}
