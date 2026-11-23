// Command embedded recognizes a wav file in its own process, without running a hark server:
// it drives the engine with the session package, and the results are exactly the same as the server's.
//
//	go run ./examples/embedded test.wav                                # fake engine, no model needed
//	go run ./examples/embedded -sherpa ws://127.0.0.1:6006 test.wav    # local SenseVoice (batch made streaming)
//
// The wav must be 16kHz / 16-bit / mono PCM.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/Bowl42/hark"
	"github.com/Bowl42/hark/api"
	"github.com/Bowl42/hark/engine/batch"
	"github.com/Bowl42/hark/engine/mock"
	"github.com/Bowl42/hark/engine/sherpa"
	"github.com/Bowl42/hark/internal/wav"
	"github.com/Bowl42/hark/session"
	"github.com/Bowl42/hark/vocab"
)

func main() {
	sherpaURL := flag.String("sherpa", "", "sherpa-onnx recognition server URL (SenseVoice); empty uses the fake engine")
	freeze := flag.Int("freeze-ms", 0, "use the freeze commit policy when > 0")
	words := flag.String("words", "", "static word list, e.g. ATI=API,哈克=hark")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintln(os.Stderr, "usage: embedded [-sherpa URL] [flags] file.wav")
		os.Exit(2)
	}
	pcm, channels, err := wav.Read(flag.Arg(0))
	if err == nil && channels != 1 {
		err = errors.New("wav must be mono")
	}
	if err != nil {
		log.Fatal(err)
	}

	// 1. Engine: any hark.Engine works.
	var eng hark.Engine
	if *sherpaURL == "" {
		eng, err = mock.New(mock.Config{})
	} else {
		var rec *sherpa.Recognizer
		if rec, err = sherpa.New(sherpa.Config{URL: *sherpaURL}); err == nil {
			eng = batch.New(rec, batch.Options{Interval: 500 * time.Millisecond})
		}
	}
	if err != nil {
		log.Fatal(err)
	}

	// 2. Personal vocabulary: wraps the engine (a static word list; for automatic learning, use
	// Store.Matcher from vocab.Open).
	if *words != "" {
		rules := map[string]string{}
		for _, kv := range strings.Split(*words, ",") {
			from, to, _ := strings.Cut(kv, "=")
			rules[from] = to
		}
		m, err := vocab.Compile(rules)
		if err != nil {
			log.Fatal(err)
		}
		eng = vocab.Wrap(eng, m)
	}

	// 3. commit policy: strict by default (commit only at the end); with -freeze-ms, commit once
	// text has been stable for that long.
	var commit api.Commit
	if *freeze > 0 {
		commit = api.Commit{Policy: api.CommitFreeze, AfterMS: *freeze}
	}

	ctx := context.Background()
	s, err := session.Open(ctx, eng, hark.Request{}, session.Options{Commit: commit})
	if err != nil {
		log.Fatal(err)
	}
	defer s.Close()

	// 4. Read results: the same updates the server pushes, merged with api.Doc.
	begin := time.Now()
	read := make(chan error, 1)
	var doc api.Doc
	go func() {
		for {
			u, err := s.Next(ctx)
			if err != nil {
				read <- err
				return
			}
			if err := doc.Apply(u); err != nil {
				read <- err
				return
			}
			fmt.Printf("%5.1fs rev %-3d commit %d/%d  %s\n", time.Since(begin).Seconds(), u.Rev, doc.Commit, len(doc.Segments), doc.Text())
		}
	}()

	// 5. Send audio at real-time speed (one chunk per 100ms).
	const chunk = 3200
	for off := 0; off < len(pcm); off += chunk {
		if err := s.Write(pcm[off:min(off+chunk, len(pcm))]); err != nil {
			log.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	fctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := s.Finish(fctx); err != nil {
		log.Fatal(err)
	}
	if err := <-read; err != io.EOF {
		log.Fatal(err)
	}
	// The session no longer keeps committed text Next has delivered, so the result is the Doc built from the updates.
	// (Without a reader, s.Snapshot() after Finish would hold the whole result.)
	fmt.Println("---")
	for i, seg := range doc.Segments {
		fmt.Printf("%d: %s\n", i, seg.Text)
	}
}
