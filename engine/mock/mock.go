// Package mock is a fake engine that needs no network: it advances through a preset script according to the
// duration of audio received. The script deliberately contains typos and later corrections, for downstream
// integration testing of live corrections and for running tests.
//
// Configuration:
//
//	mock:
//	  type: mock
//	  step_ms: 300   # milliseconds of audio received per step, default 300
package mock

import (
	"context"
	"slices"

	"github.com/Bowl42/hark"
)

type Config struct {
	StepMS int `yaml:"step_ms"`
}

// New creates a fake engine that outputs the next script step for every StepMS milliseconds of audio received (300 when StepMS is 0).
func New(c Config) (*Engine, error) {
	if c.StepMS <= 0 {
		c.StepMS = 300
	}
	return &Engine{stepBytes: c.StepMS * hark.BytesPerSecond / 1000}, nil
}

type Engine struct{ stepBytes int }

func p(t string) hark.Segment { return hark.Segment{Text: t} }
func f(t string) hark.Segment { return hark.Segment{Text: t, Final: true} }

// script: each step is a full snapshot.
var script = [][]hark.Segment{
	{p("今天")},
	{p("今天天汽")},
	{p("今天天汽很好")},
	{f("今天天气很好。")}, // the engine corrects the typo and finalizes
	{f("今天天气很好。"), p("我们")},
	{f("今天天气很好。"), p("我们去公圆")},
	{f("今天天气很好。"), p("我们去公园吧")},
	{f("今天天气很好。"), f("我们去公园吧。")},
}

func (e *Engine) Open(_ context.Context, _ hark.Request, out hark.Output) (hark.Stream, error) {
	return &stream{stepBytes: e.stepBytes, out: out}, nil
}

type stream struct {
	stepBytes int
	out       hark.Output
	received  int
	step      int
}

func (s *stream) Write(pcm []byte) error {
	s.received += len(pcm)
	for s.step < len(script) && s.received >= (s.step+1)*s.stepBytes {
		s.out.Update(nil, slices.Clone(script[s.step])) // a snapshot belongs to hark once handed over, so the global script itself must not be handed out
		s.step++
	}
	return nil
}

func (s *stream) Finish(context.Context) error {
	var final []hark.Segment
	if s.step > 0 {
		for _, seg := range script[s.step-1] {
			final = append(final, hark.Segment{Text: seg.Text, Final: true})
		}
	}
	s.out.Update(nil, final)
	return nil
}

func (s *stream) Close() error { return nil }
