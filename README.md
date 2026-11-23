# hark

A real-time speech-to-text gateway. Clients stream audio; hark turns the output of any recognition engine into a live transcript with controlled rewrites and explicit finalization.

## Features

- **Real-time use of batch-only models**: models that can only recognize a whole utterance (SenseVoice, Whisper, file transcription APIs, ...) become streaming. hark segments sentences, re-recognizes while you speak, and finalizes ([engine/batch](engine/batch/batch.go))
- **Rewrites stay sane**: changes are pushed as ops (replace characters i through j with this). Text before the `commit` watermark is guaranteed never to change; `stable` marks what upstream considers final ([protocol](docs/PROTOCOL.md))
- **Users can edit while speaking**: the client merges recognition results with the user's edits, and edited text is never overwritten by recognition ([draft](draft/draft.go))
- **Personal vocabulary**: once a user keeps correcting a word to the same spelling, hark learns to replace it automatically ([protocol](docs/PROTOCOL.md#personal-vocabulary))
- **Common backends built in**: local SenseVoice (sherpa-onnx); Volcengine (Doubao) streaming recognition ([volc](engine/volc/volc.go)); any OpenAI-compatible transcription API (OpenAI, Groq, SiliconFlow, self-hosted speaches, vLLM, whisper.cpp, ...) ([openai](engine/openai/openai.go))
- **Pluggable backends**: plug in any batch (HTTP) or streaming (WebSocket) recognition service, written in any language, with a config change ([http](engine/httpasr/httpasr.go), [ws](engine/wsasr/wsasr.go), [Python examples](examples/plugins)); or implement an engine in Go and compile it in ([hark.go](hark.go), [harkcmd](harkcmd/harkcmd.go))
- **Hours-long sessions**: engines hand over finished sentences once (others are rolled over to a fresh stream at a pause every couple of minutes), and committed text is dropped once delivered (or spooled to disk for `final` mode), so engine state, connections and memory stay bounded ([long sessions](docs/PROTOCOL.md#long-sessions))
- **Multiple tracks**: send the same audio to several engines to compare them, or recognize each channel separately
- **Works in the browser**: a single HTML file that transcribes your microphone live; copy it to integrate ([examples/browser](examples/browser/index.html))
- **Also a Go library**: use it inside your own program without running a server, with the same semantics as the server ([examples/embedded](examples/embedded/main.go))
- **Local model included**: start hark + SenseVoice with one Docker command, no cloud services needed ([deploy](deploy/docker/compose.yaml))
- **No lost speech on shutdown**: sessions in progress get 5 seconds to finish; those that run out are told explicitly that they can retry

## Quick start

Start hark + local SenseVoice with Docker (the first build downloads the model):

```bash
docker compose -f deploy/docker/compose.yaml up -d --build
```

Then try it either way:

- **Browser**: `cd examples/browser && python3 -m http.server 8000`, open http://localhost:8000, click Start, and speak into the microphone
- **Command line** (requires Go 1.26+): prepare a 16 kHz / 16-bit / mono wav and stream it at real-time speed to watch live results

  ```bash
  ffmpeg -i any-audio-file -ar 16000 -ac 1 -sample_fmt s16 test.wav
  go run ./cmd/harkc test.wav
  ```

Without Docker, build it directly: `go build ./cmd/hark && ./hark -config config.yaml`. See [docs/CONFIG.md](docs/CONFIG.md) for configuration and [deploy/docker/hark.yaml](deploy/docker/hark.yaml) for an example. The client protocol is in [docs/PROTOCOL.md](docs/PROTOCOL.md).

## License

MIT
