# Configuration

`hark -config config.yaml` (by default reads `config.yaml` in the current directory). Use `${ENV}` anywhere to reference an environment variable, so secrets don't have to live in the file; unset variables expand to empty. See [deploy/docker/hark.yaml](../deploy/docker/hark.yaml) for a full example.

## Top level

| Field | Default | Description |
|---|---|---|
| `listen` | `:8080` | Listen address |
| `tokens` | empty | Allowed tokens. Clients send `Authorization: Bearer <token>` (browsers use `?token=`). When empty, there is no authentication, which is only suitable for a private network; empty strings are ignored |
| `data_dir` | empty | Personal vocabulary directory, holds `vocab.db` (SQLite). If unset, personal vocabulary is disabled |
| `max_session` | `10m` | Maximum duration of a single session |
| `finish_timeout` | `15s` | How long to wait for the engine to finalize after the client sends finish |
| `default_engine` | see description | Engine to use when the client doesn't specify one. Can be omitted when only one engine is configured |
| `engines` | required | Engine instances: `name: {type: type, ...fields for that type}`. Names are arbitrary; clients select engines by name |

Durations need a unit, e.g. `500ms`, `15s`, `10m`.

## Engine types

### Batch: `sherpa`, `http`, `openai`

These backends can only do batch recognition; hark segments sentences and finalizes them to make them streaming. They share three fields:

| Field | Default | Description |
|---|---|---|
| `interval` | `0` | How often to re-recognize the current sentence while the user is speaking (text appears as you speak). `0` means each sentence is recognized only once, at its end. Don't set this for pay-per-request services, or costs multiply |
| `silence` | `600ms` | How much silence ends a sentence (shrinks gradually for long sentences) |
| `max_sentence` | `20s` | Maximum sentence length; longer sentences are forcibly split |

| Type | Fields | Description |
|---|---|---|
| `sherpa` | `url` (required) | sherpa-onnx non-streaming recognition server (e.g. SenseVoice), `ws://host:6006`. See [deploy/sherpa](../deploy/sherpa/Dockerfile) for setup |
| `http` | `url` (required), `token` | Any HTTP service that takes whole audio in and returns text; see [engine/httpasr](../engine/httpasr/httpasr.go) for the interface |
| `openai` | `model` (required), `api_key`, `base_url` (default `https://api.openai.com/v1`), `language` | OpenAI-compatible transcription APIs: OpenAI, Groq, SiliconFlow, self-hosted speaches, vLLM, whisper.cpp, etc.; see [engine/openai](../engine/openai/openai.go) |

### Streaming: `ws`, `volc`

| Type | Fields | Description |
|---|---|---|
| `ws` | `url` (required), `token` | Any WebSocket service that outputs text while listening; see [engine/wsasr](../engine/wsasr/wsasr.go) for the protocol |
| `volc` | `api_key`, or `app_key` + `access_key`; `resource_id` (default `volc.seedasr.sauc.duration`), `url` (default: the bigmodel_async endpoint) | Volcengine (Doubao) large-model streaming recognition; see [engine/volc](../engine/volc/volc.go) |

### Testing: `mock`

| Type | Fields | Description |
|---|---|---|
| `mock` | `step_ms` (default `300`) | Fake engine: for every this many milliseconds of audio received, it outputs a step of a script with typos and corrections. No model needed |

## Your own engine

Implement `hark.Engine` in Go (or `batch.Recognizer` for batch-only recognition) and register it as a new type:

```go
func main() {
	harkcmd.Run(harkcmd.Builtin(), harkcmd.Engine("myasr", myasr.New))
}
```

The config can then use `type: myasr`; the rest of that block is decoded into `myasr.Config` (yaml tags). Register batch recognizers with `harkcmd.Batch` to get the three shared fields above automatically.
