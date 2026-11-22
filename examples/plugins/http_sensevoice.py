"""Reference backend for hark's http engine: batch recognition with SenseVoice. Depends only on the standard library and sherpa-onnx.

    pip install sherpa-onnx
    curl -fsSL https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-sense-voice-zh-en-ja-ko-yue-2024-07-17.tar.bz2 | tar xjf -
    python http_sensevoice.py --model-dir sherpa-onnx-sense-voice-zh-en-ja-ko-yue-2024-07-17

hark config:

    engines:
      sensevoice:
        type: http
        url: http://127.0.0.1:9000/recognize

To use another model, change only recognize(): it receives 16kHz / 16-bit / mono PCM and returns text.
"""
import argparse
import array
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import sherpa_onnx


def load(model_dir, threads):
    return sherpa_onnx.OfflineRecognizer.from_sense_voice(
        model=os.path.join(model_dir, "model.int8.onnx"),
        tokens=os.path.join(model_dir, "tokens.txt"),
        num_threads=threads,
        use_itn=True,
    )


def recognize(model, pcm: bytes) -> str:
    samples = array.array("h", pcm)  # s16le, assumes a little-endian machine
    stream = model.create_stream()
    stream.accept_waveform(16000, [s / 32768 for s in samples])
    model.decode_stream(stream)
    return stream.result.text


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path != "/recognize":
            return self.reply(404, {"error": "not found"})
        if self.server.token and self.headers.get("Authorization") != "Bearer " + self.server.token:
            return self.reply(401, {"error": "unauthorized"})
        pcm = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.reply(200, {"text": recognize(self.server.model, pcm)})

    def reply(self, code, obj):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--model-dir", required=True)
    p.add_argument("--listen", default="127.0.0.1:9000")
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--token", default=os.environ.get("TOKEN", ""))
    a = p.parse_args()
    host, port = a.listen.rsplit(":", 1)
    srv = ThreadingHTTPServer((host, int(port)), Handler)
    srv.model, srv.token = load(a.model_dir, a.threads), a.token
    print(f"listening on http://{a.listen}/recognize", flush=True)
    srv.serve_forever()
