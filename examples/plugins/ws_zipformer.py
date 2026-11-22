"""Reference backend for hark's ws engine: streaming Zipformer (Chinese/English), outputs text while listening. Depends on sherpa-onnx and websockets.

    pip install sherpa-onnx websockets
    curl -fsSL https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20.tar.bz2 | tar xjf -
    python ws_zipformer.py --model-dir sherpa-onnx-streaming-zipformer-bilingual-zh-en-2023-02-20

hark config:

    engines:
      zipformer:
        type: ws
        url: ws://127.0.0.1:9001

See engine/wsasr for the protocol: recognize audio as it arrives and send back all sentences whenever the result changes; hark handles rewrites and finalization.
"""
import argparse
import array
import asyncio
import json
import os

import sherpa_onnx
import websockets


def load(d, threads):
    return sherpa_onnx.OnlineRecognizer.from_transducer(
        tokens=os.path.join(d, "tokens.txt"),
        encoder=os.path.join(d, "encoder-epoch-99-avg-1.int8.onnx"),
        decoder=os.path.join(d, "decoder-epoch-99-avg-1.onnx"),
        joiner=os.path.join(d, "joiner-epoch-99-avg-1.int8.onnx"),
        num_threads=threads,
        enable_endpoint_detection=True,  # segment sentences at pauses
    )


class Session:
    """One session: the finished sentences plus the one being spoken."""

    def __init__(self, model):
        self.model, self.stream = model, model.create_stream()
        self.done = []      # finished sentences
        self.start = 0      # start of the current sentence (in samples)
        self.samples = 0    # total samples received

    def feed(self, pcm):
        samples = array.array("h", pcm)  # s16le, assumes a little-endian machine
        self.samples += len(samples)
        self.stream.accept_waveform(16000, [s / 32768 for s in samples])
        self.decode()

    def finish(self):
        # A streaming model needs a bit of trailing context to output the last few characters: pad 0.66 s of silence (not counted in audio_range)
        self.stream.accept_waveform(16000, [0.0] * int(0.66 * 16000))
        self.stream.input_finished()
        self.decode()
        self.cut()

    def decode(self):
        while self.model.is_ready(self.stream):
            self.model.decode_stream(self.stream)
        if self.model.is_endpoint(self.stream):
            self.cut()

    def cut(self):
        text = self.spaced(self.model.get_result(self.stream))
        if text:
            self.done.append(self.segment(text, True))
        self.model.reset(self.stream)
        self.start = self.samples

    def segment(self, text, final):
        return {"text": text, "final": final, "audio_range": {"start_sample": self.start, "end_sample": self.samples}}

    def snapshot(self):
        # The sentence being spoken has no audio_range (an optional field): otherwise every audio chunk would change it, forcing a resend even when the text is unchanged
        cur = self.spaced(self.model.get_result(self.stream))
        return self.done + ([{"text": cur}] if cur else [])

    def spaced(self, text):
        """hark concatenates segments directly: when an English sentence follows English text, the space goes at the start of this segment."""
        prev = self.done[-1]["text"] if self.done else ""
        if text and prev and prev[-1].isascii() and prev[-1].isalnum() and text[0].isascii() and text[0].isalnum():
            return " " + text
        return text


async def handle(ws, model, token):
    if token and ws.request.headers.get("Authorization") != "Bearer " + token:
        await ws.send(json.dumps({"type": "error", "message": "unauthorized"}))
        return
    await ws.recv()  # start: this model takes no parameters
    s, last = Session(model), None
    async for msg in ws:
        if isinstance(msg, bytes):
            await asyncio.to_thread(s.feed, msg)
        elif json.loads(msg).get("type") == "finish":
            await asyncio.to_thread(s.finish)
        snap = s.snapshot()
        if snap != last:  # send only when the result changed
            await ws.send(json.dumps({"type": "result", "segments": snap}, ensure_ascii=False))
            last = snap
        if isinstance(msg, str):
            await ws.send(json.dumps({"type": "done"}))
            return


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--model-dir", required=True)
    p.add_argument("--listen", default="127.0.0.1:9001")
    p.add_argument("--threads", type=int, default=2)
    p.add_argument("--token", default=os.environ.get("TOKEN", ""))
    a = p.parse_args()
    model = load(a.model_dir, a.threads)
    host, port = a.listen.rsplit(":", 1)
    async with websockets.serve(lambda ws: handle(ws, model, a.token), host, int(port), max_size=None):
        print(f"listening on ws://{a.listen}", flush=True)
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
