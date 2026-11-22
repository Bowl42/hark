# sherpa-onnx recognition server + SenseVoice model (downloaded at build time), port 6006.
#   docker build -t hark-sherpa deploy/sherpa
#   docker run -p 6006:6006 hark-sherpa --num-work-threads=8   # parallelism, about cores/2
FROM python:3.12-slim AS build
RUN apt-get update && apt-get install -y --no-install-recommends curl bzip2 && rm -rf /var/lib/apt/lists/*
RUN pip install -q sherpa-onnx sherpa-onnx-bin
ARG MODEL=sherpa-onnx-sense-voice-zh-en-ja-ko-yue-2024-07-17
RUN mkdir /out && cd /tmp \
 && curl -fsSL https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/$MODEL.tar.bz2 \
    | tar xjf - $MODEL/model.int8.onnx $MODEL/tokens.txt \
 && mv $MODEL/model.int8.onnx $MODEL/tokens.txt /out/ \
 && cp "$(command -v sherpa-onnx-offline-websocket-server)" /out/server \
 && cp "$(python -c 'import sherpa_onnx, os; print(os.path.dirname(sherpa_onnx.__file__))')/lib/libonnxruntime.so" /out/

# The runtime only needs the server itself, onnxruntime and the model; no Python
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /out /sherpa
ENV LD_LIBRARY_PATH=/sherpa
WORKDIR /tmp
EXPOSE 6006
ENTRYPOINT ["/sherpa/server", "--port=6006", "--num-threads=2", \
  "--tokens=/sherpa/tokens.txt", "--sense-voice-model=/sherpa/model.int8.onnx", "--sense-voice-use-itn=true", \
  "--max-utterance-length=60", "--log-file=/tmp/sherpa.log"]
CMD ["--num-work-threads=2"]
