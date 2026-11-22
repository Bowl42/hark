FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /hark ./cmd/hark && mkdir /data

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /hark /hark
# Personal vocabulary directory: created in the image and owned by nonroot, so a named volume inherits these permissions on first mount
COPY --from=build --chown=nonroot:nonroot /data /data
EXPOSE 8080
ENTRYPOINT ["/hark", "-config", "/config.yaml"]
