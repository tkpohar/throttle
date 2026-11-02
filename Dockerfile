# syntax=docker/dockerfile:1
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/throttle ./cmd/throttle

# Distroless: no shell, no package manager, runs as non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/throttle /throttle
COPY config.example.json /etc/throttle/config.json
EXPOSE 8080
ENTRYPOINT ["/throttle", "-config", "/etc/throttle/config.json"]
