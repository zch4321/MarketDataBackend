# Multi-stage Dockerfile for md-control-plane and md-stream-runtime.
#
# Build:
#   docker build --target md-control-plane -t md-control-plane:latest .
#   docker build --target md-stream-runtime -t md-stream-runtime:latest .
#
# Run (control-plane):
#   docker run -p 8080:8080 -p 9090:9090 \
#     -e DATABASE_DSN="postgres://marketdata:${DB_PASSWORD}@postgres:5432/marketdata?sslmode=disable" \
#     md-control-plane:latest
#
# Run (stream-runtime):
#   docker run -p 9090:9090 \
#     -e DATABASE_DSN="postgres://marketdata:${DB_PASSWORD}@postgres:5432/marketdata?sslmode=disable" \
#     -e KAFKA_BROKERS="kafka:19092" \
#     md-stream-runtime:latest

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/md-control-plane ./cmd/md-control-plane
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/md-stream-runtime ./cmd/md-stream-runtime

# ---------------------------------------------------------------------------
# Runtime image (distroless static)
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS base

WORKDIR /app
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY migrations/ ./migrations/

# The default uid:gid in distroless nonroot is 65532.
USER 65532:65532

# --- md-control-plane ----------------------------------------------------
FROM base AS md-control-plane
COPY --from=builder /out/md-control-plane /app/md-control-plane
EXPOSE 8080 9090
ENTRYPOINT ["/app/md-control-plane"]

# --- md-stream-runtime ---------------------------------------------------
FROM base AS md-stream-runtime
COPY --from=builder /out/md-stream-runtime /app/md-stream-runtime
EXPOSE 9090
ENTRYPOINT ["/app/md-stream-runtime"]
