# =============================================================================
# Stage 1: Build binary using official Go Alpine image
# =============================================================================
FROM golang:alpine AS builder

WORKDIR /build

RUN apk add --no-cache git ca-certificates tzdata

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /build/agent ./cmd/agent

# =============================================================================
# Stage 2: Minimal runtime image
# =============================================================================
FROM alpine:latest

WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata curl

# Create directory for persistent SQLite database
RUN mkdir -p /data

COPY --from=builder /build/agent /app/agent

ENV PORT=8080 \
    SQLITE_DB_PATH=/data/agent.db \
    INITIAL_CAPITAL=100000 \
    MARGIN_MULTIPLIER=2.0 \
    BOOTSTRAP_MODE=true \
    MAX_ORDER_VALUE=50000 \
    MAX_SHARES_PER_ORDER=5000

EXPOSE 8080

ENTRYPOINT ["/app/agent"]
