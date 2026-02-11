# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary
RUN VERSION=$(git describe --tags --exact-match 2>/dev/null || echo "$(git describe --tags --abbrev=0 2>/dev/null || echo v0.0.0)-dev-$(git rev-parse --short HEAD)") && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w -X main.version=${VERSION}" -o aws-snapshot .

# Final stage
FROM alpine:3.19

# Install CA certificates for HTTPS calls to AWS
RUN apk --no-cache add ca-certificates

WORKDIR /

# Copy binary from builder
COPY --from=builder /app/aws-snapshot /usr/local/bin/aws-snapshot

# Run as non-root user
RUN adduser -D -H appuser
USER appuser

ENTRYPOINT ["aws-snapshot"]
