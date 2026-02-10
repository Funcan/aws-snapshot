# Build stage
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Copy go mod files first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build static binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o aws-snapshot .

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
