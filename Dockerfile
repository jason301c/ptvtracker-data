# Multi-stage build for production
FROM golang:1.23.5-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy go mod files first for better caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags='-w -s -extldflags "-static"' \
    -a -installsuffix cgo \
    -o ptvtracker \
    cmd/ptvtracker/main.go

# Production stage
FROM alpine:3.20

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1001 -S ptvtracker && \
    adduser -u 1001 -S ptvtracker -G ptvtracker

# Set working directory
WORKDIR /app

# Create directories for logs and data with proper permissions
RUN mkdir -p /app/logs /app/data && \
    chown -R ptvtracker:ptvtracker /app

# Copy the binary from builder stage
COPY --from=builder /app/ptvtracker .

# Copy SQL migrations
COPY --from=builder /app/sql ./sql

# Set ownership
RUN chown -R ptvtracker:ptvtracker /app

# Switch to non-root user
USER ptvtracker

# Set environment variables
ENV LOG_LEVEL=info
ENV LOG_FILE=/app/logs/ptvtracker.log
ENV GTFS_STATIC_DOWNLOAD_DIR=/app/data

# Expose health check port (if you add one later)
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD pgrep ptvtracker || exit 1

# Run the application
CMD ["./ptvtracker"]