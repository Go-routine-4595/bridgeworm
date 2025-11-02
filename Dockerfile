# Build stage
FROM golang:1.25-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the executable
# Docker buildx will automatically set TARGETOS and TARGETARCH
ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a -installsuffix cgo -o bridgeworm .

# Final stage
FROM alpine:latest
LABEL authors="christophebuffard"

# Install ca-certificates for HTTPS requests (if needed)
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -g 1001 -S appuser && \
    adduser -u 1001 -S appuser -G appuser &&\
    mkdir -p /app/logs && \
    chown -R appuser:appuser /app && \
    chown -R appuser:appuser /app/logs

WORKDIR /app

# Copy the executable from builder stage
COPY --from=builder /app/bridgeworm /app/bridgeworm

# Change ownership to non-root user
RUN chown appuser:appuser bridgeworm

# Switch to non-root user
USER appuser

# Expose the pprof port
EXPOSE 6060

# Set the entrypoint
ENTRYPOINT ["/app/bridgeworm"]