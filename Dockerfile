# Build stage
ARG GO_VERSION=1.25

FROM golang:${GO_VERSION}-alpine AS builder

# Set working directory
WORKDIR /build

ARG GOPROXY
ENV GOPROXY=${GOPROXY:-}

# Install build dependencies
RUN apk add git make

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN --mount=type=cache,target=/cache/go-mod \
  --mount=type=cache,target=/cache/go-build \
  go mod download

# Copy source code
COPY . .

RUN echo "Git commit hash: "$(git rev-parse HEAD)

# Build the application
RUN --mount=type=cache,target=/cache/go-mod \
  --mount=type=cache,target=/cache/go-build \
  make build

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk add --no-cache ca-certificates curl bash

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/aws-log-bridge .

# Set environment variables
ENV TZ=UTC

# Run the application
ENTRYPOINT ["/app/aws-log-bridge"]
