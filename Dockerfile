# --- Stage 1: Build Module ---
FROM golang:1.25-alpine AS builder

# Install system dependencies (make, git) if needed
RUN apk add --no-cache git make

WORKDIR /app

# Copy dependency files first to utilize Docker cache
COPY go.mod go.sum ./
RUN go mod download

# Copy the source code
COPY . .

# Build the application
# CGO_ENABLED=0 ensures a statically linked binary (important for distroless/scratch)
RUN CGO_ENABLED=0 GOOS=linux go build -o /distributor ./cmd/distributor

# --- Stage 2: Runtime Image ---
# We use a distroless image for better security (no shell, no package manager)
FROM gcr.io/distroless/static-debian12

WORKDIR /

# Copy the binary from the builder stage
COPY --from=builder /distributor /distributor

# Run as non-root user (security best practice)
USER nonroot:nonroot

# Command to run the service
ENTRYPOINT ["/distributor"]