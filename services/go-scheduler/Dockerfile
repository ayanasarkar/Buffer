# --- Build stage ---
FROM golang:1.27-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /go-scheduler .

# --- Run stage ---
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /go-scheduler .

# Config (RABBITMQ_URL, POSTGRES_URL, QUEUE_NAME, etc.) is injected via
# real environment variables at runtime -- docker-compose `environment:`
# or a Kubernetes ConfigMap/Secret -- not baked into the image. .env is
# only for local `go run` and is excluded via .dockerignore.

ENTRYPOINT ["./go-scheduler"]
