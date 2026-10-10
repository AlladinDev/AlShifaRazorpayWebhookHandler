# Razorpay webhook -> RabbitMQ (Go)

Receives Razorpay webhooks and publishes the raw request body, byte-for-byte, to RabbitMQ.
The body is never parsed. **Signature verification is currently disabled** (code is commented out).

## Run (prebuilt static binary, nothing to install)
    chmod +x razorpay-webhook-linux-amd64        # or -arm64
    export RABBITMQ_HOST=... RABBITMQ_USER=... RABBITMQ_PASSWORD=...   # see .env.example
    ./razorpay-webhook-linux-amd64

Razorpay webhook URL: https://your-domain/webhook/razorpay  (put TLS / a reverse proxy in front).
Health check: GET /healthz

## Behaviour
- POST only. 200 is returned only after RabbitMQ confirms the message (publisher confirms).
  Otherwise 500, so Razorpay retries.
- Message is persistent, published to a durable exchange; the queue is declared and bound on startup.
- `message_id` = Razorpay's `X-Razorpay-Event-Id` header. Razorpay may deliver the same event more
  than once, so make consumers idempotent on this id.
- Reconnects automatically if RabbitMQ restarts.
- `mandatory` is set: if the exchange has no bound queue, publish fails (500) instead of losing the event.

## Enable signature verification later
Remove the leading `// ` from the lines between `BEGIN-VERIFY` and `END-VERIFY` (leave the marker
lines commented) in 3 places: `verify.go`, and `main.go` (once in main(), once in the handler).
Set `RAZORPAY_WEBHOOK_SECRET`, then rebuild.

## Rebuild (dependencies are vendored, works offline)
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -trimpath -ldflags="-s -w" -o razorpay-webhook-linux-amd64 .
