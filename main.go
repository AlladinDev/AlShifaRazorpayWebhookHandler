// Razorpay webhook receiver -> RabbitMQ.
//
// Accepts POST requests from Razorpay and publishes the raw request body,
// byte-for-byte, to RabbitMQ. The body is NOT parsed or validated.
//
// Signature verification is DISABLED for now. To enable it, see verify.go and
// the BEGIN-VERIFY / END-VERIFY blocks below (3 places in total).
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

const (
	signatureHeader = "X-Razorpay-Signature"
	eventIDHeader   = "X-Razorpay-Event-Id"
)

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	// Load the .env file in the current directory
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	fmt.Print("envs loaded successfully")

	cfg := loadRabbitConfig()
	port := getenv("PORT", "8080")
	path := getenv("WEBHOOK_PATH", "/webhook/razorpay")
	maxBody, _ := strconv.ParseInt(getenv("MAX_BODY_BYTES", "1048576"), 10, 64) // 1 MiB

	// ENABLE SIGNATURE VERIFICATION (2/3): uncomment the lines between BEGIN-VERIFY and END-VERIFY.
	// BEGIN-VERIFY
	webhookSecret := os.Getenv("RAZORPAY_WEBHOOK_SECRET")
	if webhookSecret == "" {
		log.Fatal("RAZORPAY_WEBHOOK_SECRET is required when signature verification is enabled")
	}
	// END-VERIFY

	pub := newPublisher(cfg)
	if err := pub.connect(); err != nil {
		// Don't die: RabbitMQ may come up later. publish() reconnects on demand.
		log.Printf("initial RabbitMQ connection failed (will retry on requests): %v", err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})

	mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Read the exact raw body. Never re-serialize it: signatures are computed over these bytes.
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			var tooBig *http.MaxBytesError
			if errors.As(err, &tooBig) {
				http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
				return
			}
			http.Error(w, "failed to read body", http.StatusBadRequest)
			return
		}

		// ---- SIGNATURE VERIFICATION (currently DISABLED) ----
		// To enable: remove the leading "// " from every line between BEGIN-VERIFY and
		// END-VERIFY (leave the two marker lines commented). Also do the same in main()
		// (webhookSecret lines) and in verify.go.
		// BEGIN-VERIFY
		signature := r.Header.Get(signatureHeader)
		if signature == "" {
			http.Error(w, "missing signature", http.StatusUnauthorized)
			return
		}
		if !verifySignature(body, signature, webhookSecret) {
			log.Printf("rejected webhook: invalid signature (event id %q)", r.Header.Get(eventIDHeader))
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
		// END-VERIFY

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		contentType := r.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/json"
		}
		if err := pub.publish(ctx, body, contentType, r.Header.Get(eventIDHeader)); err != nil {
			log.Printf("publish failed: %v", err)
			// Non-2xx makes Razorpay retry, which is what we want when the message wasn't stored.
			http.Error(w, "failed to publish event", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      20 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	go func() {
		log.Printf("listening on :%s, webhook path %s", port, path)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server error: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	pub.close()
}
