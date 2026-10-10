package main

// Razorpay webhook signature verification (currently DISABLED).
//
// To enable:
//   1. Remove the leading "// " from every line between BEGIN-VERIFY and END-VERIFY below.
//   2. In main.go, uncomment the lines between BEGIN-VERIFY and END-VERIFY
//      (in main() and in the webhook handler).
//   3. Set RAZORPAY_WEBHOOK_SECRET in the environment.
//
// How it works: Razorpay sends hex(HMAC-SHA256(webhook_secret, raw_request_body))
// in the X-Razorpay-Signature header.

// BEGIN-VERIFY
import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
)

func verifySignature(body []byte, receivedSignature, secret string) bool {
	received, err := hex.DecodeString(receivedSignature)
	if err != nil {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	// hmac.Equal is a constant-time comparison.
	return hmac.Equal(received, mac.Sum(nil))
}

// END-VERIFY
