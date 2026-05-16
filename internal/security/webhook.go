package security

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
)

func VerifyWebhookSignature(r *http.Request, secret string) error {
	signature := r.Header.Get("X-Webhook-Signature")
	if signature == "" {
		return fmt.Errorf("missing webhook signature")
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return fmt.Errorf("reading webhook body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))

	if !hmac.Equal([]byte(signature), []byte(expected)) {
		return fmt.Errorf("invalid webhook signature")
	}

	return nil
}
