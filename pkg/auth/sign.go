package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"time"
)

type Signature struct {
	Key       string
	Timestamp int64
}

func SignPayload(payload []byte, secretKey string) Signature {
	base64Payload := base64.StdEncoding.EncodeToString(payload)

	currentTime := time.Now().Unix()
	dataToSign := fmt.Sprintf("%s:%d", base64Payload, currentTime) // Concatenate base64 payload with timestamp

	// Initialize HMAC with SHA256 and secret key
	h := hmac.New(sha256.New, []byte(secretKey))
	h.Write([]byte(dataToSign))

	// Compute the HMAC signature
	signature := h.Sum(nil)
	hexSignature := hex.EncodeToString(signature)

	return Signature{
		Key:       hexSignature,
		Timestamp: currentTime,
	}
}
