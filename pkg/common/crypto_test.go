package common

import (
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEncryptDecrypt(t *testing.T) {
	signingKeyBytes := make([]byte, 32) // 256 bits
	if _, err := rand.Read(signingKeyBytes); err != nil {
		t.Fatal(err)
	}

	encryptedVal, err := Encrypt(
		signingKeyBytes,
		"my-secret-value",
	)
	if err != nil {
		t.Fatal(err)
	}

	assert.NotEqual(t, "my-secret-value", encryptedVal)

	decryptedVal, err := Decrypt(
		signingKeyBytes,
		encryptedVal,
	)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "my-secret-value", decryptedVal)
}
