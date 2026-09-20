package common

import (
	"crypto/rand"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestParseSecretKeyRejectsMalformedValues(t *testing.T) {
	for _, value := range []string{"", "x", "sk_", "sk_not-base64"} {
		_, err := ParseSecretKey(value)
		assert.Error(t, err, "ParseSecretKey(%q)", value)
	}

	valid := "sk_" + base64.StdEncoding.EncodeToString(make([]byte, 32))
	key, err := ParseSecretKey(valid)
	require.NoError(t, err)
	assert.Len(t, key, 32)
}

func TestParseSecretKeyPointerRejectsMissingKey(t *testing.T) {
	_, err := ParseSecretKeyPointer(nil)
	assert.EqualError(t, err, "workspace signing key is unavailable")

	empty := ""
	_, err = ParseSecretKeyPointer(&empty)
	assert.EqualError(t, err, "workspace signing key is unavailable")
}
