package common

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
)

func Encrypt(secretKey []byte, plaintext string) (string, error) {
	aes, err := aes.NewCipher(secretKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(aes)
	if err != nil {
		return "", err
	}

	// We need a 12-byte nonce for GCM (modifiable if you use cipher.NewGCMWithNonceSize())
	// A nonce should always be randomly generated for every encryption.
	nonce := make([]byte, gcm.NonceSize())
	_, err = rand.Read(nonce)
	if err != nil {
		return "", err
	}

	// ciphertext here is actually nonce+ciphertext
	// So that when we decrypt, just knowing the nonce size
	// is enough to separate it from the ciphertext.
	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)

	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

func Decrypt(secretKey []byte, ciphertext64 string) (string, error) {
	ciphertextBytes, err := base64.StdEncoding.DecodeString(ciphertext64)
	if err != nil {
		return "", err
	}

	ciphertext := string(ciphertextBytes)

	aes, err := aes.NewCipher(secretKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(aes)
	if err != nil {
		return "", err
	}

	// Since we know the ciphertext is actually nonce+ciphertext
	// And len(nonce) == NonceSize(). We can separate the two.
	nonceSize := gcm.NonceSize()
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]

	plaintext, err := gcm.Open(nil, []byte(nonce), []byte(ciphertext), nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// DecryptAllSecrets decrypts all the given secrets in the order they are given.
func DecryptAllSecrets(signingKey []byte, secrets []string) ([]string, error) {
	decrypted := make([]string, len(secrets))
	for i, secret := range secrets {
		decryptedSecret, err := Decrypt(signingKey, secret)
		if err != nil {
			return nil, err
		}
		decrypted[i] = decryptedSecret
	}
	return decrypted, nil
}

func ParseSecretKey(secretKey string) ([]byte, error) {
	secret := secretKey[len("sk_"):]
	return base64.StdEncoding.DecodeString(secret)
}
