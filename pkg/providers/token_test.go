package providers

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAccessTokenIsFetchedOnceAndAgainBeforeItExpires(t *testing.T) {
	now := time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)
	fetched := 0
	token := newAccessToken(func() time.Time { return now }, func(context.Context) (string, time.Duration, error) {
		fetched++
		return fmt.Sprintf("token-%d", fetched), time.Hour, nil
	})
	header := func() string {
		req, err := http.NewRequest(http.MethodGet, "https://provider.example/", nil)
		require.NoError(t, err)
		require.NoError(t, token.bearer().add(context.Background(), req))
		return req.Header.Get("Authorization")
	}

	require.Equal(t, "Bearer token-1", header())
	now = now.Add(time.Hour - renewTokenBefore - time.Second)
	require.Equal(t, "Bearer token-1", header())
	require.Equal(t, 1, fetched)

	// From here a call might arrive after the token expired.
	now = now.Add(time.Second)
	require.Equal(t, "Bearer token-2", header())
	require.Equal(t, 2, fetched)
}

func TestAccessTokenThatCannotBeFetchedFailsTheCallAndIsTriedAgain(t *testing.T) {
	refused := errors.New("invalid client secret")
	fail := true
	token := newAccessToken(time.Now, func(context.Context) (string, time.Duration, error) {
		if fail {
			return "", 0, refused
		}
		return "token", time.Hour, nil
	})

	_, err := token.get(context.Background())
	require.ErrorIs(t, err, refused)

	fail = false
	got, err := token.get(context.Background())
	require.NoError(t, err)
	require.Equal(t, "token", got)
}

func TestAnEmptyAccessTokenIsNotUsed(t *testing.T) {
	token := newAccessToken(time.Now, func(context.Context) (string, time.Duration, error) { return "", time.Hour, nil })
	_, err := token.get(context.Background())
	require.Error(t, err)
}

// A service account's key comes as PKCS#8 or, from older tools, as PKCS#1.
// One that was put in an environment variable or copied out of a JSON file
// has its line breaks spelled out.
func TestAServiceAccountKeyIsReadInEveryFormItIsHandedOutIn(t *testing.T) {
	key := testServiceKey()
	pkcs8, err := x509.MarshalPKCS8PrivateKey(key)
	require.NoError(t, err)
	newer := string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pkcs8}))
	older := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))

	for name, text := range map[string]string{
		"PKCS#8":                  newer,
		"PKCS#1":                  older,
		"line breaks spelled out": strings.ReplaceAll(newer, "\n", `\n`),
	} {
		read, err := rsaKey(text)
		require.NoError(t, err, name)
		require.True(t, key.Equal(read), name)
	}

	elliptical, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	der, err := x509.MarshalPKCS8PrivateKey(elliptical)
	require.NoError(t, err)
	for name, text := range map[string]string{
		"not PEM":        "c2VjcmV0",
		"not a key":      string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("secret")})),
		"not an RSA key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
	} {
		_, err := rsaKey(text)
		require.Error(t, err, name)
	}
}

func TestASignedRequestCarriesItsClaimsAndCanBeVerified(t *testing.T) {
	key := testServiceKey()
	signed, err := signedJWT(key, "key-1", map[string]any{"iss": "account", "exp": 1794623400})
	require.NoError(t, err)

	parts := strings.Split(signed, ".")
	require.Len(t, parts, 3)
	decode := func(part string) map[string]any {
		data, err := base64.RawURLEncoding.DecodeString(part)
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(data, &decoded))
		return decoded
	}
	require.Equal(t, map[string]any{"alg": "RS256", "typ": "JWT", "kid": "key-1"}, decode(parts[0]))
	require.Equal(t, map[string]any{"iss": "account", "exp": float64(1794623400)}, decode(parts[1]))

	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	require.NoError(t, err)
	digest := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	require.NoError(t, rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, digest[:], signature))

	// A key that has no ID is not given one.
	unnamed, err := signedJWT(key, "", map[string]any{})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"alg": "RS256", "typ": "JWT"}, decode(strings.Split(unnamed, ".")[0]))
}
