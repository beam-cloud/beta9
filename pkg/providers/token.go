package providers

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

// A token is replaced this long before it expires, so that a call sent with
// it arrives while it is still good.
const renewTokenBefore = 2 * time.Minute

// accessToken holds the short-lived token a provider trades its credentials
// for, and fetches the next one when it is about to expire.
type accessToken struct {
	// fetch trades the credentials for a token and says how long it lasts.
	fetch func(ctx context.Context) (token string, lifetime time.Duration, err error)
	now   func() time.Time

	mu      sync.Mutex
	token   string
	expires time.Time
}

func newAccessToken(now func() time.Time, fetch func(ctx context.Context) (string, time.Duration, error)) *accessToken {
	return &accessToken{fetch: fetch, now: now}
}

func (t *accessToken) get(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.token != "" && t.now().Before(t.expires.Add(-renewTokenBefore)) {
		return t.token, nil
	}
	token, lifetime, err := t.fetch(ctx)
	if err != nil {
		return "", fmt.Errorf("get an access token: %w", err)
	}
	if token == "" {
		return "", fmt.Errorf("get an access token: the provider sent none")
	}
	t.token, t.expires = token, t.now().Add(lifetime)
	return token, nil
}

// forget drops the token, so that the next call fetches one.
func (t *accessToken) forget() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.token = ""
}

// bearer authorizes with the token as a bearer token.
func (t *accessToken) bearer() authorizer {
	return authorizer{
		add: func(ctx context.Context, req *http.Request) error {
			token, err := t.get(ctx)
			if err != nil {
				return err
			}
			req.Header.Set("Authorization", "Bearer "+token)
			return nil
		},
		renew: t.forget,
	}
}

// grant asks a token endpoint for an access token, which OAuth has it answer
// with the token and the seconds it lasts. What is asked with, and whether
// as JSON or as a form, is the provider's own. Asking twice does no harm.
func grant(ctx context.Context, login *api, request call) (token string, lifetime time.Duration, err error) {
	var granted struct {
		AccessToken string  `json:"access_token"`
		ExpiresIn   float64 `json:"expires_in"` // seconds
	}
	request.method, request.out, request.repeatable = http.MethodPost, &granted, true
	err = login.do(ctx, request)
	return granted.AccessToken, time.Duration(granted.ExpiresIn * float64(time.Second)), err
}

// rsaKey reads a service account's private key from PEM, in either of the
// two forms such keys are handed out in. A key that went through an
// environment variable or a JSON file has its line breaks written as \n.
func rsaKey(text string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(strings.ReplaceAll(text, `\n`, "\n")))
	if block == nil {
		return nil, fmt.Errorf("the private key is not in PEM form")
	}
	if key, err := x509.ParsePKCS1PrivateKey(block.Bytes); err == nil {
		return key, nil
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("read the private key: %w", err)
	}
	key, ok := parsed.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("the private key is not an RSA key")
	}
	return key, nil
}

// signedJWT returns the claims as a JSON Web Token signed with the key,
// RS256. It is what a service account proves itself with when it asks for an
// access token.
func signedJWT(key *rsa.PrivateKey, keyID string, claims map[string]any) (string, error) {
	header := map[string]string{"alg": "RS256", "typ": "JWT"}
	if keyID != "" {
		header["kid"] = keyID
	}
	encode := func(part any) (string, error) {
		data, err := json.Marshal(part)
		return base64.RawURLEncoding.EncodeToString(data), err
	}
	head, err := encode(header)
	if err != nil {
		return "", err
	}
	body, err := encode(claims)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256([]byte(head + "." + body))
	signature, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		return "", err
	}
	return head + "." + body + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}
