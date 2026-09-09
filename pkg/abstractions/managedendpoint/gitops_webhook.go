package managedendpoint

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// pushEvent is the subset of a GitHub/GitLab push payload the reconciler needs.
type pushEvent struct {
	Ref     string
	After   string
	Deleted bool
}

func readBody(r *http.Request, limit int64) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(io.LimitReader(r.Body, limit))
}

// verifyWebhook accepts GitHub's HMAC-SHA256 signature
// (X-Hub-Signature-256: sha256=<hex>), GitLab's shared token
// (X-Gitlab-Token) and a generic bearer/plain token in X-Webhook-Token.
func verifyWebhook(header http.Header, body []byte, secret string) bool {
	if sig := strings.TrimSpace(header.Get("X-Hub-Signature-256")); sig != "" {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(body)
		want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
		return hmac.Equal([]byte(strings.ToLower(sig)), []byte(want))
	}
	for _, name := range []string{"X-Gitlab-Token", "X-Webhook-Token"} {
		if token := strings.TrimSpace(header.Get(name)); token != "" {
			return subtle.ConstantTimeCompare([]byte(token), []byte(secret)) == 1
		}
	}
	if authz := strings.TrimSpace(header.Get("Authorization")); strings.HasPrefix(authz, "Bearer ") {
		return subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(authz, "Bearer ")), []byte(secret)) == 1
	}
	return false
}

// parsePush extracts ref/after from GitHub and GitLab push payloads. It
// returns false for payloads that are not pushes (pings, PR events, ...).
func parsePush(body []byte) (pushEvent, bool) {
	var raw struct {
		Ref        string `json:"ref"`
		After      string `json:"after"`
		Deleted    bool   `json:"deleted"`
		ObjectKind string `json:"object_kind"` // gitlab
		Zen        string `json:"zen"`         // github ping
	}
	if err := json.Unmarshal(body, &raw); err != nil || raw.Ref == "" || raw.Zen != "" {
		return pushEvent{}, false
	}
	if raw.ObjectKind != "" && raw.ObjectKind != "push" && raw.ObjectKind != "tag_push" {
		return pushEvent{}, false
	}
	deleted := raw.Deleted || strings.Trim(raw.After, "0") == ""
	return pushEvent{Ref: raw.Ref, After: raw.After, Deleted: deleted}, true
}

// decodeJSON reads a bounded JSON body into out.
func decodeJSON(r *http.Request, limit int64, out any) error {
	body, err := readBody(r, limit)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}
