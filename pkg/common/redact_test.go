package common

import (
	"strings"
	"testing"
)

func TestRedactSecrets(t *testing.T) {
	line := `access_key=abc secretKey:xyz Authorization: Bearer token-value password="p" normal=value`
	redacted := RedactSecrets(line)

	for _, leaked := range []string{"abc", "xyz", "token-value", `"p"`} {
		if strings.Contains(redacted, leaked) {
			t.Fatalf("redacted line leaked %q: %s", leaked, redacted)
		}
	}
	if !strings.Contains(redacted, "normal=value") {
		t.Fatalf("redacted line removed normal value: %s", redacted)
	}
}
