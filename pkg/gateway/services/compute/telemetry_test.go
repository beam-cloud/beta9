package compute

import (
	"strings"
	"testing"
)

func TestRedactTelemetryLogLine(t *testing.T) {
	line := `access_key=access-value secretKey:secret-value Authorization: Bearer auth-value password="password-value" API key phrase-value {"AZURE_CLIENT_SECRET":"azure-value","apiKey":"api-value","logApiKey":"log-token","eventApiKey":"event-token","logCredential":"log-credential","eventCredential":"event-credential","credentials":"credential-value"} AWS_SECRET_ACCESS_KEY=aws-value SECRET_TOKEN=token-value normal=value`
	redacted := redactTelemetryLogLine(line)

	for _, leaked := range []string{
		"access-value",
		"secret-value",
		"auth-value",
		"password-value",
		"azure-value",
		"api-value",
		"log-token",
		"event-token",
		"log-credential",
		"event-credential",
		"credential-value",
		"aws-value",
		"token-value",
		"phrase-value",
	} {
		if strings.Contains(redacted, leaked) {
			t.Fatalf("redacted line leaked %q: %s", leaked, redacted)
		}
	}
	if !strings.Contains(redacted, "normal=value") {
		t.Fatalf("redacted line removed normal value: %s", redacted)
	}
}
