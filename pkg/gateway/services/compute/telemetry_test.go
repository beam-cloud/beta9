package compute

import (
	"fmt"
	"strings"
	"testing"

	model "github.com/beam-cloud/beta9/pkg/compute"
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

func TestAppendPathMetricAttrsUsesBoundedFixedKeys(t *testing.T) {
	metrics := make([]model.AgentPathMetric, 0, telemetryMaxPathMetricAttrs+2)
	metrics = append(metrics, model.AgentPathMetric{
		Label:       "cache/../../arbitrary",
		Path:        "/var/lib/beta9/cache",
		UsedMB:      10,
		TotalMB:     100,
		AvailableMB: 90,
		UsagePct:    10,
	})
	for i := 0; i < telemetryMaxPathMetricAttrs+1; i++ {
		metrics = append(metrics, model.AgentPathMetric{Label: fmt.Sprintf("label-%d", i)})
	}

	attrs := map[string]string{}
	appendPathMetricAttrs(attrs, metrics)

	if attrs["path_metric_0_label"] != "cache/../../arbitrary" {
		t.Fatalf("path metric label attr = %q", attrs["path_metric_0_label"])
	}
	if attrs["path_metric_0_path"] != "/var/lib/beta9/cache" {
		t.Fatalf("path metric path attr = %q", attrs["path_metric_0_path"])
	}
	if _, ok := attrs["path_cache/../../arbitrary_path"]; ok {
		t.Fatal("agent-supplied label was used as an attribute key")
	}
	if attrs["path_metric_count"] != fmt.Sprintf("%d", telemetryMaxPathMetricAttrs) {
		t.Fatalf("path metric count = %q", attrs["path_metric_count"])
	}
	if _, ok := attrs[fmt.Sprintf("path_metric_%d_label", telemetryMaxPathMetricAttrs)]; ok {
		t.Fatal("path metrics exceeded bounded attr count")
	}
}
