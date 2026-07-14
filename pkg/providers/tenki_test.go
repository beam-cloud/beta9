package providers

import (
	"strings"
	"testing"
)

func TestRenderTenkiBootstrap(t *testing.T) {
	script, err := renderTenkiBootstrap(userDataConfig{
		TailscaleAuth:     "ts-auth-xyz",
		TailscaleUrl:      "https://headscale.example.com",
		RegistrationToken: "reg-token-123",
		MachineId:         "machine-abc",
		PoolName:          "tenki-cpu",
		ProviderName:      "tenki",
	})
	if err != nil {
		t.Fatalf("renderTenkiBootstrap returned error: %v", err)
	}

	// Every config field must land in the rendered agent invocation, and the
	// agent must be started detached so Exec can return.
	wants := []string{
		"release.beam.cloud/agent/agent",
		"setsid",
		`--token "reg-token-123"`,
		`--machine-id "machine-abc"`,
		`--tailscale-url "https://headscale.example.com"`,
		`--tailscale-auth "ts-auth-xyz"`,
		`--pool-name "tenki-cpu"`,
		`--provider-name "tenki"`,
	}
	for _, want := range wants {
		if !strings.Contains(script, want) {
			t.Errorf("bootstrap script missing %q\n--- script ---\n%s", want, script)
		}
	}

	if strings.Contains(script, "{{") {
		t.Errorf("bootstrap script has unrendered template placeholder:\n%s", script)
	}
}
