package pod

import (
	"errors"
	"testing"
)

func TestSandboxConnectErrorMessageDoesNotLeakDetails(t *testing.T) {
	got := sandboxConnectErrorMessage(errors.New("container state not found: sandbox-123 on worker 10.0.0.12"))
	if got != "Failed to connect to sandbox" {
		t.Fatalf("sandboxConnectErrorMessage leaked details: %q", got)
	}
}
