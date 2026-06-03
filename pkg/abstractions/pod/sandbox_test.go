package pod

import (
	"errors"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestSandboxConnectErrorMessageDoesNotLeakDetails(t *testing.T) {
	got := sandboxConnectErrorMessage(errors.New("container state not found: sandbox-123 on worker 10.0.0.12"))
	if got != "Failed to connect to sandbox" {
		t.Fatalf("sandboxConnectErrorMessage leaked details: %q", got)
	}
}

func TestPodRunnableStubOnlyAllowsPodAndSandboxKinds(t *testing.T) {
	tests := []struct {
		name     string
		stubType types.StubType
		want     bool
	}{
		{name: "pod run", stubType: types.StubType(types.StubTypePodRun), want: true},
		{name: "pod deployment", stubType: types.StubType(types.StubTypePodDeployment), want: true},
		{name: "sandbox", stubType: types.StubType(types.StubTypeSandbox), want: true},
		{name: "endpoint", stubType: types.StubType(types.StubTypeEndpoint), want: false},
		{name: "asgi deployment", stubType: types.StubType(types.StubTypeASGIDeployment), want: false},
		{name: "function", stubType: types.StubType(types.StubTypeFunction), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := podRunnableStub(tt.stubType); got != tt.want {
				t.Fatalf("podRunnableStub(%q) = %t, want %t", tt.stubType, got, tt.want)
			}
		})
	}
}
