package gatewayservices

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestProviderMachineStatusMapsReadyToAvailable(t *testing.T) {
	if got := providerMachineStatus(types.MachineStatusReady); got != types.MachineStatusAvailable {
		t.Fatalf("providerMachineStatus(ready) = %q, want %q", got, types.MachineStatusAvailable)
	}
	if got := providerMachineStatus(types.MachineStatusPending); got != types.MachineStatusPending {
		t.Fatalf("providerMachineStatus(pending) = %q, want %q", got, types.MachineStatusPending)
	}
}
