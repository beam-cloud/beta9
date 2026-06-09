package compute

import (
	"strings"
	"testing"
)

func TestAgentMachineAndWorkerIDsUseSharedShortIDShape(t *testing.T) {
	machineID := AgentMachineID("workspace-one", "private-pool", "host-fingerprint")
	if len(machineID) != 8 {
		t.Fatalf("machine id length = %d, want 8: %q", len(machineID), machineID)
	}
	if !isLowerHex(machineID) {
		t.Fatalf("machine id = %q, want lowercase hex", machineID)
	}
	if machineID != AgentMachineID("workspace-one", "private-pool", "host-fingerprint") {
		t.Fatal("machine id must be stable for a workspace/pool/fingerprint")
	}
	if machineID == AgentMachineID("workspace-one", "other-pool", "host-fingerprint") {
		t.Fatal("machine id should be scoped by pool")
	}

	workerID := AgentMachineWorkerID(machineID)
	if len(workerID) != 8 {
		t.Fatalf("worker id length = %d, want 8: %q", len(workerID), workerID)
	}
	if !isLowerHex(workerID) {
		t.Fatalf("worker id = %q, want lowercase hex", workerID)
	}
	if workerID != AgentMachineWorkerID(machineID) {
		t.Fatal("worker id must be stable for a machine")
	}
}

func TestManagedMachineAndNodeName(t *testing.T) {
	machineID := ManagedMachineID("workspace-one", "pool one", "reservation-seed")
	if len(machineID) != 8 {
		t.Fatalf("managed machine id length = %d, want 8: %q", len(machineID), machineID)
	}
	if !isLowerHex(machineID) {
		t.Fatalf("managed machine id = %q, want lowercase hex", machineID)
	}
	if machineID != ManagedMachineID("workspace-one", "pool one", "reservation-seed") {
		t.Fatal("managed machine id must be stable for the same seed")
	}

	name := ManagedNodeName("9202152b-e63e-47fd-825b-a193913e7403", "My Pool_GPU", machineID)
	if name != "beam-9202152b-my-pool-gpu-"+machineID {
		t.Fatalf("managed node name = %q", name)
	}
	if len(ManagedNodeName("workspace", strings.Repeat("a", 100), machineID)) > managedNodeNameMaxLen {
		t.Fatal("managed node name exceeded provider-safe length")
	}
}

func isLowerHex(value string) bool {
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return value != ""
}
