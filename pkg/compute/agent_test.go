package compute

import "testing"

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

func isLowerHex(value string) bool {
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return value != ""
}
