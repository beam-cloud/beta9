package hybrid

import (
	"crypto/sha256"
	"encoding/hex"
)

func AgentMachineWorkerID(machineID string) string {
	sum := sha256.Sum256([]byte(machineID))
	return "agent-" + hex.EncodeToString(sum[:])[:12]
}
