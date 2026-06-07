package compute

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

func AgentMachineID(workspaceID, poolName, fingerprint string) string {
	seed := fingerprint
	if seed == "" {
		seed = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return shortComputeID(workspaceID + "\x00" + poolName + "\x00" + seed)
}

func AgentMachineWorkerID(machineID string) string {
	return shortComputeID(machineID)
}

func shortComputeID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])[:8]
}
