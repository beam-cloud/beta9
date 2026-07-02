package compute

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

const managedNodeNameMaxLen = 63

func AgentMachineID(workspaceID, poolName, fingerprint string) string {
	seed := fingerprint
	if seed == "" {
		seed = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return shortComputeID(workspaceID + "\x00" + poolName + "\x00" + seed)
}

func ManagedMachineID(workspaceID, poolName, seed string) string {
	if seed == "" {
		seed = fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return shortComputeID("managed\x00" + workspaceID + "\x00" + poolName + "\x00" + seed)
}

func MarketplaceListingID(workspaceID, displayName string) string {
	return shortComputeID("marketplace\x00" + workspaceID + "\x00" + displayName + "\x00" + fmt.Sprintf("%d", time.Now().UnixNano()))
}

func MarketplacePoolName(listingID string) string {
	if listingID = strings.TrimSpace(listingID); listingID != "" {
		return "marketplace-" + listingID
	}
	return ""
}

func AgentMachineWorkerID(machineID string) string {
	return shortComputeID(machineID)
}

func ManagedNodeName(workspaceID, poolName, machineID string) string {
	workspace := cleanNodeNamePart(workspaceID)
	if len(workspace) > 8 {
		workspace = workspace[:8]
	}
	pool := cleanNodeNamePart(poolName)
	machine := cleanNodeNamePart(machineID)
	if machine == "" {
		machine = shortComputeID(workspaceID + "\x00" + poolName)
	} else if len(machine) >= managedNodeNameMaxLen {
		machine = shortComputeID(machine)
	}

	parts := []string{"beam"}
	if workspace != "" {
		parts = append(parts, workspace)
	}
	if pool != "" {
		parts = append(parts, pool)
	}

	prefix := strings.Join(parts, "-")
	prefixLen := managedNodeNameMaxLen - len(machine) - 1
	if prefixLen <= 0 {
		return machine
	}
	if len(prefix) > prefixLen {
		prefix = strings.Trim(prefix[:prefixLen], "-")
	}
	if prefix == "" {
		return machine
	}
	return prefix + "-" + machine
}

func ReservationNodeName(req ReservationRequest) string {
	if name := trimNodeName(cleanNodeNamePart(req.Name)); name != "" {
		return name
	}
	return ManagedNodeName("", req.PoolName, req.MachineID)
}

func shortComputeID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return hex.EncodeToString(sum[:])[:8]
}

func cleanNodeNamePart(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var b strings.Builder
	lastDash := false
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash && b.Len() > 0 {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func trimNodeName(value string) string {
	if len(value) <= managedNodeNameMaxLen {
		return value
	}
	return strings.Trim(value[:managedNodeNameMaxLen], "-")
}
