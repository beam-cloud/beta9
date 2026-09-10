package compute

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
	"time"
)

const managedNodeNameMaxLen = 63

func AgentMachineID(workspaceID, poolName, fingerprint string) string {
	if fingerprint == "" {
		fingerprint = nowSeed()
	}
	return shortComputeID(workspaceID, poolName, fingerprint)
}

func ManagedMachineID(workspaceID, poolName, seed string) string {
	if seed == "" {
		seed = nowSeed()
	}
	return shortComputeID("managed", workspaceID, poolName, seed)
}

// ProviderPoolName is the pool a workspace's contributed machines of one GPU
// type join (e.g. "provider-h100-ab12cd"). Pool names feed cache locality
// keys, so they are stable per workspace and GPU.
func ProviderPoolName(workspaceID, gpu string) string {
	slug := strings.Trim(cleanNodeNamePart(strings.ToLower(gpu)), "-")
	if slug == "" {
		return ""
	}
	return "provider-" + slug + "-" + shortComputeID("provider", workspaceID)
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
		machine = shortComputeID(workspaceID, poolName)
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

// shortComputeID derives a stable 8-hex-char id from its parts. Parts are
// joined with NUL — which cannot appear in any of them — so ("ab","c") and
// ("a","bc") never hash the same.
func shortComputeID(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))
	return hex.EncodeToString(sum[:])[:8]
}

func nowSeed() string {
	return strconv.FormatInt(time.Now().UnixNano(), 10)
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
