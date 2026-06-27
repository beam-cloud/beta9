package types

import (
	"encoding/json"
	"strings"
)

type DurableDiskCommandAction string

const (
	DurableDiskDriverDev  = "dev"
	DurableDiskDriverDRBD = "drbd"

	DurableDiskReplicationModeSync       = "sync"
	DurableDiskReplicationQuorumMajority = "majority"

	DurableDiskCommandActionPrepare DurableDiskCommandAction = "prepare"
	DurableDiskCommandActionDemote  DurableDiskCommandAction = "demote"
)

type DurableDiskCommandArgs struct {
	StorageNodeID string                   `json:"storage_node_id"`
	Action        DurableDiskCommandAction `json:"action"`
	Mount         Mount                    `json:"mount"`
	Nonce         string                   `json:"nonce,omitempty"`
}

func (a DurableDiskCommandArgs) ToMap() (map[string]any, error) {
	data, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func ToDurableDiskCommandArgs(m map[string]any) (*DurableDiskCommandArgs, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var result DurableDiskCommandArgs
	if err = json.Unmarshal(data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

func NormalizeDurableDiskDriver(driver string) string {
	return strings.ToLower(strings.TrimSpace(driver))
}

func SafeDurableDiskName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, "\\", "-")
	name = strings.ReplaceAll(name, "/", "-")
	if name == "" || name == "." || name == ".." {
		return "disk"
	}
	return name
}
