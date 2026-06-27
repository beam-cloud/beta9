package types

import (
	"encoding/json"
	"strings"
)

type DurableDiskEventAction string

const (
	DurableDiskDriverDev  = "dev"
	DurableDiskDriverDRBD = "drbd"

	DurableDiskReplicationModeSync       = "sync"
	DurableDiskReplicationQuorumMajority = "majority"

	DurableDiskEventActionPrepare DurableDiskEventAction = "prepare"
	DurableDiskEventActionDemote  DurableDiskEventAction = "demote"
)

type DurableDiskEventArgs struct {
	WorkerID string                 `json:"worker_id"`
	Action   DurableDiskEventAction `json:"action"`
	Mount    Mount                  `json:"mount"`
	Nonce    string                 `json:"nonce,omitempty"`
}

func (a DurableDiskEventArgs) ToMap() (map[string]any, error) {
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

func ToDurableDiskEventArgs(m map[string]any) (*DurableDiskEventArgs, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}

	var result DurableDiskEventArgs
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
