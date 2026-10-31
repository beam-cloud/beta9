package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

// Completed assignments support final accounting and telemetry only.
type CompletedContainerRepository interface {
	RetainCompletedContainer(context.Context, *types.ContainerState) error
	GetCompletedContainer(context.Context, string, string) (*types.ContainerState, error)
}

func completedContainerKey(workerID, containerID string) string {
	digest := sha256.Sum256([]byte(workerID + "\x00" + containerID))
	return "completed-container:" + hex.EncodeToString(digest[:])
}

func (r *ContainerRedisRepository) RetainCompletedContainer(ctx context.Context, state *types.ContainerState) error {
	if state == nil || state.WorkerId == "" || state.ContainerId == "" {
		return fmt.Errorf("assignment is required")
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, completedContainerKey(state.WorkerId, state.ContainerId), data, 24*time.Hour).Err()
}

func (r *ContainerRedisRepository) GetCompletedContainer(ctx context.Context, workerID, containerID string) (*types.ContainerState, error) {
	data, err := r.rdb.Get(ctx, completedContainerKey(workerID, containerID)).Bytes()
	if err != nil {
		return nil, err
	}
	var state types.ContainerState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.WorkerId != workerID || state.ContainerId != containerID {
		return nil, fmt.Errorf("assignment does not match")
	}
	return &state, nil
}
