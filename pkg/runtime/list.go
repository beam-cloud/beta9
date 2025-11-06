package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
)

// ListContainers lists all containers managed by the runtime
func (r *Runsc) List(ctx context.Context) ([]State, error) {
	args := r.baseArgs()
	args = append(args, "list", "--format=json")

	cmd := exec.CommandContext(ctx, r.cfg.RunscPath, args...)
	
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	// Parse the JSON output
	var containers []struct {
		ID     string `json:"id"`
		Pid    int    `json:"pid"`
		Status string `json:"status"`
		Bundle string `json:"bundle"`
	}

	if err := json.Unmarshal(stdout.Bytes(), &containers); err != nil {
		return nil, fmt.Errorf("failed to parse list output: %w", err)
	}

	states := make([]State, 0, len(containers))
	for _, c := range containers {
		states = append(states, State{
			ID:     c.ID,
			Pid:    c.Pid,
			Status: c.Status,
		})
	}

	return states, nil
}

// ListContainers lists all containers managed by the runtime
func (r *Runc) List(ctx context.Context) ([]State, error) {
	containers, err := r.handle.List(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	states := make([]State, 0, len(containers))
	for _, c := range containers {
		states = append(states, State{
			ID:     c.ID,
			Pid:    c.Pid,
			Status: c.Status,
		})
	}

	return states, nil
}
