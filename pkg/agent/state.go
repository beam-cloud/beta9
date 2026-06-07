package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

type runtimeState struct {
	GatewayURL  string          `json:"gateway_url"`
	WorkspaceID string          `json:"workspace_id"`
	PoolName    string          `json:"pool_name"`
	MachineID   string          `json:"machine_id"`
	AgentToken  string          `json:"agent_token"`
	Bootstrap   bootstrapConfig `json:"bootstrap"`
	UpdatedAt   time.Time       `json:"updated_at"`
}

func loadRuntimeState(gatewayURL string) (*joinResponse, error) {
	path, err := runtimeStatePath()
	if err != nil {
		return nil, err
	}

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var state runtimeState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if strings.TrimRight(state.GatewayURL, "/") != strings.TrimRight(gatewayURL, "/") || state.AgentToken == "" {
		return nil, nil
	}

	return &joinResponse{
		Ok:          true,
		WorkspaceID: state.WorkspaceID,
		PoolName:    state.PoolName,
		MachineID:   state.MachineID,
		AgentToken:  state.AgentToken,
		Bootstrap:   state.Bootstrap,
	}, nil
}

func saveRuntimeState(gatewayURL string, res *joinResponse) error {
	if res == nil || !res.Ok || res.AgentToken == "" {
		return nil
	}

	path, err := runtimeStatePath()
	if err != nil {
		return err
	}

	state := runtimeState{
		GatewayURL:  strings.TrimRight(gatewayURL, "/"),
		WorkspaceID: res.WorkspaceID,
		PoolName:    res.PoolName,
		MachineID:   res.MachineID,
		AgentToken:  res.AgentToken,
		Bootstrap:   res.Bootstrap,
		UpdatedAt:   time.Now(),
	}
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, 0600)
}

func runtimeStatePath() (string, error) {
	dir, err := agentStateDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, types.AgentRuntimeStateFileName), nil
}

func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		_ = os.Remove(tmpName)
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return fmt.Errorf("sync state directory: %w", err)
	}
	return nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
