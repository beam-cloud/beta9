package vm

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/labstack/echo/v4"
)

func validateMetadata(metadata map[string]string) error {
	if len(metadata) > 64 {
		return fmt.Errorf("metadata supports at most 64 entries")
	}
	size := 0
	for key, value := range metadata {
		if len(key) == 0 || len(key) > 128 || len(value) > 1024 || strings.ContainsRune(key+value, 0) {
			return fmt.Errorf("metadata keys must be 1–128 bytes and values at most 1024 bytes, without NUL")
		}
		size += len(key) + len(value)
	}
	if size > 16384 {
		return fmt.Errorf("metadata must fit in 16 KiB")
	}
	return nil
}

type updateRequest struct {
	IdleTimeout  *int64             `json:"idle_timeout,omitempty"`
	IdleAction   *string            `json:"idle_action,omitempty"`
	AutoResume   *bool              `json:"auto_resume,omitempty"`
	Metadata     *map[string]string `json:"metadata,omitempty"`
	BlockNetwork *bool              `json:"block_network,omitempty"`
	AllowList    *[]string          `json:"allow_list,omitempty"`
}

func (s *Service) update(c echo.Context) error {
	ctx, info := requestContext(c)
	var req updateRequest
	if err := decode(c, &req); err != nil {
		return err
	}
	if req.Metadata != nil {
		if err := validateMetadata(*req.Metadata); err != nil {
			return apiError(err)
		}
	}
	v, unlock, err := s.lockedVM(ctx, info.Workspace.Id, c.Param("name"))
	if err != nil {
		return apiError(err)
	}
	defer unlock()
	if v.DesiredState == "deleted" {
		return echo.NewHTTPError(404, "VM removed")
	}
	oldSpec := v.Spec
	if req.IdleTimeout != nil {
		v.Spec.IdleTimeout = *req.IdleTimeout
	}
	if req.IdleAction != nil {
		v.Spec.IdleAction = *req.IdleAction
	}
	if req.AutoResume != nil {
		v.Spec.AutoResume = *req.AutoResume
	}
	if req.BlockNetwork != nil {
		v.Spec.BlockNetwork = *req.BlockNetwork
	}
	if req.AllowList != nil {
		v.Spec.AllowList = *req.AllowList
	}
	if err := validate(&v.Spec); err != nil {
		return apiError(err)
	}
	if req.Metadata != nil {
		v.Metadata = *req.Metadata
	}
	// Apply through the existing worker firewall, and carry this same policy
	// on every cold boot. Updating a stopping guest would race disk shutdown.
	if (req.BlockNetwork != nil || req.AllowList != nil) && v.ContainerID != "" {
		if v.Status != "running" {
			return echo.NewHTTPError(409, "wait for the VM to finish starting or stopping before updating its network")
		}
		if err := s.applyNetwork(ctx, v, v.Spec); err != nil {
			return apiError(err)
		}
	}
	if err := s.repo.SaveVM(ctx, v); err != nil {
		if (req.BlockNetwork != nil || req.AllowList != nil) && v.ContainerID != "" {
			rollback, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = s.applyNetwork(rollback, v, oldSpec)
		}
		return apiError(err)
	}
	// A new idle timeout starts at the update, rather than unexpectedly
	// stopping a VM immediately because it was previously inactive.
	if req.IdleTimeout != nil {
		if err := s.repo.TouchVM(ctx, v.ID); err != nil {
			return apiError(err)
		}
	}
	return c.JSON(200, s.response(v))
}

func (s *Service) applyNetwork(ctx context.Context, v *types.VM, spec types.VMSpec) error {
	resp, err := s.runtime.SandboxUpdateNetworkPermissions(ctx, &pb.PodSandboxUpdateNetworkPermissionsRequest{ContainerId: v.ContainerID, StubId: v.StubID, BlockNetwork: spec.BlockNetwork, AllowList: spec.AllowList})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("update network permissions: %s", resp.ErrorMsg)
	}
	return nil
}

// Shared by explicit start and access-triggered cold boots. Preserve the stop
// barrier and reset launch retries only when a caller asks to activate the VM.
func (s *Service) activate(ctx context.Context, info *auth.AuthInfo, v *types.VM) error {
	if v.DesiredState == "paused" && v.MemoryCheckpointID == "" {
		return echo.NewHTTPError(409, "memory pause did not complete; use start with cold=true to recover from disk")
	}
	if v.MemoryCheckpointID != "" && v.TokenID != info.Token.ExternalId {
		return echo.NewHTTPError(409, "memory resume requires the original owner credential; use cold=true with a new credential")
	}
	if (v.DesiredState == "stopped" || v.DesiredState == "paused") && (v.ContainerID != "" || v.StopSnapshotID != "") {
		if err := s.stop(ctx, v, false); err != nil {
			return err
		}
	}
	if err := s.validateMemoryDisks(ctx, v); err != nil {
		return err
	}
	v.DesiredState, v.Error, v.TokenID = "running", "", info.Token.ExternalId
	v.LaunchAttempts = 0
	if err := s.repo.TouchVM(ctx, v.ID); err != nil {
		return err
	}
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}
	return s.start(ctx, info, v)
}

func (s *Service) wakeForAccess(parent context.Context, record *types.VM, port uint32) (*types.VM, error) {
	ctx, cancel := context.WithTimeout(parent, 180*time.Second)
	defer cancel()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		v, unlock, err := s.lockedVM(ctx, record.WorkspaceID, record.ID)
		if err == nil {
			if v.DesiredState == "deleted" || !v.Spec.AutoResume || !slices.Contains(v.Spec.Ports, port) {
				unlock()
				return nil, echo.NewHTTPError(409, "VM access changed while resuming")
			}
			token, tokenErr := s.backend.GetTokenByExternalId(ctx, v.WorkspaceID, v.TokenID)
			if tokenErr != nil {
				unlock()
				return nil, echo.NewHTTPError(503, "VM authorization unavailable")
			}
			if token == nil || !token.Active || token.DisabledByClusterAdmin {
				unlock()
				return nil, echo.NewHTTPError(403, "VM access is revoked")
			}
			workspace, workspaceErr := s.backend.GetWorkspaceByExternalId(ctx, v.WorkspaceExternalID)
			if workspaceErr != nil {
				unlock()
				return nil, echo.NewHTTPError(503, "VM authorization unavailable")
			}
			info := &auth.AuthInfo{Workspace: &workspace, Token: token}
			ctx = auth.ContextWithAuthInfo(ctx, info)
			if v.DesiredState != "running" {
				err = s.activate(ctx, info, v)
			}
			if err == nil {
				state, stateErr := s.runtimeState(v)
				err = stateErr
				if state != nil && state.Status == types.ContainerStatusRunning {
					v.Status = "running"
					// Do not forward a POST until the app's listening socket is
					// ready. A cold boot restarts systemd services, not processes.
					command, _ := json.Marshal([]string{"python3", "-c", fmt.Sprintf("import socket; socket.create_connection(('127.0.0.1', %d), 1).close()", port)})
					probe, probeErr := s.runtime.SandboxExec(ctx, &pb.PodSandboxExecRequest{ContainerId: v.ContainerID, Command: string(command), Cwd: "/", Wait: true})
					if probeErr == nil && probe.Ok && probe.Done && probe.ExitCode == 0 {
						unlock()
						return v, nil
					}
				}
			}
			unlock()
			if err != nil {
				return nil, echo.NewHTTPError(503, "unable to resume VM: "+err.Error())
			}
		} else if !strings.Contains(err.Error(), "operation already in progress") {
			return nil, apiError(err)
		}
		select {
		case <-ctx.Done():
			return nil, echo.NewHTTPError(503, "VM did not become ready before the resume deadline")
		case <-tick.C:
		}
	}
}
