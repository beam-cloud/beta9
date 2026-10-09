package vm

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

func rootDisk(v *types.VM) string { return vmIDPrefix + strings.TrimPrefix(v.ID, vmIDPrefix) }

func (s *Service) failed(ctx context.Context, v *types.VM, err error) {
	v.Error = err.Error()
	v.Status = "error"
	if v.ContainerID != "" {
		if state, e := s.containers.GetContainerState(v.ContainerID); e == nil && state.Status == types.ContainerStatusRunning {
			v.Status = "running"
		}
	}
	if e := s.repo.SaveVM(ctx, v); e != nil {
		log.Error().Err(e).Str("vm_id", v.ID).Msg("persist VM failure")
	}
}

func (s *Service) prepare(ctx context.Context, v *types.VM) error {
	if v.StubID != "" {
		return nil
	}
	env := append([]string{}, v.Spec.Env...)
	env = append(env, "BEAM_VM_SYSTEMD=1", "BEAM_VM_ID="+v.ID, "BEAM_VM_DESKTOP="+fmt.Sprint(v.Spec.Desktop), "BEAM_VM_SSH="+fmt.Sprint(v.Spec.SSH), "BEAM_VM_SSH_PUBLIC_KEY="+v.Spec.SSHPublicKey)
	if v.Spec.Desktop {
		env = append(env, "DISPLAY=:1", "XAUTHORITY=/run/beam-desktop/.Xauthority")
	}
	secrets := []*pb.SecretVar{}
	for _, name := range v.Spec.Secrets {
		secrets = append(secrets, &pb.SecretVar{Name: name})
	}
	// Use the normal immutable stub and placement validation, including CPU
	// limits, disk snapshot ownership and microvm-only pool selection.
	req := &pb.GetOrCreateStubRequest{
		ImageId: v.Spec.ImageID, Name: types.StubTypeVM + "/" + v.ID, StubType: types.StubTypeVM,
		Cpu: v.Spec.CPU, Memory: v.Spec.Memory, KeepWarmSeconds: -1, UseVm: true,
		Authorized: true, Env: env, Secrets: secrets, Ports: v.Spec.RuntimePorts(),
		DockerEnabled: v.Spec.DockerEnabled, BlockNetwork: v.Spec.BlockNetwork, AllowList: v.Spec.AllowList,
		Hostname: v.Name, Entrypoint: []string{"/opt/beam-vm/boot"},
		Disks: []*pb.DurableDisk{{Name: rootDisk(v), Size: v.Spec.DiskSize, MountPath: "/", Filesystem: "ext4", Driver: "qcow", SourceSnapshotId: v.Spec.SourceSnapshotID}},
	}
	for _, disk := range v.Spec.Disks {
		req.Disks = append(req.Disks, proto.Clone(disk).(*pb.DurableDisk))
	}
	for _, volume := range v.Spec.Volumes {
		req.Volumes = append(req.Volumes, proto.Clone(volume).(*pb.Volume))
	}
	if v.Spec.Pool != "" {
		req.Pool = &pb.PoolConfig{Name: v.Spec.Pool}
	}
	resp, err := s.gateway.GetOrCreateStub(ctx, req)
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("%s", resp.ErrMsg)
	}
	v.StubID = resp.StubId
	// start persists the stub and runtime identities together before enqueue.
	return nil
}

func (s *Service) start(ctx context.Context, info *auth.AuthInfo, v *types.VM) error {
	state, err := s.runtimeState(v)
	if err != nil {
		return err
	}
	if state != nil {
		if state.Status == types.ContainerStatusStopping {
			return fmt.Errorf("VM is still stopping")
		}
		v.Status = "starting"
		if state.Status == types.ContainerStatusRunning {
			v.Status = "running"
			v.EverRunning = true
			v.LaunchAttempts = 0
			v.MemoryCheckpointID, v.MemoryDiskSnapshots = "", nil
		}
		return s.repo.SaveVM(ctx, v)
	}
	v.ContainerID = ""
	if err := s.validateMemoryDisks(ctx, v); err != nil {
		return err
	}
	v.LaunchAttempts++
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}
	if err := s.prepare(ctx, v); err != nil {
		return err
	}
	// Persist before enqueue. A retry uses this exact runtime identity.
	if v.ContainerID == "" {
		v.Generation++
		v.ContainerID = types.StubTypeVM + "-" + v.StubID + "-" + randomHexID()[:8]
	}
	v.Status = "starting"
	v.Error = ""
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}
	err = s.runtime.RunVM(ctx, info, v.StubID, v.ContainerID, v.Spec, v.MemoryCheckpointID, v.MemoryDiskSnapshots)
	if err != nil {
		if _, stateErr := s.containers.GetContainerState(v.ContainerID); (&types.ErrContainerStateNotFound{}).From(stateErr) {
			v.ContainerID = ""
			if saveErr := s.repo.SaveVM(ctx, v); saveErr != nil {
				return saveErr
			}
		}
	}
	return err
}

// Redis outages and finalization failures must never look like absent compute.
func (s *Service) runtimeState(v *types.VM) (*types.ContainerState, error) {
	if v.ContainerID == "" {
		return nil, nil
	}
	state, err := s.containers.GetContainerState(v.ContainerID)
	if err != nil {
		if !(&types.ErrContainerStateNotFound{}).From(err) {
			return nil, err
		}
		if code, err := s.containers.GetContainerExitCode(v.ContainerID); err == nil && code == int(types.ContainerExitCodeUnknownError) {
			return nil, fmt.Errorf("VM runtime finalization failed; retaining runtime %s for recovery", v.ContainerID)
		}
		return nil, nil
	}
	return state, nil
}

// Launch completion is visible before the reconciler persists it. Project only
// starting -> running; pause/stop intent and checkpoint ownership stay durable.
func (s *Service) launchStatus(v *types.VM) (string, error) {
	if v.DesiredState == "running" && v.Status == "starting" {
		state, err := s.runtimeState(v)
		if err != nil {
			return "", err
		}
		if state != nil && state.Status == types.ContainerStatusRunning {
			return "running", nil
		}
	}
	return v.Status, nil
}

func (s *Service) snapshotRoot(ctx context.Context, v *types.VM) error {
	if v.ContainerID == "" {
		if v.RootSnapshotID == "" {
			return fmt.Errorf("VM has no committed root snapshot")
		}
		return nil
	}
	resp, err := s.runtime.SandboxSnapshotDisks(ctx, &pb.PodSandboxSnapshotDisksRequest{ContainerId: v.ContainerID})
	if err != nil {
		return err
	}
	if !resp.Ok {
		return fmt.Errorf("root snapshot failed: %s", resp.ErrorMsg)
	}
	for _, disk := range resp.Snapshots {
		if disk.DiskName == rootDisk(v) && disk.SnapshotId != "" {
			v.RootSnapshotID = disk.SnapshotId
			return s.repo.SaveVM(ctx, v)
		}
	}
	return fmt.Errorf("snapshot response did not contain this VM's durable root")
}

func (s *Service) capture(ctx context.Context, v *types.VM, kind, name, description string) (*types.VMArtifact, error) {
	if name == "" {
		name = kind + "-" + uuid.NewString()[:8]
	}
	if !validName.MatchString(name) {
		return nil, fmt.Errorf("invalid artifact name")
	}
	if err := s.snapshotRoot(ctx, v); err != nil {
		return nil, err
	}
	a := artifact(v, uuid.NewString(), kind, name, description)
	return a, s.repo.CreateVMArtifact(ctx, v.WorkspaceID, a)
}

func artifact(v *types.VM, id, kind, name, description string) *types.VMArtifact {
	spec := v.Spec
	spec.SSHPublicKey, spec.SourceSnapshotID = "", ""
	return &types.VMArtifact{ID: id, Name: name, VMID: v.ID, Kind: kind, RootSnapshotID: v.RootSnapshotID, Spec: spec, Description: description, CreatedAt: time.Now().UTC()}
}

func (s *Service) stop(ctx context.Context, v *types.VM, visible bool) error {
	if visible && v.StopSnapshotID == "" && v.ContainerID != "" {
		v.StopSnapshotID = uuid.NewString()
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return err
		}
	}
	if v.ContainerID == "" {
		return s.finishStop(ctx, v)
	}
	state, err := s.containers.GetContainerState(v.ContainerID)
	if err != nil && !(&types.ErrContainerStateNotFound{}).From(err) {
		return err
	}
	if err == nil && state.Status != types.ContainerStatusStopping && (v.DesiredState != "paused" || v.MemoryCheckpointID == "") {
		// Commit the root before releasing compute. --no-snapshot skips only
		// the named artifact, never filesystem durability.
		if state.Status == types.ContainerStatusRunning {
			if err := s.snapshotRoot(ctx, v); err != nil {
				return err
			}
		}
		v.Status = "stopping"
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return err
		}
		resp, err := s.gateway.StopContainer(ctx, &pb.StopContainerRequest{ContainerId: v.ContainerID})
		if err != nil {
			return err
		}
		if !resp.Ok {
			return fmt.Errorf("%s", resp.ErrorMsg)
		}
	}
	// Worker removal is the final disk sync/detach barrier. A deadline leaves
	// stopped intent and the old container identity for the reconciler.
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		_, err := s.containers.GetContainerState(v.ContainerID)
		if (&types.ErrContainerStateNotFound{}).From(err) {
			code, codeErr := s.containers.GetContainerExitCode(v.ContainerID)
			if codeErr != nil {
				// A rejected scheduling request has no worker finalization or
				// disk writes. Its explicit terminal status is also a barrier;
				// missing Redis state alone never is.
				status, err := s.containers.GetContainerRequestStatus(v.ContainerID)
				if err != nil || status != types.ContainerRequestStatusFailed {
					return fmt.Errorf("unable to confirm final disk sync: %w", codeErr)
				}
				code = int(types.ContainerExitCodeScheduler)
			}
			if code == int(types.ContainerExitCodeUnknownError) {
				return fmt.Errorf("VM exited with a finalization error; inspect worker logs before restarting")
			}
			latest, err := s.backend.GetLatestDiskSnapshot(ctx, v.WorkspaceID, rootDisk(v))
			if err != nil && v.EverRunning {
				return fmt.Errorf("unable to confirm final root snapshot: %w", err)
			}
			if err == nil {
				v.RootSnapshotID = latest.ExternalId
			}
			v.ContainerID = ""
			v.Error = ""
			return s.finishStop(ctx, v)
		}
		if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

// Persist the final root before recording the optional artifact. A replacement
// gateway retries the same artifact identity if it died between these writes.
func (s *Service) finishStop(ctx context.Context, v *types.VM) error {
	v.Status = "stopped"
	if v.DesiredState == "paused" {
		v.Status = "paused"
	}
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}
	if v.StopSnapshotID != "" && v.RootSnapshotID != "" {
		a := artifact(v, v.StopSnapshotID, "snapshot", "stop-"+v.StopSnapshotID[:8], "")
		if err := s.repo.CreateVMArtifact(ctx, v.WorkspaceID, a); err != nil {
			return err
		}
	}
	v.StopSnapshotID = ""
	return s.repo.SaveVM(ctx, v)
}

func (s *Service) stopOrDelete(ctx context.Context, v *types.VM) error {
	if err := s.stop(ctx, v, false); err != nil {
		return err
	}
	if v.DesiredState != "deleted" {
		return nil
	}
	if err := s.backend.DeleteDisk(ctx, v.WorkspaceID, rootDisk(v)); err != nil {
		return err
	}
	v.Status = "deleted"
	return s.repo.SaveVM(ctx, v)
}

func (s *Service) reconcile(ctx context.Context) {
	timer := time.NewTicker(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		vms, err := s.repo.ListVMs(ctx, 0)
		if err != nil {
			log.Warn().Err(err).Msg("list VMs for reconciliation")
			continue
		}
		var operations sync.WaitGroup
		slots := make(chan struct{}, 4)
		for _, record := range vms {
			if record.DesiredState == "deleted" && record.Status == "deleted" {
				continue
			}
			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				operations.Wait()
				return
			}
			operations.Add(1)
			go func(record *types.VM) {
				defer operations.Done()
				defer func() { <-slots }()
				op, cancel := context.WithTimeout(ctx, 3*time.Minute)
				defer cancel()
				unlock, err := s.repo.LockVM(op, record.ID)
				if err != nil {
					return
				}
				defer unlock()
				v, err := s.repo.GetVM(op, record.WorkspaceID, record.ID)
				if err == nil {
					err = s.reconcileVM(op, v)
				}
				if err != nil && v != nil {
					s.failed(op, v, err)
				}
			}(record)
		}
		operations.Wait()
	}
}

func (s *Service) reconcileVM(ctx context.Context, v *types.VM) error {
	workspace, err := s.backend.GetWorkspaceByExternalId(ctx, v.WorkspaceExternalID)
	if err != nil {
		return err
	}
	// Shutdown is an internal, workspace-scoped operation. It must remain
	// possible after revocation; no inactive credential authorizes a launch.
	info := &auth.AuthInfo{Workspace: &workspace, Token: &types.Token{}}
	ctx = auth.ContextWithAuthInfo(ctx, info)
	if v.DesiredState != "running" {
		return s.stopOrDelete(ctx, v)
	}
	token, err := s.backend.GetTokenByExternalId(ctx, v.WorkspaceID, v.TokenID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if token == nil || !token.Active || token.DisabledByClusterAdmin {
		v.DesiredState = "stopped"
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return err
		}
		return s.stopOrDelete(ctx, v)
	}
	info.Token = token
	if v.Status == "error" && (v.LaunchAttempts >= 5 || time.Since(v.UpdatedAt) < 30*time.Second) {
		return nil
	}
	state, err := s.runtimeState(v)
	if err != nil {
		return err
	}
	if state != nil {
		if state.Status == types.ContainerStatusRunning {
			v.EverRunning = true
			v.LaunchAttempts = 0
			if v.Status != "running" || v.MemoryCheckpointID != "" {
				v.Status = "running"
				v.Error = ""
				v.MemoryCheckpointID, v.MemoryDiskSnapshots = "", nil
				if err := s.repo.SaveVM(ctx, v); err != nil {
					return err
				}
			}
			if v.Spec.IdleTimeout > 0 && time.Since(v.LastActiveAt) >= time.Duration(v.Spec.IdleTimeout)*time.Second {
				claimed, err := s.repo.ClaimVMIdleStop(ctx, v.ID, time.Now().Add(-time.Duration(v.Spec.IdleTimeout)*time.Second))
				if err != nil {
					return err
				}
				if claimed {
					if v.Spec.IdleAction == "pause" {
						return s.pause(ctx, v)
					}
					v.DesiredState = "stopped"
					if err := s.repo.SaveVM(ctx, v); err != nil {
						return err
					}
					return s.stop(ctx, v, false)
				}
			}
			if time.Since(v.UpdatedAt) >= time.Minute {
				return s.snapshotRoot(ctx, v)
			}
			return nil
		}
		return nil
	}
	return s.start(ctx, info, v)
}
