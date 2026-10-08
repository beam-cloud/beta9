package vm

import (
	"context"
	"fmt"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"strings"
	"sync"
	"time"
)

func rootDisk(v *types.VM) string { return "vm-" + v.ID }

func runtimePorts(spec types.VMSpec) []uint32 {
	ports := append([]uint32{}, spec.Ports...)
	for _, private := range spec.PrivatePorts {
		found := false
		for _, port := range ports {
			if port == private {
				found = true
				break
			}
		}
		if !found {
			ports = append(ports, private)
		}
	}
	return ports
}

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
	req := &pb.GetOrCreateStubRequest{ImageId: v.Spec.ImageID, Name: "vm/" + v.ID, StubType: types.StubTypeSandbox}
	// Use the normal immutable stub and placement validation, including CPU
	// limits, disk snapshot ownership and microvm-only pool selection.
	req.Cpu = v.Spec.CPU
	req.Memory = v.Spec.Memory
	req.KeepWarmSeconds = -1
	req.UseVm = true
	req.Authorized = true
	req.Env = env
	req.Secrets = secrets
	req.Ports = runtimePorts(v.Spec)
	req.CheckpointEnabled = false
	req.DockerEnabled = v.Spec.DockerEnabled
	req.Hostname = v.Name
	req.Entrypoint = []string{"/opt/beam-vm/boot"}
	req.Disks = []*pb.DurableDisk{{Name: rootDisk(v), Size: v.Spec.DiskSize, MountPath: "/", Filesystem: "ext4", Driver: "qcow", SourceSnapshotId: v.Spec.SourceSnapshotID}}
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
	return s.repo.SaveVM(ctx, v)
}

func (s *Service) start(ctx context.Context, info *auth.AuthInfo, v *types.VM) error {
	if v.ContainerID != "" {
		state, err := s.containers.GetContainerState(v.ContainerID)
		if err == nil {
			if state.Status == types.ContainerStatusStopping {
				return fmt.Errorf("VM is still stopping")
			}
			v.Status = "starting"
			if state.Status == types.ContainerStatusRunning {
				v.Status = "running"
				v.EverRunning = true
				v.LaunchAttempts = 0
			}
			return s.repo.SaveVM(ctx, v)
		}
		if !(&types.ErrContainerStateNotFound{}).From(err) {
			return err
		}
		v.ContainerID = ""
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
		v.ContainerID = "sandbox-" + v.StubID + "-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:8]
	}
	v.Status = "starting"
	v.Error = ""
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}
	err := s.runtime.RunVM(ctx, info, v.StubID, v.ContainerID, runtimePorts(v.Spec))
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
	spec := v.Spec
	spec.SSHPublicKey = ""
	spec.SourceSnapshotID = ""
	a := &types.VMArtifact{ID: uuid.NewString(), Name: name, VMID: v.ID, Kind: kind, RootSnapshotID: v.RootSnapshotID, Spec: spec, Description: description, CreatedAt: time.Now().UTC()}
	return a, s.repo.CreateVMArtifact(ctx, v.WorkspaceID, a)
}

func (s *Service) stop(ctx context.Context, v *types.VM, visible bool) error {
	if visible && v.StopSnapshotID == "" && v.ContainerID != "" {
		v.StopSnapshotID = uuid.NewString()
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return err
		}
	}
	if v.ContainerID == "" {
		v.Status = "stopped"
		return s.finishStop(ctx, v)
	}
	state, err := s.containers.GetContainerState(v.ContainerID)
	if err != nil && !(&types.ErrContainerStateNotFound{}).From(err) {
		return err
	}
	if err == nil && state.Status != types.ContainerStatusStopping {
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
			v.Status = "stopped"
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
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}
	if v.StopSnapshotID != "" && v.RootSnapshotID != "" {
		spec := v.Spec
		spec.SSHPublicKey, spec.SourceSnapshotID = "", ""
		a := &types.VMArtifact{ID: v.StopSnapshotID, Name: "stop-" + v.StopSnapshotID[:8], VMID: v.ID, Kind: "snapshot", RootSnapshotID: v.RootSnapshotID, Spec: spec, CreatedAt: time.Now().UTC()}
		if err := s.repo.CreateVMArtifact(ctx, v.WorkspaceID, a); err != nil {
			return err
		}
	}
	v.StopSnapshotID = ""
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
	token, err := s.backend.GetTokenByExternalId(ctx, v.WorkspaceID, v.TokenID)
	if err != nil {
		return err
	}
	if !token.Active || token.DisabledByClusterAdmin {
		return fmt.Errorf("VM owner token is inactive")
	}
	info := &auth.AuthInfo{Workspace: &workspace, Token: token}
	ctx = auth.ContextWithAuthInfo(ctx, info)
	if v.DesiredState != "running" {
		if err := s.stop(ctx, v, false); err != nil {
			return err
		}
		if v.DesiredState == "deleted" {
			if err := s.backend.DeleteDisk(ctx, v.WorkspaceID, rootDisk(v)); err != nil {
				return err
			}
			v.Status = "deleted"
			return s.repo.SaveVM(ctx, v)
		}
		return nil
	}
	if v.Status == "error" && (v.LaunchAttempts >= 5 || time.Since(v.UpdatedAt) < 30*time.Second) {
		return nil
	}
	if v.ContainerID != "" {
		state, err := s.containers.GetContainerState(v.ContainerID)
		if err == nil {
			if state.Status == types.ContainerStatusRunning {
				v.EverRunning = true
				v.LaunchAttempts = 0
				if v.Status != "running" {
					v.Status = "running"
					v.Error = ""
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
		if !(&types.ErrContainerStateNotFound{}).From(err) {
			return err
		}
		// After a lost runtime, use a new identity and the latest committed
		// generation. Do not imply zero write loss on worker failure.
		if v.Status != "starting" {
			v.ContainerID = ""
		}
	}
	return s.start(ctx, info, v)
}
