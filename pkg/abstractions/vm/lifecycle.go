package vm

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
	"google.golang.org/protobuf/proto"
)

func rootDisk(v *types.VM) string {
	return vmIDPrefix + strings.TrimPrefix(v.ID, vmIDPrefix)
}

const vmFailureSaveTimeout = 10 * time.Second

// failed records how an operation on a VM ended. One that ran out of time
// still does, which also moves updated_at and so spaces out the next attempt.
func (s *Service) failed(ctx context.Context, v *types.VM, err error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), vmFailureSaveTimeout)
	defer cancel()
	v.Error = err.Error()
	v.Waiting = ""
	v.Status = types.VMStateError
	if v.ContainerID != "" {
		if state, e := s.containers.GetContainerState(v.ContainerID); e == nil && state.Status == types.ContainerStatusRunning {
			v.Status = types.VMStateRunning
		}
	}

	if e := s.repo.SaveVM(ctx, v); e != nil {
		log.Error().Err(e).Str("vm_id", v.ID).Msg("persist VM failure")
	}
}

// The code billing gives a workspace that has run out of credit.
const insufficientCredits = "insufficient_credits"

// queue records that a VM was refused a start for a reason that passes on
// its own, and reports whether it was: its workspace runs as much as its
// concurrency limit allows, or has run out of credit. Such a VM has not
// failed, and is queued until it is let in (VMStateQueued). One that showed
// an error between the times it asks, which is every pass, would look like
// one that keeps crashing. A workspace that billing has blocked, or whose
// credit could not be read, is told so by the error instead.
func (s *Service) queue(ctx context.Context, v *types.VM, err error) bool {
	var limit *types.ThrottledByConcurrencyLimitError
	var credit *types.InsufficientCreditsError
	waits := ""
	switch {
	case errors.As(err, &limit):
		waits = waitingQuota
	case errors.As(err, &credit) && (credit.Code == "" || credit.Code == insufficientCredits):
		waits = waitingCredits
	}

	if waits == "" || v.ContainerID != "" {
		return false
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), vmFailureSaveTimeout)
	defer cancel()
	v.Status, v.Waiting, v.Error = types.VMStateQueued, waits, ""
	if e := s.repo.SaveVM(ctx, v); e != nil {
		log.Error().Err(e).Str("vm_id", v.ID).Msg("persist queued VM")
	}

	return true
}

func (s *Service) prepare(ctx context.Context, v *types.VM) error {
	if v.StubID != "" {
		return nil
	}

	env := append(slices.Clone(v.Spec.Env),
		"BEAM_VM_SYSTEMD=1",
		"BEAM_VM_ID="+v.ID,
		"BEAM_VM_DESKTOP="+fmt.Sprint(v.Spec.Desktop),
		"BEAM_VM_SSH="+fmt.Sprint(v.Spec.SSH),
		"BEAM_VM_SSH_PUBLIC_KEY="+v.Spec.SSHPublicKey,
	)
	if v.Spec.Headless {
		env = append(env, "BEAM_VM_HEADLESS=1")
	}
	if v.Spec.Desktop {
		env = append(env, "DISPLAY=:1", "XAUTHORITY=/run/beam-desktop/.Xauthority")
		if v.Spec.DesktopTransport != "" {
			env = append(env, types.VMDesktopTransportEnv+"="+string(v.Spec.DesktopTransport))
		}
	}
	if v.Spec.Kiosk != nil {
		data, err := json.Marshal(v.Spec.Kiosk)
		if err != nil {
			return err
		}
		env = append(env, "BEAM_VM_KIOSK="+base64.StdEncoding.EncodeToString(data))
	}

	secrets := []*pb.SecretVar{}
	for _, name := range v.Spec.Secrets {
		secrets = append(secrets, &pb.SecretVar{Name: name})
	}

	// Use the normal immutable stub and placement validation, including CPU
	// limits, disk snapshot ownership and microvm-only pool selection.
	req := &pb.GetOrCreateStubRequest{
		ImageId:         v.Spec.ImageID,
		CpuTier:         string(v.Spec.CPUTier),
		Name:            types.StubTypeVM + "/" + v.ID,
		AppName:         types.VMAppName,
		StubType:        types.StubTypeVM,
		Cpu:             v.Spec.CPU,
		Memory:          v.Spec.Memory,
		Gpu:             v.Spec.GPU,
		KeepWarmSeconds: -1,
		UseVm:           true,
		Authorized:      true,
		Env:             env,
		Secrets:         secrets,
		Ports:           v.Spec.RuntimePorts(),
		DockerEnabled:   v.Spec.DockerEnabled,
		BlockNetwork:    v.Spec.BlockNetwork,
		AllowList:       v.Spec.AllowList,
		Hostname:        v.Name,
		Entrypoint:      []string{"/opt/beam-vm/boot"},
		Disks: []*pb.DurableDisk{{
			Name:             rootDisk(v),
			Size:             v.Spec.DiskSize,
			MountPath:        "/",
			Filesystem:       "ext4",
			Driver:           "qcow",
			SourceSnapshotId: v.Spec.SourceSnapshotID,
			// Left unset unless the spec sets it, as stubs made before it are.
			CaptureExitSnapshot: v.Spec.CaptureExitSnapshot,
		}},
	}
	if v.Spec.GPU != "" {
		req.GpuCount = 1
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

	// A root layer is no option of a stub's request, which anyone can make:
	// it reaches the stub's configuration only from here.
	resp, err := s.gateway.GetOrCreateStub(types.WithRootLayer(ctx, v.Spec.RootLayer), req)
	if err != nil {
		return err
	}

	if !resp.Ok {
		// The gateway asks billing before it makes a stub, and answers with
		// the words of the scheduler's refusal, which is what this then is.
		if code, reason, _ := strings.Cut(resp.ErrMsg, ": "); code == insufficientCredits {
			return &types.InsufficientCreditsError{WorkspaceId: v.WorkspaceExternalID, Code: code, Reason: reason}
		}

		return fmt.Errorf("%s", resp.ErrMsg)
	}

	v.StubID = resp.StubId
	// start persists the stub and runtime identities together before enqueue.
	return nil
}

func (s *Service) start(ctx context.Context, info *auth.AuthInfo, v *types.VM) error {
	if v.Spec.DisplayTransport() == types.VMDesktopTransportWebRTC && s.webrtc == nil {
		return fmt.Errorf("desktop WebRTC requires the shared TCP gateway")
	}
	if err := s.ensurePublicIPv6(ctx, v); err != nil {
		return err
	}

	state, err := s.runtimeState(v)
	if err != nil {
		return err
	}

	if state != nil {
		if state.Status == types.ContainerStatusStopping {
			return fmt.Errorf("VM is still stopping")
		}

		v.Status = types.VMStateStarting
		if state.Status == types.ContainerStatusRunning {
			v.Status = types.VMStateRunning
			v.EverRunning = true
			v.LaunchAttempts = 0
			v.MemoryCheckpointID, v.MemoryDiskSnapshots = "", nil
		}

		return s.repo.SaveVM(ctx, v)
	}

	retiredContainerID := v.ContainerID
	retiredCode := 0
	retiredProofObserved := false
	if retiredContainerID != "" {
		retiredCode, err = s.containers.GetContainerExitCode(retiredContainerID)
		if err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("read VM finalization proof before retiring runtime %s: %w", retiredContainerID, err)
		}
		retiredProofObserved = err == nil
		if retiredProofObserved && retiredCode == int(types.ContainerExitCodeUnknownError) && keepsDisks(v) {
			return fmt.Errorf("VM runtime finalization failed; retaining runtime %s for recovery", retiredContainerID)
		}
	}
	v.ContainerID = ""
	if err := s.validateMemoryDisks(ctx, v); err != nil {
		return err
	}

	v.LaunchAttempts++
	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}
	if retiredProofObserved {
		s.acknowledgeExitCode(retiredContainerID, retiredCode)
	}

	if err := s.prepare(ctx, v); err != nil {
		return err
	}

	// Persist before enqueue. A retry uses this exact runtime identity.
	if v.ContainerID == "" {
		v.Generation++
		v.ContainerID = types.StubTypeVM + "-" + v.StubID + "-" + randomHexID()[:8]
	}

	// A queued VM is starting once it is let in, and not each time it asks.
	if v.Status != types.VMStateQueued {
		v.Status = types.VMStateStarting
	}

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
	} else if v.Status == types.VMStateQueued {
		v.Status, v.Waiting = types.VMStateStarting, ""
		return s.repo.SaveVM(ctx, v)
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

		if code, err := s.containers.GetContainerExitCode(v.ContainerID); err == nil && code == int(types.ContainerExitCodeUnknownError) && keepsDisks(v) {
			return nil, fmt.Errorf("VM runtime finalization failed; retaining runtime %s for recovery", v.ContainerID)
		}

		return nil, nil
	}

	return state, nil
}

// keepsDisks reports whether a VM's worker keeps any of its disks when it
// exits. Only then is there anything to recover from a runtime whose
// finalization failed: the worker removes the disks of any other VM, such
// as a runner whose guest was killed, however it exits.
func keepsDisks(v *types.VM) bool {
	return types.CapturesExitSnapshot(v.Spec.CaptureExitSnapshot) || len(v.Spec.Disks) > 0
}

// Launch completion is visible before the reconciler persists it. Project only
// starting -> running; pause/stop intent and checkpoint ownership stay durable.
func (s *Service) launchStatus(v *types.VM) (string, error) {
	if v.DesiredState == types.VMStateRunning && v.Status == types.VMStateStarting {
		state, err := s.runtimeState(v)
		if err != nil {
			return "", err
		}

		if state != nil && state.Status == types.ContainerStatusRunning {
			return types.VMStateRunning, nil
		}
	}

	return v.Status, nil
}

type rootSnapshotFreezeError struct{ error }

func (s *Service) snapshotRoot(ctx context.Context, v *types.VM) error {
	// The worker uploads nothing of a root that is not captured, so there
	// is no snapshot to commit, or to take a template or a fork from.
	if !types.CapturesExitSnapshot(v.Spec.CaptureExitSnapshot) {
		return errRootNotCaptured
	}

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
		err := fmt.Errorf("root snapshot failed: %s", resp.ErrorMsg)
		if resp.GuestFreezeFailed {
			return rootSnapshotFreezeError{err}
		}
		return err
	}

	for _, disk := range resp.Snapshots {
		if disk.DiskName == rootDisk(v) && disk.SnapshotId != "" {
			v.RootSnapshotID = disk.SnapshotId
			v.Error = ""
			return s.repo.SaveVM(ctx, v)
		}
	}

	return fmt.Errorf("snapshot response did not contain this VM's durable root")
}

func (s *Service) capture(ctx context.Context, v *types.VM, kind, name, description string, public bool) (*types.VMArtifact, error) {
	if public {
		if _, snapshotImage := types.SnapshotImageStub(v.Spec.ImageID); snapshotImage {
			return nil, fmt.Errorf("public templates require a built base image")
		}
		if kind != types.VMArtifactTemplate {
			return nil, fmt.Errorf("only templates can be published")
		}
	}

	id := uuid.NewString()
	if name == "" {
		name = id
		if kind == "template" {
			name = kind + "-" + id[:8]
		}
	} else if !validName.MatchString(name) {
		return nil, fmt.Errorf("invalid artifact name")
	}

	if err := s.snapshotRoot(ctx, v); err != nil {
		return nil, err
	}

	a := artifact(v, id, kind, name, description)
	if public {
		a.Public = true
		a.Spec = a.Spec.PublicTemplate()
	}
	return a, s.repo.CreateVMArtifact(ctx, v.WorkspaceID, a)
}

func artifact(v *types.VM, id, kind, name, description string) *types.VMArtifact {
	spec := v.Spec
	spec.SSHPublicKey, spec.SourceSnapshotID = "", ""
	return &types.VMArtifact{
		ID:             id,
		Name:           name,
		VMID:           v.ID,
		VMName:         v.Name,
		Kind:           kind,
		RootSnapshotID: v.RootSnapshotID,
		Spec:           spec,
		Description:    description,
		CreatedAt:      time.Now().UTC(),
	}
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

	if err == nil && state.Status != types.ContainerStatusStopping && (v.DesiredState != types.VMStatePaused || v.MemoryCheckpointID == "") {
		// Commit the root before releasing compute. --no-snapshot skips only
		// the named artifact, never filesystem durability. A preempted VM's
		// worker is already stopping it, and its final sync commits the root
		// once the VM has given its capacity back. A removed VM's root goes
		// with it, and a guest that can no longer freeze its filesystem must
		// not keep the compute of a VM that is gone.
		if state.Status == types.ContainerStatusRunning && v.StopReason != types.VMStopReasonPreempted && v.DesiredState != types.VMStateDeleted {
			if err := s.snapshotRoot(ctx, v); err != nil {
				var freezeErr rootSnapshotFreezeError
				if !errors.As(err, &freezeErr) {
					return err
				}
				// A stuck guest may never freeze its filesystem. Let the
				// worker stop the VM and require its normal final disk sync.
				log.Warn().Err(err).Str("vm_id", v.ID).Msg("guest disk freeze failed before VM stop")
			}
		}

		v.Status = types.VMStateStopping
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
				switch {
				case err == nil && status == types.ContainerRequestStatusFailed:
				case errors.Is(codeErr, redis.Nil) && !keepsDisks(v):
					// A worker that is lost reports no exit, ever. It kept
					// none of this VM's disks, so there is no final sync to
					// confirm, and the VM is not left to wait for one.
				default:
					return fmt.Errorf("unable to confirm final disk sync: %w", codeErr)
				}

				code = int(types.ContainerExitCodeScheduler)
			}

			if code == int(types.ContainerExitCodeUnknownError) && keepsDisks(v) {
				return fmt.Errorf("VM exited with a finalization error; inspect worker logs before restarting")
			}

			// A root that is not captured has no final snapshot to confirm:
			// its worker removed it instead.
			latest, err := s.backend.GetLatestDiskSnapshot(ctx, v.WorkspaceID, rootDisk(v))
			if err != nil && v.EverRunning && types.CapturesExitSnapshot(v.Spec.CaptureExitSnapshot) {
				return fmt.Errorf("unable to confirm final root snapshot: %w", err)
			}

			if err == nil {
				v.RootSnapshotID = latest.ExternalId
			}

			retiredContainerID := v.ContainerID
			v.ContainerID = ""
			v.Error = ""
			if err := s.finishStop(ctx, v); err != nil {
				return err
			}
			s.acknowledgeExitCode(retiredContainerID, code)
			return nil
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

func (s *Service) acknowledgeExitCode(containerID string, code int) {
	if containerID == "" {
		return
	}
	if err := s.containers.AcknowledgeContainerExitCode(containerID, code); err != nil {
		log.Warn().Err(err).Str("container_id", containerID).Msg("retain VM finalization proof after acknowledgement failure")
	}
}

// Persist the final root before recording the optional artifact. A replacement
// gateway retries the same artifact identity if it died between these writes.
func (s *Service) finishStop(ctx context.Context, v *types.VM) error {
	v.Status, v.Waiting = types.VMStateStopped, ""
	if v.DesiredState == types.VMStatePaused {
		v.Status = types.VMStatePaused
	}

	if err := s.repo.SaveVM(ctx, v); err != nil {
		return err
	}

	if v.StopSnapshotID != "" && v.RootSnapshotID != "" {
		a := artifact(v, v.StopSnapshotID, "snapshot", v.StopSnapshotID, "")
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

	if v.DesiredState != types.VMStateDeleted {
		return nil
	}

	if err := s.backend.DeleteDisk(ctx, v.WorkspaceID, rootDisk(v)); err != nil {
		return err
	}

	v.Status = types.VMStateDeleted
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

		vms, err := s.repo.ListVMsToReconcile(ctx, s.publicNetwork != nil)
		if err != nil {
			log.Warn().Err(err).Msg("list VMs for reconciliation")
			continue
		}

		s.watch(vms)
		var operations sync.WaitGroup
		slots := make(chan struct{}, 4)
		for _, record := range vms {
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
				s.reconcileUnlessBusy(ctx, record, "")
			}(record)
		}

		operations.Wait()
	}
}

// reconcileUnlessBusy reconciles a VM that no other operation holds. Given
// the container that exited, it does so only for a VM that is still to run
// in that one.
func (s *Service) reconcileUnlessBusy(ctx context.Context, record *types.VM, exited string) {
	op, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()
	unlock, err := s.repo.TryLockVM(op, record.ID)
	if err != nil {
		return
	}

	defer unlock()
	v, err := s.repo.GetVM(op, record.WorkspaceID, record.ID)
	if err == nil {
		if exited != "" && (v.ContainerID != exited || v.DesiredState != types.VMStateRunning) {
			return
		}
		err = s.reconcileVM(op, v)
	}

	if err != nil && v != nil {
		s.failed(op, v, err)
	}
}

// watch notes the container of each VM that is to be running, for
// relaunchExited.
func (s *Service) watch(vms []*types.VM) {
	running := map[string]*types.VM{}
	for _, v := range vms {
		if v.DesiredState == types.VMStateRunning && v.ContainerID != "" {
			running[v.ContainerID] = v
		}
	}

	s.runningLock.Lock()
	s.running = running
	s.runningLock.Unlock()
}

// exitedVM is the VM the last pass found in a container, once.
func (s *Service) exitedVM(containerID string) *types.VM {
	s.runningLock.Lock()
	defer s.runningLock.Unlock()
	v := s.running[containerID]
	delete(s.running, containerID)
	return v
}

// relaunchExited reconciles a VM when its container is gone rather than at
// the next pass, so a guest that reboots does not wait for one. The passes
// find the exits missed here, and those of a container no pass has seen, so
// a guest that keeps exiting is launched no more often than a pass allows.
func (s *Service) relaunchExited(ctx context.Context, exits <-chan string) {
	slots := make(chan struct{}, 4)
	for {
		select {
		case <-ctx.Done():
			return
		case containerID := <-exits:
			record := s.exitedVM(containerID)
			if record == nil {
				continue
			}

			select {
			case slots <- struct{}{}:
			case <-ctx.Done():
				return
			}

			go func() {
				defer func() { <-slots }()
				s.reconcileUnlessBusy(ctx, record, containerID)
			}()
		}
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
	runner, err := s.reconcileRunner(ctx, v)
	if err != nil {
		return err
	}

	if v.DesiredState != types.VMStateRunning {
		if err := s.stopOrDelete(ctx, v); err != nil {
			return err
		}
		s.backfillPublicIPv6(ctx, v)
		return nil
	}

	token, err := s.backend.GetTokenByExternalId(ctx, v.WorkspaceID, v.TokenID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if token == nil || !token.Active || token.DisabledByClusterAdmin {
		v.DesiredState = types.VMStateStopped
		// Stopping a VM whose root is not captured would leave nothing to
		// start, so it is removed instead.
		if !types.CapturesExitSnapshot(v.Spec.CaptureExitSnapshot) {
			v.DesiredState = types.VMStateDeleted
		}
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return err
		}

		return s.stopOrDelete(ctx, v)
	}

	info.Token = token
	// A queued VM that has a runtime was let in by a gateway that did not
	// get to say so. It is starting, and waits for nothing.
	if v.Status == types.VMStateQueued && v.ContainerID != "" {
		v.Status, v.Waiting = types.VMStateStarting, ""
		if err := s.repo.SaveVM(ctx, v); err != nil {
			return err
		}
	}

	pause := 30 * time.Second
	if runner && queued(v) && v.Waiting != waitingCredits {
		// Room in the workspace's limit comes at any moment, and to be
		// refused again costs nothing. Credit comes when someone adds it,
		// and billing is asked no more often for a runner than for any VM.
		pause = 0
	}

	refused := v.Status == types.VMStateError || v.Status == types.VMStateQueued
	if v.Status == types.VMStateError && v.LaunchAttempts >= 5 || refused && time.Since(v.UpdatedAt) < pause {
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
			if v.Status != types.VMStateRunning || v.MemoryCheckpointID != "" {
				v.Status = types.VMStateRunning
				v.Error = ""
				v.MemoryCheckpointID, v.MemoryDiskSnapshots = "", nil
				if err := s.repo.SaveVM(ctx, v); err != nil {
					return err
				}
			}

			// A VM whose root is not captured cannot start again, so it is
			// never stopped or paused for being idle.
			if v.Spec.IdleTimeout > 0 && types.CapturesExitSnapshot(v.Spec.CaptureExitSnapshot) && time.Since(v.LastActiveAt) >= time.Duration(v.Spec.IdleTimeout)*time.Second {
				active, err := s.hasActivity(ctx, v.ID)
				if err != nil {
					return err
				}
				if active {
					return nil
				}
				claimed, err := s.repo.ClaimVMIdleStop(ctx, v.ID, time.Now().Add(-time.Duration(v.Spec.IdleTimeout)*time.Second))
				if err != nil {
					return err
				}

				if claimed {
					if v.Spec.IdleAction == "pause" {
						// An idle VM that cannot keep its memory still
						// gives its compute back, and keeps its disks.
						err := s.pause(ctx, v)
						if !errors.As(err, &errPauseRefused{}) {
							return err
						}
						log.Warn().Err(err).Str("vm_id", v.ID).Msg("stopping idle VM")
					}

					v.DesiredState = types.VMStateStopped
					if err := s.repo.SaveVM(ctx, v); err != nil {
						return err
					}

					return s.stop(ctx, v, false)
				}
			}

			// Runner disks are discarded with their single job, as is any
			// root that is not captured.
			if !runner && types.CapturesExitSnapshot(v.Spec.CaptureExitSnapshot) && time.Since(v.UpdatedAt) >= time.Minute {
				if err := s.snapshotRoot(ctx, v); err != nil {
					return err
				}
			}

			// Backfill legacy VMs after lifecycle work: allocation failures
			// must never delay shutdown or idle expiry.
			s.backfillPublicIPv6(ctx, v)
			return nil
		}

		return nil
	}

	err = s.start(ctx, info, v)
	if runner && s.queue(ctx, v, err) {
		return nil
	}

	return err
}
