package vm

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type vmStore struct {
	repository.VMRepository
	rows          map[string]*types.VM
	artifacts     []types.VMArtifact
	artifactError bool
	saveError     error
}

func (r *vmStore) CreateVM(ctx context.Context, v *types.VM) error { return r.SaveVM(ctx, v) }
func (r *vmStore) SaveVM(_ context.Context, v *types.VM) error {
	if r.saveError != nil {
		return r.saveError
	}
	copy := *v
	copy.UpdatedAt = time.Now()
	r.rows[v.ID] = &copy
	return nil
}
func (r *vmStore) GetVM(_ context.Context, ws uint, id string) (*types.VM, error) {
	for _, v := range r.rows {
		if v.WorkspaceID == ws && (v.ID == id || v.Name == id) {
			copy := *v
			return &copy, nil
		}
	}
	return nil, sql.ErrNoRows
}
func (r *vmStore) GetVMByHandle(_ context.Context, handle string) (*types.VM, error) {
	for _, v := range r.rows {
		if v.Handle == handle {
			copy := *v
			return &copy, nil
		}
	}
	return nil, sql.ErrNoRows
}

func (r *vmStore) ListVMs(_ context.Context, workspace uint) ([]*types.VM, error) {
	result := []*types.VM{}
	for _, v := range r.rows {
		if workspace == 0 || v.WorkspaceID == workspace {
			copy := *v
			result = append(result, &copy)
		}
	}
	return result, nil
}
func (r *vmStore) ListVMsToReconcile(ctx context.Context, _ bool) ([]*types.VM, error) {
	return r.ListVMs(ctx, 0)
}
func (r *vmStore) LockVM(context.Context, string) (func(), error)           { return func() {}, nil }
func (r *vmStore) TryLockVM(ctx context.Context, id string) (func(), error) { return r.LockVM(ctx, id) }
func (r *vmStore) TouchVM(_ context.Context, id string) error {
	r.rows[id].LastActiveAt = time.Now()
	return nil
}

func (r *vmStore) ClaimVMIdleStop(_ context.Context, id string, cutoff time.Time) (bool, error) {
	v := r.rows[id]
	if v.DesiredState != "running" || v.LastActiveAt.After(cutoff) {
		return false, nil
	}
	v.DesiredState = "stopped"
	if v.Spec.IdleAction == "pause" {
		v.DesiredState = "paused"
	}
	return true, nil
}

type createRaceStore struct {
	*vmStore
	locked bool
}

func (r *createRaceStore) LockVM(context.Context, string) (func(), error) {
	if r.locked {
		return nil, fmt.Errorf("VM operation already in progress")
	}
	r.locked = true
	return func() { r.locked = false }, nil
}

func (r *createRaceStore) CreateVM(ctx context.Context, v *types.VM) error {
	if err := r.vmStore.CreateVM(ctx, v); err != nil {
		return err
	}
	// Reconciliation can discover the row immediately after its insertion.
	// It must not claim the VM before the creating request finishes launching.
	_, err := r.LockVM(ctx, v.ID)
	if err == nil {
		return fmt.Errorf("reconciler claimed newly published VM")
	}
	return nil
}

func TestCreatePreventsReconcilerClaimingPublishedVM(t *testing.T) {
	s, _, info, _, _ := fixture()
	store := &createRaceStore{vmStore: s.repo.(*vmStore)}
	s.repo = store
	v, err := s.createVM(auth.ContextWithAuthInfo(context.Background(), info), info, "created-vm", types.VMSpec{ImageID: "image"})
	require.NoError(t, err)
	require.NotEmpty(t, v.ContainerID)
	require.False(t, store.locked)
}
func (r *vmStore) CreateVMArtifact(_ context.Context, _ uint, a *types.VMArtifact) error {
	if r.artifactError {
		return fmt.Errorf("artifact write unavailable")
	}
	for _, existing := range r.artifacts {
		if existing.ID == a.ID {
			return nil
		}
	}
	r.artifacts = append(r.artifacts, *a)
	return nil
}

func TestInterruptedStopFinishesExactlyOneArtifact(t *testing.T) {
	s, v, info, _, gateway := fixture()
	store := s.repo.(*vmStore)
	store.artifactError = true
	v.DesiredState = "stopped"
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	require.ErrorContains(t, s.stop(ctx, v, true), "artifact write unavailable")
	require.Empty(t, v.ContainerID)
	require.NotEmpty(t, v.StopSnapshotID)
	id := v.StopSnapshotID
	store.artifactError = false
	recovered, err := store.GetVM(ctx, v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.NoError(t, s.stop(ctx, recovered, false))
	require.Empty(t, recovered.StopSnapshotID)
	require.Len(t, store.artifacts, 1)
	require.Equal(t, id, store.artifacts[0].ID)
	require.Equal(t, "root-after-shutdown", store.artifacts[0].RootSnapshotID)
	require.Equal(t, 1, gateway.stops)
	require.NoError(t, s.stop(ctx, recovered, true))
	require.Len(t, store.artifacts, 1)
}

func TestVMDefaultsAndDiskSizeValidation(t *testing.T) {
	for _, disk := range []string{"", "50GiB", "50Gi", "53687091200"} {
		spec := types.VMSpec{ImageID: "image", Desktop: true, DiskSize: disk}
		require.NoError(t, validate(&spec))
		require.Equal(t, int64(4000), spec.CPU)
		require.Equal(t, int64(8192), spec.Memory)
		require.Equal(t, "53687091200", spec.DiskSize)
	}
	spec := types.VMSpec{ImageID: "image"}
	require.NoError(t, validate(&spec))
	require.Equal(t, int64(4000), spec.CPU)
	require.Equal(t, int64(8192), spec.Memory)
	explicit := types.VMSpec{ImageID: "image", CPU: 1000, Memory: 1024}
	require.NoError(t, validate(&explicit))
	require.Equal(t, int64(1000), explicit.CPU)
	require.Equal(t, int64(1024), explicit.Memory)
	for _, disk := range []string{"-1Gi", "0", "bad", "99999999999999999999999999999Ti"} {
		require.Error(t, validate(&types.VMSpec{ImageID: "image", DiskSize: disk}))
	}
	require.Error(t, validate(&types.VMSpec{ImageID: "image", SSH: true, SSHPublicKey: "ssh-ed25519 broken"}))
}
func (r *vmStore) ListVMArtifacts(_ context.Context, _ uint, kind string) ([]types.VMArtifact, error) {
	result := []types.VMArtifact{}
	for _, a := range r.artifacts {
		if a.Kind == kind {
			result = append(result, a)
		}
	}
	return result, nil
}
func (r *vmStore) GetVMArtifact(_ context.Context, _ uint, kind, name string) (*types.VMArtifact, error) {
	for _, a := range r.artifacts {
		if a.Kind == kind && (a.ID == name || a.Name == name) {
			return &a, nil
		}
	}
	return nil, sql.ErrNoRows
}
func (r *vmStore) ListVMArtifactPage(_ context.Context, _ uint, kind string, filter types.VMArtifactFilter) ([]types.VMArtifact, int, error) {
	matches := []types.VMArtifact{}
	for _, a := range r.artifacts {
		if a.Kind == kind && (filter.VMID == "" || a.VMID == filter.VMID) {
			matches = append(matches, a)
		}
	}
	slices.SortFunc(matches, func(a, b types.VMArtifact) int { return b.CreatedAt.Compare(a.CreatedAt) })
	return matches[min(filter.Offset, len(matches)):min(filter.Offset+filter.Limit, len(matches))], len(matches), nil
}
func (r *vmStore) ListVMPage(_ context.Context, ws uint, filter types.VMFilter) (*types.VMPage, error) {
	page := &types.VMPage{Counts: map[string]int{}}
	matches := []*types.VM{}
	for _, v := range r.rows {
		if v.WorkspaceID != ws || (v.DesiredState == "deleted" && !filter.All) {
			continue
		}
		page.Counts[v.Status]++
		if strings.Contains(strings.ToLower(v.Name+" "+v.ID), strings.ToLower(filter.Search)) && (len(filter.Statuses) == 0 || slices.Contains(filter.Statuses, v.Status)) {
			copy := *v
			matches = append(matches, &copy)
		}
	}
	slices.SortFunc(matches, func(a, b *types.VM) int { return b.CreatedAt.Compare(a.CreatedAt) })
	page.Total = len(matches)
	page.VMs = matches[min(filter.Offset, len(matches)):min(filter.Offset+filter.Limit, len(matches))]
	return page, nil
}

type vmContainers struct {
	repository.ContainerRepository
	states        map[string]*types.ContainerState
	exitCode      int
	readError     error
	exitError     error
	requestStatus types.ContainerRequestStatus
	acknowledged  []string
	acknowledge   func(string, int) error
}

func (r *vmContainers) GetContainerState(id string) (*types.ContainerState, error) {
	if r.readError != nil {
		return nil, r.readError
	}
	state, ok := r.states[id]
	if !ok {
		return nil, &types.ErrContainerStateNotFound{ContainerId: id}
	}
	return state, nil
}
func (r *vmContainers) GetContainerExitCode(string) (int, error) { return r.exitCode, r.exitError }
func (r *vmContainers) AcknowledgeContainerExitCode(id string, code int) error {
	r.acknowledged = append(r.acknowledged, id)
	if r.acknowledge != nil {
		return r.acknowledge(id, code)
	}
	return nil
}
func (r *vmContainers) GetContainerRequestStatus(string) (types.ContainerRequestStatus, error) {
	return r.requestStatus, nil
}

func TestStopAcceptsExplicitSchedulingFailureButNotMissingState(t *testing.T) {
	for _, status := range []types.ContainerRequestStatus{types.ContainerRequestStatusFailed, ""} {
		s, v, info, _, _ := fixture()
		containers := s.containers.(*vmContainers)
		delete(containers.states, v.ContainerID)
		containers.exitError = fmt.Errorf("redis: nil")
		containers.requestStatus = status
		err := s.stop(auth.ContextWithAuthInfo(context.Background(), info), v, false)
		if status == types.ContainerRequestStatusFailed {
			require.NoError(t, err)
			require.Empty(t, v.ContainerID)
		} else {
			require.ErrorContains(t, err, "unable to confirm final disk sync")
			require.NotEmpty(t, v.ContainerID)
		}
	}
}

func TestStartupFailureAllowsVMRetry(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	containers := s.containers.(*vmContainers)
	delete(containers.states, v.ContainerID)
	containers.exitCode = int(types.ContainerExitCodeStartupFailed)
	original := v.ContainerID

	require.NoError(t, s.start(auth.ContextWithAuthInfo(context.Background(), info), info, v))
	require.NotEqual(t, original, v.ContainerID)
	require.Len(t, runtime.requests, 1)
}

type retirementStore struct {
	*vmStore
	failRetirement bool
	onRetirement   func()
}

func (r *retirementStore) SaveVM(ctx context.Context, v *types.VM) error {
	if r.failRetirement && v.ContainerID == "" {
		return errors.New("retirement persistence unavailable")
	}
	if err := r.vmStore.SaveVM(ctx, v); err != nil {
		return err
	}
	if v.ContainerID == "" && r.onRetirement != nil {
		r.onRetirement()
	}
	return nil
}

func TestVMProofAcknowledgementFollowsDurableAttemptRetirement(t *testing.T) {
	for _, action := range []string{"stop", "restart"} {
		for _, failSave := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/save-fails-%t", action, failSave), func(t *testing.T) {
				s, v, info, _, _ := fixture()
				oldID := v.ContainerID
				containers := s.containers.(*vmContainers)
				store := &retirementStore{vmStore: s.repo.(*vmStore), failRetirement: failSave}
				store.onRetirement = func() { containers.exitCode = int(types.ContainerExitCodeUnknownError) }
				persisted := *v
				store.rows[v.ID] = &persisted
				s.repo = store
				delete(containers.states, oldID)
				containers.acknowledge = func(id string, code int) error {
					require.Equal(t, oldID, id)
					require.Equal(t, int(types.ContainerExitCodeUser), code)
					require.NotEqual(t, oldID, store.rows[v.ID].ContainerID, "acknowledged before retiring persisted identity")
					return errors.New("ack unavailable") // Retiring the VM must still succeed.
				}
				ctx := auth.ContextWithAuthInfo(context.Background(), info)
				var err error
				if action == "stop" {
					v.DesiredState = "stopped"
					err = s.stop(ctx, v, false)
				} else {
					err = s.start(ctx, info, v)
				}
				if failSave {
					require.ErrorContains(t, err, "retirement persistence unavailable")
					require.Empty(t, containers.acknowledged)
					require.Equal(t, oldID, store.rows[v.ID].ContainerID)
				} else {
					require.NoError(t, err)
					require.Equal(t, []string{oldID}, containers.acknowledged)
				}
			})
		}
	}
}

func TestStartRecoversGatewayCrashAfterIdentitySaveBeforeEnqueue(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	containers := s.containers.(*vmContainers)
	oldID, generation := v.ContainerID, v.Generation
	v.Status, v.DesiredState = "starting", "running"
	// start durably reserves an identity before RunVM admits it to Redis.
	// A replacement gateway must recover that gap without inventing proof.
	require.NoError(t, s.repo.SaveVM(context.Background(), v))
	delete(containers.states, oldID)
	containers.exitError = redis.Nil
	err := s.start(auth.ContextWithAuthInfo(context.Background(), info), info, v)
	require.NoError(t, err)
	require.NotEqual(t, oldID, v.ContainerID)
	require.Equal(t, generation+1, v.Generation)
	require.Len(t, runtime.requests, 1)
	require.Empty(t, containers.acknowledged)
}

type failedRetirementProofRead struct {
	*vmContainers
	reads int
}

func (r *failedRetirementProofRead) GetContainerExitCode(id string) (int, error) {
	r.reads++
	if r.reads == 2 {
		return -1, errors.New("Redis connection unavailable")
	}
	return r.vmContainers.GetContainerExitCode(id)
}

func TestStartPreservesIdentityWhenRetirementProofReadFails(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	containers := s.containers.(*vmContainers)
	oldID, generation := v.ContainerID, v.Generation
	delete(containers.states, oldID)
	proofReads := &failedRetirementProofRead{vmContainers: containers}
	s.containers = proofReads
	err := s.start(auth.ContextWithAuthInfo(context.Background(), info), info, v)
	require.ErrorContains(t, err, "read VM finalization proof")
	require.Equal(t, 2, proofReads.reads)
	require.Equal(t, oldID, v.ContainerID)
	require.Equal(t, generation, v.Generation)
	require.Empty(t, runtime.requests)
	require.Empty(t, containers.acknowledged)
}

func TestFinalizationErrorPreservesRuntimeForRecovery(t *testing.T) {
	for _, reconcile := range []bool{false, true} {
		s, v, info, runtime, _ := fixture()
		containers := s.containers.(*vmContainers)
		delete(containers.states, v.ContainerID)
		containers.exitCode = int(types.ContainerExitCodeUnknownError)
		original := v.ContainerID
		ctx := auth.ContextWithAuthInfo(context.Background(), info)
		var err error
		if reconcile {
			err = s.reconcileVM(ctx, v)
		} else {
			err = s.start(ctx, info, v)
		}
		require.ErrorContains(t, err, "finalization failed")
		require.Equal(t, original, v.ContainerID)
		require.Empty(t, runtime.requests)
	}
}

type vmBackend struct {
	repository.BackendRepository
	latest       string
	token        *types.Token
	tokenError   error
	deletedDisks int
}

func (b *vmBackend) GetWorkspaceByExternalId(_ context.Context, id string) (types.Workspace, error) {
	return types.Workspace{Id: 7, ExternalId: id}, nil
}

func (b *vmBackend) GetTokenByExternalId(_ context.Context, _ uint, id string) (*types.Token, error) {
	if b.tokenError != nil {
		return nil, b.tokenError
	}
	if b.token != nil {
		return b.token, nil
	}
	return &types.Token{ExternalId: id, Active: true}, nil
}

func (b *vmBackend) DeleteDisk(context.Context, uint, string) error {
	b.deletedDisks++
	return nil
}

func (b *vmBackend) GetLatestDiskSnapshot(context.Context, uint, string) (*types.DiskSnapshot, error) {
	return &types.DiskSnapshot{ExternalId: b.latest}, nil
}

type vmRuntime struct {
	pb.UnimplementedPodServiceServer
	containers      *vmContainers
	requests        []string
	checkpoint      string
	launchToken     string
	checkpointDisks map[string]string
	memoryResponse  *pb.PodSandboxSnapshotMemoryResponse
	snapshotError   bool
	snapshots       int
	forwarded       string
	diskName        string
	boundPorts      []int32
	network         *pb.PodSandboxUpdateNetworkPermissionsRequest
	connectError    error
	forwardStatus   int
}

func (r *vmRuntime) SandboxConnect(_ context.Context, req *pb.PodSandboxConnectRequest) (*pb.PodSandboxConnectResponse, error) {
	if r.connectError != nil {
		return nil, r.connectError
	}
	state := r.containers.states[req.ContainerId]
	return &pb.PodSandboxConnectResponse{Ok: state != nil && state.Status == types.ContainerStatusRunning}, nil
}

func (r *vmRuntime) RunVM(_ context.Context, info *auth.AuthInfo, stub, cid string, _ types.VMSpec, checkpoint string, disks map[string]string) error {
	r.launchToken = info.Token.ExternalId
	r.checkpoint = checkpoint
	r.checkpointDisks = disks
	r.requests = append(r.requests, cid)
	r.containers.states[cid] = &types.ContainerState{ContainerId: cid, StubId: stub, Status: types.ContainerStatusRunning}
	return nil
}
func (r *vmRuntime) VMPortReady(context.Context, string, string, uint32) (bool, error) {
	return true, nil
}
func (r *vmRuntime) ForwardVM(c echo.Context, stub, cid string) error {
	r.forwarded = fmt.Sprintf("%s:%s:%s", cid, c.Param("port"), c.Param("subPath"))
	status := r.forwardStatus
	if status == 0 {
		status = http.StatusOK
	}
	return c.String(status, "desktop")
}
func (r *vmRuntime) TunnelVM(echo.Context, string, uint32) error { return nil }
func (r *vmRuntime) SandboxExposePort(_ context.Context, in *pb.PodSandboxExposePortRequest) (*pb.PodSandboxExposePortResponse, error) {
	r.boundPorts = append(r.boundPorts, in.Port)
	return &pb.PodSandboxExposePortResponse{Ok: true}, nil
}
func (r *vmRuntime) SandboxSnapshotDisks(_ context.Context, in *pb.PodSandboxSnapshotDisksRequest) (*pb.PodSandboxSnapshotDisksResponse, error) {
	r.snapshots++
	if r.snapshotError {
		return &pb.PodSandboxSnapshotDisksResponse{Ok: false, ErrorMsg: "upload failed"}, nil
	}
	return &pb.PodSandboxSnapshotDisksResponse{Ok: true, Snapshots: []*pb.PodSandboxDiskSnapshot{{DiskName: r.containers.states[in.ContainerId].StubId, SnapshotId: "wrong-root"}, {DiskName: r.diskName, SnapshotId: "root-live"}}}, nil
}

type vmGateway struct {
	containers *vmContainers
	backend    *vmBackend
	stub       *pb.GetOrCreateStubRequest
	stops      int
}

func (g *vmGateway) GetOrCreateStub(_ context.Context, in *pb.GetOrCreateStubRequest) (*pb.GetOrCreateStubResponse, error) {
	g.stub = in
	return &pb.GetOrCreateStubResponse{Ok: true, StubId: uuid.NewString()}, nil
}
func (g *vmGateway) StopContainer(_ context.Context, in *pb.StopContainerRequest) (*pb.StopContainerResponse, error) {
	g.stops++
	delete(g.containers.states, in.ContainerId)
	g.backend.latest = "root-after-shutdown"
	return &pb.StopContainerResponse{Ok: true}, nil
}

func fixture() (*Service, *types.VM, *auth.AuthInfo, *vmRuntime, *vmGateway) {
	store := &vmStore{rows: map[string]*types.VM{}}
	containers := &vmContainers{states: map[string]*types.ContainerState{}, exitCode: int(types.ContainerExitCodeUser)}
	backend := &vmBackend{latest: "root-after-shutdown"}
	runtime := &vmRuntime{containers: containers}
	gateway := &vmGateway{containers: containers, backend: backend}
	s := &Service{repo: store, containers: containers, backend: backend, runtime: runtime, gateway: gateway, domain: "vm.example.com"}
	v := &types.VM{ID: uuid.NewString(), WorkspaceID: 7, Name: "dev", Handle: "dev-" + uuid.NewString(), DesiredState: "running", Status: "running", EverRunning: true, StubID: uuid.NewString(), Spec: types.VMSpec{ImageID: "base", CPU: 1000, Memory: 1024, DiskSize: "50GiB", Ports: []uint32{7681, 8080}, Desktop: true}}
	v.ContainerID = "sandbox-" + v.StubID + "-12345678"
	runtime.diskName = rootDisk(v)
	containers.states[v.ContainerID] = &types.ContainerState{ContainerId: v.ContainerID, StubId: v.StubID, Status: types.ContainerStatusRunning}
	store.rows[v.ID] = v
	info := &auth.AuthInfo{Workspace: &types.Workspace{Id: 7, ExternalId: uuid.NewString()}, Token: &types.Token{ExternalId: uuid.NewString()}}
	return s, v, info, runtime, gateway
}

func TestGetProjectsLaunchCompletionBeforeReconciliation(t *testing.T) {
	for _, desired := range []string{"running", "paused", "stopped"} {
		t.Run(desired, func(t *testing.T) {
			s, v, info, _, _ := fixture()
			v.Status, v.DesiredState = "starting", desired
			v.MemoryCheckpointID = "owned-checkpoint"
			c, rec := actionContext(info, v.Name, "", "")
			require.NoError(t, s.get(c))
			var response types.VM
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			if desired == "running" {
				require.Equal(t, "running", response.Status)
			} else {
				require.Equal(t, "starting", response.Status)
			}
			require.Equal(t, "starting", v.Status)
			require.Equal(t, "owned-checkpoint", v.MemoryCheckpointID)
		})
	}
}

func TestBlockingLaunchResponseVerifiesExecWithoutConsumingCheckpoint(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	v.Status = "starting"
	v.MemoryCheckpointID = "owned-checkpoint"
	c, rec := actionContext(info, v.Name, "start", "")
	c.Request().URL.RawQuery = "wait=exec"
	require.NoError(t, s.launchResponse(c, 201, v))
	var response struct {
		types.VM
		ExecReady bool `json:"exec_ready"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.True(t, response.ExecReady)
	require.Equal(t, "running", response.Status)
	require.Equal(t, "starting", v.Status)
	require.Equal(t, "owned-checkpoint", v.MemoryCheckpointID)
	runtime.connectError = errors.New("not ready")
	c, _ = actionContext(info, v.Name, "start", "")
	c.Request().URL.RawQuery = "wait=exec"
	require.Error(t, s.launchResponse(c, 201, v))
}

func TestBlockingLaunchResponseKeepsPendingRestoreAccepted(t *testing.T) {
	for _, storedStatus := range []string{"starting", "running"} {
		for _, status := range []int{http.StatusOK, http.StatusCreated} {
			t.Run(fmt.Sprintf("%s/%d", storedStatus, status), func(t *testing.T) {
				s, v, info, runtime, _ := fixture()
				v.Status = storedStatus
				v.MemoryCheckpointID = "owned-checkpoint"
				runtime.containers.states[v.ContainerID].Status = types.ContainerStatusPending
				c, rec := actionContext(info, v.Name, "start", "")
				c.Request().URL.RawQuery = "wait=exec"
				require.NoError(t, s.launchResponse(c, status, v))
				require.Equal(t, status, rec.Code)
				var response struct {
					types.VM
					ExecReady bool `json:"exec_ready"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
				require.Equal(t, "starting", response.Status)
				require.False(t, response.ExecReady, "pending restore must still require authenticated readiness")
				require.Equal(t, storedStatus, v.Status)
				require.Equal(t, "owned-checkpoint", v.MemoryCheckpointID)
			})
		}
	}
}

func TestStopPreservesFinalRootWithoutVisibleSnapshot(t *testing.T) {
	s, v, info, runtime, gateway := fixture()
	v.DesiredState = "stopped"
	err := s.stop(auth.ContextWithAuthInfo(context.Background(), info), v, false)
	require.NoError(t, err)
	require.Equal(t, 1, runtime.snapshots)
	require.Equal(t, 1, gateway.stops)
	require.Equal(t, "root-after-shutdown", v.RootSnapshotID)
	require.Empty(t, v.ContainerID)
	require.Equal(t, "stopped", v.Status)
	require.Empty(t, s.repo.(*vmStore).artifacts)
}

func TestAutomaticSnapshotsUseTheirIDAndRetainSourceName(t *testing.T) {
	s, v, _, _, _ := fixture()
	v.DesiredState = types.VMStateStopped
	require.NoError(t, s.stop(context.Background(), v, true))
	artifacts := s.repo.(*vmStore).artifacts
	require.Len(t, artifacts, 1)
	require.Equal(t, artifacts[0].ID, artifacts[0].Name)
	require.Equal(t, v.ID, artifacts[0].VMID)
	require.Equal(t, v.Name, artifacts[0].VMName)
	manual, err := s.capture(context.Background(), v, "snapshot", "", "")
	require.NoError(t, err)
	require.Equal(t, manual.ID, manual.Name)
	custom, err := s.capture(context.Background(), v, "snapshot", "before-upgrade", "")
	require.NoError(t, err)
	require.Equal(t, "before-upgrade", custom.Name)
}

func TestCreateUsesConfiguredDefaultPoolAndPreservesExplicitPool(t *testing.T) {
	for _, test := range []struct {
		defaultPool, requestedPool, wantPool string
	}{
		{"vms", "", "vms"},
		{"vms", "my-vms", "my-vms"},
		{"", "", ""},
	} {
		t.Run(test.defaultPool+"/"+test.requestedPool, func(t *testing.T) {
			s, base, info, _, gateway := fixture()
			s.defaultPool = test.defaultPool
			spec := base.Spec
			spec.Pool = test.requestedPool
			ctx := auth.ContextWithAuthInfo(context.Background(), info)
			v, err := s.createVM(ctx, info, "new-vm", spec)
			require.NoError(t, err)
			require.Equal(t, test.wantPool, v.Spec.Pool)
			saved, err := s.repo.GetVM(ctx, info.Workspace.Id, v.ID)
			require.NoError(t, err)
			require.Equal(t, test.wantPool, saved.Spec.Pool)
			if test.wantPool == "" {
				require.Nil(t, gateway.stub.Pool)
			} else {
				require.Equal(t, test.wantPool, gateway.stub.Pool.Name)
			}
		})
	}
}

func TestRunningVMRetainsSnapshotFailureUntilRootCommitRecovers(t *testing.T) {
	s, v, _, runtime, _ := fixture()
	ctx := context.Background()
	v.RootSnapshotID = "previous-root"
	v.UpdatedAt = time.Now().Add(-2 * time.Minute)
	runtime.snapshotError = true
	err := s.reconcileVM(ctx, v)
	require.ErrorContains(t, err, "root snapshot failed")
	s.failed(ctx, v, err)
	store := s.repo.(*vmStore)
	require.Equal(t, "running", store.rows[v.ID].Status)
	require.NotEmpty(t, store.rows[v.ID].Error)

	// Running/exec-ready alone must not erase a failed disk commit.
	runtime.snapshotError = false
	v.UpdatedAt = time.Now()
	require.NoError(t, s.reconcileVM(ctx, v))
	require.NotEmpty(t, store.rows[v.ID].Error)
	require.Equal(t, "previous-root", store.rows[v.ID].RootSnapshotID)

	v.UpdatedAt = time.Now().Add(-2 * time.Minute)
	require.NoError(t, s.reconcileVM(ctx, v))
	require.Equal(t, "root-live", store.rows[v.ID].RootSnapshotID)
	require.Empty(t, store.rows[v.ID].Error)
	require.Equal(t, "running", store.rows[v.ID].Status)
	require.Equal(t, v.ContainerID, store.rows[v.ID].ContainerID)
}

func TestSnapshotErrorClearRequiresMatchingRootAndSuccessfulPersistence(t *testing.T) {
	for _, scenario := range []string{"wrong-root", "save-failed"} {
		t.Run(scenario, func(t *testing.T) {
			s, v, _, runtime, _ := fixture()
			v.RootSnapshotID, v.Error = "previous-root", "root snapshot failed: freeze timeout"
			store := s.repo.(*vmStore)
			require.NoError(t, store.SaveVM(context.Background(), v)) // Detached durable row.
			if scenario == "wrong-root" {
				runtime.diskName = "another-vm"
			} else {
				store.saveError = errors.New("snapshot persistence unavailable")
			}
			require.Error(t, s.snapshotRoot(context.Background(), v))
			require.Equal(t, "previous-root", store.rows[v.ID].RootSnapshotID)
			require.Equal(t, "root snapshot failed: freeze timeout", store.rows[v.ID].Error)
		})
	}
}

func TestStopSnapshotFailureKeepsRuntimeAlive(t *testing.T) {
	s, v, info, runtime, gateway := fixture()
	runtime.snapshotError = true
	err := s.stop(auth.ContextWithAuthInfo(context.Background(), info), v, false)
	require.ErrorContains(t, err, "upload failed")
	require.Zero(t, gateway.stops)
	require.NotEmpty(t, v.ContainerID)
}

func TestStopRejectsFinalSyncFailure(t *testing.T) {
	s, v, info, _, _ := fixture()
	s.containers.(*vmContainers).exitCode = int(types.ContainerExitCodeUnknownError)
	err := s.stop(auth.ContextWithAuthInfo(context.Background(), info), v, false)
	require.ErrorContains(t, err, "finalization error")
	require.NotEmpty(t, v.ContainerID)
}

func TestStableURLsAndNewRuntimeOnResume(t *testing.T) {
	s, v, info, _, _ := fixture()
	s.urls(v)
	desktop := v.DesktopURL
	oldCID := v.ContainerID
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	require.NoError(t, s.stop(ctx, v, false))
	v.DesiredState = "running"
	require.NoError(t, s.start(ctx, info, v))
	s.urls(v)
	require.Equal(t, desktop, v.DesktopURL)
	require.NotEqual(t, oldCID, v.ContainerID)
	require.Contains(t, v.ContainerID, types.StubTypeVM+"-"+v.StubID+"-")
}

func TestPrepareForcesCPUVMAndIndependentRoot(t *testing.T) {
	s, v, info, _, gateway := fixture()
	v.StubID = ""
	v.Spec.SourceSnapshotID = "source-root"
	require.NoError(t, s.prepare(auth.ContextWithAuthInfo(context.Background(), info), v))
	require.True(t, gateway.stub.UseVm)
	require.Equal(t, types.StubTypeVM, gateway.stub.StubType)
	require.Equal(t, types.VMAppName, gateway.stub.AppName)
	require.Equal(t, types.StubTypeVM+"/"+v.ID, gateway.stub.Name)
	require.Empty(t, gateway.stub.Gpu)
	require.Zero(t, gateway.stub.GpuCount)
	require.Equal(t, rootDisk(v), gateway.stub.Disks[0].Name)
	require.Equal(t, "/", gateway.stub.Disks[0].MountPath)
	require.Equal(t, "source-root", gateway.stub.Disks[0].SourceSnapshotId)
	require.Contains(t, gateway.stub.Env, "BEAM_VM_SYSTEMD=1")
	require.Contains(t, gateway.stub.Env, "BEAM_VM_ID="+v.ID)
}

type launchPersistenceStore struct {
	*vmStore
	failRuntimeSave bool
}

func (r *launchPersistenceStore) SaveVM(ctx context.Context, v *types.VM) error {
	if r.failRuntimeSave && v.ContainerID != "" {
		return errors.New("runtime identity persistence unavailable")
	}
	return r.vmStore.SaveVM(ctx, v)
}

type persistedLaunchRuntime struct {
	*vmRuntime
	store *vmStore
	t     *testing.T
	vmID  string
}

func (r *persistedLaunchRuntime) RunVM(ctx context.Context, info *auth.AuthInfo, stub, cid string, spec types.VMSpec, checkpoint string, disks map[string]string) error {
	stored := r.store.rows[r.vmID]
	require.Equal(r.t, stub, stored.StubID)
	require.Equal(r.t, cid, stored.ContainerID)
	require.Equal(r.t, "starting", stored.Status)
	return r.vmRuntime.RunVM(ctx, info, stub, cid, spec, checkpoint, disks)
}

func TestStartPersistsBothLaunchIdentitiesBeforeScheduling(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	delete(runtime.containers.states, v.ContainerID)
	v.ContainerID, v.StubID = "", ""
	store := &launchPersistenceStore{vmStore: s.repo.(*vmStore), failRuntimeSave: true}
	s.repo = store
	s.runtime = &persistedLaunchRuntime{vmRuntime: runtime, store: store.vmStore, t: t, vmID: v.ID}
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	require.ErrorContains(t, s.start(ctx, info, v), "identity persistence unavailable")
	require.Empty(t, runtime.requests, "failed persistence must never enqueue compute")
	stored, err := store.GetVM(ctx, info.Workspace.Id, v.ID)
	require.NoError(t, err)
	require.Empty(t, stored.ContainerID)
	require.Empty(t, stored.StubID)
	require.Equal(t, 1, stored.LaunchAttempts)
	store.failRuntimeSave = false
	require.NoError(t, s.start(ctx, info, stored))
	require.Len(t, runtime.requests, 1)
}

func TestBackendOutageNeverMeansRuntimeAbsent(t *testing.T) {
	s, v, info, _, gateway := fixture()
	s.containers.(*vmContainers).readError = fmt.Errorf("redis unavailable")
	require.ErrorContains(t, s.start(context.Background(), info, v), "redis unavailable")
	require.ErrorContains(t, s.stop(context.Background(), v, false), "redis unavailable")
	require.Zero(t, gateway.stops)
}

func TestHostProxyPreservesWebsocketPathAndRejectsPrivatePort(t *testing.T) {
	s, v, _, runtime, _ := fixture()
	e := echo.New()
	v.Spec.PrivatePorts = []uint32{5432}
	e.Pre(s.hostRoute)
	e.Any("/vm/:handle/:port/*", s.proxy)
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error { c.Response().Header().Set("X-VM-Middleware", "visited"); return next(c) }
	})
	for _, test := range []struct {
		port   string
		status int
	}{{"8080", 200}, {"2222", 404}, {"9999", 404}, {"5432", 404}} {
		req := httptest.NewRequest("GET", "http://"+v.Handle+"-"+test.port+".vm.example.com/websockify?token=x", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, test.status, rec.Code)
		require.Equal(t, "visited", rec.Header().Get("X-VM-Middleware"))
	}
	require.Equal(t, v.ContainerID+":8080:websockify", runtime.forwarded)
}

func TestCreateRejectsGPUFieldsAndCrossWorkspaceAccess(t *testing.T) {
	s, _, info, _, _ := fixture()
	e := echo.New()
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error { return next(&auth.HttpAuthContext{Context: c, AuthInfo: info}) }
	})
	e.POST("/:workspaceId", auth.WithStrictWorkspaceAuth(s.create))
	for _, test := range []struct {
		workspace, body string
		status          int
	}{{info.Workspace.ExternalId, `{"name":"bad","spec":{"image_id":"base","gpu":"A100"}}`, 400}, {"other", `{"name":"bad","spec":{"image_id":"base"}}`, 401}} {
		req := httptest.NewRequest("POST", "/"+test.workspace, strings.NewReader(test.body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, test.status, rec.Code)
	}
}

type vmImageVerifierFunc func(context.Context, *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error)

func (f vmImageVerifierFunc) VerifyImageBuild(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error) {
	return f(ctx, in)
}

func TestVMCreateResolvesCachedImageInAuthenticatedWorkspace(t *testing.T) {
	s, _, info, runtime, gateway := fixture()
	lookups := 0
	s.images = vmImageVerifierFunc(func(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error) {
		lookups++
		require.Equal(t, 1, lookups, "retry must recover the resolved image even if its registry tag changed")
		actual, ok := auth.AuthInfoFromContext(ctx)
		require.True(t, ok)
		require.Equal(t, info.Workspace.Id, actual.Workspace.Id)
		require.Equal(t, "ubuntu:22.04", in.ExistingImageUri)
		return &pb.VerifyImageBuildResponse{Valid: true, Exists: true, ImageId: "cached-vm"}, nil
	})
	e := echo.New()
	api := e.Group("/vm", func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error { return next(&auth.HttpAuthContext{Context: c, AuthInfo: info}) }
	})
	registerVMRoutes(api, s)
	body := fmt.Sprintf(`{"name":"fast-vm","request_id":%q,"spec":{"desktop":true},"image":{"existing_image_uri":"ubuntu:22.04","ignore_python":true}}`, uuid.NewString())
	var id string
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/vm/self?wait=exec", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
		var result struct {
			types.VM
			ExecReady bool `json:"exec_ready"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
		require.True(t, result.ExecReady)
		stored, err := s.repo.GetVM(context.Background(), info.Workspace.Id, result.ID)
		require.NoError(t, err)
		require.Equal(t, info.Workspace.ExternalId, stored.WorkspaceExternalID)
		require.Equal(t, "cached-vm", result.Spec.ImageID)
		if i > 0 {
			require.Equal(t, id, result.ID)
		}
		id = result.ID
	}
	require.Equal(t, "cached-vm", gateway.stub.ImageId)
	require.Len(t, runtime.requests, 1, "retry must not allocate a second VM")
	require.Equal(t, 1, lookups)
}

func TestVMCachedImageMissAndWorkspaceAliasRejectBeforeAllocation(t *testing.T) {
	for _, test := range []struct {
		name, workspace, token, image string
		status                        int
	}{
		{"missing image", "self", "", `{"existing_image_uri":"ubuntu:22.04"}`, 428},
		{"restricted token", "self", string(types.TokenTypeWorkspaceRestricted), `{}`, 401},
		{"other workspace", "other", "", `{}`, 401},
		{"GPU image", "self", "", `{"gpu":"A100"}`, 400},
		{"rebuild", "self", "", `{"force_rebuild":true}`, 400},
		{"unknown field", "self", "", `{"unexpected":true}`, 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, _, info, runtime, gateway := fixture()
			info.Token.TokenType = test.token
			s.images = vmImageVerifierFunc(func(context.Context, *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error) {
				return &pb.VerifyImageBuildResponse{Valid: true, Exists: false}, nil
			})
			e := echo.New()
			api := e.Group("/vm", func(next echo.HandlerFunc) echo.HandlerFunc {
				return func(c echo.Context) error { return next(&auth.HttpAuthContext{Context: c, AuthInfo: info}) }
			})
			registerVMRoutes(api, s)
			req := httptest.NewRequest(http.MethodPost, "/vm/"+test.workspace, strings.NewReader(`{"name":"new-vm","spec":{},"image":`+test.image+`}`))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)
			require.Equal(t, test.status, rec.Code, rec.Body.String())
			require.Len(t, s.repo.(*vmStore).rows, 1)
			require.Empty(t, runtime.requests)
			require.Nil(t, gateway.stub)
		})
	}
}

func TestTemplateOverridesRetainImageAndRoot(t *testing.T) {
	s, v, info, _, gateway := fixture()
	store := s.repo.(*vmStore)
	store.artifacts = []types.VMArtifact{{ID: uuid.NewString(), Name: "base", Kind: "template", RootSnapshotID: "template-root", Spec: types.VMSpec{ImageID: "template-image", CPU: 2000, Memory: 2048, DiskSize: "50GiB", Desktop: true}}}
	e := echo.New()
	req := httptest.NewRequest("POST", "/", strings.NewReader(`{"name":"fork","template":"base","spec":{"cpu":3000,"env":["HELLO=world"]}}`))
	rec := httptest.NewRecorder()
	c := &auth.HttpAuthContext{Context: e.NewContext(req, rec), AuthInfo: info}
	require.NoError(t, s.create(c))
	require.Equal(t, 201, rec.Code)
	var child types.VM
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &child))
	require.Equal(t, int64(3000), child.Spec.CPU)
	require.Equal(t, int64(2048), child.Spec.Memory)
	require.Equal(t, "template-image", child.Spec.ImageID)
	require.Equal(t, "template-image", gateway.stub.ImageId)
	require.True(t, child.Spec.Desktop)
	require.Equal(t, "template-root", gateway.stub.Disks[0].SourceSnapshotId)
	require.NotEqual(t, rootDisk(v), gateway.stub.Disks[0].Name)
	require.NotContains(t, child.Handle, strings.ReplaceAll(child.ID, "-", ""), "routing capability must be independent of the resource UUID")
}

func TestListingsPageOnlyWhenLimitIsRequested(t *testing.T) {
	s, v, info, _, _ := fixture()
	store := s.repo.(*vmStore)
	now := time.Now()
	for i, status := range []string{"running", "paused", "stopped"} {
		other := &types.VM{ID: uuid.NewString(), WorkspaceID: v.WorkspaceID, Name: fmt.Sprintf("web-%d", i), Handle: uuid.NewString(), DesiredState: status, Status: status, CreatedAt: now.Add(time.Duration(i) * time.Minute)}
		store.rows[other.ID] = other
	}
	store.artifacts = []types.VMArtifact{
		{ID: uuid.NewString(), Name: "older", VMID: v.ID, Kind: "snapshot", CreatedAt: now.Add(-2 * time.Minute)},
		{ID: uuid.NewString(), Name: "elsewhere", VMID: uuid.NewString(), Kind: "snapshot", CreatedAt: now.Add(-time.Minute)},
		{ID: uuid.NewString(), Name: "newer", VMID: v.ID, Kind: "snapshot", CreatedAt: now, Spec: types.VMSpec{Env: []string{"TOKEN=secret"}}},
	}
	e := echo.New()
	api := e.Group("/vm", func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error { return next(&auth.HttpAuthContext{Context: c, AuthInfo: info}) }
	})
	registerVMRoutes(api, s)
	get := func(path string, result any) {
		t.Helper()
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), result))
	}

	// The SDK, CLI and MCP tools list without a limit and expect arrays.
	var vms []types.VM
	get("/vm/self", &vms)
	require.Len(t, vms, 4)
	var snapshots []types.VMArtifact
	get("/vm/self/artifacts/snapshot", &snapshots)
	require.Len(t, snapshots, 3)
	get("/vm/self/artifacts/snapshot?vm=dev", &snapshots)
	require.Len(t, snapshots, 2)

	var page vmPage
	get("/vm/self?limit=1&offset=1&status=running,paused&q=WEB", &page)
	require.Equal(t, 2, page.Total)
	require.Equal(t, map[string]int{"running": 2, "paused": 1, "stopped": 1}, page.Counts)
	require.Len(t, page.Data, 1)
	require.Equal(t, "web-0", page.Data[0].Name)
	get("/vm/self?limit=0", &page)
	require.Equal(t, 4, page.Total)
	require.Empty(t, page.Data)

	var artifacts artifactPage
	get("/vm/self/artifacts/snapshot?vm=dev&limit=1", &artifacts)
	require.Equal(t, 2, artifacts.Total)
	require.Len(t, artifacts.Data, 1)
	require.Equal(t, "newer", artifacts.Data[0].Name)
	require.Empty(t, artifacts.Data[0].Spec.Env)

	for path, status := range map[string]int{
		"/vm/self?limit=201":                          http.StatusBadRequest,
		"/vm/self?limit=10&offset=-1":                 http.StatusBadRequest,
		"/vm/self/artifacts/snapshot?limit=x":         http.StatusBadRequest,
		"/vm/self/artifacts/snapshot?vm=gone&limit=1": http.StatusNotFound,
	} {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		require.Equal(t, status, rec.Code, path)
	}
}

func actionContext(info *auth.AuthInfo, name, action, body string) (*auth.HttpAuthContext, *httptest.ResponseRecorder) {
	e := echo.New()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetParamNames("name", "action")
	c.SetParamValues(name, action)
	return &auth.HttpAuthContext{Context: c, AuthInfo: info}, rec
}

func TestRunningStartDoesNotContendWithSnapshot(t *testing.T) {
	for _, action := range []string{"start", "resume", "wake"} {
		t.Run(action, func(t *testing.T) {
			s, v, info, runtime, _ := fixture()
			v.TokenID = info.Token.ExternalId
			store := &createRaceStore{vmStore: s.repo.(*vmStore), locked: true}
			s.repo = store
			v.LastActiveAt = time.Now().Add(-time.Hour)
			before := v.LastActiveAt
			c, rec := actionContext(info, v.Name, action, "{}")
			c.Request().URL.RawQuery = "wait=exec"
			require.NoError(t, s.action(c))
			require.Equal(t, http.StatusOK, rec.Code)
			var response struct {
				types.VM
				ExecReady bool `json:"exec_ready"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
			require.True(t, response.ExecReady)
			require.Equal(t, v.ContainerID, response.ContainerID)
			require.Empty(t, runtime.requests)
			require.True(t, store.locked, "the snapshot retains its lifecycle lock")
			require.True(t, store.rows[v.ID].LastActiveAt.After(before))
		})
	}
}

func TestRunningStartStillLocksLifecycleChanges(t *testing.T) {
	for _, scenario := range []string{"cold", "owner", "checkpoint", "stopping", "paused"} {
		t.Run(scenario, func(t *testing.T) {
			s, v, info, _, _ := fixture()
			v.TokenID = info.Token.ExternalId
			s.repo = &createRaceStore{vmStore: s.repo.(*vmStore), locked: true}
			body := "{}"
			switch scenario {
			case "cold":
				body = `{"cold":true}`
			case "owner":
				v.TokenID = "another-owner"
			case "checkpoint":
				v.MemoryCheckpointID = "pending-restore"
			case "stopping":
				s.containers.(*vmContainers).states[v.ContainerID].Status = types.ContainerStatusStopping
			case "paused":
				v.DesiredState = "paused"
			}
			c, _ := actionContext(info, v.Name, "start", body)
			err := s.action(c)
			require.Error(t, err)
			require.ErrorContains(t, err, "operation already in progress")
		})
	}
}

func TestActivityLeasesAreIndependentAndExplicitlyReleased(t *testing.T) {
	s, v, info, _, _ := fixture()
	// A background snapshot owns the lifecycle lock throughout this test.
	s.repo = &createRaceStore{vmStore: s.repo.(*vmStore), locked: true}
	redis := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{redis.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })
	s.rdb = rdb
	first, second := uuid.NewString(), uuid.NewString()
	for _, lease := range []string{first, second} {
		c, _ := actionContext(info, v.Name, "touch", `{}`)
		c.Request().URL.RawQuery = "lease=" + lease
		require.NoError(t, s.action(c))
	}
	for i, lease := range []string{first, second} {
		c, _ := actionContext(info, v.Name, "touch", `{}`)
		c.Request().URL.RawQuery = "lease=" + lease + "&release=1"
		require.NoError(t, s.action(c))
		active, err := s.hasActivity(context.Background(), v.ID)
		require.NoError(t, err)
		require.Equal(t, i == 0, active)
	}
}

func TestFailedCaptureDoesNotChangeSourceLifecycle(t *testing.T) {
	for _, action := range []string{"fork", "snapshot", "template"} {
		t.Run(action, func(t *testing.T) {
			s, v, info, runtime, _ := fixture()
			runtime.snapshotError = true
			c, _ := actionContext(info, v.Name, action, `{}`)
			require.ErrorContains(t, s.action(c), "root snapshot failed")
			saved, err := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
			require.NoError(t, err)
			require.Equal(t, "running", saved.Status)
			require.Empty(t, saved.Error)
		})
	}
}

func TestPrivateBindDoesNotPublishURLsAndIsIdempotent(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	for range 2 {
		c, rec := actionContext(info, v.Name, "bind", `{"port":5432}`)
		require.NoError(t, s.action(c))
		var response types.VM
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
		require.NotContains(t, response.URLs, uint32(5432))
		require.NotContains(t, response.Spec.Ports, uint32(5432))
		require.Equal(t, []uint32{5432}, response.Spec.PrivatePorts)
	}
	require.Equal(t, []int32{5432, 5432}, runtime.boundPorts)
}

func TestDisabledSSHPortIsRejected(t *testing.T) {
	require.Error(t, validate(&types.VMSpec{ImageID: "image", Ports: []uint32{2222}}))
}

func TestManagementResponsesOmitEnvironmentWithoutChangingStoredSpec(t *testing.T) {
	s, v, info, _, gateway := fixture()
	v.Spec.Env = []string{"CREDENTIAL=private-value"}
	c, rec := actionContext(info, v.Name, "touch", `{}`)
	require.NoError(t, s.action(c))
	require.NotContains(t, rec.Body.String(), "private-value")
	for _, handler := range []func(echo.Context) error{s.get, s.list} {
		c, rec = actionContext(info, v.Name, "", "")
		require.NoError(t, handler(c))
		require.NotContains(t, rec.Body.String(), "private-value")
	}
	childSpec := v.Spec
	childSpec.SourceSnapshotID = "template-root"
	child, err := s.createVM(auth.ContextWithAuthInfo(context.Background(), info), info, "child", childSpec)
	require.NoError(t, err)
	require.Contains(t, gateway.stub.Env, "CREDENTIAL=private-value")
	require.Nil(t, s.response(child).Spec.Env)
	require.Equal(t, v.Spec.Env, child.Spec.Env)
	a := artifact(v, uuid.NewString(), "template", "base", "")
	require.NoError(t, s.repo.CreateVMArtifact(context.Background(), v.WorkspaceID, a))
	c, rec = actionContext(info, v.Name, "", "")
	c.SetParamNames("kind")
	c.SetParamValues("template")
	require.NoError(t, s.artifacts(c))
	require.NotContains(t, rec.Body.String(), "private-value")
	require.Equal(t, v.Spec.Env, s.repo.(*vmStore).artifacts[0].Spec.Env)
}

func TestRevocationStopsComputeAndAllowsDeletion(t *testing.T) {
	for _, desired := range []string{"running", "stopped", "deleted"} {
		for _, missing := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/missing=%v", desired, missing), func(t *testing.T) {
				s, v, _, runtime, gateway := fixture()
				backend := s.backend.(*vmBackend)
				backend.token = &types.Token{Active: false}
				if missing {
					backend.tokenError = sql.ErrNoRows
				}
				v.DesiredState = desired
				require.NoError(t, s.reconcileVM(context.Background(), v))
				require.Empty(t, v.ContainerID)
				require.Equal(t, 1, gateway.stops)
				require.Empty(t, runtime.requests)
				if desired == "deleted" {
					require.Equal(t, "deleted", v.Status)
					require.Equal(t, 1, backend.deletedDisks)
				} else {
					require.Equal(t, "stopped", v.Status)
					require.Equal(t, "stopped", v.DesiredState)
				}
			})
		}
	}
}

func TestCapabilityAccessFailsClosedAndReportsTokenOutages(t *testing.T) {
	for _, test := range []struct {
		name   string
		token  *types.Token
		err    error
		status int
	}{
		{"revoked", &types.Token{Active: false}, nil, 403},
		{"disabled", &types.Token{Active: true, DisabledByClusterAdmin: true}, nil, 403},
		{"missing", nil, sql.ErrNoRows, 403},
		{"outage", nil, fmt.Errorf("token store unavailable"), 503},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, v, _, runtime, _ := fixture()
			backend := s.backend.(*vmBackend)
			backend.token, backend.tokenError = test.token, test.err
			e := echo.New()
			e.GET("/vm/:handle/:port/", s.proxy)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequest("GET", "/vm/"+v.Handle+"/8080/", nil))
			require.Equal(t, test.status, rec.Code)
			require.Empty(t, runtime.forwarded)
		})
	}
}

func TestListProjectsLaunchCompletionBeforeStatusFiltering(t *testing.T) {
	s, v, info, _, _ := fixture()
	v.Status = "starting"
	response := vmRequest(managementAPI(s, info), "GET", "/"+info.Workspace.ExternalId+"?status=running", "")
	require.Equal(t, 200, response.Code, response.Body.String())
	var result []types.VM
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &result))
	require.Len(t, result, 1)
	require.Equal(t, "running", result[0].Status)
	require.Equal(t, "starting", v.Status)
}
