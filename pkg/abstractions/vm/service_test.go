package vm

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type vmStore struct {
	repository.VMRepository
	rows          map[string]*types.VM
	artifacts     []types.VMArtifact
	artifactError bool
}

func (r *vmStore) CreateVM(ctx context.Context, v *types.VM) error { return r.SaveVM(ctx, v) }
func (r *vmStore) SaveVM(_ context.Context, v *types.VM) error {
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
func (r *vmStore) LockVM(context.Context, string) (func(), error) { return func() {}, nil }
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

func TestDesktopDefaultsAndDiskSizeValidation(t *testing.T) {
	for _, disk := range []string{"50GiB", "50Gi", "53687091200"} {
		spec := types.VMSpec{ImageID: "image", Desktop: true, DiskSize: disk}
		require.NoError(t, validate(&spec))
		require.Equal(t, int64(2000), spec.CPU)
		require.Equal(t, int64(2048), spec.Memory)
		require.Equal(t, "53687091200", spec.DiskSize)
	}
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

type vmContainers struct {
	repository.ContainerRepository
	states        map[string]*types.ContainerState
	exitCode      int
	readError     error
	exitError     error
	requestStatus types.ContainerRequestStatus
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
	checkpointDisks map[string]string
	memoryResponse  *pb.PodSandboxSnapshotMemoryResponse
	snapshotError   bool
	snapshots       int
	forwarded       string
	diskName        string
	boundPorts      []int32
	network         *pb.PodSandboxUpdateNetworkPermissionsRequest
	connectError    error
}

func (r *vmRuntime) SandboxConnect(_ context.Context, req *pb.PodSandboxConnectRequest) (*pb.PodSandboxConnectResponse, error) {
	if r.connectError != nil {
		return nil, r.connectError
	}
	state := r.containers.states[req.ContainerId]
	return &pb.PodSandboxConnectResponse{Ok: state != nil && state.Status == types.ContainerStatusRunning}, nil
}

func (r *vmRuntime) RunVM(_ context.Context, _ *auth.AuthInfo, stub, cid string, _ types.VMSpec, checkpoint string, disks map[string]string) error {
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
	return c.String(200, "desktop")
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

func actionContext(info *auth.AuthInfo, name, action, body string) (*auth.HttpAuthContext, *httptest.ResponseRecorder) {
	e := echo.New()
	req := httptest.NewRequest("POST", "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetParamNames("name", "action")
	c.SetParamValues(name, action)
	return &auth.HttpAuthContext{Context: c, AuthInfo: info}, rec
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
