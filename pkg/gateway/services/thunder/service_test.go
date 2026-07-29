package thunder

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

func TestServiceCreateAndDeleteClientEnrollment(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	containerRepo := repository.NewContainerRedisRepositoryForTest(rdb)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	if err := workerRepo.AddWorker(&types.Worker{Id: "worker-1", PoolName: "pool-1", MachineId: "machine-1"}); err != nil {
		t.Fatal(err)
	}
	if err := containerRepo.SetContainerState("container-1", &types.ContainerState{
		ContainerId: "container-1",
		WorkspaceId: "workspace-1",
		WorkerId:    "worker-1",
		MachineId:   "machine-1",
		Gpu:         "H100",
		GpuCount:    2,
	}); err != nil {
		t.Fatal(err)
	}

	var createZoneCalls int
	var createTokenCalls int
	var deleteTokenCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer central-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == thunderZonesPath:
			createZoneCalls++
			var payload createZoneRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload.DisplayName != "workspace-1-pool-1" {
				t.Fatalf("zone displayName = %q", payload.DisplayName)
			}
			_ = json.NewEncoder(w).Encode(Zone{ZoneID: "zone-1", DisplayName: payload.DisplayName})
		case r.Method == http.MethodPost && r.URL.Path == thunderEnrollmentTokenPath:
			createTokenCalls++
			payload := map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload["zoneId"] != "zone-1" || payload["role"] != thunderEnrollmentRoleClient || payload["gpuType"] != "h100" || payload["gpuCount"] != float64(2) {
				t.Fatalf("client enrollment payload = %+v", payload)
			}
			_ = json.NewEncoder(w).Encode(EnrollmentToken{EnrollmentTokenID: "token-1", EnrollmentToken: "tr_client", ZoneID: "zone-1", Role: thunderEnrollmentRoleClient, GPUType: "h100", GPUCount: 2})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/enrollment-tokens/token-1/node":
			deleteTokenCalls++
			_ = json.NewEncoder(w).Encode(DeleteEnrollmentTokenNodeResponse{EnrollmentTokenID: "token-1", Role: thunderEnrollmentRoleClient, NodeDeleted: true})
		default:
			t.Fatalf("unexpected request = %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	service, err := NewService(ServiceOpts{
		Repository:    NewRedisRepository(rdb),
		Client:        NewClient(server.URL, "central-token", server.Client()),
		ContainerRepo: containerRepo,
		WorkerRepo:    workerRepo,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := thunderWorkerAuthContext(types.TokenTypeWorker, "")

	createResp, err := service.CreateClientEnrollment(ctx, &pb.CreateClientEnrollmentRequest{ContainerId: "container-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !createResp.Ok || createResp.ErrorMsg != "" {
		t.Fatalf("CreateClientEnrollment() = %+v", createResp)
	}
	if !strings.Contains(createResp.InstallCommand, "THUNDER_CENTRAL_URL='"+server.URL+"'") || !strings.Contains(createResp.InstallCommand, "THUNDER_ENROLLMENT_TOKEN='tr_client'") {
		t.Fatalf("install command = %q", createResp.InstallCommand)
	}
	if createZoneCalls != 1 || createTokenCalls != 1 {
		t.Fatalf("createZoneCalls=%d createTokenCalls=%d", createZoneCalls, createTokenCalls)
	}

	state, found, err := service.repo.GetClientEnrollment(context.Background(), "container-1")
	if err != nil || !found {
		t.Fatalf("GetClientEnrollment() found=%v err=%v", found, err)
	}
	if state.EnrollmentTokenID != "token-1" || state.WorkspaceID != "workspace-1" || state.PoolName != "pool-1" || state.MachineID != "machine-1" {
		t.Fatalf("client enrollment state = %+v", state)
	}
	zone, found, err := service.repo.GetZone(context.Background(), "workspace-1", "pool-1")
	if err != nil || !found {
		t.Fatalf("GetZone() found=%v err=%v", found, err)
	}
	if zone.ThunderZoneID != "zone-1" {
		t.Fatalf("zone = %+v", zone)
	}

	deleteResp, err := service.DeleteClientEnrollment(ctx, &pb.DeleteClientEnrollmentRequest{ContainerId: "container-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !deleteResp.Ok || deleteResp.ErrorMsg != "" {
		t.Fatalf("DeleteClientEnrollment() = %+v", deleteResp)
	}
	if deleteTokenCalls != 1 {
		t.Fatalf("deleteTokenCalls = %d", deleteTokenCalls)
	}
	_, found, err = service.repo.GetClientEnrollment(context.Background(), "container-1")
	if err != nil || found {
		t.Fatalf("GetClientEnrollment() after delete found=%v err=%v", found, err)
	}
}

func TestServiceCreateClientEnrollmentReusesExistingZone(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	containerRepo := repository.NewContainerRedisRepositoryForTest(rdb)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	if err := workerRepo.AddWorker(&types.Worker{Id: "worker-1", PoolName: "pool-1", MachineId: "machine-1"}); err != nil {
		t.Fatal(err)
	}
	if err := containerRepo.SetContainerState("container-1", &types.ContainerState{
		ContainerId: "container-1",
		WorkspaceId: "workspace-1",
		WorkerId:    "worker-1",
		Gpu:         "A10G",
		GpuCount:    1,
	}); err != nil {
		t.Fatal(err)
	}
	repo := NewRedisRepository(rdb)
	if err := repo.SaveZone(context.Background(), &ZoneState{WorkspaceID: "workspace-1", PoolName: "pool-1", ThunderZoneID: "zone-existing"}); err != nil {
		t.Fatal(err)
	}

	var createZoneCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == thunderZonesPath {
			createZoneCalls++
			t.Fatal("CreateZone should not be called for an existing zone")
		}
		if r.Method != http.MethodPost || r.URL.Path != thunderEnrollmentTokenPath {
			t.Fatalf("unexpected request = %s %s", r.Method, r.URL.Path)
		}
		payload := map[string]any{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		if payload["zoneId"] != "zone-existing" {
			t.Fatalf("zoneId = %v", payload["zoneId"])
		}
		_ = json.NewEncoder(w).Encode(EnrollmentToken{EnrollmentTokenID: "token-1", EnrollmentToken: "tr_client", ZoneID: "zone-existing", Role: thunderEnrollmentRoleClient})
	}))
	defer server.Close()

	service, err := NewService(ServiceOpts{Repository: repo, Client: NewClient(server.URL, "central-token", server.Client()), ContainerRepo: containerRepo, WorkerRepo: workerRepo})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := service.CreateClientEnrollment(thunderWorkerAuthContext(types.TokenTypeWorker, ""), &pb.CreateClientEnrollmentRequest{ContainerId: "container-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Ok || resp.ErrorMsg != "" {
		t.Fatalf("CreateClientEnrollment() = %+v", resp)
	}
	if createZoneCalls != 0 {
		t.Fatalf("createZoneCalls = %d", createZoneCalls)
	}
}

func TestServiceCreateClientEnrollmentRequiresWorkerToken(t *testing.T) {
	service := &Service{}
	resp, err := service.CreateClientEnrollment(context.Background(), &pb.CreateClientEnrollmentRequest{ContainerId: "container-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Ok || !strings.Contains(resp.ErrorMsg, "worker token") {
		t.Fatalf("CreateClientEnrollment() = %+v", resp)
	}
}

func TestServiceDeleteClientEnrollmentIsIdempotent(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()
	service, err := NewService(ServiceOpts{
		Repository:    NewRedisRepository(rdb),
		Client:        NewClient("https://central.example", "central-token", nil),
		ContainerRepo: repository.NewContainerRedisRepositoryForTest(rdb),
		WorkerRepo:    repository.NewWorkerRedisRepositoryForTest(rdb),
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := service.DeleteClientEnrollment(thunderWorkerAuthContext(types.TokenTypeWorker, ""), &pb.DeleteClientEnrollmentRequest{ContainerId: "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Ok || resp.ErrorMsg != "" {
		t.Fatalf("DeleteClientEnrollment() = %+v", resp)
	}
}

func TestServicePrivateWorkerTokenIsWorkspaceScoped(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	containerRepo := repository.NewContainerRedisRepositoryForTest(rdb)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	if err := workerRepo.AddWorker(&types.Worker{Id: "worker-1", PoolName: "pool-1"}); err != nil {
		t.Fatal(err)
	}
	if err := containerRepo.SetContainerState("container-1", &types.ContainerState{ContainerId: "container-1", WorkspaceId: "workspace-1", WorkerId: "worker-1", Gpu: "H100", GpuCount: 1}); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(ServiceOpts{
		Repository:    NewRedisRepository(rdb),
		Client:        NewClient("https://central.example", "central-token", nil),
		ContainerRepo: containerRepo,
		WorkerRepo:    workerRepo,
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := service.CreateClientEnrollment(thunderWorkerAuthContext(types.TokenTypeWorkerPrivate, "workspace-2"), &pb.CreateClientEnrollmentRequest{ContainerId: "container-1"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Ok || !strings.Contains(resp.ErrorMsg, "workspace") {
		t.Fatalf("CreateClientEnrollment() = %+v", resp)
	}
}

func TestServiceCreateAndDeleteNodeEnrollment(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	repo := NewRedisRepository(rdb)
	agentToken := "agent-token"
	validator := &fakeAgentStateValidator{state: &model.AgentTokenState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1"}}

	var createZoneCalls int
	var createTokenCalls int
	var deleteTokenCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer central-token" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == thunderZonesPath:
			createZoneCalls++
			var payload createZoneRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload.DisplayName != "workspace-1-pool-1" {
				t.Fatalf("zone displayName = %q", payload.DisplayName)
			}
			_ = json.NewEncoder(w).Encode(Zone{ZoneID: "zone-1", DisplayName: payload.DisplayName})
		case r.Method == http.MethodPost && r.URL.Path == thunderEnrollmentTokenPath:
			createTokenCalls++
			payload := map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatal(err)
			}
			if payload["zoneId"] != "zone-1" || payload["role"] != thunderEnrollmentRoleServer {
				t.Fatalf("node enrollment payload = %+v", payload)
			}
			if _, ok := payload["gpuType"]; ok {
				t.Fatalf("node enrollment payload included gpuType: %+v", payload)
			}
			if _, ok := payload["gpuCount"]; ok {
				t.Fatalf("node enrollment payload included gpuCount: %+v", payload)
			}
			_ = json.NewEncoder(w).Encode(EnrollmentToken{EnrollmentTokenID: "node-token-1", EnrollmentToken: "tr_node", ZoneID: "zone-1", Role: thunderEnrollmentRoleServer})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/enrollment-tokens/node-token-1/node":
			deleteTokenCalls++
			_ = json.NewEncoder(w).Encode(DeleteEnrollmentTokenNodeResponse{EnrollmentTokenID: "node-token-1", Role: thunderEnrollmentRoleServer, NodeDeleted: true})
		default:
			t.Fatalf("unexpected request = %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	service, err := NewService(ServiceOpts{
		Repository:          repo,
		Client:              NewClient(server.URL, "central-token", server.Client()),
		ContainerRepo:       repository.NewContainerRedisRepositoryForTest(rdb),
		WorkerRepo:          repository.NewWorkerRedisRepositoryForTest(rdb),
		AgentStateValidator: validator,
	})
	if err != nil {
		t.Fatal(err)
	}

	createResp, err := service.CreateNodeEnrollment(context.Background(), &pb.CreateNodeEnrollmentRequest{AgentToken: agentToken})
	if err != nil {
		t.Fatal(err)
	}
	if !createResp.Ok || createResp.ErrorMsg != "" || createResp.EnrollmentToken != "tr_node" {
		t.Fatalf("CreateNodeEnrollment() = %+v", createResp)
	}
	if createZoneCalls != 1 || createTokenCalls != 1 {
		t.Fatalf("createZoneCalls=%d createTokenCalls=%d", createZoneCalls, createTokenCalls)
	}

	state, found, err := repo.GetNodeEnrollment(context.Background(), "workspace-1", "pool-1", "machine-1")
	if err != nil || !found {
		t.Fatalf("GetNodeEnrollment() found=%v err=%v", found, err)
	}
	if state.EnrollmentTokenID != "node-token-1" {
		t.Fatalf("node enrollment state = %+v", state)
	}
	zone, found, err := repo.GetZone(context.Background(), "workspace-1", "pool-1")
	if err != nil || !found {
		t.Fatalf("GetZone() found=%v err=%v", found, err)
	}
	if zone.ThunderZoneID != "zone-1" {
		t.Fatalf("zone = %+v", zone)
	}

	deleteResp, err := service.DeleteNodeEnrollment(context.Background(), &pb.DeleteNodeEnrollmentRequest{AgentToken: agentToken})
	if err != nil {
		t.Fatal(err)
	}
	if !deleteResp.Ok || deleteResp.ErrorMsg != "" {
		t.Fatalf("DeleteNodeEnrollment() = %+v", deleteResp)
	}
	if deleteTokenCalls != 1 {
		t.Fatalf("deleteTokenCalls = %d", deleteTokenCalls)
	}
	_, found, err = repo.GetNodeEnrollment(context.Background(), "workspace-1", "pool-1", "machine-1")
	if err != nil || found {
		t.Fatalf("GetNodeEnrollment() after delete found=%v err=%v", found, err)
	}
}

func TestServiceCreateNodeEnrollmentWithExistingStateIsIdempotent(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	repo := NewRedisRepository(rdb)
	agentToken := "agent-token"
	validator := &fakeAgentStateValidator{state: &model.AgentTokenState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1"}}
	if err := repo.SaveNodeEnrollment(context.Background(), &NodeEnrollmentState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1", EnrollmentTokenID: "node-token-existing"}, 0); err != nil {
		t.Fatal(err)
	}

	service, err := NewService(ServiceOpts{
		Repository:          repo,
		Client:              NewClient("https://central.example", "central-token", nil),
		ContainerRepo:       repository.NewContainerRedisRepositoryForTest(rdb),
		WorkerRepo:          repository.NewWorkerRedisRepositoryForTest(rdb),
		AgentStateValidator: validator,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := service.CreateNodeEnrollment(context.Background(), &pb.CreateNodeEnrollmentRequest{AgentToken: agentToken})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Ok || resp.ErrorMsg != "" || resp.EnrollmentToken != "" {
		t.Fatalf("CreateNodeEnrollment() = %+v", resp)
	}
}

func TestServiceCreateNodeEnrollmentRequiresValidAgentToken(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()
	service, err := NewService(ServiceOpts{
		Repository:          NewRedisRepository(rdb),
		Client:              NewClient("https://central.example", "central-token", nil),
		ContainerRepo:       repository.NewContainerRedisRepositoryForTest(rdb),
		WorkerRepo:          repository.NewWorkerRedisRepositoryForTest(rdb),
		AgentStateValidator: &fakeAgentStateValidator{err: errors.New("managed pool no longer exists")},
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := service.CreateNodeEnrollment(context.Background(), &pb.CreateNodeEnrollmentRequest{AgentToken: "missing"})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Ok || !strings.Contains(resp.ErrorMsg, "managed pool no longer exists") {
		t.Fatalf("CreateNodeEnrollment() = %+v", resp)
	}
}

type fakeAgentStateValidator struct {
	state *model.AgentTokenState
	err   error
	token string
}

func (v *fakeAgentStateValidator) ResolveAgentState(ctx context.Context, agentToken string) (*model.AgentTokenState, error) {
	v.token = agentToken
	if v.err != nil {
		return nil, v.err
	}
	return v.state, nil
}

func thunderWorkerAuthContext(tokenType, workspaceID string) context.Context {
	authInfo := &auth.AuthInfo{Token: &types.Token{TokenType: tokenType}}
	if workspaceID != "" {
		authInfo.Workspace = &types.Workspace{ExternalId: workspaceID}
	}
	return auth.ContextWithAuthInfo(context.Background(), authInfo)
}
