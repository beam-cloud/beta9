package thunder

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	thundersdk "github.com/Thunder-Compute/thunder-sdk"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
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
	handlerErrors := newThunderHTTPHandlerErrors()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer central-token" {
			handlerErrors.failf(w, "authorization = %q", got)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/zones/ensure":
			createZoneCalls++
			var payload thundersdk.CreateZoneRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				handlerErrors.failf(w, "decode request body: %v", err)
				return
			}
			if payload.DisplayName != "workspace-1-pool-1" {
				handlerErrors.failf(w, "zone displayName = %q", payload.DisplayName)
				return
			}
			_ = json.NewEncoder(w).Encode(thundersdk.CreateZoneResponse{ZoneID: "zone-1", DisplayName: payload.DisplayName})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/enrollment-tokens":
			createTokenCalls++
			payload := map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				handlerErrors.failf(w, "decode request body: %v", err)
				return
			}
			if payload["zoneId"] != "zone-1" || payload["role"] != thundersdk.RoleClient || payload["gpuType"] != "h100" || payload["gpuCount"] != float64(2) {
				handlerErrors.failf(w, "client enrollment payload = %+v", payload)
				return
			}
			_ = json.NewEncoder(w).Encode(thundersdk.EnrollmentToken{EnrollmentTokenID: "token-1", EnrollmentToken: "tr_client", ZoneID: "zone-1", Role: thundersdk.RoleClient, GPUType: "h100", GPUCount: 2})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/enrollment-tokens/token-1/node":
			deleteTokenCalls++
			_ = json.NewEncoder(w).Encode(thundersdk.DeleteEnrollmentServerResponse{EnrollmentTokenID: "token-1", Role: thundersdk.RoleClient, ServerDeleted: true})
		default:
			handlerErrors.failf(w, "unexpected request = %s %s", r.Method, r.URL.Path)
			return
		}
	}))
	defer server.Close()

	service, err := NewService(ServiceOpts{
		Repository:    repository.NewThunderRedisRepository(rdb),
		Client:        thundersdk.NewClient(server.URL, "central-token", thundersdk.WithHTTPClient(server.Client())),
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
	handlerErrors.assertEmpty(t)
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
	handlerErrors.assertEmpty(t)
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
	repo := repository.NewThunderRedisRepository(rdb)
	if err := repo.SaveZone(context.Background(), &repository.ThunderZoneState{WorkspaceID: "workspace-1", PoolName: "pool-1", ThunderZoneID: "zone-existing"}); err != nil {
		t.Fatal(err)
	}

	var createZoneCalls int
	handlerErrors := newThunderHTTPHandlerErrors()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/api/v1/zones/ensure" {
			createZoneCalls++
			handlerErrors.failf(w, "CreateZone should not be called for an existing zone")
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/enrollment-tokens" {
			handlerErrors.failf(w, "unexpected request = %s %s", r.Method, r.URL.Path)
			return
		}
		payload := map[string]any{}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			handlerErrors.failf(w, "decode request body: %v", err)
			return
		}
		if payload["zoneId"] != "zone-existing" {
			handlerErrors.failf(w, "zoneId = %v", payload["zoneId"])
			return
		}
		_ = json.NewEncoder(w).Encode(thundersdk.EnrollmentToken{EnrollmentTokenID: "token-1", EnrollmentToken: "tr_client", ZoneID: "zone-existing", Role: thundersdk.RoleClient})
	}))
	defer server.Close()

	service, err := NewService(ServiceOpts{Repository: repo, Client: thundersdk.NewClient(server.URL, "central-token", thundersdk.WithHTTPClient(server.Client())), ContainerRepo: containerRepo, WorkerRepo: workerRepo})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := service.CreateClientEnrollment(thunderWorkerAuthContext(types.TokenTypeWorker, ""), &pb.CreateClientEnrollmentRequest{ContainerId: "container-1"})
	if err != nil {
		t.Fatal(err)
	}
	handlerErrors.assertEmpty(t)
	if !resp.Ok || resp.ErrorMsg != "" {
		t.Fatalf("CreateClientEnrollment() = %+v", resp)
	}
	if createZoneCalls != 0 {
		t.Fatalf("createZoneCalls = %d", createZoneCalls)
	}
}

func TestServiceCreateClientEnrollmentReplacesExistingToken(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	containerRepo := repository.NewContainerRedisRepositoryForTest(rdb)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	if err := workerRepo.AddWorker(&types.Worker{Id: "worker-1", PoolName: "pool-1", MachineId: "machine-1"}); err != nil {
		t.Fatal(err)
	}
	if err := containerRepo.SetContainerState("container-1", &types.ContainerState{ContainerId: "container-1", WorkspaceId: "workspace-1", WorkerId: "worker-1", MachineId: "machine-1", Gpu: "H100", GpuCount: 1}); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewThunderRedisRepository(rdb)
	if err := repo.SaveZone(context.Background(), &repository.ThunderZoneState{WorkspaceID: "workspace-1", PoolName: "pool-1", ThunderZoneID: "zone-existing"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveClientEnrollment(context.Background(), &repository.ThunderClientEnrollmentState{ContainerID: "container-1", WorkspaceID: "workspace-1", WorkerID: "worker-1", MachineID: "machine-1", PoolName: "pool-1", EnrollmentTokenID: "token-old"}); err != nil {
		t.Fatal(err)
	}

	var createTokenCalls int
	var deleted []string
	handlerErrors := newThunderHTTPHandlerErrors()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/enrollment-tokens":
			createTokenCalls++
			_ = json.NewEncoder(w).Encode(thundersdk.EnrollmentToken{EnrollmentTokenID: "token-new", EnrollmentToken: "tr_client_new", ZoneID: "zone-existing", Role: thundersdk.RoleClient})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/enrollment-tokens/"):
			deleted = append(deleted, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/enrollment-tokens/"), "/node"))
			_ = json.NewEncoder(w).Encode(thundersdk.DeleteEnrollmentServerResponse{ServerDeleted: true})
		default:
			handlerErrors.failf(w, "unexpected request = %s %s", r.Method, r.URL.Path)
			return
		}
	}))
	defer server.Close()

	service, err := NewService(ServiceOpts{Repository: repo, Client: thundersdk.NewClient(server.URL, "central-token", thundersdk.WithHTTPClient(server.Client())), ContainerRepo: containerRepo, WorkerRepo: workerRepo})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := service.CreateClientEnrollment(thunderWorkerAuthContext(types.TokenTypeWorker, ""), &pb.CreateClientEnrollmentRequest{ContainerId: "container-1"})
	if err != nil {
		t.Fatal(err)
	}
	handlerErrors.assertEmpty(t)
	if !resp.Ok || resp.ErrorMsg != "" || !strings.Contains(resp.InstallCommand, "tr_client_new") {
		t.Fatalf("CreateClientEnrollment() = %+v", resp)
	}
	if createTokenCalls != 1 {
		t.Fatalf("createTokenCalls = %d", createTokenCalls)
	}
	if len(deleted) != 1 || deleted[0] != "token-old" {
		t.Fatalf("deleted tokens = %+v", deleted)
	}
	state, found, err := repo.GetClientEnrollment(context.Background(), "container-1")
	if err != nil || !found {
		t.Fatalf("GetClientEnrollment() found=%v err=%v", found, err)
	}
	if state.EnrollmentTokenID != "token-new" {
		t.Fatalf("client enrollment state = %+v", state)
	}
}

func TestServiceCreateClientEnrollmentSucceedsWhenPreviousTokenRevokeFails(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	containerRepo := repository.NewContainerRedisRepositoryForTest(rdb)
	workerRepo := repository.NewWorkerRedisRepositoryForTest(rdb)
	if err := workerRepo.AddWorker(&types.Worker{Id: "worker-1", PoolName: "pool-1", MachineId: "machine-1"}); err != nil {
		t.Fatal(err)
	}
	if err := containerRepo.SetContainerState("container-1", &types.ContainerState{ContainerId: "container-1", WorkspaceId: "workspace-1", WorkerId: "worker-1", MachineId: "machine-1", Gpu: "H100", GpuCount: 1}); err != nil {
		t.Fatal(err)
	}
	repo := repository.NewThunderRedisRepository(rdb)
	if err := repo.SaveZone(context.Background(), &repository.ThunderZoneState{WorkspaceID: "workspace-1", PoolName: "pool-1", ThunderZoneID: "zone-existing"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveClientEnrollment(context.Background(), &repository.ThunderClientEnrollmentState{ContainerID: "container-1", WorkspaceID: "workspace-1", WorkerID: "worker-1", MachineID: "machine-1", PoolName: "pool-1", EnrollmentTokenID: "token-old"}); err != nil {
		t.Fatal(err)
	}

	var deleted []string
	handlerErrors := newThunderHTTPHandlerErrors()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/enrollment-tokens":
			_ = json.NewEncoder(w).Encode(thundersdk.EnrollmentToken{EnrollmentTokenID: "token-new", EnrollmentToken: "tr_client_new", ZoneID: "zone-existing", Role: thundersdk.RoleClient})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/enrollment-tokens/"):
			deleted = append(deleted, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/enrollment-tokens/"), "/node"))
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "internal", "message": "delete failed"})
		default:
			handlerErrors.failf(w, "unexpected request = %s %s", r.Method, r.URL.Path)
			return
		}
	}))
	defer server.Close()

	service, err := NewService(ServiceOpts{Repository: repo, Client: thundersdk.NewClient(server.URL, "central-token", thundersdk.WithHTTPClient(server.Client())), ContainerRepo: containerRepo, WorkerRepo: workerRepo})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := service.CreateClientEnrollment(thunderWorkerAuthContext(types.TokenTypeWorker, ""), &pb.CreateClientEnrollmentRequest{ContainerId: "container-1"})
	if err != nil {
		t.Fatal(err)
	}
	handlerErrors.assertEmpty(t)
	if !resp.Ok || resp.ErrorMsg != "" || !strings.Contains(resp.InstallCommand, "tr_client_new") {
		t.Fatalf("CreateClientEnrollment() = %+v", resp)
	}
	if len(deleted) != 1 || deleted[0] != "token-old" {
		t.Fatalf("deleted enrollment token ids = %+v", deleted)
	}
	state, found, err := repo.GetClientEnrollment(context.Background(), "container-1")
	if err != nil || !found {
		t.Fatalf("GetClientEnrollment() found=%v err=%v", found, err)
	}
	if state.EnrollmentTokenID != "token-new" {
		t.Fatalf("client enrollment state = %+v", state)
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
		Repository:    repository.NewThunderRedisRepository(rdb),
		Client:        thundersdk.NewClient("https://central.example", "central-token"),
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
		Repository:    repository.NewThunderRedisRepository(rdb),
		Client:        thundersdk.NewClient("https://central.example", "central-token"),
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

	repo := repository.NewThunderRedisRepository(rdb)
	agentToken := "agent-token"
	validator := &fakeAgentStateValidator{state: &model.AgentTokenState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1"}}

	var createZoneCalls int
	var createTokenCalls int
	var deleteTokenCalls int
	handlerErrors := newThunderHTTPHandlerErrors()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer central-token" {
			handlerErrors.failf(w, "authorization = %q", got)
			return
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/zones/ensure":
			createZoneCalls++
			var payload thundersdk.CreateZoneRequest
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				handlerErrors.failf(w, "decode request body: %v", err)
				return
			}
			if payload.DisplayName != "workspace-1-pool-1" {
				handlerErrors.failf(w, "zone displayName = %q", payload.DisplayName)
				return
			}
			_ = json.NewEncoder(w).Encode(thundersdk.CreateZoneResponse{ZoneID: "zone-1", DisplayName: payload.DisplayName})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/enrollment-tokens":
			createTokenCalls++
			payload := map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				handlerErrors.failf(w, "decode request body: %v", err)
				return
			}
			if payload["zoneId"] != "zone-1" || payload["role"] != thundersdk.RoleServer {
				handlerErrors.failf(w, "node enrollment payload = %+v", payload)
				return
			}
			if _, ok := payload["gpuType"]; ok {
				handlerErrors.failf(w, "node enrollment payload included gpuType: %+v", payload)
				return
			}
			if _, ok := payload["gpuCount"]; ok {
				handlerErrors.failf(w, "node enrollment payload included gpuCount: %+v", payload)
				return
			}
			_ = json.NewEncoder(w).Encode(thundersdk.EnrollmentToken{EnrollmentTokenID: "node-token-1", EnrollmentToken: "tr_node", ZoneID: "zone-1", Role: thundersdk.RoleServer})
		case r.Method == http.MethodDelete && r.URL.Path == "/api/v1/enrollment-tokens/node-token-1/node":
			deleteTokenCalls++
			_ = json.NewEncoder(w).Encode(thundersdk.DeleteEnrollmentServerResponse{EnrollmentTokenID: "node-token-1", Role: thundersdk.RoleServer, ServerDeleted: true})
		default:
			handlerErrors.failf(w, "unexpected request = %s %s", r.Method, r.URL.Path)
			return
		}
	}))
	defer server.Close()

	service, err := NewService(ServiceOpts{
		Repository:          repo,
		Client:              thundersdk.NewClient(server.URL, "central-token", thundersdk.WithHTTPClient(server.Client())),
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
	handlerErrors.assertEmpty(t)
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
	handlerErrors.assertEmpty(t)
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

func TestServiceCreateNodeEnrollmentReplacesExistingToken(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	repo := repository.NewThunderRedisRepository(rdb)
	agentToken := "agent-token"
	validator := &fakeAgentStateValidator{state: &model.AgentTokenState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1"}}
	if err := repo.SaveZone(context.Background(), &repository.ThunderZoneState{WorkspaceID: "workspace-1", PoolName: "pool-1", ThunderZoneID: "zone-existing"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveNodeEnrollment(context.Background(), &repository.ThunderNodeEnrollmentState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1", EnrollmentTokenID: "node-token-old"}); err != nil {
		t.Fatal(err)
	}

	var createTokenCalls int
	var deleted []string
	handlerErrors := newThunderHTTPHandlerErrors()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/enrollment-tokens":
			createTokenCalls++
			payload := map[string]any{}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				handlerErrors.failf(w, "decode request body: %v", err)
				return
			}
			if payload["zoneId"] != "zone-existing" || payload["role"] != thundersdk.RoleServer {
				handlerErrors.failf(w, "node enrollment payload = %+v", payload)
				return
			}
			_ = json.NewEncoder(w).Encode(thundersdk.EnrollmentToken{EnrollmentTokenID: "node-token-new", EnrollmentToken: "tr_node_new", ZoneID: "zone-existing", Role: thundersdk.RoleServer})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/enrollment-tokens/"):
			deleted = append(deleted, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/enrollment-tokens/"), "/node"))
			_ = json.NewEncoder(w).Encode(thundersdk.DeleteEnrollmentServerResponse{ServerDeleted: true})
		default:
			handlerErrors.failf(w, "unexpected request = %s %s", r.Method, r.URL.Path)
			return
		}
	}))
	defer server.Close()

	service, err := NewService(ServiceOpts{
		Repository:          repo,
		Client:              thundersdk.NewClient(server.URL, "central-token", thundersdk.WithHTTPClient(server.Client())),
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
	handlerErrors.assertEmpty(t)
	if !resp.Ok || resp.ErrorMsg != "" || resp.EnrollmentToken != "tr_node_new" {
		t.Fatalf("CreateNodeEnrollment() = %+v", resp)
	}
	if createTokenCalls != 1 {
		t.Fatalf("createTokenCalls = %d", createTokenCalls)
	}
	if len(deleted) != 1 || deleted[0] != "node-token-old" {
		t.Fatalf("deleted tokens = %+v", deleted)
	}
	state, found, err := repo.GetNodeEnrollment(context.Background(), "workspace-1", "pool-1", "machine-1")
	if err != nil || !found {
		t.Fatalf("GetNodeEnrollment() found=%v err=%v", found, err)
	}
	if state.EnrollmentTokenID != "node-token-new" {
		t.Fatalf("node enrollment state = %+v", state)
	}
}

func TestServiceCreateNodeEnrollmentSucceedsWhenPreviousTokenRevokeFails(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()

	repo := repository.NewThunderRedisRepository(rdb)
	if err := repo.SaveZone(context.Background(), &repository.ThunderZoneState{WorkspaceID: "workspace-1", PoolName: "pool-1", ThunderZoneID: "zone-existing"}); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveNodeEnrollment(context.Background(), &repository.ThunderNodeEnrollmentState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1", EnrollmentTokenID: "node-token-old"}); err != nil {
		t.Fatal(err)
	}

	var deleted []string
	handlerErrors := newThunderHTTPHandlerErrors()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/enrollment-tokens":
			_ = json.NewEncoder(w).Encode(thundersdk.EnrollmentToken{EnrollmentTokenID: "node-token-new", EnrollmentToken: "tr_node_new", ZoneID: "zone-existing", Role: thundersdk.RoleServer})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/api/v1/enrollment-tokens/"):
			deleted = append(deleted, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/api/v1/enrollment-tokens/"), "/node"))
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "internal", "message": "delete failed"})
		default:
			handlerErrors.failf(w, "unexpected request = %s %s", r.Method, r.URL.Path)
			return
		}
	}))
	defer server.Close()

	service, err := NewService(ServiceOpts{
		Repository:          repo,
		Client:              thundersdk.NewClient(server.URL, "central-token", thundersdk.WithHTTPClient(server.Client())),
		ContainerRepo:       repository.NewContainerRedisRepositoryForTest(rdb),
		WorkerRepo:          repository.NewWorkerRedisRepositoryForTest(rdb),
		AgentStateValidator: &fakeAgentStateValidator{state: &model.AgentTokenState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := service.CreateNodeEnrollment(context.Background(), &pb.CreateNodeEnrollmentRequest{AgentToken: "agent-token"})
	if err != nil {
		t.Fatal(err)
	}
	handlerErrors.assertEmpty(t)
	if !resp.Ok || resp.ErrorMsg != "" || resp.EnrollmentToken != "tr_node_new" {
		t.Fatalf("CreateNodeEnrollment() = %+v", resp)
	}
	if len(deleted) != 1 || deleted[0] != "node-token-old" {
		t.Fatalf("deleted enrollment token ids = %+v", deleted)
	}
	state, found, err := repo.GetNodeEnrollment(context.Background(), "workspace-1", "pool-1", "machine-1")
	if err != nil || !found {
		t.Fatalf("GetNodeEnrollment() found=%v err=%v", found, err)
	}
	if state.EnrollmentTokenID != "node-token-new" {
		t.Fatalf("node enrollment state = %+v", state)
	}
}

func TestServiceDeleteClientEnrollmentSkipsChangedTokenUnderPoolLock(t *testing.T) {
	repo := &lockingThunderRepository{
		clientBeforeLock: &repository.ThunderClientEnrollmentState{
			ContainerID:       "container-1",
			WorkspaceID:       "workspace-1",
			PoolName:          "pool-1",
			EnrollmentTokenID: "token-old",
		},
		clientInsideLock: &repository.ThunderClientEnrollmentState{
			ContainerID:       "container-1",
			WorkspaceID:       "workspace-1",
			PoolName:          "pool-1",
			EnrollmentTokenID: "token-new",
		},
	}
	service := &Service{repo: repo, client: thundersdk.NewClient("https://central.example", "central-token")}

	resp, err := service.DeleteClientEnrollment(thunderWorkerAuthContext(types.TokenTypeWorker, ""), &pb.DeleteClientEnrollmentRequest{ContainerId: "container-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Ok || resp.ErrorMsg != "" {
		t.Fatalf("DeleteClientEnrollment() = %+v", resp)
	}
	if repo.lockedWorkspaceID != "workspace-1" || repo.lockedPoolName != "pool-1" {
		t.Fatalf("pool lock = workspace %q pool %q", repo.lockedWorkspaceID, repo.lockedPoolName)
	}
	if repo.clientGetCalls != 2 {
		t.Fatalf("client get calls = %d", repo.clientGetCalls)
	}
	if repo.deletedClientID != "" {
		t.Fatalf("deleted client id = %q", repo.deletedClientID)
	}
}

func TestServiceDeleteNodeEnrollmentSkipsChangedTokenUnderPoolLock(t *testing.T) {
	repo := &lockingThunderRepository{
		nodeBeforeLock: &repository.ThunderNodeEnrollmentState{
			WorkspaceID:       "workspace-1",
			PoolName:          "pool-1",
			MachineID:         "machine-1",
			EnrollmentTokenID: "node-token-old",
		},
		nodeInsideLock: &repository.ThunderNodeEnrollmentState{
			WorkspaceID:       "workspace-1",
			PoolName:          "pool-1",
			MachineID:         "machine-1",
			EnrollmentTokenID: "node-token-new",
		},
	}
	service := &Service{
		repo:           repo,
		client:         thundersdk.NewClient("https://central.example", "central-token"),
		agentValidator: &fakeAgentStateValidator{state: &model.AgentTokenState{WorkspaceID: "workspace-1", PoolName: "pool-1", MachineID: "machine-1"}},
	}

	resp, err := service.DeleteNodeEnrollment(context.Background(), &pb.DeleteNodeEnrollmentRequest{AgentToken: "agent-token"})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Ok || resp.ErrorMsg != "" {
		t.Fatalf("DeleteNodeEnrollment() = %+v", resp)
	}
	if repo.lockedWorkspaceID != "workspace-1" || repo.lockedPoolName != "pool-1" {
		t.Fatalf("pool lock = workspace %q pool %q", repo.lockedWorkspaceID, repo.lockedPoolName)
	}
	if repo.nodeGetOutsideLockCalls != 1 {
		t.Fatalf("node get outside lock calls = %d", repo.nodeGetOutsideLockCalls)
	}
	if repo.deletedNodeMachineID != "" {
		t.Fatalf("deleted node machine id = %q", repo.deletedNodeMachineID)
	}
}

func TestServiceCreateNodeEnrollmentRequiresValidAgentToken(t *testing.T) {
	rdb := newThunderRedisClient(t)
	defer rdb.Close()
	service, err := NewService(ServiceOpts{
		Repository:          repository.NewThunderRedisRepository(rdb),
		Client:              thundersdk.NewClient("https://central.example", "central-token"),
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

type thunderHTTPHandlerErrors struct {
	mu  sync.Mutex
	err error
}

func newThunderHTTPHandlerErrors() *thunderHTTPHandlerErrors {
	return &thunderHTTPHandlerErrors{}
}

func (e *thunderHTTPHandlerErrors) failf(w http.ResponseWriter, format string, args ...any) {
	err := fmt.Errorf(format, args...)
	e.mu.Lock()
	if e.err == nil {
		e.err = err
	}
	e.mu.Unlock()
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

func (e *thunderHTTPHandlerErrors) assertEmpty(t *testing.T) {
	t.Helper()
	e.mu.Lock()
	err := e.err
	e.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
}

type lockingThunderRepository struct {
	inLock                  bool
	lockedWorkspaceID       string
	lockedPoolName          string
	clientGetCalls          int
	nodeGetOutsideLockCalls int
	clientBeforeLock        *repository.ThunderClientEnrollmentState
	clientInsideLock        *repository.ThunderClientEnrollmentState
	nodeBeforeLock          *repository.ThunderNodeEnrollmentState
	nodeInsideLock          *repository.ThunderNodeEnrollmentState
	deletedClientID         string
	deletedNodeMachineID    string
}

func (r *lockingThunderRepository) WithPoolLock(ctx context.Context, workspaceID, poolName string, fn func(context.Context) error) error {
	r.lockedWorkspaceID = workspaceID
	r.lockedPoolName = poolName
	r.inLock = true
	defer func() { r.inLock = false }()
	return fn(ctx)
}

func (r *lockingThunderRepository) GetClientEnrollment(ctx context.Context, containerID string) (*repository.ThunderClientEnrollmentState, bool, error) {
	r.clientGetCalls++
	state := r.clientBeforeLock
	if r.inLock {
		state = r.clientInsideLock
	}
	if state == nil {
		return nil, false, nil
	}
	copy := *state
	return &copy, true, nil
}

func (r *lockingThunderRepository) SaveClientEnrollment(ctx context.Context, state *repository.ThunderClientEnrollmentState) error {
	return errors.New("unexpected SaveClientEnrollment call")
}

func (r *lockingThunderRepository) DeleteClientEnrollment(ctx context.Context, containerID string) error {
	if !r.inLock {
		return errors.New("DeleteClientEnrollment called outside pool lock")
	}
	r.deletedClientID = containerID
	return nil
}

func (r *lockingThunderRepository) ListClientEnrollments(ctx context.Context) ([]*repository.ThunderClientEnrollmentState, error) {
	return nil, errors.New("unexpected ListClientEnrollments call")
}

func (r *lockingThunderRepository) GetNodeEnrollment(ctx context.Context, workspaceID, poolName, machineID string) (*repository.ThunderNodeEnrollmentState, bool, error) {
	state := r.nodeBeforeLock
	if !r.inLock {
		r.nodeGetOutsideLockCalls++
	} else {
		state = r.nodeInsideLock
	}
	if state == nil {
		return nil, false, nil
	}
	copy := *state
	return &copy, true, nil
}

func (r *lockingThunderRepository) SaveNodeEnrollment(ctx context.Context, state *repository.ThunderNodeEnrollmentState) error {
	return errors.New("unexpected SaveNodeEnrollment call")
}

func (r *lockingThunderRepository) DeleteNodeEnrollment(ctx context.Context, workspaceID, poolName, machineID string) error {
	if !r.inLock {
		return errors.New("DeleteNodeEnrollment called outside pool lock")
	}
	r.deletedNodeMachineID = machineID
	return nil
}

func (r *lockingThunderRepository) ListNodeEnrollments(ctx context.Context, workspaceID, poolName string) ([]*repository.ThunderNodeEnrollmentState, error) {
	return nil, errors.New("unexpected ListNodeEnrollments call")
}

func (r *lockingThunderRepository) GetZone(ctx context.Context, workspaceID, poolName string) (*repository.ThunderZoneState, bool, error) {
	return nil, false, errors.New("unexpected GetZone call")
}

func (r *lockingThunderRepository) SaveZone(ctx context.Context, state *repository.ThunderZoneState) error {
	return errors.New("unexpected SaveZone call")
}

func (r *lockingThunderRepository) DeleteZone(ctx context.Context, workspaceID, poolName string) error {
	return errors.New("unexpected DeleteZone call")
}

func (r *lockingThunderRepository) ListZones(ctx context.Context, workspaceID string) ([]*repository.ThunderZoneState, error) {
	return nil, errors.New("unexpected ListZones call")
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

func newThunderRedisClient(t *testing.T) *common.RedisClient {
	t.Helper()
	rdb, err := repository.NewRedisClientForTest()
	if err != nil {
		t.Fatal(err)
	}
	return rdb
}
