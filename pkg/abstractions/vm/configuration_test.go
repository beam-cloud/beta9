package vm

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

func managementAPI(s *Service, info *auth.AuthInfo) *echo.Echo {
	e := echo.New()
	e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error { return next(&auth.HttpAuthContext{Context: c, AuthInfo: info}) }
	})
	e.GET("/:workspaceId", auth.WithStrictWorkspaceAuth(s.list))
	e.POST("/:workspaceId", auth.WithStrictWorkspaceAuth(s.create))
	e.PATCH("/:workspaceId/:name", auth.WithStrictWorkspaceAuth(s.update))
	e.POST("/:workspaceId/:name/:action", auth.WithStrictWorkspaceAuth(s.action))
	return e
}

func proxyAPI(s *Service) *echo.Echo {
	e := echo.New()
	e.Any("/vm/:handle/:port/*", s.proxy)
	return e
}

func vmRequest(e *echo.Echo, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	return rec
}

func TestRootDiskCapacity(t *testing.T) {
	for _, test := range []struct {
		size    string
		code    int
		message string
	}{
		{"1GiB", http.StatusCreated, ""},
		{"100Gi", http.StatusCreated, ""},
		{"100GiB", http.StatusCreated, ""},
		{"107374182400", http.StatusCreated, ""},
		{"107374182401", http.StatusBadRequest, "100 GiB"},
		{"100.001Gi", http.StatusBadRequest, "integer number of bytes"},
		{"101GiB", http.StatusBadRequest, "100 GiB"},
		{"1TiB", http.StatusBadRequest, "100 GiB"},
	} {
		t.Run(test.size, func(t *testing.T) {
			s, _, info, runtime, _ := fixture()
			body := fmt.Sprintf(`{"name":"capacity","spec":{"image_id":"base","disk_size":%q}}`, test.size)
			rec := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId, body)
			require.Equal(t, test.code, rec.Code, rec.Body.String())
			if test.code == http.StatusBadRequest {
				require.Empty(t, runtime.requests)
				require.Contains(t, rec.Body.String(), test.message)
			}
		})
	}
}

func TestDesktopMinimumMemory(t *testing.T) {
	for _, memory := range []int64{512, 1023, 1024, 1536} {
		t.Run(fmt.Sprint(memory), func(t *testing.T) {
			s, _, info, runtime, _ := fixture()
			body := fmt.Sprintf(`{"name":"desktop-min","spec":{"image_id":"base","desktop":true,"cpu":1000,"memory":%d}}`, memory)
			rec := vmRequest(managementAPI(s, info), "POST", "/"+info.Workspace.ExternalId, body)
			if memory < 1024 {
				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
				require.Contains(t, rec.Body.String(), "desktop requires at least 1024 MiB")
				require.Empty(t, runtime.requests)
			} else {
				require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			}
		})
	}
}

func TestResponsesRedactVolumeCredentialsWithoutMutatingStoredSpec(t *testing.T) {
	s, v, _, _, _ := fixture()
	v.Spec.Volumes = []*pb.Volume{{Config: &pb.MountPointConfig{BucketName: "bucket", AccessKey: "volume-access", SecretKey: "volume-secret"}}}
	for _, spec := range []types.VMSpec{s.response(v).Spec, artifactResponse(types.VMArtifact{Spec: v.Spec}).Spec} {
		require.Empty(t, spec.Volumes[0].Config.AccessKey)
		require.Empty(t, spec.Volumes[0].Config.SecretKey)
		require.Equal(t, "bucket", spec.Volumes[0].Config.BucketName)
	}
	require.Equal(t, "volume-access", v.Spec.Volumes[0].Config.AccessKey)
	require.Equal(t, "volume-secret", v.Spec.Volumes[0].Config.SecretKey)
}

func TestMountsCannotHideNestedDisksOrVolumes(t *testing.T) {
	for _, mounts := range [][]string{{"/data", "/data/cache"}, {"/data/cache", "/data"}} {
		seen := map[string]bool{"/": true}
		require.NoError(t, validateMountPath(mounts[0], seen))
		require.ErrorContains(t, validateMountPath(mounts[1], seen), "overlap")
	}
}

func TestInternalProcessControlPortCannotBePublished(t *testing.T) {
	spec := types.VMSpec{ImageID: "base", Ports: []uint32{uint32(types.WorkerSandboxProcessManagerPort)}}
	require.Error(t, validate(&spec))
	spec.Ports, spec.PrivatePorts = nil, []uint32{uint32(types.WorkerSandboxProcessManagerPort)}
	require.Error(t, validate(&spec))
}

func TestVMSystemPortProtection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		spec      types.VMSpec
		protected []uint32
		invalid   bool
	}{
		{"terminal", types.VMSpec{}, []uint32{7681}, false},
		{"desktop", types.VMSpec{Desktop: true}, []uint32{7681, 8080}, false},
		{"public terminal", types.VMSpec{PublicPorts: []uint32{7681}}, nil, false},
		{"public desktop", types.VMSpec{Desktop: true, PublicPorts: []uint32{8080}}, []uint32{7681}, false},
		{"conflicting", types.VMSpec{PublicPorts: []uint32{7681}, ProtectedPorts: []uint32{7681}}, nil, true},
		{"public ssh", types.VMSpec{PublicPorts: []uint32{2222}}, nil, true},
		{"protected ssh", types.VMSpec{ProtectedPorts: []uint32{2222}}, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.spec.ImageID = "base"
			err := validate(&tc.spec)
			if tc.invalid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tc.protected, tc.spec.ProtectedPorts)
			require.NoError(t, validate(&tc.spec), "validation is idempotent")
		})
	}
}

func TestToggleSystemPortsWithoutRuntimeRebind(t *testing.T) {
	for _, port := range []uint32{7681, 8080} {
		s, v, _, runtime, _ := fixture()
		for _, protected := range []bool{true, false, true} {
			require.NoError(t, s.configurePort(context.Background(), v, "expose", port, &protected))
			require.Contains(t, v.Spec.Ports, port)
			require.Equal(t, protected, slices.Contains(v.Spec.ProtectedPorts, port))
			require.Equal(t, !protected, slices.Contains(v.Spec.PublicPorts, port))
			require.NotEmpty(t, v.TrafficAccessToken)
		}
		require.Empty(t, runtime.boundPorts)
		for _, action := range []string{"expose", "bind", "unexpose"} {
			require.Error(t, s.configurePort(context.Background(), v, action, port, nil))
			protected := true
			require.Error(t, s.configurePort(context.Background(), v, action, 2222, &protected))
		}
	}
}

func TestWorkspaceTokenSecretRequiresExplicitBinding(t *testing.T) {
	for _, name := range []string{"BETA9_TOKEN", "BEAM_TOKEN", "BETA9_GATEWAY_HOST", "BEAM_VM_ID"} {
		spec := types.VMSpec{ImageID: "base", Secrets: []string{name}}
		err := validate(&spec)
		if name == "BETA9_TOKEN" || name == "BEAM_TOKEN" {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
		}
	}
}

func TestCreationRequestIsWorkspaceScopedAndDoesNotLaunchTwice(t *testing.T) {
	s, _, info, runtime, _ := fixture()
	e := managementAPI(s, info)
	id := uuid.NewString()
	body := `{"name":"retry-safe","request_id":"` + id + `","metadata":{"user":"123"},"spec":{"image_id":"base"}}`
	first := vmRequest(e, "POST", "/"+info.Workspace.ExternalId, body)
	require.Equal(t, 201, first.Code, first.Body.String())
	replayed := vmRequest(e, "POST", "/"+info.Workspace.ExternalId, body)
	require.Equal(t, 201, replayed.Code, replayed.Body.String())
	var a, b types.VM
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &a))
	require.NoError(t, json.Unmarshal(replayed.Body.Bytes(), &b))
	require.Equal(t, a.ID, b.ID)
	require.Equal(t, a.Handle, b.Handle)
	require.Len(t, runtime.requests, 1)
	require.Empty(t, a.CreationDigest)
	require.Empty(t, a.TrafficAccessToken)
	access := vmRequest(e, "POST", "/"+info.Workspace.ExternalId+"/"+a.ID+"/access-token", `{}`)
	require.Equal(t, 200, access.Code)
	stored, err := s.repo.GetVM(context.Background(), info.Workspace.Id, a.ID)
	require.NoError(t, err)
	require.Contains(t, access.Body.String(), stored.TrafficAccessToken)
	conflict := vmRequest(e, "POST", "/"+info.Workspace.ExternalId, strings.Replace(body, "retry-safe", "changed", 1))
	require.Equal(t, 409, conflict.Code)
	require.Len(t, runtime.requests, 1)
}

func TestMetadataFilterMatchesPresentKeysAndUpdateClears(t *testing.T) {
	s, v, info, _, _ := fixture()
	v.Spec.Memory = 2048
	v.Metadata = map[string]string{"user": "123", "empty": ""}
	e := managementAPI(s, info)
	path := "/" + info.Workspace.ExternalId
	for _, test := range []struct {
		filter string
		count  int
	}{{`{"user":"123"}`, 1}, {`{"user":"other"}`, 0}, {`{"absent":""}`, 0}, {`{"empty":""}`, 1}} {
		rec := vmRequest(e, "GET", path+"?metadata="+strings.ReplaceAll(test.filter, `"`, "%22"), "")
		require.Equal(t, 200, rec.Code, rec.Body.String())
		var rows []types.VM
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &rows))
		require.Len(t, rows, test.count)
	}
	rec := vmRequest(e, "PATCH", path+"/"+v.ID, `{"metadata":{},"idle_timeout":90,"auto_resume":true}`)
	require.Equal(t, 200, rec.Code, rec.Body.String())
	stored, err := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.Empty(t, stored.Metadata)
	require.Equal(t, int64(90), stored.Spec.IdleTimeout)
	require.True(t, stored.Spec.AutoResume)
	bad := vmRequest(e, "PATCH", path+"/"+v.ID, `{"idle_timeout":-1}`)
	require.Equal(t, 400, bad.Code)
	stored, _ = s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
	require.Equal(t, int64(90), stored.Spec.IdleTimeout)
}

func (r *vmRuntime) SandboxUpdateNetworkPermissions(_ context.Context, req *pb.PodSandboxUpdateNetworkPermissionsRequest) (*pb.PodSandboxUpdateNetworkPermissionsResponse, error) {
	r.network = req
	return &pb.PodSandboxUpdateNetworkPermissionsResponse{Ok: true}, nil
}

func TestNetworkChangesReachWorkerAndFutureLaunches(t *testing.T) {
	s, v, info, runtime, gateway := fixture()
	v.Spec.Memory = 2048
	e := managementAPI(s, info)
	path := "/" + info.Workspace.ExternalId + "/" + v.ID
	rec := vmRequest(e, "PATCH", path, `{"block_network":true,"allow_list":[]}`)
	require.Equal(t, 200, rec.Code, rec.Body.String())
	require.True(t, runtime.network.BlockNetwork)
	stored, _ := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
	stored.StubID = ""
	require.NoError(t, s.prepare(auth.ContextWithAuthInfo(context.Background(), info), stored))
	require.True(t, gateway.stub.BlockNetwork)
	rec = vmRequest(e, "PATCH", path, `{"allow_list":["1.1.1.1/32"]}`)
	require.Equal(t, 400, rec.Code)
	require.Empty(t, runtime.network.AllowList)
}

func TestProtectedPortDeniesBeforeWakeAndRotationRevokesOldToken(t *testing.T) {
	s, v, info, runtime, _ := fixture()
	v.Spec.ProtectedPorts = []uint32{8080}
	v.TrafficAccessToken = "secret"
	delete(runtime.containers.states, v.ContainerID)
	v.ContainerID, v.DesiredState, v.Status = "", "stopped", "stopped"
	v.Spec.AutoResume = true
	e := proxyAPI(s)
	for _, token := range []string{"", "wrong", "secret"} {
		req := httptest.NewRequest("GET", "/vm/"+v.Handle+"/8080/", nil)
		req.Header.Set("X-Beam-VM-Token", token)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)
		if token == "secret" {
			require.Equal(t, 200, rec.Code, rec.Body.String())
			require.Len(t, runtime.requests, 1)
			require.Empty(t, req.Header.Get("X-Beam-VM-Token"))
		} else {
			require.Equal(t, 403, rec.Code)
			require.Empty(t, runtime.requests)
		}
	}
	api := managementAPI(s, info)
	rec := vmRequest(api, "POST", "/"+info.Workspace.ExternalId+"/"+v.ID+"/rotate-access-token", `{}`)
	require.Equal(t, 200, rec.Code)
	req := httptest.NewRequest("GET", "/vm/"+v.Handle+"/8080/", nil)
	req.Header.Set("X-Beam-VM-Token", "secret")
	rec = httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, 403, rec.Code)
	require.Len(t, runtime.requests, 1, "a revoked token must not launch more compute")
}

func TestAccessResumesStoppedVMAndKeepsItsURL(t *testing.T) {
	s, v, _, runtime, _ := fixture()
	v.Spec.AutoResume = true
	v.DesiredState, v.Status, v.ContainerID = "stopped", "stopped", ""
	v.RootSnapshotID = "committed"
	s.repo.(*vmStore).rows[v.ID] = v
	e := proxyAPI(s)
	req := httptest.NewRequest("POST", "/vm/"+v.Handle+"/8080/path", strings.NewReader("body"))
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)
	require.Equal(t, 200, rec.Code, rec.Body.String())
	require.Len(t, runtime.requests, 1)
	require.Contains(t, runtime.forwarded, ":8080:path")
	stored, _ := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
	require.Equal(t, v.Handle, stored.Handle)
	require.Equal(t, "running", stored.DesiredState)
}

func TestUpdateProjectsLaunchReadinessWithoutPersistingLifecycle(t *testing.T) {
	for _, test := range []struct {
		name, status, desired, response string
		runtime                         types.ContainerStatus
		missing, lookupError            bool
	}{
		{name: "ready launch", status: "starting", desired: "running", response: "running", runtime: types.ContainerStatusRunning},
		{name: "pending launch", status: "starting", desired: "running", response: "starting", runtime: types.ContainerStatusPending},
		{name: "absent runtime", status: "starting", desired: "running", response: "starting", missing: true},
		{name: "pause intent", status: "starting", desired: "paused", response: "starting", runtime: types.ContainerStatusRunning},
		{name: "stop intent", status: "starting", desired: "stopped", response: "starting", runtime: types.ContainerStatusRunning},
		{name: "paused", status: "paused", desired: "paused", response: "paused", runtime: types.ContainerStatusRunning},
		{name: "stopped", status: "stopped", desired: "stopped", response: "stopped", runtime: types.ContainerStatusRunning},
		{name: "stopping", status: "stopping", desired: "stopped", response: "stopping", runtime: types.ContainerStatusRunning},
		{name: "runtime error", status: "starting", desired: "running", lookupError: true},
	} {
		for _, network := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/network=%t", test.name, network), func(t *testing.T) {
				s, v, info, runtime, _ := fixture()
				v.Status, v.DesiredState = test.status, test.desired
				v.MemoryCheckpointID = "owned-checkpoint"
				containers := s.containers.(*vmContainers)
				containers.states[v.ContainerID].Status = test.runtime
				if test.missing {
					delete(containers.states, v.ContainerID)
				}
				if test.lookupError {
					containers.readError = fmt.Errorf("runtime lookup unavailable")
				}
				body := `{"idle_timeout":90,"idle_action":"pause","auto_resume":true}`
				wantCode := http.StatusOK
				if network {
					body = `{"block_network":true}`
					if test.response != "running" {
						wantCode = http.StatusConflict
					}
				}
				if test.lookupError {
					wantCode = http.StatusBadRequest // Existing API error mapping.
				}
				rec := vmRequest(managementAPI(s, info), "PATCH", "/"+info.Workspace.ExternalId+"/"+v.ID, body)
				require.Equal(t, wantCode, rec.Code, rec.Body.String())
				if wantCode == http.StatusOK {
					var response types.VM
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
					require.Equal(t, test.response, response.Status)
				}
				stored, err := s.repo.GetVM(context.Background(), v.WorkspaceID, v.ID)
				require.NoError(t, err)
				require.Equal(t, test.status, stored.Status)
				require.Equal(t, test.desired, stored.DesiredState)
				require.Equal(t, "owned-checkpoint", stored.MemoryCheckpointID)
				if wantCode != http.StatusOK {
					require.Zero(t, stored.Spec.IdleTimeout)
					require.False(t, stored.Spec.BlockNetwork)
				}
				require.Equal(t, network && wantCode == http.StatusOK, runtime.network != nil)
			})
		}
	}
}
