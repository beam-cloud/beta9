package vm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestKioskLaunchSpecAndRedaction(t *testing.T) {
	s, _, info, _, gateway := fixture()
	spec := types.VMSpec{ImageID: "kiosk-image", Kiosk: &types.VMKioskSpec{
		Transport: "websockets", Command: []string{"editor", "/workspace/project with spaces"}, Env: map[string]string{"TOKEN": "private"},
		MCP: &types.VMKioskMCP{Command: []string{"app-mcp"}, Port: 8000},
	}}
	require.NoError(t, validate(&spec))
	require.True(t, spec.Desktop)
	require.Equal(t, "/workspace", spec.Kiosk.Cwd)
	require.Equal(t, "/mcp", spec.Kiosk.MCP.Path)
	require.Contains(t, spec.Ports, uint32(8000))
	require.Contains(t, spec.ProtectedPorts, uint32(8000))
	require.Contains(t, spec.PrivatePorts, types.VMKioskStatusPort)
	require.NotContains(t, spec.Ports, types.VMKioskStatusPort)
	require.NoError(t, validate(&spec), "normalization is idempotent across updates and restores")
	v, err := s.createVM(context.Background(), info, "application", spec)
	require.NoError(t, err)
	var config types.VMKioskSpec
	for _, env := range gateway.stub.Env {
		if encoded, ok := strings.CutPrefix(env, "BEAM_VM_KIOSK="); ok {
			data, err := base64.StdEncoding.DecodeString(encoded)
			require.NoError(t, err)
			require.NoError(t, json.Unmarshal(data, &config))
		}
	}
	require.Equal(t, spec.Kiosk, &config)
	response := s.response(v)
	require.Empty(t, response.Spec.Kiosk.Env)
	require.Equal(t, "private", v.Spec.Kiosk.Env["TOKEN"])
	require.Equal(t, response.DesktopURL, response.KioskURL)
	require.Equal(t, strings.TrimRight(response.URLs[8000], "/")+"/mcp", response.MCPURL)
	a := artifact(v, "artifact", "snapshot", "saved", "")
	require.Equal(t, spec.Kiosk, a.Spec.Kiosk)
	require.Empty(t, artifactResponse(*a).Spec.Kiosk.Env)
}

func TestInvalidKioskNeverStartsCompute(t *testing.T) {
	for name, kiosk := range map[string]string{
		"empty command":     `{"command":[]}`,
		"shell string":      `{"command":"editor project"}`,
		"relative cwd":      `{"command":["editor"],"cwd":"project"}`,
		"reserved env":      `{"command":["editor"],"env":{"BEAM_VM_ID":"other"}}`,
		"reserved MCP port": `{"command":["editor"],"mcp":{"command":["mcp"],"port":8081}}`,
		"bad MCP path":      `{"command":["editor"],"mcp":{"command":["mcp"],"port":8000,"path":"https://elsewhere/mcp"}}`,
		"unknown field":     `{"command":["editor"],"shell":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			s, _, info, runtime, _ := fixture()
			body := `{"name":"invalid","spec":{"image_id":"kiosk","kiosk":` + kiosk + `}}`
			rec := vmRequest(managementAPI(s, info), http.MethodPost, "/"+info.Workspace.ExternalId, body)
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			require.Empty(t, runtime.requests)
		})
	}
}

func TestKioskArtifactsReopenWithIndependentIdentity(t *testing.T) {
	for _, kind := range []string{"snapshot", "template"} {
		t.Run(kind, func(t *testing.T) {
			s, v, info, _, gateway := fixture()
			v.Spec.Kiosk = &types.VMKioskSpec{
				Command: []string{"application", "/workspace/saved-project"},
				Env:     map[string]string{"APPLICATION_SETTING": "saved-setting"},
				MCP:     &types.VMKioskMCP{Command: []string{"application-mcp"}, Port: 8000},
			}
			require.NoError(t, validate(&v.Spec))
			a, err := s.capture(context.Background(), v, kind, "saved-project", "")
			require.NoError(t, err)
			body := `{"name":"reopened","` + kind + `":"saved-project","spec":{"cpu":2000}}`
			rec := vmRequest(managementAPI(s, info), http.MethodPost, "/"+info.Workspace.ExternalId, body)
			require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			var child types.VM
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &child))
			require.NotEqual(t, v.ID, child.ID)
			require.NotEqual(t, v.Handle, child.Handle)
			require.Equal(t, v.Spec.Kiosk.Command, child.Spec.Kiosk.Command)
			require.Empty(t, child.Spec.Kiosk.Env)
			stored, err := s.repo.GetVM(context.Background(), info.Workspace.Id, child.ID)
			require.NoError(t, err)
			require.NotEmpty(t, stored.TrafficAccessToken)
			require.NotEqual(t, v.TrafficAccessToken, stored.TrafficAccessToken)
			require.Equal(t, "saved-setting", stored.Spec.Kiosk.Env["APPLICATION_SETTING"])
			require.Equal(t, a.RootSnapshotID, gateway.stub.Disks[0].SourceSnapshotId)
			require.NotEqual(t, rootDisk(v), gateway.stub.Disks[0].Name)
		})
	}
}

func TestKioskInternalAndMCPPortsCannotBePublishedWithoutProtection(t *testing.T) {
	s, v, _, _, _ := fixture()
	v.Spec.Kiosk = &types.VMKioskSpec{Command: []string{"application"}, MCP: &types.VMKioskMCP{Port: 8000}}
	public := false
	require.ErrorContains(t, s.configurePort(context.Background(), v, "expose", types.VMKioskStatusPort, &public), "reserved")
	require.ErrorContains(t, s.configurePort(context.Background(), v, "expose", 8000, &public), "protected")
}

func TestKioskGatewayRoutingAndAuthorization(t *testing.T) {
	s, v, _, runtime, _ := fixture()
	v.Spec.Kiosk = &types.VMKioskSpec{Command: []string{"application"}}
	v.Spec.ProtectedPorts = []uint32{8080}
	v.TrafficAccessToken = "private"
	for _, test := range []struct {
		method, path, token, action, forwarded string
		code                                   int
	}{
		{http.MethodGet, "", "", "", "", 403},
		{http.MethodGet, "_beam/kiosk/status", "", "", "", 403},
		{http.MethodGet, "stream/api/ws", "", "", "", 403},
		{http.MethodGet, "_beam/kiosk/webrtc.js", "", "", "", 403},
		{http.MethodPost, "_beam/kiosk/webrtc/session", "", "webrtc", "", 403},
		{http.MethodPost, "_beam/kiosk/webrtc/session", "private", "", "", 403},
		{http.MethodGet, "_beam/kiosk/webrtc.js", "private", "", "", 200},
		{http.MethodGet, "stream/api/ws", "private", "", "8080:api/ws", 200},
		{http.MethodGet, "stream/assets/client.js", "private", "", "8080:assets/client.js", 200},
		{http.MethodGet, "_beam/kiosk/status", "private", "", "8081:status", 200},
		{http.MethodPost, "_beam/kiosk/restart", "private", "", "", 403},
		{http.MethodGet, "_beam/kiosk/restart", "private", "restart", "", 403},
		{http.MethodPost, "_beam/kiosk/restart", "private", "restart", "8081:restart", 200},
	} {
		t.Run(test.method+" "+test.path+" "+test.token+test.action, func(t *testing.T) {
			runtime.forwarded = ""
			req := httptest.NewRequest(test.method, "/vm/"+v.Handle+"/8080/"+test.path, nil)
			req.Header.Set("X-Beam-VM-Token", test.token)
			req.Header.Set("X-Beam-Kiosk-Action", test.action)
			rec := httptest.NewRecorder()
			proxyAPI(s).ServeHTTP(rec, req)
			require.Equal(t, test.code, rec.Code, rec.Body.String())
			if test.forwarded == "" {
				require.Empty(t, runtime.forwarded)
			} else {
				require.Equal(t, v.ContainerID+":"+test.forwarded, runtime.forwarded)
			}
		})
	}
	req := httptest.NewRequest(http.MethodGet, "/vm/"+v.Handle+"/8080/", nil)
	req.Header.Set("X-Beam-VM-Token", "private")
	req.Header.Set("Accept", "text/html")
	rec := httptest.NewRecorder()
	proxyAPI(s).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "data-beam-kiosk-entry")
	require.Contains(t, rec.Body.String(), `_beam/kiosk/webrtc.js`)
	require.NotContains(t, rec.Body.String(), "vnc.html")
	require.NotContains(t, rec.Body.String(), "scale(0.5)")
}
