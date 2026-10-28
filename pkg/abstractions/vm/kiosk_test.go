package vm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestKioskLaunchSpecAndRedaction(t *testing.T) {
	s, _, info, _, gateway := fixture()
	spec := types.VMSpec{ImageID: "kiosk-image", Kiosk: &types.VMKioskSpec{
		Transport: "websockets", Command: []string{"editor", "/workspace/project with spaces"}, Env: map[string]string{"TOKEN": "private"},
		MCP: map[string]types.VMKioskMCPServer{
			"app":  {Command: "app-mcp", Args: []string{"--token", "argument-secret"}, Env: map[string]string{"TOKEN": "server-secret"}},
			"http": {URL: "http://127.0.0.1:9000/mcp?token=query-secret", Headers: map[string]string{"Authorization": "Bearer backend-secret"}},
		},
	}}
	require.NoError(t, validate(&spec))
	require.True(t, spec.Desktop)
	require.Equal(t, "/workspace", spec.Kiosk.Cwd)
	require.Equal(t, "app-mcp", spec.Kiosk.MCP["app"].Command)
	require.Contains(t, spec.PrivatePorts, types.VMKioskMCPPort)
	require.NotContains(t, spec.Ports, types.VMKioskMCPPort)
	require.Contains(t, spec.PrivatePorts, types.VMKioskStatusPort)
	require.NotContains(t, spec.Ports, types.VMKioskStatusPort)
	require.NoError(t, validate(&spec), "normalization is idempotent across updates and restores")
	v, err := s.createVM(context.Background(), info, types.VMCreateOptions{Name: "application", Spec: spec})
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
	require.Empty(t, response.Spec.Kiosk.MCP["app"].Env)
	require.Empty(t, response.Spec.Kiosk.MCP["app"].Args)
	require.Empty(t, response.Spec.Kiosk.MCP["http"].Headers)
	require.Equal(t, "[redacted]", response.Spec.Kiosk.MCP["http"].URL)
	require.Equal(t, spec.Kiosk.MCP["app"].Args, v.Spec.Kiosk.MCP["app"].Args)
	require.Equal(t, spec.Kiosk.MCP["http"].URL, v.Spec.Kiosk.MCP["http"].URL)
	require.Equal(t, "Bearer backend-secret", v.Spec.Kiosk.MCP["http"].Headers["Authorization"])
	require.Equal(t, "server-secret", v.Spec.Kiosk.MCP["app"].Env["TOKEN"])
	require.Equal(t, "private", v.Spec.Kiosk.Env["TOKEN"])
	require.Equal(t, response.DesktopURL, response.KioskURL)
	require.Equal(t, strings.TrimRight(response.DesktopURL, "/")+types.VMKioskMCPPath, response.MCPURL)
	a := artifact(v, "artifact", "snapshot", "saved", "")
	require.Equal(t, spec.Kiosk, a.Spec.Kiosk)
	require.Empty(t, artifactResponse(*a).Spec.Kiosk.Env)
	require.Empty(t, artifactResponse(*a).Spec.Kiosk.MCP["app"].Args)
	require.Equal(t, "[redacted]", artifactResponse(*a).Spec.Kiosk.MCP["http"].URL)
}

func TestInvalidKioskNeverStartsCompute(t *testing.T) {
	for name, kiosk := range map[string]string{
		"MCP relative cwd":        `{"command":["editor"],"mcp":{"app":{"command":"node","cwd":"project"}}}`,
		"MCP reserved env":        `{"command":["editor"],"mcp":{"app":{"command":"node","env":{"BEAM_VM_ID":"other"}}}}`,
		"MCP header injection":    `{"command":["editor"],"mcp":{"app":{"url":"http://localhost/mcp","headers":{"Authorization":"token\r\nother:value"}}}}`,
		"MCP invalid name":        `{"command":["editor"],"mcp":{"app/name":{"command":"node"}}}`,
		"MCP HTTP launch options": `{"command":["editor"],"mcp":{"app":{"url":"http://localhost/mcp","args":["--flag"]}}}`,
		"MCP stdio headers":       `{"command":["editor"],"mcp":{"app":{"command":"node","headers":{"X-Key":"secret"}}}}`,
		"empty MCP command":       `{"command":["editor"],"mcp":{"app":{}}}`,
		"empty command":           `{"command":[]}`,
		"shell string":            `{"command":"editor project"}`,
		"relative cwd":            `{"command":["editor"],"cwd":"project"}`,
		"reserved env":            `{"command":["editor"],"env":{"BEAM_VM_ID":"other"}}`,
		"ambiguous MCP transport": `{"command":["editor"],"mcp":{"app":{"command":"node","url":"http://localhost/mcp"}}}`,
		"bad MCP URL":             `{"command":["editor"],"mcp":{"app":{"url":"file:///mcp"}}}`,
		"unknown field":           `{"command":["editor"],"shell":true}`,
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
				MCP:     map[string]types.VMKioskMCPServer{"app": {Command: "application-mcp"}},
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
	for _, test := range []struct {
		port uint32
		want string
	}{
		{types.VMDesktopPort, "protected"},
		{types.VMKioskStatusPort, "reserved"},
		{types.VMKioskMediaPort, "reserved"},
		{types.VMKioskMCPPort, "reserved"},
	} {
		t.Run(strconv.FormatUint(uint64(test.port), 10), func(t *testing.T) {
			s, v, _, _, _ := fixture()
			v.Spec.Kiosk = &types.VMKioskSpec{
				Command:   []string{"application"},
				Transport: types.VMKioskTransportWebRTC,
				MCP:       map[string]types.VMKioskMCPServer{"app": {Command: "adapter"}},
			}
			public := false
			require.ErrorContains(t, s.configurePort(context.Background(), v, "expose", test.port, &public), test.want)
			v.Spec.Ports = append(v.Spec.Ports, test.port)
			v.Spec.PublicPorts = []uint32{test.port}
			require.ErrorContains(t, validate(&v.Spec), test.want)
		})
	}
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
		{http.MethodPost, "mcp", "", "", "", 403},
		{http.MethodPost, "mcp", "private", "", "8082:mcp", 200},
		{http.MethodGet, "mcp", "private", "", "8082:mcp", 200},
		{http.MethodDelete, "mcp", "private", "", "8082:mcp", 200},
		{http.MethodGet, "_beam/kiosk/status", "", "", "", 403},
		{http.MethodGet, "stream/api/ws", "", "", "", 403},
		{http.MethodGet, "_beam/kiosk/webrtc.js", "", "", "", 403},
		{http.MethodPost, "_beam/kiosk/webrtc/session", "", "webrtc", "", 403},
		{http.MethodPost, "_beam/kiosk/webrtc/session", "private", "", "", 403},
		{http.MethodGet, "_beam/kiosk/webrtc.js", "private", "", "", 200},
		{http.MethodGet, "stream/api/ws", "private", "", "8080:api/ws", 200},
		{http.MethodGet, "stream/assets/client.js", "private", "", "8080:assets/client.js", 200},
		{http.MethodGet, "stream/", "private", "", "8080:", 200},
		{http.MethodGet, "streaming/client.js", "private", "", "8080:streaming/client.js", 200},
		{http.MethodGet, "_beam/kiosk/status", "private", "", "8081:status", 200},
		{http.MethodPost, "_beam/kiosk/status", "private", "", "", 405},
		{http.MethodGet, "_beam/kiosk/status/other", "private", "", "8080:_beam/kiosk/status/other", 200},
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
