package gateway

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
)

func TestAgentInstallScriptShellSyntax(t *testing.T) {
	cmd := exec.Command("sh", "-n")
	cmd.Stdin = strings.NewReader(agentInstallScript)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install script has invalid shell syntax: %v\n%s", err, out)
	}
}

func TestAgentInstallScriptDownloadsAgentFromGateway(t *testing.T) {
	if strings.Contains(agentInstallScript, "github.com/beam-cloud/beta9/releases") {
		t.Fatal("install script should not depend on guessed GitHub release artifact names")
	}
	for _, want := range []string{
		"${GATEWAY}/install/agent/${OS_NAME}/${ARCH_NAME}",
		"${GATEWAY}/install/agent/linux/${ARCH}",
		"ensure_linux_docker",
		"ensure_linux_nvidia_container_runtime",
		"--cache-dir",
		"BEAM_AGENT_CACHE_DIR",
		"BEAM_AGENT_INSTALL_NVIDIA_TOOLKIT",
		"https://nvidia.github.io/libnvidia-container/stable/deb/nvidia-container-toolkit.list",
		"https://nvidia.github.io/libnvidia-container/stable/rpm/nvidia-container-toolkit.repo",
		"nvidia-ctk runtime configure --runtime=docker",
	} {
		if !strings.Contains(agentInstallScript, want) {
			t.Fatalf("install script missing %q", want)
		}
	}
	if !strings.Contains(agentInstallScript, `elif [ "$DEV" = "1" ]; then`) || !strings.Contains(agentInstallScript, `${GATEWAY}/install/agent/linux/${ARCH}?dev=1`) {
		t.Fatal("install script should only request source-built Linux agent binaries in dev mode")
	}
}

func TestAgentInstallScriptInstallFromURLWritesDestination(t *testing.T) {
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatal(err)
	}

	fakeCurl := filepath.Join(binDir, "curl")
	if err := os.WriteFile(fakeCurl, []byte(`#!/bin/sh
set -eu
out=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    shift
    out="$1"
  fi
  shift || true
done
printf 'agent-binary' > "$out"
`), 0755); err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(tmp, "agent", "beam-agent-linux-arm64")
	script := strings.TrimSuffix(agentInstallScript, "\nmain \"$@\"\n") +
		"\ninstall_from_url https://example.com/agent " + target + " Test\n"
	cmd := exec.Command("sh")
	cmd.Stdin = strings.NewReader(script)
	cmd.Env = append(os.Environ(), "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("install_from_url failed: %v\n%s", err, out)
	}

	data, err := os.ReadFile(target)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "agent-binary" {
		t.Fatalf("target content = %q", string(data))
	}
}

func TestAgentInstallScriptUsesInvokingUserHomeForMacOSDocker(t *testing.T) {
	for _, want := range []string{
		"HOST_HOME=\"$(agent_host_home)\"",
		"${HOST_HOME}/.beam/bin/beam-agent-linux-${ARCH}",
		"${HOST_HOME}/.beam/agent",
		"${SUDO_USER:-}",
		"${BEAM_AGENT_HOME:-}",
	} {
		if !strings.Contains(agentInstallScript, want) {
			t.Fatalf("install script missing %q", want)
		}
	}
}

func TestAgentInstallScriptDefaultsMacOSWorkerPlatform(t *testing.T) {
	for _, want := range []string{
		`export BEAM_AGENT_WORKER_PLATFORM="linux/amd64"`,
		"-e BEAM_AGENT_WORKER_PLATFORM",
	} {
		if !strings.Contains(agentInstallScript, want) {
			t.Fatalf("install script missing %q", want)
		}
	}
}

func TestAgentInstallScriptConfiguresExistingNvidiaToolkit(t *testing.T) {
	tmp := t.TempDir()
	binDir := filepath.Join(tmp, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatal(err)
	}

	runtimeMarker := filepath.Join(tmp, "nvidia-runtime")
	restartMarker := filepath.Join(tmp, "docker-restart")
	fakes := map[string]string{
		"nvidia-smi": `#!/bin/sh
set -eu
[ "${1:-}" = "--query-gpu=index" ]
printf '0\n'
`,
		"docker": `#!/bin/sh
set -eu
[ "${1:-}" = "info" ]
if [ -f "$NVIDIA_RUNTIME_MARKER" ]; then
  printf '{"nvidia":{}}\n'
else
  printf '{"runc":{}}\n'
fi
`,
		"nvidia-ctk": `#!/bin/sh
set -eu
[ "$*" = "runtime configure --runtime=docker" ]
printf 'configured\n' > "$NVIDIA_RUNTIME_MARKER"
`,
		"systemctl": `#!/bin/sh
set -eu
printf '%s\n' "$*" > "$NVIDIA_RESTART_MARKER"
`,
	}
	for name, contents := range fakes {
		if err := os.WriteFile(filepath.Join(binDir, name), []byte(contents), 0755); err != nil {
			t.Fatal(err)
		}
	}

	script := strings.TrimSuffix(agentInstallScript, "\nmain \"$@\"\n") + `
OS=linux
EXECUTOR=worker-container
ensure_linux_nvidia_container_runtime
`
	cmd := exec.Command("sh")
	cmd.Stdin = strings.NewReader(script)
	cmd.Env = append(os.Environ(),
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"NVIDIA_RUNTIME_MARKER="+runtimeMarker,
		"NVIDIA_RESTART_MARKER="+restartMarker,
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("NVIDIA runtime configuration failed: %v\n%s", err, out)
	}
	if data, err := os.ReadFile(runtimeMarker); err != nil || string(data) != "configured\n" {
		t.Fatalf("runtime marker = %q, %v", data, err)
	}
	if data, err := os.ReadFile(restartMarker); err != nil || string(data) != "restart docker\n" {
		t.Fatalf("restart marker = %q, %v", data, err)
	}
}

func TestAgentBinaryHandlerServesConfiguredBinary(t *testing.T) {
	path := writeAgentBinary(t)
	t.Setenv(types.AgentBinaryPathEnv, path)

	rec := httptest.NewRecorder()
	ctx := newAgentBinaryContext(rec, runtime.GOOS, runtime.GOARCH)
	if err := agentBinaryHandler()(ctx); err != nil {
		t.Fatal(err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); got != "agent-binary" {
		t.Fatalf("body = %q", got)
	}
}

func TestAgentBinaryHandlerRejectsWrongPlatform(t *testing.T) {
	rec := httptest.NewRecorder()
	ctx := newAgentBinaryContext(rec, "not-"+runtime.GOOS, "not-"+runtime.GOARCH)
	err := agentBinaryHandler()(ctx)
	if err == nil {
		t.Fatal("expected platform mismatch error")
	}
	httpErr, ok := err.(*echo.HTTPError)
	if !ok || httpErr.Code != http.StatusNotFound {
		t.Fatalf("err = %#v, want 404 echo error", err)
	}
}

func TestAgentBinaryHandlerBuildsMissingBinaryFromSource(t *testing.T) {
	sourceDir := fakeAgentSourceTree(t, "built-agent")
	t.Setenv(types.AgentSourceDirEnv, sourceDir)
	t.Setenv(types.AgentBuildCacheDirEnv, t.TempDir())

	rec := httptest.NewRecorder()
	ctx := newAgentBinaryContext(rec, "linux", "arm64")
	if err := agentBinaryHandler()(ctx); err != nil {
		t.Fatal(err)
	}

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := rec.Body.String(); got != "built-agent" {
		t.Fatalf("body = %q", got)
	}
}

func TestAgentBinaryHandlerDevQueryBuildsEvenWhenStaticBinaryExists(t *testing.T) {
	staticPath := writeAgentBinary(t)
	sourceDir := fakeAgentSourceTree(t, "dev-agent")
	t.Setenv(types.AgentBinaryPathEnv, staticPath)
	t.Setenv(types.AgentSourceDirEnv, sourceDir)
	t.Setenv(types.AgentBuildCacheDirEnv, t.TempDir())

	rec := httptest.NewRecorder()
	ctx := newAgentBinaryContext(rec, "linux", "arm64")
	ctx.Request().URL.RawQuery = "dev=1"
	if err := agentBinaryHandler()(ctx); err != nil {
		t.Fatal(err)
	}
	if got := rec.Body.String(); got != "dev-agent" {
		t.Fatalf("body = %q, want dev-built binary", got)
	}
}

func writeAgentBinary(t *testing.T) string {
	t.Helper()

	file, err := os.CreateTemp(t.TempDir(), "beam-agent-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("agent-binary"); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(file.Name(), 0755); err != nil {
		t.Fatal(err)
	}
	return file.Name()
}

func fakeAgentSourceTree(t *testing.T, output string) string {
	t.Helper()

	sourceDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(sourceDir, "go.mod"), []byte("module test\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(sourceDir, "cmd", "agent"), 0755); err != nil {
		t.Fatal(err)
	}

	binDir := t.TempDir()
	fakeGo := filepath.Join(binDir, "go")
	script := `#!/bin/sh
set -eu
out=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    shift
    out="$1"
  fi
  shift || true
done
printf '` + output + `' > "$out"
`
	if err := os.WriteFile(fakeGo, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return sourceDir
}

func newAgentBinaryContext(rec *httptest.ResponseRecorder, osName, arch string) echo.Context {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/install/agent/"+osName+"/"+arch, nil)
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("os", "arch")
	ctx.SetParamValues(osName, arch)
	return ctx
}
