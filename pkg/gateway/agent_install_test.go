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
	} {
		if !strings.Contains(agentInstallScript, want) {
			t.Fatalf("install script missing %q", want)
		}
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
	sourceDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(sourceDir, "go.mod"), []byte("module test\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(sourceDir, "cmd", "agent"), 0755); err != nil {
		t.Fatal(err)
	}

	binDir := t.TempDir()
	fakeGo := filepath.Join(binDir, "go")
	if err := os.WriteFile(fakeGo, []byte(`#!/bin/sh
set -eu
out=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    shift
    out="$1"
  fi
  shift || true
done
printf 'built-agent' > "$out"
`), 0755); err != nil {
		t.Fatal(err)
	}

	t.Setenv(types.AgentSourceDirEnv, sourceDir)
	t.Setenv(types.AgentBuildCacheDirEnv, t.TempDir())
	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))

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

func newAgentBinaryContext(rec *httptest.ResponseRecorder, osName, arch string) echo.Context {
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/install/agent/"+osName+"/"+arch, nil)
	ctx := e.NewContext(req, rec)
	ctx.SetParamNames("os", "arch")
	ctx.SetParamValues(osName, arch)
	return ctx
}
