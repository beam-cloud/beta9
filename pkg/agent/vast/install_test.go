package vast

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallRejectsEmptySentinelTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "sentinel-token")
	if err := os.WriteFile(tokenFile, []byte("\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, _, err := installSentinelToken(InstallOptions{SentinelTokenFile: tokenFile})
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("err = %v, want empty sentinel token error", err)
	}
}

func TestInstallAcceptsNonEmptySentinelTokenFile(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "sentinel-token")
	if err := os.WriteFile(tokenFile, []byte("secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	token, path, err := installSentinelToken(InstallOptions{SentinelTokenFile: tokenFile})
	if err != nil {
		t.Fatal(err)
	}
	if token != "secret" || path != tokenFile {
		t.Fatalf("token = %q path = %q", token, path)
	}
}

func TestVastHostUnitEscapesServiceTemplateSpecifier(t *testing.T) {
	unit := compatControllerUnit(InstallOptions{
		BinaryPath:       "/usr/local/bin/agent",
		GatewayURL:       "https://gateway.example",
		StateDir:         "/var/lib/beam/agent-vast",
		ListenAddr:       DefaultListenAddr,
		GPUServicePrefix: DefaultGPUServicePrefix,
	}, "/var/lib/beam/agent-vast/sentinel-token")

	execStart := unitLine(unit, "ExecStart=")
	if !strings.Contains(execStart, `"vast" "_controller"`) {
		t.Fatalf("ExecStart did not use internal controller command: %s", execStart)
	}
	if !strings.Contains(execStart, `"--service-template" "beam-agent-vast-compat-gpu@%%s.service"`) {
		t.Fatalf("ExecStart did not escape service template: %s", execStart)
	}
}

func TestVastGPUUnitExpandsSystemdTemplateInstance(t *testing.T) {
	unit := compatGPUAgentUnit(InstallOptions{
		BinaryPath: "/usr/local/bin/agent",
		GatewayURL: "https://gateway.example",
		StateDir:   "/var/lib/beam/agent-vast",
	}, "/var/lib/beam/agent-vast/join-token")

	execStart := unitLine(unit, "ExecStart=")
	if !strings.Contains(execStart, `"vast" "_gpu-agent"`) {
		t.Fatalf("ExecStart did not use internal GPU agent command: %s", execStart)
	}
	if !strings.Contains(execStart, `"--gpu-index" %i`) {
		t.Fatalf("ExecStart did not keep systemd instance specifier expandable: %s", execStart)
	}
	if strings.Contains(execStart, `%%i`) || strings.Contains(execStart, `"%i"`) {
		t.Fatalf("ExecStart escaped or quoted systemd instance specifier: %s", execStart)
	}
}

func unitLine(unit, prefix string) string {
	for _, line := range strings.Split(unit, "\n") {
		if strings.HasPrefix(line, prefix) {
			return line
		}
	}
	return ""
}
