package vast

import (
	"strings"
	"testing"
)

func TestVastHostUnitEscapesServiceTemplateSpecifier(t *testing.T) {
	unit := vastHostUnit(InstallOptions{
		BinaryPath:       "/usr/local/bin/agent",
		GatewayURL:       "https://gateway.example",
		StateDir:         "/var/lib/beam/agent-vast",
		ListenAddr:       DefaultListenAddr,
		GPUServicePrefix: DefaultGPUServicePrefix,
	}, "/var/lib/beam/agent-vast/sentinel-token")

	execStart := unitLine(unit, "ExecStart=")
	if !strings.Contains(execStart, `"--service-template" "beam-agent-vast-gpu@%%s.service"`) {
		t.Fatalf("ExecStart did not escape service template: %s", execStart)
	}
}

func TestVastGPUUnitExpandsSystemdTemplateInstance(t *testing.T) {
	unit := vastGPUUnit(InstallOptions{
		BinaryPath: "/usr/local/bin/agent",
		GatewayURL: "https://gateway.example",
		StateDir:   "/var/lib/beam/agent-vast",
	}, "/var/lib/beam/agent-vast/join-token")

	execStart := unitLine(unit, "ExecStart=")
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
