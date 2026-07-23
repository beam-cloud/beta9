package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
)

func TestSystemdInstallWritesUnitAndStartsService(t *testing.T) {
	tmp := t.TempDir()
	runner := &fakeRunner{}
	manager := Systemd{
		Runner:  runner,
		UnitDir: tmp,
	}
	spec := Spec{
		Name:       "beam agent!",
		BinaryPath: types.DefaultAgentBinaryPath,
		Args: []string{
			"join",
			"--gateway", "https://gateway.beam.cloud",
			"--join-token", "token with spaces",
		},
		Env: map[string]string{
			types.AgentStateDirEnv:    filepath.Join(tmp, "state"),
			types.AgentWorkerImageEnv: "registry.example.com/worker:latest",
		},
		StateDir: filepath.Join(tmp, "state"),
	}

	if err := manager.Install(context.Background(), spec); err != nil {
		t.Fatal(err)
	}

	unit, err := os.ReadFile(filepath.Join(tmp, types.DefaultAgentServiceName+types.AgentServiceUnitExtension))
	if err != nil {
		t.Fatal(err)
	}
	unitText := string(unit)
	for _, want := range []string{
		`Description=Beam Agent`,
		`WorkingDirectory=` + filepath.Join(tmp, "state"),
		`Environment="BEAM_AGENT_STATE_DIR=` + filepath.Join(tmp, "state") + `"`,
		`Environment="BEAM_WORKER_IMAGE=registry.example.com/worker:latest"`,
		`Environment="HOME=` + filepath.Join(tmp, "state") + `"`,
		`ExecStart="` + types.DefaultAgentBinaryPath + `" "join" "--gateway" "https://gateway.beam.cloud" "--join-token" "token with spaces"`,
		`RequiresMountsFor=` + filepath.Join(tmp, "state"),
		`StartLimitIntervalSec=0`,
		`Restart=always`,
		`RestartSec=30`,
		`Environment="XDG_CONFIG_HOME=` + filepath.Join(tmp, "state", ".config") + `"`,
	} {
		if !strings.Contains(unitText, want) {
			t.Fatalf("unit missing %q:\n%s", want, unitText)
		}
	}
	if strings.Contains(unitText, `WorkingDirectory="`) {
		t.Fatalf("WorkingDirectory must not be quoted:\n%s", unitText)
	}

	if got, want := strings.Join(runner.commands, "\n"), strings.Join([]string{
		types.AgentSystemctlCommand + " daemon-reload",
		types.AgentSystemctlCommand + " enable --now " + types.DefaultAgentServiceName + types.AgentServiceUnitExtension,
	}, "\n"); got != want {
		t.Fatalf("unexpected commands:\ngot:\n%s\nwant:\n%s", got, want)
	}
}

func TestSystemdUnitEscapesSpecifierInWorkingDirectory(t *testing.T) {
	unit := SystemdUnit(Spec{
		Name:       types.DefaultAgentServiceName,
		BinaryPath: types.DefaultAgentBinaryPath,
		StateDir:   "/var/lib/beam/%agent",
	})

	if !strings.Contains(unit, "WorkingDirectory=/var/lib/beam/%%agent\n") {
		t.Fatalf("unit did not escape systemd specifier:\n%s", unit)
	}
}

func TestLaunchdPlistRendersAgentProgram(t *testing.T) {
	spec := Spec{
		Name:       "private pool",
		BinaryPath: types.DefaultAgentBinaryPath,
		Args: []string{
			"join",
			"--gateway", "https://gateway.beam.cloud",
			"--join-token", "token&value",
		},
		Env: map[string]string{
			types.AgentStateDirEnv: "/Users/luke/.beam/agent",
		},
		StateDir: "/Users/luke/.beam/agent",
	}

	plist := LaunchdPlist(spec)
	for _, want := range []string{
		`<string>dev.beam.agent.private-pool</string>`,
		`<string>` + types.DefaultAgentBinaryPath + `</string>`,
		`<string>--join-token</string>`,
		`<string>token&amp;value</string>`,
		`<key>BEAM_AGENT_STATE_DIR</key>`,
		`<true/>`,
	} {
		if !strings.Contains(plist, want) {
			t.Fatalf("plist missing %q:\n%s", want, plist)
		}
	}
}

func TestSelectRejectsUnsupportedManager(t *testing.T) {
	if _, err := Select("bogus", nil); err == nil {
		t.Fatal("expected unsupported manager error")
	}
}

type fakeRunner struct {
	commands []string
}

func (r *fakeRunner) LookPath(name string) (string, error) {
	if name == "" {
		return "", errors.New("empty command")
	}
	return "/usr/bin/" + name, nil
}

func (r *fakeRunner) Run(_ context.Context, name string, args ...string) error {
	r.commands = append(r.commands, strings.Join(append([]string{name}, args...), " "))
	return nil
}
