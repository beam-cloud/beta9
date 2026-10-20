package service

import (
	"context"
	"errors"
	"os"
	"os/exec"
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
		`ExecStartPre="/bin/sh" "-ec" "for governor in /sys/devices/system/cpu/cpufreq/policy*/scaling_governor; do\n  [ -e \"$$governor\" ] || continue\n  echo performance > \"$$governor\"\ndone"`,
		`ExecStart="` + types.DefaultAgentBinaryPath + `" "join" "--gateway" "https://gateway.beam.cloud" "--join-token" "token with spaces"`,
		`ExecStopPost="/bin/sh" "-c" "if [ -x /usr/local/bin/uninstall-thunder.sh ]; then sudo THUNDER_UNINSTALL_NOWARN=1 /usr/local/bin/uninstall-thunder.sh; fi"`,
		`RequiresMountsFor=` + filepath.Join(tmp, "state"),
		`StartLimitIntervalSec=0`,
		`Restart=always`,
		`RestartSec=30`,
		`SuccessExitStatus=130`,
		`TimeoutStopSec=900`,
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

func TestCPUPerformancePolicy(t *testing.T) {
	root := filepath.Join(t.TempDir(), "cpufreq")
	script := strings.ReplaceAll(cpuPerformanceScript, "/sys/devices/system/cpu/cpufreq", root)
	run := func() {
		t.Helper()
		if out, err := exec.Command("/bin/sh", "-ec", script).CombinedOutput(); err != nil {
			t.Fatalf("apply CPU policy: %v: %s", err, out)
		}
	}
	// Machines without cpufreq must still start.
	run()
	for _, name := range []string{"policy0", "policy1"} {
		path := filepath.Join(root, name, "scaling_governor")
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("schedutil\n"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		run()
		for _, name := range []string{"policy0", "policy1"} {
			data, err := os.ReadFile(filepath.Join(root, name, "scaling_governor"))
			if err != nil || string(data) != "performance\n" {
				t.Fatalf("%s governor = %q, error = %v", name, data, err)
			}
		}
	}
	// A policy that cannot be applied must fail the pre-start hook.
	if err := os.MkdirAll(filepath.Join(root, "policy2", "scaling_governor"), 0755); err != nil {
		t.Fatal(err)
	}
	if err := exec.Command("/bin/sh", "-ec", script).Run(); err == nil {
		t.Fatal("unwritable policy was ignored")
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

func TestSystemdUnitRequiresManagedIngress(t *testing.T) {
	spec := Spec{BinaryPath: types.DefaultAgentBinaryPath}
	if unit := SystemdUnit(spec); strings.Contains(unit, types.AgentSystemdIngressService) {
		t.Fatalf("unit should not depend on managed ingress by default:\n%s", unit)
	}

	spec.Env = map[string]string{types.AgentManagedIngressEnv: "true"}
	unit := SystemdUnit(spec)
	for _, want := range []string{
		"Requires=" + types.AgentSystemdIngressService + "\n",
		"After=" + types.AgentSystemdIngressService + "\n",
	} {
		if !strings.Contains(unit, want) {
			t.Fatalf("unit missing %q:\n%s", want, unit)
		}
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
