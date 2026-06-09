package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

type Systemd struct {
	Runner  Runner
	UnitDir string
}

func (m Systemd) Name() string {
	return types.AgentServiceManagerSystemd
}

func (m Systemd) Available() bool {
	if runtime.GOOS != "linux" {
		return false
	}
	if _, err := os.Stat(types.AgentSystemdRuntimeDir); err != nil {
		return false
	}
	_, err := m.runner().LookPath(types.AgentSystemctlCommand)
	return err == nil
}

func (m Systemd) Install(ctx context.Context, spec Spec) error {
	spec, err := spec.normalized()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(spec.StateDir, 0755); err != nil {
		return err
	}

	unitDir := firstNonEmpty(m.UnitDir, types.AgentSystemdUnitDir)
	if err := os.MkdirAll(unitDir, 0755); err != nil {
		return err
	}

	unitPath := filepath.Join(unitDir, spec.Name+types.AgentServiceUnitExtension)
	if err := os.WriteFile(unitPath, []byte(SystemdUnit(spec)), 0644); err != nil {
		return err
	}

	runner := m.runner()
	for _, args := range [][]string{
		{"daemon-reload"},
		{"enable", spec.Name + types.AgentServiceUnitExtension},
		{"restart", spec.Name + types.AgentServiceUnitExtension},
	} {
		if err := runner.Run(ctx, types.AgentSystemctlCommand, args...); err != nil {
			return fmt.Errorf("%s %s: %w", types.AgentSystemctlCommand, strings.Join(args, " "), err)
		}
	}
	return nil
}

func (m Systemd) runner() Runner {
	if m.Runner != nil {
		return m.Runner
	}
	return osRunner{}
}

func SystemdUnit(spec Spec) string {
	spec, _ = spec.normalized()
	spec.Env = systemdEnv(spec.Env, spec.StateDir)

	var b strings.Builder
	writeLines(&b,
		"[Unit]",
		"Description="+spec.Description,
		"Wants="+types.AgentSystemdNetworkOnlineTarget+" "+types.AgentSystemdDockerService,
		"After="+types.AgentSystemdNetworkOnlineTarget+" "+types.AgentSystemdDockerService,
		"",
		"[Service]",
		"Type=simple",
		"WorkingDirectory="+systemdPath(spec.StateDir),
	)
	for _, key := range sortedEnvKeys(spec.Env) {
		b.WriteString("Environment=" + systemdQuote(key+"="+spec.Env[key]) + "\n")
	}
	writeLines(&b,
		"ExecStart="+systemdCommand(append([]string{spec.BinaryPath}, spec.Args...)),
		"Restart=always",
		"RestartSec=5",
		"KillSignal=SIGINT",
		"TimeoutStopSec=30",
		"LimitNOFILE=1048576",
		"",
		"[Install]",
		"WantedBy=multi-user.target",
	)
	return b.String()
}

func writeLines(b *strings.Builder, lines ...string) {
	for _, line := range lines {
		b.WriteString(line + "\n")
	}
}

func sortedEnvKeys(env map[string]string) []string {
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func systemdEnv(env map[string]string, stateDir string) map[string]string {
	out := make(map[string]string, len(env)+2)
	for key, value := range env {
		out[key] = value
	}
	if _, ok := out[types.AgentHomeEnv]; !ok {
		out[types.AgentHomeEnv] = stateDir
	}
	if _, ok := out[types.AgentXDGConfigHomeEnv]; !ok {
		out[types.AgentXDGConfigHomeEnv] = filepath.Join(stateDir, ".config")
	}
	return out
}

func systemdCommand(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, systemdQuote(arg))
	}
	return strings.Join(quoted, " ")
}

func systemdQuote(value string) string {
	replacer := strings.NewReplacer(
		`\`, `\\`,
		`"`, `\"`,
		"\n", `\n`,
		"%", "%%",
	)
	return `"` + replacer.Replace(value) + `"`
}

func systemdPath(value string) string {
	return strings.ReplaceAll(value, "%", "%%")
}
