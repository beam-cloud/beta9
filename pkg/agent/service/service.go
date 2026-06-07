package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

var invalidServiceNameChars = regexp.MustCompile(`[^a-zA-Z0-9_.@-]+`)

type Spec struct {
	Name        string
	Description string
	BinaryPath  string
	Args        []string
	Env         map[string]string
	StateDir    string
}

type Manager interface {
	Name() string
	Available() bool
	Install(context.Context, Spec) error
}

type Runner interface {
	LookPath(string) (string, error)
	Run(context.Context, string, ...string) error
}

type osRunner struct{}

func (osRunner) LookPath(name string) (string, error) {
	return exec.LookPath(name)
}

func (osRunner) Run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func Select(managerName string, runner Runner) (Manager, error) {
	if runner == nil {
		runner = osRunner{}
	}
	name := normalizeManager(managerName)

	switch name {
	case types.DefaultAgentServiceManager:
		for _, manager := range managers(runner) {
			if manager.Available() {
				return manager, nil
			}
		}
		return nil, errors.New("no supported service manager found; rerun with --foreground")
	case types.AgentServiceManagerSystemd:
		return Systemd{Runner: runner}, nil
	case types.AgentServiceManagerLaunchd:
		return Launchd{Runner: runner}, nil
	case types.AgentServiceManagerNone:
		return nil, errors.New("service installation disabled")
	default:
		return nil, fmt.Errorf("unsupported service manager %q", managerName)
	}
}

func managers(runner Runner) []Manager {
	return []Manager{
		Systemd{Runner: runner},
		Launchd{Runner: runner},
	}
}

func normalizeManager(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return types.DefaultAgentServiceManager
	}
	return strings.ReplaceAll(name, "_", "-")
}

func (s Spec) normalized() (Spec, error) {
	s.Name = serviceName(s.Name)
	if strings.TrimSpace(s.BinaryPath) == "" {
		return Spec{}, errors.New("agent binary path is required")
	}
	if strings.TrimSpace(s.StateDir) == "" {
		s.StateDir = types.DefaultAgentStateDir
	}
	if strings.TrimSpace(s.Description) == "" {
		s.Description = types.DefaultAgentServiceDescription
	}

	cleanEnv := map[string]string{
		types.AgentPathEnv: types.AgentServiceDefaultPath,
	}
	for key, value := range s.Env {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		cleanEnv[key] = value
	}
	s.Env = cleanEnv
	s.BinaryPath = strings.TrimSpace(s.BinaryPath)
	s.StateDir = filepath.Clean(s.StateDir)
	return s, nil
}

func serviceName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = types.DefaultAgentServiceName
	}
	name = invalidServiceNameChars.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-_.@")
	if name == "" {
		return types.DefaultAgentServiceName
	}
	return name
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
