package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	agentservice "github.com/beam-cloud/beta9/pkg/agent/service"
	"github.com/beam-cloud/beta9/pkg/types"
)

type installFlags struct {
	*joinFlags
	managerName string
	serviceName string
	stateDir    string
}

func runInstallService(ctx context.Context, args []string) error {
	flags, opts := newInstallFlags()
	if err := flags.Parse(args); err != nil {
		return err
	}

	manager, spec, err := opts.serviceSpec()
	if err != nil {
		return err
	}
	if err := manager.Install(ctx, spec); err != nil {
		return err
	}

	if manager.Name() == types.AgentServiceManagerSystemd {
		fmt.Fprintf(os.Stdout, "=> Installed and started agent service %q with systemd (enabled at boot)\n", spec.Name)
	} else {
		fmt.Fprintf(os.Stdout, "=> Installed agent service %q with %s\n", spec.Name, manager.Name())
	}
	return nil
}

func newInstallFlags() (*flag.FlagSet, *installFlags) {
	flags, join := newJoinFlags("install-service")
	opts := &installFlags{
		joinFlags:   join,
		managerName: types.DefaultAgentServiceManager,
		serviceName: types.DefaultAgentServiceName,
		stateDir:    defaultStateDir(),
	}
	flags.StringVar(&opts.managerName, "manager", opts.managerName, "service manager: auto, systemd, or launchd")
	flags.StringVar(&opts.serviceName, "service-name", opts.serviceName, "service name")
	flags.StringVar(&opts.stateDir, "state-dir", opts.stateDir, "agent state directory")
	return flags, opts
}

func (f *installFlags) serviceSpec() (agentservice.Manager, agentservice.Spec, error) {
	if strings.TrimSpace(f.GatewayURL) == "" || (strings.TrimSpace(f.JoinToken) == "" && strings.TrimSpace(f.JoinTokenFile) == "") {
		return nil, agentservice.Spec{}, fmt.Errorf("gateway and join-token are required")
	}
	if f.JoinToken != "" {
		tokenPath, err := writeJoinToken(f.stateDir, f.JoinToken)
		if err != nil {
			return nil, agentservice.Spec{}, err
		}
		f.JoinToken = ""
		f.JoinTokenFile = tokenPath
	}

	manager, err := agentservice.Select(f.managerName, nil)
	if err != nil {
		return nil, agentservice.Spec{}, err
	}

	return manager, agentservice.Spec{
		Name:        f.serviceName,
		Description: types.DefaultAgentServiceDescription,
		BinaryPath:  executablePath(),
		Args:        f.args(),
		Env:         serviceEnv(f.WorkerImage, f.stateDir, f.CacheDir),
		StateDir:    f.stateDir,
	}, nil
}

func writeJoinToken(stateDir, token string) (string, error) {
	if err := os.MkdirAll(stateDir, 0700); err != nil {
		return "", err
	}
	path := filepath.Join(stateDir, types.AgentJoinTokenFileName)
	return path, os.WriteFile(path, []byte(strings.TrimSpace(token)+"\n"), 0600)
}

func executablePath() string {
	path, err := os.Executable()
	if err != nil {
		return types.DefaultAgentServiceName
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		return resolved
	}
	return path
}

func serviceEnv(workerImage, stateDir, cacheDir string) map[string]string {
	env := map[string]string{types.AgentStateDirEnv: stateDir}
	if workerImage != "" {
		env[types.AgentWorkerImageEnv] = workerImage
	}
	if strings.TrimSpace(cacheDir) != "" {
		env[types.AgentCacheDirEnv] = cacheDir
	}
	for _, key := range serviceEnvKeys {
		if _, exists := env[key]; exists {
			continue
		}
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			env[key] = value
		}
	}
	return env
}

var serviceEnvKeys = []string{
	types.AgentDockerHostsEnv,
	types.AgentRegistryForwardEnv,
	types.AgentTargetHostEnv,
	types.AgentFingerprintEnv,
	types.AgentHostnameEnv,
	types.AgentCacheDirEnv,
	types.AgentStorageModeEnv,
	types.AgentCPUAffinityEnforcedEnv,
}

func defaultStateDir() string {
	if runtime.GOOS == "linux" {
		return types.DefaultAgentStateDir
	}
	if dir, err := os.UserCacheDir(); err == nil {
		return filepath.Join(dir, types.BeamStateDirName, types.AgentStateDirName)
	}
	return types.DefaultAgentStateDir
}
