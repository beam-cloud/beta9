package vast

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/beam-cloud/beta9/pkg/agent"
	"github.com/beam-cloud/beta9/pkg/types"
)

type SystemdController struct{}

func (SystemdController) Start(ctx context.Context, unit string) error {
	return runSystemctl(ctx, "start", unit)
}

func (SystemdController) Stop(ctx context.Context, unit string) error {
	return runSystemctl(ctx, "stop", unit)
}

func runSystemctl(ctx context.Context, args ...string) error {
	out, err := exec.CommandContext(ctx, types.AgentSystemctlCommand, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", types.AgentSystemctlCommand, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

type AgentContainerCleaner struct{}

func (AgentContainerCleaner) RemoveManagedWorkerContainersForMachine(machineID string) error {
	return agent.RemoveManagedWorkerContainersForMachine(machineID)
}

func executablePath() string {
	path, err := os.Executable()
	if err != nil {
		return types.DefaultAgentServiceName
	}
	return path
}
