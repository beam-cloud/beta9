package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

type workerRuntimeManager struct {
	executor    string
	runtime     *workerContainerRuntime
	mu          sync.Mutex
	supervisors map[string]*workerRuntimeSupervisor
	noticeOnce  sync.Once
	stderr      io.Writer
}

type workerRuntimeSupervisor struct {
	slot   *pb.AgentWorkerSlot
	cancel context.CancelFunc
}

type workerContainerRuntime struct {
	bootstrap bootstrapConfig
	opts      JoinOptions
	stdout    io.Writer
	stderr    io.Writer
}

func newWorkerRuntimeManager(bootstrap bootstrapConfig, opts JoinOptions, stdout, stderr io.Writer) *workerRuntimeManager {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	return &workerRuntimeManager{
		executor: bootstrap.Executor,
		runtime: &workerContainerRuntime{
			bootstrap: bootstrap,
			opts:      opts,
			stdout:    stdout,
			stderr:    stderr,
		},
		stderr:      stderr,
		supervisors: map[string]*workerRuntimeSupervisor{},
	}
}

func (m *workerRuntimeManager) reconcile(ctx context.Context, slots []*pb.AgentWorkerSlot) error {
	if m == nil {
		return nil
	}

	if m.executor != types.DefaultAgentWorkerContainerMode {
		if len(slots) > 0 {
			m.noticeOnce.Do(func() {
				fmt.Fprintf(m.stderr, "agent executor %q does not start worker containers; desired slots are ignored\n", m.executor)
			})
		}
		m.stopAll()
		return nil
	}

	if runtime.GOOS != "linux" {
		if len(slots) > 0 {
			return fmt.Errorf("worker-container executor requires Linux; this machine joined as %s/%s", runtime.GOOS, runtime.GOARCH)
		}
		m.stopAll()
		return nil
	}

	seen := map[string]struct{}{}
	for _, slot := range slots {
		if slot == nil || slot.WorkerId == "" {
			continue
		}
		seen[slot.WorkerId] = struct{}{}
		m.ensureSlot(ctx, slot)
	}
	m.stopSlotsNotIn(seen)
	return nil
}

func (m *workerRuntimeManager) ensureSlot(ctx context.Context, slot *pb.AgentWorkerSlot) {
	slot = cloneAgentWorkerSlot(slot)

	m.mu.Lock()
	defer m.mu.Unlock()

	if current, ok := m.supervisors[slot.WorkerId]; ok {
		if sameWorkerSlot(current.slot, slot) {
			return
		}
		current.cancel()
		delete(m.supervisors, slot.WorkerId)
	}

	slotCtx, cancel := context.WithCancel(ctx)
	m.supervisors[slot.WorkerId] = &workerRuntimeSupervisor{slot: cloneAgentWorkerSlot(slot), cancel: cancel}
	go m.superviseSlot(slotCtx, slot)
}

func (m *workerRuntimeManager) superviseSlot(ctx context.Context, slot *pb.AgentWorkerSlot) {
	backoff := time.Second
	for {
		err := m.runtime.run(ctx, slot)
		if ctx.Err() != nil {
			return
		}
		if err == nil {
			err = fmt.Errorf("worker exited")
		}
		fmt.Fprintf(m.stderr, "worker slot %s exited: %v\n", slot.WorkerId, err)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff, 30*time.Second)
	}
}

func (r *workerContainerRuntime) run(ctx context.Context, slot *pb.AgentWorkerSlot) error {
	if !commandExists("docker") {
		return fmt.Errorf("docker is required for worker-container executor")
	}

	stateDir, err := agentStateDir()
	if err != nil {
		return err
	}
	dirs := agentWorkerDirs(stateDir, slot.WorkerId)
	for _, dir := range []string{dirs.Tmp} {
		_ = os.RemoveAll(dir)
	}
	for _, dir := range dirs.All() {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	configPath := filepath.Join(dirs.Slot, "config.json")
	if err := writeWorkerConfig(configPath, r.bootstrap, slot); err != nil {
		return err
	}

	name := "beam-agent-" + sanitizeDockerName(slot.WorkerId)
	image := firstNonEmpty(r.opts.WorkerImage, os.Getenv(types.AgentWorkerImageEnv), slot.WorkerImage)
	if image == "" {
		return fmt.Errorf("worker image is required for slot %s", slot.WorkerId)
	}

	if err := removeManagedWorkerContainer(name, slot); err != nil {
		return err
	}
	defer func() {
		if err := removeManagedWorkerContainer(name, slot); err != nil {
			fmt.Fprintf(r.stderr, "failed to remove worker container %s: %v\n", name, err)
		}
	}()

	args := dockerRunArgs(name, image, configPath, r.bootstrap, slot, dirs)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdout = r.stdout
	cmd.Stderr = r.stderr

	done := make(chan error, 1)
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		_ = exec.Command("docker", "rm", "-f", name).Run()
		<-done
		return ctx.Err()
	}
}

func (m *workerRuntimeManager) stopSlotsNotIn(seen map[string]struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for workerID, supervisor := range m.supervisors {
		if _, ok := seen[workerID]; ok {
			continue
		}
		supervisor.cancel()
		delete(m.supervisors, workerID)
	}
}

func (m *workerRuntimeManager) stopAll() {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for workerID, supervisor := range m.supervisors {
		supervisor.cancel()
		delete(m.supervisors, workerID)
	}
}

func cloneAgentWorkerSlot(slot *pb.AgentWorkerSlot) *pb.AgentWorkerSlot {
	if slot == nil {
		return nil
	}
	clone := *slot
	return &clone
}

func sameWorkerSlot(a, b *pb.AgentWorkerSlot) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.WorkerId == b.WorkerId &&
		a.WorkerToken == b.WorkerToken &&
		a.PoolName == b.PoolName &&
		a.MachineId == b.MachineId &&
		a.Cpu == b.Cpu &&
		a.Memory == b.Memory &&
		a.Gpu == b.Gpu &&
		a.GpuCount == b.GpuCount &&
		a.GpuAssignment == b.GpuAssignment &&
		a.NetworkPrefix == b.NetworkPrefix &&
		a.WorkerImage == b.WorkerImage &&
		a.NetworkSlotPoolSize == b.NetworkSlotPoolSize &&
		a.ContainerStartConcurrency == b.ContainerStartConcurrency
}

func removeManagedWorkerContainer(name string, slot *pb.AgentWorkerSlot) error {
	owned, exists, err := dockerContainerOwnedByAgent(name, slot)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if !owned {
		return fmt.Errorf("docker container %q already exists and is not managed by the Beam agent", name)
	}

	out, err := exec.Command("docker", "rm", "-f", name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("remove worker container %q: %w: %s", name, err, strings.TrimSpace(string(out)))
	}
	return nil
}

func dockerContainerOwnedByAgent(name string, slot *pb.AgentWorkerSlot) (bool, bool, error) {
	out, err := exec.Command("docker", "inspect", "--format", "{{json .}}", name).CombinedOutput()
	if err != nil {
		msg := strings.ToLower(string(out) + err.Error())
		if strings.Contains(msg, "no such object") || strings.Contains(msg, "no such container") {
			return false, false, nil
		}
		return false, false, fmt.Errorf("inspect docker container %q: %w: %s", name, err, strings.TrimSpace(string(out)))
	}

	owned, err := dockerContainerInspectOwnedByAgent(out, slot)
	if err != nil {
		return false, true, fmt.Errorf("inspect docker container %q: %w", name, err)
	}
	return owned, true, nil
}

type dockerContainerInspect struct {
	Config struct {
		Labels map[string]string `json:"Labels"`
		Env    []string          `json:"Env"`
	} `json:"Config"`
}

func dockerContainerInspectOwnedByAgent(data []byte, slot *pb.AgentWorkerSlot) (bool, error) {
	var inspect dockerContainerInspect
	if err := json.Unmarshal(data, &inspect); err != nil {
		return false, err
	}

	if inspect.Config.Labels[types.AgentDockerLabelManaged] == "true" {
		return dockerContainerLabelsMatchSlot(inspect.Config.Labels, slot), nil
	}
	return dockerContainerEnvMatchesSlot(inspect.Config.Env, slot), nil
}

func dockerContainerLabelsMatchSlot(labels map[string]string, slot *pb.AgentWorkerSlot) bool {
	if slot == nil {
		return true
	}
	return labels[types.AgentDockerLabelWorkerID] == slot.WorkerId &&
		labels[types.AgentDockerLabelMachineID] == slot.MachineId &&
		labels[types.AgentDockerLabelPoolName] == slot.PoolName
}

func dockerContainerEnvMatchesSlot(env []string, slot *pb.AgentWorkerSlot) bool {
	if slot == nil {
		return false
	}
	values := map[string]string{}
	for _, item := range env {
		key, value, ok := strings.Cut(item, "=")
		if ok {
			values[key] = value
		}
	}
	return values[types.WorkerIDEnv] == slot.WorkerId &&
		values[types.WorkerMachineEnv] == slot.MachineId &&
		values[types.WorkerPoolEnv] == slot.PoolName
}
