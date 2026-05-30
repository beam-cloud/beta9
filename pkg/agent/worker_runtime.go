package agent

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
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
	m.supervisors[slot.WorkerId] = &workerRuntimeSupervisor{slot: slot, cancel: cancel}
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
		if backoff < 30*time.Second {
			backoff *= 2
		}
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
	for _, dir := range []string{dirs["tmp"]} {
		_ = os.RemoveAll(dir)
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}

	configPath := filepath.Join(dirs["slot"], "config.json")
	if err := writeWorkerConfig(configPath, r.bootstrap, slot, dirs); err != nil {
		return err
	}

	name := "beam-agent-" + sanitizeDockerName(slot.WorkerId)
	image := firstNonEmpty(r.opts.WorkerImage, os.Getenv("BEAM_WORKER_IMAGE"), slot.WorkerImage)
	if image == "" {
		return fmt.Errorf("worker image is required for slot %s", slot.WorkerId)
	}

	_ = exec.Command("docker", "rm", "-f", name).Run()
	defer func() { _ = exec.Command("docker", "rm", "-f", name).Run() }()

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

func agentWorkerDirs(stateDir, workerID string) map[string]string {
	slotName := sanitizeDockerName(workerID)
	return map[string]string{
		"slot":        filepath.Join(stateDir, "slots", slotName),
		"images":      filepath.Join(stateDir, "images"),
		"tmp":         filepath.Join(stateDir, "tmp", slotName),
		"data":        filepath.Join(stateDir, "data"),
		"workspace":   filepath.Join(stateDir, "workspace-data"),
		"cache":       filepath.Join(stateDir, "cache"),
		"checkpoints": filepath.Join(stateDir, "checkpoints"),
		"logs":        filepath.Join(stateDir, "logs", slotName),
	}
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
