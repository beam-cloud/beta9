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
	stdout      io.Writer
	stderr      io.Writer
	telemetry   *agentTelemetry
}

type workerRuntimeSupervisor struct {
	slot   *pb.AgentWorkerSlot
	cancel context.CancelFunc
}

type workerContainerRuntime struct {
	bootstrap bootstrapConfig
	opts      types.AgentJoinOptions
	stdout    io.Writer
	stderr    io.Writer
	telemetry *agentTelemetry
	imageMu   sync.Mutex
	images    map[string]string
}

func newWorkerRuntimeManager(bootstrap bootstrapConfig, opts types.AgentJoinOptions, stdout, stderr, agentLogs io.Writer, telemetry *agentTelemetry) *workerRuntimeManager {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if agentLogs == nil {
		agentLogs = stderr
	}
	return &workerRuntimeManager{
		executor: bootstrap.Executor,
		runtime: &workerContainerRuntime{
			bootstrap: bootstrap,
			opts:      opts,
			stdout:    stdout,
			stderr:    stderr,
			telemetry: telemetry,
			images:    map[string]string{},
		},
		stdout:      stdout,
		stderr:      agentLogs,
		telemetry:   telemetry,
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
	statusf(m.stdout, "Preparing worker %q", slot.WorkerId)
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

	name := types.DefaultAgentServiceName + "-" + sanitizeDockerName(slot.WorkerId)
	image := firstNonEmpty(r.opts.WorkerImage, os.Getenv(types.AgentWorkerImageEnv), slot.WorkerImage)
	if image == "" {
		return fmt.Errorf("worker image is required for slot %s", slot.WorkerId)
	}

	imageID, err := r.pullImage(ctx, image)
	if err != nil {
		return err
	}

	if err := removeOtherManagedWorkerContainers(name, slot); err != nil {
		fmt.Fprintf(r.stderr, "failed to clean stale worker containers: %v\n", err)
	}
	if err := removeManagedWorkerContainer(name, slot); err != nil {
		return err
	}
	defer func() {
		if err := removeManagedWorkerContainer(name, slot); err != nil {
			fmt.Fprintf(r.stderr, "failed to remove worker container %s: %v\n", name, err)
		}
	}()

	args := dockerRunArgs(name, image, imageID, configPath, r.bootstrap, slot, dirs)
	cmd := exec.CommandContext(ctx, "docker", args...)
	stdoutLogs := r.telemetry.logWriter(types.AgentTelemetrySourceWorker, slot.WorkerId, types.EventLogStreamStdout)
	stderrLogs := r.telemetry.logWriter(types.AgentTelemetrySourceWorker, slot.WorkerId, types.EventLogStreamStderr)
	defer closeRuntimeWriter(stdoutLogs)
	defer closeRuntimeWriter(stderrLogs)
	stdout := newDetailLogWriter(r.stdout)
	stderr := newDetailLogWriter(r.stderr)
	defer closeRuntimeWriter(stdout)
	defer closeRuntimeWriter(stderr)
	cmd.Stdout = io.MultiWriter(
		stdout,
		stdoutLogs,
	)
	cmd.Stderr = io.MultiWriter(
		stderr,
		stderrLogs,
	)

	done := make(chan error, 1)
	statusf(r.stdout, "Starting worker %q", slot.WorkerId)
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

func (r *workerContainerRuntime) pullImage(ctx context.Context, image string) (string, error) {
	key := workerImagePullKey(image)

	r.imageMu.Lock()
	if imageID := r.images[key]; imageID != "" {
		r.imageMu.Unlock()
		return imageID, nil
	}
	r.imageMu.Unlock()

	statusf(r.stdout, "Pulling worker image %q", image)
	imageID, err := pullDockerImage(ctx, image, r.stdout)
	if err != nil {
		return "", err
	}
	statusf(r.stdout, "Worker image ready")

	r.imageMu.Lock()
	r.images[key] = imageID
	r.imageMu.Unlock()
	return imageID, nil
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

func (m *workerRuntimeManager) stats() agentWorkerStats {
	if m == nil {
		return agentWorkerStats{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return agentWorkerStats{WorkerCount: uint32(len(m.supervisors))}
}

func closeRuntimeWriter(w io.Writer) {
	if closer, ok := w.(io.Closer); ok {
		_ = closer.Close()
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
