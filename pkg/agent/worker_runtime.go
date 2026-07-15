package agent

import (
	"context"
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
	"google.golang.org/protobuf/proto"
)

type workerRuntimeManager struct {
	executor    string
	runtime     *workerContainerRuntime
	mu          sync.Mutex
	supervisors map[string]*workerRuntimeSupervisor
	noticeOnce  sync.Once
	statusOut   io.Writer
	statusErr   io.Writer
	telemetry   *agentTelemetry
}

type workerRuntimeSupervisor struct {
	slot    *pb.AgentWorkerSlot
	cancel  context.CancelFunc
	updates chan *pb.AgentWorkerSlot
}

type workerContainerRuntime struct {
	bootstrap bootstrapConfig
	opts      types.AgentJoinOptions
	stdout    io.Writer
	stderr    io.Writer
	statusOut io.Writer
	statusErr io.Writer
	telemetry *agentTelemetry
	imageMu   sync.Mutex
	images    map[string]string
}

func newWorkerRuntimeManager(bootstrap bootstrapConfig, opts types.AgentJoinOptions, stdout, stderr, statusOut, statusErr io.Writer, telemetry *agentTelemetry) *workerRuntimeManager {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	if statusOut == nil {
		statusOut = stdout
	}
	if statusErr == nil {
		statusErr = stderr
	}
	return &workerRuntimeManager{
		executor: bootstrap.Executor,
		runtime: &workerContainerRuntime{
			bootstrap: bootstrap,
			opts:      opts,
			stdout:    stdout,
			stderr:    stderr,
			statusOut: statusOut,
			statusErr: statusErr,
			telemetry: telemetry,
			images:    map[string]string{},
		},
		statusOut:   statusOut,
		statusErr:   statusErr,
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
				fmt.Fprintf(m.statusErr, "agent executor %q does not start worker containers; desired slots are ignored\n", m.executor)
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
		current.slot = cloneAgentWorkerSlot(slot)
		select {
		case <-current.updates:
		default:
		}
		current.updates <- cloneAgentWorkerSlot(slot)
		return
	}

	slotCtx, cancel := context.WithCancel(ctx)
	supervisor := &workerRuntimeSupervisor{
		slot:    cloneAgentWorkerSlot(slot),
		cancel:  cancel,
		updates: make(chan *pb.AgentWorkerSlot, 1),
	}
	m.supervisors[slot.WorkerId] = supervisor
	statusf(m.statusOut, "Preparing worker %q", slot.WorkerId)
	go m.superviseSlot(slotCtx, supervisor)
}

func (m *workerRuntimeManager) superviseSlot(ctx context.Context, supervisor *workerRuntimeSupervisor) {
	desired := cloneAgentWorkerSlot(supervisor.slot)
	backoff := time.Second
	for {
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		current := cloneAgentWorkerSlot(desired)
		go func(slot *pb.AgentWorkerSlot) { done <- m.runtime.run(runCtx, slot) }(current)

		restart := false
		for !restart {
			select {
			case <-ctx.Done():
				stop()
				<-done
				return
			case next := <-supervisor.updates:
				desired = latestWorkerSlotUpdate(next, supervisor.updates)
				for !sameWorkerSlot(current, desired) {
					prepared := cloneAgentWorkerSlot(desired)
					if _, err := m.runtime.pullImage(ctx, strings.TrimSpace(prepared.GetWorkerImage())); err != nil {
						fmt.Fprintf(m.statusErr, "worker slot %s prepare failed: %v\n", desired.WorkerId, err)
						var ok bool
						desired, ok = waitWorkerSlot(ctx, desired, supervisor.updates, backoff)
						if !ok {
							stop()
							<-done
							return
						}
						backoff = nextBackoff(backoff, 30*time.Second)
						continue
					}
					desired = latestWorkerSlotUpdate(desired, supervisor.updates)
					if sameWorkerSlot(prepared, desired) {
						break
					}
				}
				// The replacement is local before the current worker is stopped.
				stop()
				<-done
				backoff = time.Second
				restart = true
			case err := <-done:
				stop()
				if ctx.Err() != nil {
					return
				}
				if err == nil {
					err = fmt.Errorf("worker exited")
				}
				fmt.Fprintf(m.statusErr, "worker slot %s exited: %v\n", current.WorkerId, err)
				var ok bool
				desired, ok = waitWorkerSlot(ctx, desired, supervisor.updates, backoff)
				if !ok {
					return
				}
				backoff = nextBackoff(backoff, 30*time.Second)
				restart = true
			}
		}
	}
}

func waitWorkerSlot(ctx context.Context, desired *pb.AgentWorkerSlot, updates <-chan *pb.AgentWorkerSlot, delay time.Duration) (*pb.AgentWorkerSlot, bool) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return desired, false
	case next := <-updates:
		return latestWorkerSlotUpdate(next, updates), true
	case <-timer.C:
		return desired, true
	}
}

func latestWorkerSlotUpdate(slot *pb.AgentWorkerSlot, updates <-chan *pb.AgentWorkerSlot) *pb.AgentWorkerSlot {
	latest := cloneAgentWorkerSlot(slot)
	for {
		select {
		case next := <-updates:
			latest = cloneAgentWorkerSlot(next)
		default:
			return latest
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
	dirs := agentWorkerDirs(stateDir, r.opts.CacheDir, slot.WorkerId)
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
	// The gateway has already folded any explicit join-time override into the
	// slot. Keeping image selection here slot-only prevents an untouched
	// generated install command from pinning the machine forever.
	image := strings.TrimSpace(slot.WorkerImage)
	if image == "" {
		return fmt.Errorf("worker image is required for slot %s", slot.WorkerId)
	}

	imageID, err := r.pullImage(ctx, image)
	if err != nil {
		return err
	}

	if err := removeOtherManagedWorkerContainers(name, slot); err != nil {
		fmt.Fprintf(r.statusErr, "failed to clean stale worker containers: %v\n", err)
	}
	if err := removeManagedWorkerContainer(name, slot); err != nil {
		return err
	}
	defer func() {
		if err := removeManagedWorkerContainer(name, slot); err != nil {
			fmt.Fprintf(r.statusErr, "failed to remove worker container %s: %v\n", name, err)
		}
	}()

	args := dockerRunArgs(name, image, imageID, configPath, r.bootstrap, slot, dirs)
	cmd := exec.Command("docker", args...)
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
	statusf(r.statusOut, "Starting worker %q", slot.WorkerId)
	if err := cmd.Start(); err != nil {
		return err
	}
	go func() { done <- cmd.Wait() }()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		if err := stopWorkerContainer(name); err != nil {
			fmt.Fprintf(r.statusErr, "failed to gracefully stop worker %s: %v\n", name, err)
			_ = exec.Command("docker", "rm", "-f", name).Run()
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = exec.Command("docker", "rm", "-f", name).Run()
			if cmd.Process != nil {
				_ = cmd.Process.Kill()
			}
		}
		return ctx.Err()
	}
}

func stopWorkerContainer(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", "stop", "--time", "30", name).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker stop: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (r *workerContainerRuntime) pullImage(ctx context.Context, image string) (string, error) {
	key := workerImagePullKey(image)

	r.imageMu.Lock()
	if imageID := r.images[key]; imageID != "" {
		r.imageMu.Unlock()
		return imageID, nil
	}
	r.imageMu.Unlock()

	statusf(r.statusOut, "Pulling worker image %q", image)
	imageID, err := pullDockerImage(ctx, image, r.statusOut)
	if err != nil {
		return "", err
	}
	statusf(r.statusOut, "Worker image ready")

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
	return proto.Clone(slot).(*pb.AgentWorkerSlot)
}

func sameWorkerSlot(a, b *pb.AgentWorkerSlot) bool {
	return proto.Equal(a, b)
}
