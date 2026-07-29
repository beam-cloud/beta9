package worker

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
)

const (
	thunderLibraryPath           = "/etc/thunder/libthunder.so"
	thunderNvidiaSMIPath         = "/usr/bin/nvidia-smi"
	thunderNvidiaMLLibraryPath   = "/usr/lib/x86_64-linux-gnu/libnvidia-ml.so.1"
	thunderCudaDriverLibraryPath = "/usr/lib/x86_64-linux-gnu/libcuda.so.1"
)

type thunderSetupState int

const (
	thunderSetupPending thunderSetupState = iota
	thunderSetupReady
	thunderSetupFailed
)

type thunderSetupStatus struct {
	done chan struct{}
	once sync.Once

	mu    sync.RWMutex
	state thunderSetupState
	err   error
}

func newThunderSetupStatus() *thunderSetupStatus {
	return &thunderSetupStatus{
		done:  make(chan struct{}),
		state: thunderSetupPending,
	}
}

func (s *thunderSetupStatus) complete(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if err != nil {
			s.state = thunderSetupFailed
			s.err = err
		} else {
			s.state = thunderSetupReady
		}
		close(s.done)
	})
}

func (s *thunderSetupStatus) wait(ctx context.Context) error {
	select {
	case <-s.done:
		s.mu.RLock()
		defer s.mu.RUnlock()
		if s.state == thunderSetupFailed {
			if s.err != nil {
				return fmt.Errorf("Thunder client setup failed: %w", s.err)
			}
			return fmt.Errorf("Thunder client setup failed")
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("Thunder client setup did not complete: %w", ctx.Err())
	}
}

type thunderSetupTracker struct {
	mu       sync.Mutex
	statuses map[string]*thunderSetupStatus
}

func newThunderSetupTracker() *thunderSetupTracker {
	return &thunderSetupTracker{statuses: map[string]*thunderSetupStatus{}}
}

func (t *thunderSetupTracker) Begin(containerId string) {
	if t == nil || containerId == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.statuses[containerId]; exists {
		return
	}
	t.statuses[containerId] = newThunderSetupStatus()
}

func (t *thunderSetupTracker) Complete(containerId string, err error) {
	if t == nil || containerId == "" {
		return
	}
	t.mu.Lock()
	status, exists := t.statuses[containerId]
	if !exists {
		status = newThunderSetupStatus()
		t.statuses[containerId] = status
	}
	t.mu.Unlock()
	status.complete(err)
}

func (t *thunderSetupTracker) Wait(ctx context.Context, containerId string) error {
	if t == nil || containerId == "" {
		return nil
	}
	t.mu.Lock()
	status := t.statuses[containerId]
	t.mu.Unlock()
	if status == nil {
		return nil
	}
	return status.wait(ctx)
}

func (t *thunderSetupTracker) Delete(containerId string) {
	if t == nil || containerId == "" {
		return
	}
	t.mu.Lock()
	status := t.statuses[containerId]
	delete(t.statuses, containerId)
	t.mu.Unlock()
	if status != nil {
		status.complete(fmt.Errorf("Thunder client setup cancelled"))
	}
}

type ContainerThunderManager struct {
	client       pb.ThunderServiceClient
	installCache *common.SafeMap[string]
}

func NewContainerThunderManager(client pb.ThunderServiceClient) *ContainerThunderManager {
	return &ContainerThunderManager{
		client:       client,
		installCache: common.NewSafeMap[string](),
	}
}

func (c *ContainerThunderManager) AssignGPUDevices(request *types.ContainerRequest) ([]int, error) {
	if request == nil {
		return nil, fmt.Errorf("missing container request")
	}
	if c == nil || c.client == nil {
		return nil, fmt.Errorf("Thunder service client is required for virtualized GPU requests")
	}
	containerID := strings.TrimSpace(request.ContainerId)
	if containerID == "" {
		return nil, fmt.Errorf("container id is required for Thunder client enrollment")
	}

	log.Info().Str("container_id", containerID).Msg("requesting Thunder client enrollment")
	response, err := handleGRPCResponse(c.client.CreateClientEnrollment(context.Background(), &pb.CreateClientEnrollmentRequest{ContainerId: containerID}))
	if err != nil {
		log.Error().Str("container_id", containerID).Err(err).Msg("failed to assign Thunder virtual GPU")
		return nil, err
	}
	installCommand := strings.TrimSpace(response.InstallCommand)
	if installCommand == "" {
		return nil, fmt.Errorf("Thunder client enrollment did not include an install command")
	}
	c.installCache.Set(containerID, installCommand)

	log.Info().Str("container_id", containerID).Msg("assigned Thunder virtual GPU")
	return []int{}, nil
}

func (c *ContainerThunderManager) GetContainerGPUDevices(containerId string) []int {
	return []int{}
}

func (c *ContainerThunderManager) UnassignGPUDevices(containerId string) {
	containerId = strings.TrimSpace(containerId)
	if containerId == "" {
		return
	}
	if c == nil || c.client == nil {
		log.Error().Str("container_id", containerId).Msg("Thunder service client unavailable for virtual GPU unassign")
		return
	}

	log.Info().Str("container_id", containerId).Msg("unassigning Thunder virtual GPU")
	if _, err := handleGRPCResponse(c.client.DeleteClientEnrollment(context.Background(), &pb.DeleteClientEnrollmentRequest{ContainerId: containerId})); err != nil {
		log.Error().Str("container_id", containerId).Err(err).Msg("failed to unregister Thunder virtual GPU client")
	} else {
		log.Info().Str("container_id", containerId).Msg("unassigned Thunder virtual GPU")
	}
	c.installCache.Delete(containerId)
}

func (c *ContainerThunderManager) CDIDevices(assignedDevices []int) []string {
	return []string{}
}

func (c *ContainerThunderManager) InjectEnvVars(env []string) []string {
	return withLDPreload((&ContainerNvidiaManager{}).InjectEnvVars(env), thunderLibraryPath)
}

func (c *ContainerThunderManager) InjectAssignedEnvVars(env []string, assignedDevices []int) []string {
	return env
}

func (c *ContainerThunderManager) InjectMounts(mounts []specs.Mount) []specs.Mount {
	mounts = (&ContainerNvidiaManager{}).InjectMounts(mounts)
	mounts = append(mounts, thunderBindMount(thunderNvidiaSMIPath))
	mounts = append(mounts, thunderBindMount(thunderNvidiaMLLibraryPath))
	mounts = append(mounts, thunderBindMount(thunderCudaDriverLibraryPath))
	return mounts
}

func thunderBindMount(path string) specs.Mount {
	return specs.Mount{
		Type:        "bind",
		Source:      path,
		Destination: path,
		Options: []string{
			"rbind",
			"rprivate",
			"nosuid",
			"nodev",
			"rw",
		},
	}
}

func (s *Worker) installThunderClient(ctx context.Context, request *types.ContainerRequest) error {
	if s == nil || request == nil || !request.GpuVirtualized {
		return nil
	}
	manager, ok := s.containerThunderManager.(*ContainerThunderManager)
	if !ok || manager == nil {
		return fmt.Errorf("thunder manager unavailable")
	}
	cmd, ok := manager.installCache.Get(request.ContainerId)
	if !ok || strings.TrimSpace(cmd) == "" {
		return fmt.Errorf("missing Thunder install command for container %s", request.ContainerId)
	}
	instance, ok := s.containerInstances.Get(request.ContainerId)
	if !ok || instance == nil || instance.Runtime == nil {
		return fmt.Errorf("container runtime unavailable for Thunder install")
	}

	env := append([]string(nil), instance.Spec.Process.Env...)
	if !containsEnvKey(env, "PATH") {
		env = append(env, "PATH="+strings.Join(defaultContainerPath, ":"))
	}
	cwd := "/"
	if instance.Spec != nil && instance.Spec.Process != nil {
		if instance.Spec.Process.Cwd != "" {
			cwd = instance.Spec.Process.Cwd
		}
	}
	installCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	log.Info().Str("container_id", request.ContainerId).Msg("installing Thunder client in sandbox")
	if instance.SandboxProcessManager != nil && instance.SandboxProcessManagerReady {
		if err := runSandboxProcessManagerCommand(installCtx, instance.SandboxProcessManager, []string{"sh", "-c", cmd}, cwd, env, "Thunder client install"); err != nil {
			return fmt.Errorf("failed to install Thunder client: %w", err)
		}
	} else {
		proc := specs.Process{
			Args: []string{"sh", "-c", cmd},
			Cwd:  cwd,
			Env:  env,
		}
		if err := instance.Runtime.Exec(installCtx, request.ContainerId, proc, nil); err != nil {
			return fmt.Errorf("failed to install Thunder client: %w", err)
		}
	}
	log.Info().Str("container_id", request.ContainerId).Msg("installed Thunder client in sandbox")
	return nil
}

func containsEnvKey(env []string, key string) bool {
	prefix := key + "="
	for _, item := range env {
		if strings.HasPrefix(item, prefix) {
			return true
		}
	}
	return false
}
