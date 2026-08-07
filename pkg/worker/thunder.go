package worker

import (
	"context"
	"fmt"
	"strings"
	"time"

	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/runtime"
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

func (s *Worker) thunderStartupHook(request *types.ContainerRequest, spec *specs.Spec) (runtime.StartupHook, error) {
	if s == nil || request == nil || !s.gpuVirtualizedForRequest(request) {
		return nil, nil
	}
	manager, ok := s.containerThunderManager.(*ContainerThunderManager)
	if !ok || manager == nil {
		return nil, fmt.Errorf("thunder manager unavailable")
	}
	cmd, ok := manager.installCache.Get(request.ContainerId)
	if !ok || strings.TrimSpace(cmd) == "" {
		return nil, fmt.Errorf("missing Thunder install command for container %s", request.ContainerId)
	}
	if spec == nil || spec.Process == nil {
		return nil, fmt.Errorf("container spec unavailable for Thunder install")
	}

	env := append([]string(nil), spec.Process.Env...)
	if !containsEnvKey(env, "PATH") {
		env = append(env, "PATH="+strings.Join(defaultContainerPath, ":"))
	}
	cwd := "/"
	if spec.Process.Cwd != "" {
		cwd = spec.Process.Cwd
	}

	return runtime.StartupExecHook{
		HookName: "thunder_client_install",
		Process: specs.Process{
			Args: []string{"sh", "-c", cmd},
			Cwd:  cwd,
			Env:  env,
		},
		Timeout: 2 * time.Minute,
	}, nil
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
