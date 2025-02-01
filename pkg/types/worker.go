package types

import (
	"fmt"
	"runtime"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
)

const (
	WorkerLifecycleStatsKey        string        = "beta9.worker.usage.spawner.lifecycle"
	WorkerDurationStatsKey         string        = "beta9.worker.usage.spawner.duration"
	WorkerUserCodeVolume           string        = "/mnt/code"
	WorkerUserOutputVolume         string        = "/data/outputs"
	WorkerDurationEmissionInterval time.Duration = 30 * time.Second
	WorkerKeepAliveInterval        time.Duration = 15 * time.Second
)

const (
	BuildContainerPrefix string = "build-"
)

type ContainerResourceUsage struct {
	ContainerID       string `json:"ContainerID"`
	CpuMillicoresUsed int64  `json:"CpuMillicoresUsed"`
	MemoryUsed        int    `json:"MemoryUsed"`
	GpuMemoryUsed     int64  `json:"GpuMemoryUsed"`
	GpuMemoryTotal    int64  `json:"GpuMemoryTotal"`
	GpuType           string `json:"GpuType"`
}

// @go2proto
type Mount struct {
	LocalPath        string            `json:"local_path"`
	MountPath        string            `json:"mount_path"`
	LinkPath         string            `json:"link_path"`
	ReadOnly         bool              `json:"read_only"`
	MountType        string            `json:"mount_type"`
	MountPointConfig *MountPointConfig `json:"mountpoint_config"`
}

func (m *Mount) ToProto() *pb.Mount {
	var mountPointConfig *pb.MountPointConfig
	if m.MountPointConfig != nil {
		mountPointConfig = m.MountPointConfig.ToProto()
	}

	return &pb.Mount{
		LocalPath:        m.LocalPath,
		MountPath:        m.MountPath,
		LinkPath:         m.LinkPath,
		ReadOnly:         m.ReadOnly,
		MountType:        m.MountType,
		MountPointConfig: mountPointConfig,
	}
}

func NewMountFromProto(in *pb.Mount) *Mount {
	var mountPointConfig *MountPointConfig
	if in.MountPointConfig != nil {
		mountPointConfig = NewMountPointConfigFromProto(in.MountPointConfig)
	}

	return &Mount{
		LocalPath:        in.LocalPath,
		MountPath:        in.MountPath,
		LinkPath:         in.LinkPath,
		ReadOnly:         in.ReadOnly,
		MountType:        in.MountType,
		MountPointConfig: mountPointConfig,
	}
}

type ExitCodeError struct {
	ExitCode int
}

func (e *ExitCodeError) Error() string {
	return fmt.Sprintf("exit code error: %s", WorkerContainerExitCodes[e.ExitCode])
}

const (
	WorkerContainerExitCodeInvalidCustomImage = 555
	WorkerContainerExitCodeIncorrectImageArch = 556
	WorkerContainerExitCodeIncorrectImageOs   = 557
	WorkerContainerExitCodeUnknownError       = 1
	WorkerContainerExitCodeSuccess            = 0
)

var WorkerContainerExitCodes = map[int]string{
	WorkerContainerExitCodeSuccess:            "Success",
	WorkerContainerExitCodeUnknownError:       "UnknownError: An unknown error occurred.",
	WorkerContainerExitCodeIncorrectImageArch: "InvalidArch: Image must be built for the " + runtime.GOARCH + " architecture.",
	WorkerContainerExitCodeInvalidCustomImage: "InvalidCustomImage: Custom image not found. Check your image reference and registry credentials.",
	WorkerContainerExitCodeIncorrectImageOs:   "InvalidOS: Image must be built for Linux.",
}

const (
	// Used specifically for runc states.
	// Not the same as the scheduler container states.
	RuncContainerStatusCreated string = "created"
	RuncContainerStatusRunning string = "running"
	RuncContainerStatusPaused  string = "paused"
	RuncContainerStatusStopped string = "stopped"
)
