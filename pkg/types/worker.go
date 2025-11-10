package types

import (
	"fmt"
	"runtime"
	"slices"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
)

const (
	WorkerLifecycleStatsKey                  string        = "beta9.worker.usage.spawner.lifecycle"
	WorkerDurationStatsKey                   string        = "beta9.worker.usage.spawner.duration"
	WorkerUserCodeVolume                     string        = "/mnt/code"
	WorkerUserOutputVolume                   string        = "/outputs"
	WorkerContainerVolumePath                string        = "/volumes"
	WorkerContainerUploadsHostPath           string        = "/tmp/container-uploads"
	WorkerContainerUploadsMountPath          string        = "/tmp/.beta9"
	WorkerDurationEmissionInterval           time.Duration = 30 * time.Second
	WorkerKeepAliveInterval                  time.Duration = 15 * time.Second
	WorkerShellPort                          int32         = 2222
	WorkerSandboxProcessManagerPort          int32         = 7111
	WorkerSandboxProcessManagerWorkerPath    string        = "/usr/local/bin/goproc"
	WorkerSandboxProcessManagerContainerPath string        = "/usr/bin/goproc"
)

const (
	BuildContainerPrefix string = "build-"
)

func TempContainerWorkspace(containerId string) string {
	return fmt.Sprintf("/tmp/%s/workspace", containerId)
}

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
	ExitCode ContainerExitCode
}

func (e *ExitCodeError) Error() string {
	return fmt.Sprintf("exit code error: %s", WorkerContainerExitCodes[e.ExitCode])
}

type ContainerExitCode int

func (c ContainerExitCode) IsFailed() bool {
	return !slices.Contains(
		[]ContainerExitCode{
			ContainerExitCodeSuccess,
			ContainerExitCodeOomKill,
			ContainerExitCodeScheduler,
			ContainerExitCodeTtl,
			ContainerExitCodeUser,
			ContainerExitCodeAdmin,
		},
		c,
	)
}

const (
	ContainerExitCodeInvalidCustomImage ContainerExitCode = 555
	ContainerExitCodeIncorrectImageArch ContainerExitCode = 556
	ContainerExitCodeIncorrectImageOs   ContainerExitCode = 557
	ContainerExitCodeUnknownError       ContainerExitCode = 1
	ContainerExitCodeSuccess            ContainerExitCode = 0
	ContainerExitCodeOomKill            ContainerExitCode = 137 // 128 + 9 (base value + SIGKILL), used to indicate OOM kill
	ContainerExitCodeSigterm            ContainerExitCode = 143 // 128 + 15 (base value + SIGTERM), used to indicate a graceful termination
	ContainerExitCodeScheduler          ContainerExitCode = 558
	ContainerExitCodeTtl                ContainerExitCode = 559
	ContainerExitCodeUser               ContainerExitCode = 560
	ContainerExitCodeAdmin              ContainerExitCode = 561
)

const (
	WorkerContainerExitCodeOomKillMessage   = "Container killed due to an out-of-memory error"
	WorkerContainerExitCodeSchedulerMessage = "Container stopped"
	WorkerContainerExitCodeTtlMessage       = "Container stopped due to TTL expiration"
	WorkerContainerExitCodeUserMessage      = "Container stopped by user"
	WorkerContainerExitCodeAdminMessage     = "Container stopped by admin"
)

var ExitCodeMessages = map[ContainerExitCode]string{
	ContainerExitCodeOomKill:   WorkerContainerExitCodeOomKillMessage,
	ContainerExitCodeScheduler: WorkerContainerExitCodeSchedulerMessage,
	ContainerExitCodeTtl:       WorkerContainerExitCodeTtlMessage,
	ContainerExitCodeUser:      WorkerContainerExitCodeUserMessage,
	ContainerExitCodeAdmin:     WorkerContainerExitCodeAdminMessage,
}

var WorkerContainerExitCodes = map[ContainerExitCode]string{
	ContainerExitCodeSuccess:            "Success",
	ContainerExitCodeUnknownError:       "UnknownError: An unknown error occurred.",
	ContainerExitCodeIncorrectImageArch: "InvalidArch: Image must be built for the " + runtime.GOARCH + " architecture.",
	ContainerExitCodeInvalidCustomImage: "InvalidCustomImage: Custom image not found. Check your image reference and registry credentials.",
	ContainerExitCodeIncorrectImageOs:   "InvalidOS: Image must be built for Linux.",
}

const (
	// Used specifically for runc states.
	// Not the same as the scheduler container states.
	RuncContainerStatusCreated string = "created"
	RuncContainerStatusRunning string = "running"
	RuncContainerStatusPaused  string = "paused"
	RuncContainerStatusStopped string = "stopped"
)

// @go2proto
type ProcessInfo struct {
	Running  bool     `json:"running"`
	PID      int32    `json:"pid"`
	Cmd      string   `json:"cmd"`
	Cwd      string   `json:"cwd"`
	Env      []string `json:"env"`
	ExitCode int32    `json:"exit_code"`
}
