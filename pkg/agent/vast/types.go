package vast

import (
	"context"
	"io"
	"net/http"
	"time"
)

const (
	CommandInstall          = "install"
	CommandSentinel         = "sentinel"
	CommandController       = "_controller"
	CommandGPUAgent         = "_gpu-agent"
	LegacyCommandHost       = "host"
	LegacyCommandAgent      = "gpu-agent"
	PreemptReason           = "vast_preempt"
	LeaseLostReason         = "vast_sentinel_lost"
	DefaultStateDir         = "/var/lib/beam/agent-vast"
	DefaultListenAddr       = "0.0.0.0:48888"
	DefaultLeaseTTL         = 15 * time.Second
	DefaultHeartbeat        = 5 * time.Second
	DefaultPreemptTimeout   = 60 * time.Second
	DefaultReconcilePeriod  = 2 * time.Second
	DefaultHostServiceName  = "beam-agent-vast-compat"
	DefaultGPUServicePrefix = "beam-agent-vast-compat-gpu"
	DefaultGPUServiceName   = DefaultGPUServicePrefix + "@%s.service"
	// DefaultSentinelImage is pinned to a versioned tag so hosts do not
	// silently pick up new sentinel builds. Operators can override it with
	// the `vast install --sentinel-image` flag.
	DefaultSentinelImage = "beam/vast-sentinel:v0.1.0"
)

type GPU struct {
	Index string
	UUID  string
	Name  string
}

type ServiceController interface {
	Start(context.Context, string) error
	Stop(context.Context, string) error
}

type ContainerCleaner interface {
	RemoveManagedWorkerContainersForMachine(machineID string) error
}

type ControllerOptions struct {
	GatewayURL        string
	StateDir          string
	ListenAddr        string
	SentinelToken     string
	SentinelTokenFile string
	LeaseTTL          time.Duration
	ReconcilePeriod   time.Duration
	ServiceTemplate   string
	HTTPClient        *http.Client
	Services          ServiceController
	Cleaner           ContainerCleaner
	DetectGPUs        func(context.Context) ([]GPU, error)
	Stdout            io.Writer
	Stderr            io.Writer
}

type HostOptions = ControllerOptions

type SentinelOptions struct {
	HostURL        string
	Token          string
	TokenFile      string
	GPUUUID        string
	Heartbeat      time.Duration
	PreemptTimeout time.Duration
	HTTPClient     *http.Client
	DetectGPUUUID  func(context.Context) (string, error)
	Stdout         io.Writer
	Stderr         io.Writer
}

type GPUAgentOptions struct {
	GatewayURL                string
	JoinToken                 string
	JoinTokenFile             string
	StateDir                  string
	GPUIndex                  string
	MaxCPU                    string
	MaxMemory                 string
	WorkerImage               string
	NetworkSlots              uint
	ContainerStartConcurrency uint
	DetectGPUs                func(context.Context) ([]GPU, error)
	Stdout                    io.Writer
	Stderr                    io.Writer
}

type InstallOptions struct {
	GatewayURL                string
	JoinToken                 string
	JoinTokenFile             string
	StateDir                  string
	ListenAddr                string
	SentinelToken             string
	SentinelTokenFile         string
	HostServiceName           string
	GPUServicePrefix          string
	UnitDir                   string
	BinaryPath                string
	MaxCPU                    string
	MaxMemory                 string
	WorkerImage               string
	NetworkSlots              uint
	ContainerStartConcurrency uint
	SentinelImage             string
	VastMachineID             string
	PublicHostURL             string
	DryRun                    bool
	Stdout                    io.Writer
	Stderr                    io.Writer
}
