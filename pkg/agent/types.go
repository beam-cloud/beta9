package agent

import (
	"io"

	"github.com/beam-cloud/beta9/pkg/compute/httpjson"
)

const (
	agentDockerHostAliasesEnv    = "BEAM_AGENT_DOCKER_HOST_ALIASES"
	agentLocalRegistryForwardEnv = "BEAM_AGENT_LOCAL_REGISTRY_FORWARD"

	agentContainerImagesPath           = "/images"
	agentContainerTmpPath              = "/tmp"
	agentContainerDataPath             = "/data"
	agentContainerWorkspaceStoragePath = "/workspace/data"
	agentContainerCachePath            = "/var/lib/beta9/cache"
	agentContainerCheckpointPath       = "/checkpoints"
	agentContainerLogsPath             = "/var/log/worker"
	agentContainerConfigPath           = "/etc/beam/agent-worker.json"
	agentContainerCacheFSMountPath     = "/cache"
)

type JoinOptions struct {
	GatewayURL                string
	JoinToken                 string
	DevMode                   bool
	ExecutorOverride          string
	TransportOverride         string
	WorkerImage               string
	MaxCPU                    string
	MaxMemory                 string
	MaxGPUs                   uint
	GPUIDs                    string
	NetworkSlots              uint
	ContainerStartConcurrency uint
	Stdout                    io.Writer
	Stderr                    io.Writer
}

type Client struct {
	http httpjson.Client
}

type joinRequest struct {
	JoinToken                 string   `json:"joinToken"`
	MachineFingerprint        string   `json:"machineFingerprint"`
	Hostname                  string   `json:"hostname"`
	OS                        string   `json:"os"`
	Arch                      string   `json:"arch"`
	CPUCount                  uint32   `json:"cpuCount"`
	CPUMillicores             int64    `json:"cpuMillicores"`
	MemoryMB                  uint64   `json:"memoryMb"`
	GPU                       []string `json:"gpu"`
	GPUIDs                    []string `json:"gpuIds"`
	GPUCount                  uint32   `json:"gpuCount"`
	Preflight                 []check  `json:"preflight"`
	Schedulable               bool     `json:"schedulable"`
	Executor                  string   `json:"executor"`
	NetworkSlotPoolSize       uint32   `json:"networkSlotPoolSize"`
	ContainerStartConcurrency uint32   `json:"containerStartConcurrency"`
}

type joinResponse struct {
	Ok          bool            `json:"ok"`
	ErrMsg      string          `json:"errMsg"`
	WorkspaceID string          `json:"workspaceId"`
	PoolName    string          `json:"poolName"`
	MachineID   string          `json:"machineId"`
	AgentToken  string          `json:"agentToken"`
	Bootstrap   bootstrapConfig `json:"bootstrap"`
}

type bootstrapConfig struct {
	GatewayHTTPURL  string `json:"gatewayHttpUrl"`
	GatewayGRPCHost string `json:"gatewayGrpcHost"`
	GatewayGRPCPort int    `json:"gatewayGrpcPort"`
	GatewayGRPCTLS  bool   `json:"gatewayGrpcTls"`
	WorkspaceID     string `json:"workspaceId"`
	PoolName        string `json:"poolName"`
	Transport       string `json:"transport"`
	Executor        string `json:"executor"`
	Fallback        string `json:"fallback"`
}

type transportCredentialResponse struct {
	Ok         bool   `json:"ok"`
	ErrMsg     string `json:"errMsg"`
	AuthKey    string `json:"authKey"`
	ControlURL string `json:"controlUrl"`
	Hostname   string `json:"hostname"`
	Ephemeral  bool   `json:"ephemeral"`
}

type check struct {
	Name     string `json:"name"`
	Ok       bool   `json:"ok"`
	Message  string `json:"message"`
	Severity string `json:"severity"`
}
