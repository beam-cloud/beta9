package agent

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
	Preflight   []check         `json:"preflight,omitempty"`
	Schedulable bool            `json:"schedulable,omitempty"`
}

type bootstrapConfig struct {
	GatewayHTTPURL         string                  `json:"gatewayHttpUrl"`
	GatewayGRPCHost        string                  `json:"gatewayGrpcHost"`
	GatewayGRPCPort        int                     `json:"gatewayGrpcPort"`
	GatewayGRPCTLS         bool                    `json:"gatewayGrpcTls"`
	WorkspaceID            string                  `json:"workspaceId"`
	PoolName               string                  `json:"poolName"`
	Transport              string                  `json:"transport"`
	Executor               string                  `json:"executor"`
	Fallback               string                  `json:"fallback"`
	ImageRegistryStore     string                  `json:"imageRegistryStore"`
	ImageClipVersion       uint32                  `json:"imageClipVersion"`
	ImageLocalCacheEnabled bool                    `json:"imageLocalCacheEnabled"`
	Telemetry              telemetryConfig         `json:"telemetry"`
	Billing                *billingBootstrapConfig `json:"billing,omitempty"`
}

// billingBootstrapConfig is only present for marketplace pools; it lets the
// worker meter buyer usage and report it to the billing service.
type billingBootstrapConfig struct {
	UsageEndpoint     string  `json:"usageEndpoint"`
	UsageToken        string  `json:"usageToken"`
	CostHookEndpoint  string  `json:"costHookEndpoint"`
	CostHookToken     string  `json:"costHookToken"`
	BillableMarginPct float64 `json:"billableMarginPct"`
}

type telemetryConfig struct {
	Enabled      bool                `json:"enabled"`
	StreamPrefix string              `json:"streamPrefix"`
	Logs         telemetrySinkConfig `json:"logs"`
	Events       telemetrySinkConfig `json:"events"`
}

type telemetrySinkConfig struct {
	Destination  string `json:"destination"`
	Credential   string `json:"credential"`
	StreamPrefix string `json:"streamPrefix"`
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
