package types

import (
	"maps"
	"slices"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
)

// VM lifecycle states are persisted and exposed by the management API.
const (
	VMStateRunning  = "running"
	VMStateStarting = "starting"
	VMStateStopped  = "stopped"
	VMStateStopping = "stopping"
	VMStatePaused   = "paused"
	VMStatePausing  = "pausing"
	VMStateError    = "error"
	VMStateDeleted  = "deleted"
)

// Writable VM root capacity in bytes, layered over the separate base image.
const (
	DefaultVMRootSizeBytes int64 = 50 << 30
	MaxVMRootSizeBytes     int64 = 100 << 30
)

// VMAppName owns every VM stub in a workspace. App listings skip it because
// VMs are managed through the VM service, not as apps.
const VMAppName = "__beam_vms__"

// IsPersistentVM identifies a microVM whose lifecycle belongs to the VM service.
func (s *StubConfigV1) IsPersistentVM() bool {
	return s != nil && isPersistentVM(s.UseVM, s.Env)
}

func (r *ContainerRequest) IsPersistentVM() bool {
	return r != nil && isPersistentVM(r.UseVM, r.Env)
}

func isPersistentVM(useVM bool, env []string) bool {
	return useVM && slices.Contains(env, "BEAM_VM_SYSTEMD=1")
}

// WorkerShutdownGraceSeconds covers task drain, systemd shutdown and disk cleanup.
func WorkerShutdownGraceSeconds(stopGrace int64) int64 {
	if stopGrace <= 0 {
		stopGrace = 30
	}
	return stopGrace + max(stopGrace, 120) + 60
}

// VMSpec is CPU-only. Unknown fields (including GPU options) are rejected by
// the API rather than silently changing the isolation or placement contract.
type VMSpec struct {
	ImageID          string            `json:"image_id"`
	CPU              int64             `json:"cpu"`    // millicores
	Memory           int64             `json:"memory"` // MiB
	DiskSize         string            `json:"disk_size"`
	Pool             string            `json:"pool,omitempty"`
	Env              []string          `json:"env,omitempty"`
	Secrets          []string          `json:"secrets,omitempty"`
	Ports            []uint32          `json:"ports,omitempty"`
	PrivatePorts     []uint32          `json:"private_ports,omitempty"`
	Desktop          bool              `json:"desktop"`
	Kiosk            *VMKioskSpec      `json:"kiosk,omitempty"`
	DockerEnabled    bool              `json:"docker_enabled"`
	SSH              bool              `json:"ssh"`
	SSHPublicKey     string            `json:"ssh_public_key,omitempty"`
	IdleTimeout      int64             `json:"idle_timeout"`
	IdleAction       string            `json:"idle_action,omitempty"`
	AutoResume       bool              `json:"auto_resume"`
	BlockNetwork     bool              `json:"block_network"`
	AllowList        []string          `json:"allow_list,omitempty"`
	ProtectedPorts   []uint32          `json:"protected_ports,omitempty"`
	PublicPorts      []uint32          `json:"public_ports,omitempty"`
	Disks            []*pb.DurableDisk `json:"disks,omitempty"`
	Volumes          []*pb.Volume      `json:"volumes,omitempty"`
	SourceSnapshotID string            `json:"source_snapshot_id,omitempty"`
}

// VMCreateOptions carries parsed inputs for creation and forks.
type VMCreateOptions struct {
	Name      string
	Spec      VMSpec
	Metadata  map[string]string
	RequestID string
	Image     *pb.VerifyImageBuildRequest
}

// VMKioskSpec launches an ordinary application on an isolated graphical session.
// Commands are argv, never shell source. Env values follow the VM's redaction rules.
type VMKioskSpec struct {
	Transport VMKioskTransport  `json:"transport,omitempty"`
	Command   []string          `json:"command"`
	Cwd       string            `json:"cwd,omitempty"`
	Env       map[string]string `json:"env,omitempty"`
	MCP       *VMKioskMCP       `json:"mcp,omitempty"`
}

type VMKioskMCP struct {
	Command []string `json:"command"`
	Port    uint32   `json:"port"`
	Path    string   `json:"path,omitempty"`
}

// VM service ports are shared by validation, routing and the guest protocol.
const (
	VMSSHPort                         = 2222
	VMTerminalPort                    = 7681
	VMDesktopPort                     = 8080
	VMKioskStatusPort          uint32 = 8081
	VMKioskMediaPort           uint32 = 8443
	VMKioskMaxICEFrameBytes           = 8192
	VMKioskMaxCommandArgs             = 128
	VMKioskMaxCommandBytes            = 32 << 10
	VMKioskMaxEnvBytes                = 16 << 10
	VMKioskMaxConfigBytes             = 64 << 10
	VMKioskMaxSDPBytes                = 64 << 10
	VMKioskMaxMediaSections           = 8
	VMKioskMaxGatewayAddresses        = 8
)

// Kiosk browser protocol, routed only after VM traffic authentication.
const (
	VMKioskControlPath  = "/_beam/kiosk"
	VMKioskStreamPath   = "/stream"
	VMKioskActionHeader = "X-Beam-Kiosk-Action"
)

type VMKioskTransport string

const (
	VMKioskTransportWebRTC     VMKioskTransport = "webrtc"
	VMKioskTransportWebSockets VMKioskTransport = "websockets"
)

type VMWebRTCSessionDescription struct {
	SDP string `json:"sdp"`
}

// Clone isolates normalization and redaction from snapshots and their parent VM.
func (k *VMKioskSpec) Clone() *VMKioskSpec {
	if k == nil {
		return nil
	}
	copy := *k
	copy.Command = slices.Clone(k.Command)
	copy.Env = maps.Clone(k.Env)
	if k.MCP != nil {
		mcp := *k.MCP
		mcp.Command = slices.Clone(k.MCP.Command)
		copy.MCP = &mcp
	}
	return &copy
}

func (k *VMKioskSpec) PrivatePorts() []uint32 {
	if k == nil {
		return nil
	}
	var ports []uint32
	if k.Transport == VMKioskTransportWebRTC {
		ports = append(ports, VMKioskMediaPort)
	}
	return append(ports, VMKioskStatusPort)
}

func (s VMSpec) RuntimePorts() []uint32 {
	ports := append([]uint32{}, s.Ports...)
	for _, port := range s.PrivatePorts {
		if !slices.Contains(ports, port) {
			ports = append(ports, port)
		}
	}
	return ports
}

type VM struct {
	ID                  string            `json:"id"`
	WorkspaceID         uint              `json:"-"`
	WorkspaceExternalID string            `json:"-"`
	TokenID             string            `json:"-"`
	Name                string            `json:"name"`
	Metadata            map[string]string `json:"metadata,omitempty"`
	CreationDigest      string            `json:"creation_digest,omitempty"`
	TrafficAccessToken  string            `json:"traffic_access_token,omitempty"`
	Handle              string            `json:"handle"`
	PublicIPv6          string            `json:"public_ipv6,omitempty"`
	Spec                VMSpec            `json:"spec"`
	StubID              string            `json:"stub_id,omitempty"`
	ContainerID         string            `json:"container_id,omitempty"`
	DesiredState        string            `json:"desired_state"`
	Status              string            `json:"status"`
	Error               string            `json:"error,omitempty"`
	Generation          int64             `json:"generation"`
	LaunchAttempts      int               `json:"launch_attempts"`
	EverRunning         bool              `json:"ever_running"`
	RootSnapshotID      string            `json:"root_snapshot_id,omitempty"`
	StopSnapshotID      string            `json:"stop_snapshot_id,omitempty"`
	MemoryCheckpointID  string            `json:"memory_checkpoint_id,omitempty"`
	MemoryDiskSnapshots map[string]string `json:"memory_disk_snapshots,omitempty"`
	CreatedAt           time.Time         `json:"created_at"`
	UpdatedAt           time.Time         `json:"updated_at"`
	LastActiveAt        time.Time         `json:"last_active_at"`
	TerminalURL         string            `json:"terminal_url,omitempty"`
	DesktopURL          string            `json:"desktop_url,omitempty"`
	KioskURL            string            `json:"kiosk_url,omitempty"`
	MCPURL              string            `json:"mcp_url,omitempty"`
	BrowserAuth         bool              `json:"browser_auth,omitempty"`
	URLs                map[uint32]string `json:"urls"`
}

// VMArtifact references existing immutable qcow snapshot data; it never owns
// the source VM's disk. Templates survive removal of the source VM.
type VMArtifact struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	VMID           string    `json:"vm_id"`
	VMName         string    `json:"vm_name,omitempty"`
	VMDeleted      bool      `json:"vm_deleted,omitempty"`
	Kind           string    `json:"kind"`
	RootSnapshotID string    `json:"root_snapshot_id"`
	Spec           VMSpec    `json:"spec"`
	CreatedAt      time.Time `json:"created_at"`
	Description    string    `json:"description,omitempty"`
}

// VMFilter selects one page of a workspace's VMs, newest first. A zero Limit
// returns only the counts.
type VMFilter struct {
	Limit, Offset int
	// Stored statuses; reconciliation keeps them within seconds of the runtime.
	Statuses []string
	// Case-insensitive substring of the name or ID.
	Search   string
	Metadata map[string]string
	All      bool
}

// VMPage counts the VMs in scope by status, so each status can show its count
// before it is selected. Statuses and Search narrow only VMs and Total.
type VMPage struct {
	VMs    []*VM
	Total  int
	Counts map[string]int
}

// VMArtifactFilter selects one page of artifacts, newest first.
type VMArtifactFilter struct {
	Limit, Offset int
	VMID          string
}
