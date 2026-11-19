package types

import (
	"maps"
	"slices"
	"strings"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
)

// VMStopReasonPreempted marks a Spot VM stopped to reclaim its capacity. It
// stays stopped until an explicit start; wake and auto-resume do not restart it.
const VMStopReasonPreempted = "preempted"

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

// IsHeadlessVM identifies a VM that takes none of its guest's ports: it has
// no web terminal, and its process manager listens on a socket that the
// worker reaches over vsock instead of on a port.
func (r *ContainerRequest) IsHeadlessVM() bool {
	return r.IsPersistentVM() && slices.Contains(r.Env, "BEAM_VM_HEADLESS=1")
}

func (r *ContainerRequest) VMID() string {
	for _, env := range r.Env {
		if id, ok := strings.CutPrefix(env, "BEAM_VM_ID="); ok {
			return id
		}
	}
	return ""
}

// WorkerShutdownGraceSeconds covers task drain, systemd shutdown and disk cleanup.
func WorkerShutdownGraceSeconds(stopGrace int64) int64 {
	if stopGrace <= 0 {
		stopGrace = 30
	}
	return stopGrace + max(stopGrace, 120) + 60
}

// VMSpec describes a persistent microVM. GPU requests assign one device.
type VMSpec struct {
	ImageID          string             `json:"image_id"`
	CPU              int64              `json:"cpu"`    // millicores
	Memory           int64              `json:"memory"` // MiB
	GPU              string             `json:"gpu,omitempty"`
	DiskSize         string             `json:"disk_size"`
	Pool             string             `json:"pool,omitempty"`
	CPUTier          CPUTier            `json:"cpu_tier,omitempty"`
	Env              []string           `json:"env,omitempty"`
	Secrets          []string           `json:"secrets,omitempty"`
	Ports            []uint32           `json:"ports,omitempty"`
	PrivatePorts     []uint32           `json:"private_ports,omitempty"`
	Desktop          bool               `json:"desktop"`
	DesktopTransport VMDesktopTransport `json:"desktop_transport,omitempty"`
	Kiosk            *VMKioskSpec       `json:"kiosk,omitempty"`
	DockerEnabled    bool               `json:"docker_enabled"`
	SSH              bool               `json:"ssh"`
	SSHPublicKey     string             `json:"ssh_public_key,omitempty"`
	IdleTimeout      int64              `json:"idle_timeout"`
	IdleAction       string             `json:"idle_action,omitempty"`
	AutoResume       bool               `json:"auto_resume"`
	BlockNetwork     bool               `json:"block_network"`
	AllowList        []string           `json:"allow_list,omitempty"`
	ProtectedPorts   []uint32           `json:"protected_ports,omitempty"`
	PublicPorts      []uint32           `json:"public_ports,omitempty"`
	Disks            []*pb.DurableDisk  `json:"disks,omitempty"`
	Volumes          []*pb.Volume       `json:"volumes,omitempty"`
	SourceSnapshotID string             `json:"source_snapshot_id,omitempty"`
	// Headless VMs take none of their guest's ports, so that what runs in
	// one finds the machine it expects. GitHub runners are headless; the API
	// does not accept it.
	Headless bool `json:"headless,omitempty"`
	// Whether the root disk outlives the VM's container. Unset means true
	// (see CapturesExitSnapshot). A VM that turns it off leaves nothing to
	// start it from again, so it can be deleted but not stopped or paused.
	// GitHub runners turn it off; the API does not accept it.
	CaptureExitSnapshot *bool `json:"capture_exit_snapshot,omitempty"`
	// RootLayer goes under the VM's image (see RootLayer). GitHub runners
	// have their repository's cache there; the API does not accept it.
	RootLayer *RootLayer `json:"root_layer,omitempty"`
}

// VMCreateOptions carries parsed inputs for creation and forks.
type VMCreateOptions struct {
	Name      string
	Spec      VMSpec
	Metadata  map[string]string
	RequestID string
	Image     *pb.VerifyImageBuildRequest
	// Older immutable desktop images use the empty (VNC) transport. Forks
	// retain their image contract unless the caller explicitly changes it.
	PreserveDesktopTransport bool
}

// VMKioskSpec launches an ordinary application on an isolated graphical session.
// Commands are argv, never shell source. Env values follow the VM's redaction rules.
type VMKioskSpec struct {
	Transport VMKioskTransport            `json:"transport,omitempty"`
	Command   []string                    `json:"command"`
	Cwd       string                      `json:"cwd,omitempty"`
	Env       map[string]string           `json:"env,omitempty"`
	MCP       map[string]VMKioskMCPServer `json:"mcp,omitempty"`
}

// VMKioskMCPServer follows the standard mcpServers launch configuration.
type VMKioskMCPServer struct {
	Command string            `json:"command,omitempty"`
	Args    []string          `json:"args,omitempty"`
	Env     map[string]string `json:"env,omitempty"`
	Cwd     string            `json:"cwd,omitempty"`
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// VM service ports are shared by validation, routing and the guest protocol.
const (
	VMSSHPort                         = 2222
	VMTerminalPort                    = 7681
	VMDesktopPort                     = 8080
	VMKioskStatusPort          uint32 = 8081
	VMKioskMCPPort             uint32 = 8082
	VMKioskMediaPort           uint32 = 8443
	VMKioskMaxICEFrameBytes           = 8192
	VMKioskMaxCommandArgs             = 128
	VMKioskMaxMCPServers              = 16
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
	VMKioskMCPPath      = "/mcp"
	VMKioskStreamPath   = "/stream"
	VMKioskActionHeader = "X-Beam-Kiosk-Action"
)

type VMDesktopTransport string

// Kiosk and full desktops use the same authenticated streaming protocol.
type VMKioskTransport = VMDesktopTransport

const (
	VMDesktopTransportEnv = "BEAM_VM_DESKTOP_TRANSPORT"
	VMDesktopLauncherPath = "/opt/beam-vm/desktop"
	VMDesktopSessionPath  = "/opt/beam-vm/kiosk"
)

const (
	VMDesktopTransportWebRTC     VMDesktopTransport = "webrtc"
	VMDesktopTransportWebSockets VMDesktopTransport = "websockets"
	VMKioskTransportWebRTC                          = VMDesktopTransportWebRTC
	VMKioskTransportWebSockets                      = VMDesktopTransportWebSockets
)

func (t VMDesktopTransport) Valid() bool {
	return t == "" || t == VMDesktopTransportWebRTC || t == VMDesktopTransportWebSockets
}

func (s VMSpec) DisplayTransport() VMDesktopTransport {
	if s.Kiosk != nil {
		return s.Kiosk.Transport
	}
	if s.Desktop {
		return s.DesktopTransport
	}
	return ""
}

// An empty transport identifies desktops built before the Selkies contract.
func (s VMSpec) StreamingDesktop() bool {
	return s.Kiosk != nil || s.DisplayTransport() != ""
}

func (s VMSpec) DesktopPrivatePorts() []uint32 {
	if !s.StreamingDesktop() {
		return nil
	}
	var ports []uint32
	if s.DisplayTransport() == VMDesktopTransportWebRTC {
		ports = append(ports, VMKioskMediaPort)
	}
	ports = append(ports, VMKioskStatusPort)
	if s.Kiosk != nil {
		ports = append(ports, VMKioskMCPPort)
	}
	return ports
}

// PublicTemplate shares installed software without inheriting the publisher's
// workspace credentials, placement or attached storage. Root files are included;
// publishers must prepare a clean machine before sharing its complete root.
func (s VMSpec) PublicTemplate() VMSpec {
	s.Env, s.Secrets, s.Disks, s.Volumes = nil, nil, nil, nil
	s.Pool, s.SSH, s.SSHPublicKey, s.SourceSnapshotID = "", false, "", ""
	s.Ports = slices.DeleteFunc(slices.Clone(s.Ports), func(port uint32) bool { return port == VMSSHPort })
	s.Kiosk = s.Kiosk.Clone()
	if s.Kiosk != nil {
		// MCP adapters are workspace bindings, not reusable launch configuration.
		s.Kiosk.Env, s.Kiosk.MCP = nil, nil
	}
	return s
}

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
		copy.MCP = make(map[string]VMKioskMCPServer, len(k.MCP))
		for name, server := range k.MCP {
			server.Args = slices.Clone(server.Args)
			server.Env = maps.Clone(server.Env)
			server.Headers = maps.Clone(server.Headers)
			copy.MCP[name] = server
		}
	}
	return &copy
}

func (k *VMKioskSpec) WithoutCredentials() *VMKioskSpec {
	copy := k.Clone()
	if copy == nil {
		return nil
	}
	copy.Env = nil
	for name, server := range copy.MCP {
		server.Args, server.Env, server.Headers = nil, nil, nil
		if server.URL != "" {
			server.URL = "[redacted]"
		}
		copy.MCP[name] = server
	}
	return copy
}

func (k *VMKioskSpec) PrivatePorts() []uint32 {
	return (VMSpec{Kiosk: k}).DesktopPrivatePorts()
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
	StopReason          string            `json:"stop_reason,omitempty"`
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
	CPUTierEligible     *bool             `json:"cpu_tier_eligible,omitempty"` // set only in responses
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
	Public         bool      `json:"public,omitempty"`
}

const (
	VMArtifactTemplate = "template"
	VMArtifactSnapshot = "snapshot"
)

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

// The statuses of a GitHub Actions job, in the order it moves through them.
const (
	GitHubJobQueued     = "queued"
	GitHubJobInProgress = "in_progress"
	GitHubJobCompleted  = "completed"
)

// GitHubRunnerInstallation connects a GitHub account, through an installation
// of the GitHub App, to the workspace that runs its jobs and is billed for
// them. Runners launch with the Beam token that connected it. No runner
// starts while it is paused, which leaves its jobs queued on GitHub.
// EnhancedCache gives its runners their repository's cache
// (GitHubRunnerCache).
type GitHubRunnerInstallation struct {
	ID            int64     `db:"id" json:"id"`
	WorkspaceID   uint      `db:"workspace_id" json:"-"`
	TokenID       string    `db:"token_id" json:"-"`
	Account       string    `db:"account" json:"account"`
	AccountType   string    `db:"account_type" json:"account_type"`
	Paused        bool      `db:"paused" json:"paused"`
	EnhancedCache bool      `db:"enhanced_cache" json:"-"`
	CreatedAt     time.Time `db:"created_at" json:"created_at"`
}

// GitHubRunnerCache is the disk that holds what the jobs of one repository
// of a workspace download and build again and again: the standard cache
// directories of their tools, whose entries are named by their content or
// their version. Every runner of the repository starts with SnapshotID of
// it under its image, read-only (RootLayer), so a job finds the entries
// there instead of fetching or building them.
//
// A repository has its own disk, so nothing that a job of one repository
// leaves is ever in front of the jobs of another. Repository is its full
// name in lower case.
//
// SnapshotID is the generation runners get, and is empty until there is
// one. It moves only to a generation that a job on the repository's default
// branch left (GitHubRunnerCacheCandidate). One runner at a time is started
// to leave one, and PublisherAt is when the last was. A repository has one
// disk that is not retired. A retired one is deleted, with everything
// stored for it.
type GitHubRunnerCache struct {
	WorkspaceID uint       `db:"workspace_id"`
	Repository  string     `db:"repository"`
	DiskName    string     `db:"disk_name"`
	SnapshotID  string     `db:"snapshot_id"`
	CreatedAt   time.Time  `db:"created_at"`
	PublisherAt *time.Time `db:"publisher_at"`
	RetiredAt   *time.Time `db:"retired_at"`
}

// GitHubRunnerCacheCandidate is a generation of a repository's cache that no
// runner gets yet: SnapshotID, which the worker of the runner VM named
// Runner published. That runner started on Parent, which is the generation
// runners get now, so the candidate is Parent and what the runner's job
// added. It becomes the generation runners get if that job is one of the
// repository whose code its default branch holds.
type GitHubRunnerCacheCandidate struct {
	WorkspaceID uint      `db:"workspace_id"`
	Repository  string    `db:"repository"`
	DiskName    string    `db:"disk_name"`
	Parent      string    `db:"parent"`
	SnapshotID  string    `db:"snapshot_id"`
	Runner      string    `db:"runner"`
	CreatedAt   time.Time `db:"created_at"`
}

// GitHubRunnerJob is a workflow job that asked for a Beam runner.
//
// Runners are interchangeable, so a job has two of them: VM is the runner
// started for it, and RunnerName the one GitHub ran it on, which may have been
// started for another job with the same label. ProvisionAt is when to start
// the job's runner, and is unset once there is nothing left to start. Error
// is why the last attempt to start it failed.
//
// RunID is the workflow run the job is part of, which is what GitHub cancels,
// and is zero for a job recorded before runs were.
//
// Waiting tells a user what a queued job that no runner has taken waits for.
// What is stored is why its runner starts later than at once, and is empty
// while one is being started. The API completes it with what the row does
// not say, as that the account is paused.
//
// ReceivedAt, RegisteredAt and VMReadyAt are when the steps that get the job
// its runner were done: the gateway first heard of the job, VM was registered
// with GitHub, and VM could run commands. The last two are of VM, and are
// unset again when another runner is started for the job. RunnerReadyAt is
// when a runner that GitHub could give the job was first ready, whichever it
// was: from then on, the job waits for GitHub. It is unset only when the
// job's runner stopped before it took a job. Timeline is how the API returns
// the four, and is not stored.
type GitHubRunnerJob struct {
	ID             int64      `db:"id" json:"id"`
	RunID          int64      `db:"run_id" json:"run_id,omitempty"`
	InstallationID int64      `db:"installation_id" json:"-"`
	WorkspaceID    uint       `db:"workspace_id" json:"-"`
	Repository     string     `db:"repository" json:"repository"`
	Workflow       string     `db:"workflow" json:"workflow"`
	Name           string     `db:"name" json:"name"`
	URL            string     `db:"url" json:"url"`
	Label          string     `db:"label" json:"label"`
	Status         string     `db:"status" json:"status"`
	Conclusion     string     `db:"conclusion" json:"conclusion,omitempty"`
	RunnerName     string     `db:"runner_name" json:"runner_name,omitempty"`
	CreatedAt      time.Time  `db:"created_at" json:"created_at"`
	StartedAt      *time.Time `db:"started_at" json:"started_at,omitempty"`
	CompletedAt    *time.Time `db:"completed_at" json:"completed_at,omitempty"`
	VM             string     `db:"vm" json:"-"`
	Attempts       int        `db:"attempts" json:"-"`
	ProvisionAt    *time.Time `db:"provision_at" json:"-"`
	Error          string     `db:"error" json:"error,omitempty"`
	Waiting        string     `db:"waiting" json:"waiting,omitempty"`
	ReceivedAt     *time.Time `db:"received_at" json:"-"`
	RegisteredAt   *time.Time `db:"registered_at" json:"-"`
	VMReadyAt      *time.Time `db:"vm_ready_at" json:"-"`
	RunnerReadyAt  *time.Time `db:"runner_ready_at" json:"-"`
	// DefaultBranch is whether GitHub said the job runs on its repository's
	// default branch, when the job was first heard of.
	DefaultBranch bool `db:"default_branch" json:"-"`

	Timeline GitHubRunnerTimeline `db:"-" json:"timeline,omitzero"`
}

// GitHubRunnerTimeline is when each step that got a job its runner was done,
// as RFC 3339 with milliseconds. A step that was not reached, or not timed,
// is empty.
type GitHubRunnerTimeline struct {
	ReceivedAt   string `json:"received_at,omitempty"`
	RegisteredAt string `json:"registered_at,omitempty"`
	VMReadyAt    string `json:"vm_ready_at,omitempty"`
	// When a runner was first ready for the job: Beam's part of its time in
	// the queue ends here, and GitHub's begins.
	RunnerReadyAt string `json:"runner_ready_at,omitempty"`
}

// GitHubRunnerRun is a workflow run with jobs that asked for a Beam runner,
// in the repository and account that it is cancelled in.
type GitHubRunnerRun struct {
	InstallationID int64  `db:"installation_id"`
	Repository     string `db:"repository"`
	RunID          int64  `db:"run_id"`
}

// GitHubRunnerJobFilter selects one page of a workspace's jobs, newest first.
// A zero Limit returns only the total and the repositories.
type GitHubRunnerJobFilter struct {
	Limit, Offset int
	// Case-insensitive substring of the name, workflow or repository.
	Search                string
	Repository            string
	Statuses, Conclusions []string
}

// GitHubRunnerJobPage names every repository the workspace has jobs of, so
// that each can be chosen before it is selected. The filter narrows only Jobs
// and Total.
type GitHubRunnerJobPage struct {
	Jobs         []GitHubRunnerJob
	Total        int
	Repositories []string
}

// GitHubRunner is what the lifecycle of a runner VM depends on: where it is
// registered, whether that account is still connected, and the status of the
// job GitHub gave it, which is empty while it waits for one. Waiting and
// Ready are what the job it was started for has been told of it: what that
// job waits for, and whether the runner's VM was found ready. Queued is
// whether that job still waits for a runner.
type GitHubRunner struct {
	InstallationID int64  `db:"installation_id"`
	Repository     string `db:"repository"`
	Connected      bool   `db:"connected"`
	JobStatus      string `db:"job_status"`
	Waiting        string `db:"waiting"`
	Ready          bool   `db:"ready"`
	Queued         bool   `db:"queued"`
}

// GitHubRunnerPool is what an idle runner is weighed against: the jobs of
// its repository and label, which are the ones it could take, and the other
// runners that could take them.
//
// Queued is how many of the jobs wait for a runner, and Unheld how many of
// those are told nothing of why, while no runner is being started for them.
// Busy is how many runners are running one of the jobs. Ahead names the idle
// runners that come before this one by name, in that order: of several idle
// runners, each asking for itself, the same ones are then the first.
type GitHubRunnerPool struct {
	Queued int
	Unheld int
	Busy   int
	Ahead  []string
}
