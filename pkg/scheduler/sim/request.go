package schedsim

import (
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const defaultRunFor = time.Minute

var workspaceStorageID uint = 1

// Request describes a workload to submit. Build one with Endpoint, Function
// or CPU, then narrow it with the With methods.
type Request struct {
	kind        string
	stubType    string
	cpu         int64
	memory      int64
	gpus        []string
	gpuCount    uint32
	pool        string
	preemptable bool
	workspace   string
	runFor      time.Duration
	warmup      bool
}

// Endpoint is a deployed endpoint replica that asks for one GPU, with the
// types in order of preference. Endpoints can run on preemptable pools.
func Endpoint(gpus ...string) Request {
	return Request{kind: "endpoint", stubType: types.StubTypeEndpointDeployment, cpu: 1000, memory: 1024,
		gpus: gpus, gpuCount: 1, preemptable: true, workspace: "sim-workspace", runFor: defaultRunFor}
}

// Function is a one-off function run that asks for one GPU. Functions do not
// run on preemptable pools.
func Function(gpus ...string) Request {
	return Request{kind: "function", stubType: types.StubTypeFunction, cpu: 1000, memory: 1024,
		gpus: gpus, gpuCount: 1, workspace: "sim-workspace", runFor: defaultRunFor}
}

// CPU is a function run with no GPU.
func CPU(millicores, memoryMB int64) Request {
	return Request{kind: "cpu", stubType: types.StubTypeFunction, cpu: millicores, memory: memoryMB,
		workspace: "sim-workspace", runFor: defaultRunFor}
}

// InPool pins the request to one pool, as a pool selector on the stub does.
func (r Request) InPool(name string) Request { r.pool = name; return r }

// For sets how long the container runs once a worker starts it.
func (r Request) For(d time.Duration) Request { r.runFor = d; return r }

// WithGPUs sets how many GPUs the container needs.
func (r Request) WithGPUs(n uint32) Request { r.gpuCount = n; return r }

// WithResources sets the container's CPU in millicores and memory in MB.
func (r Request) WithResources(millicores, memoryMB int64) Request {
	r.cpu, r.memory = millicores, memoryMB
	return r
}

// InWorkspace submits the request from another workspace.
func (r Request) InWorkspace(id string) Request { r.workspace = id; return r }

// Preemptable sets whether the workload accepts a preemptable pool.
func (r Request) Preemptable(ok bool) Request { r.preemptable = ok; return r }

func (r Request) containerRequest(id string) *types.ContainerRequest {
	request := &types.ContainerRequest{
		ContainerId:  id,
		Cpu:          r.cpu,
		Memory:       r.memory,
		GpuCount:     r.gpuCount,
		PoolSelector: r.pool,
		Preemptable:  r.preemptable,
		WorkspaceId:  r.workspace,
		StubId:       "stub-" + r.kind,
		Stub:         types.StubWithRelated{Stub: types.Stub{ExternalId: "stub-" + r.kind, Type: types.StubType(r.stubType)}},
		// Workspaces have storage, as they do in production. Without it a
		// request cannot run on a machine that joined through the agent.
		Workspace: types.Workspace{ExternalId: r.workspace, Name: r.workspace, Storage: &types.WorkspaceStorage{Id: &workspaceStorageID}},
	}
	switch len(r.gpus) {
	case 0:
		request.GpuCount = 0
	case 1:
		request.Gpu = r.gpus[0]
	default:
		request.GpuRequest = append([]string(nil), r.gpus...)
	}
	return request
}

func (r Request) String() string {
	if len(r.gpus) == 0 {
		return r.kind
	}
	return r.kind + "(" + strings.Join(r.gpus, ",") + ")"
}
