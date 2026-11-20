// Package schedsim runs the real scheduler against simulated workers on a
// fake clock, so capacity behaviour can be tested with no cluster, Redis
// server, provider or credentials.
//
// A Sim wires the production scheduling loop, pool controllers, sizers and
// provisioner to in-memory Redis, a fake Kubernetes API and fake providers.
// Simulated workers stand in for worker pods: they start when their Job is
// created, take the requests the scheduler places on them, hold capacity
// while a container runs, and can vanish without warning. Simulated machines
// stand in for what a pool buys: they boot, join their pool, run a worker,
// and can be taken back by their provider. Nothing in the scheduler or the
// provisioner knows it is simulated.
//
// Time only moves in Advance, and every loop runs on the goroutine that
// called it, in time order. A scenario is therefore deterministic.
package schedsim

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/provisioner"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/test/bufconn"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	// schedulerInterval is how often each scheduler takes a batch from the
	// backlog. Production polls every 50ms; the result is the same.
	schedulerInterval = 100 * time.Millisecond
	// workerInterval is how often simulated workers heartbeat and take work.
	workerInterval = time.Second

	// failureCheckInterval is how often the requests a scheduler gave up on
	// are noted. It is well inside types.ContainerRequestStatusTTL.
	failureCheckInterval = time.Minute

	defaultBootDelay = 30 * time.Second
	// defaultIdleTimeout is how long a real worker stays up with nothing to
	// run before it exits.
	defaultIdleTimeout = 5 * time.Minute
)

type Sim struct {
	t      testing.TB
	ctx    context.Context
	cancel context.CancelFunc

	Clock  *common.FakeClock
	Config types.AppConfig

	redis         *miniredis.Miniredis
	rdb           *common.RedisClient
	kube          *fake.Clientset
	backend       *backend
	workerRepo    repository.WorkerRepository
	containerRepo repository.ContainerRepository
	workspaceRepo repository.WorkspaceRepository
	computeRepo   repository.ComputeRepository

	schedulers []*scheduler.Scheduler

	// What pools with a provisioning block buy from, and what they bought.
	providers    map[string]*Provider
	registry     *providers.Registry
	store        *provisioner.Store
	machines     map[string]*machine // by instance id
	machineOrder []string            // instance ids, oldest first
	deletedPools map[string]bool

	mu         sync.Mutex
	pools      map[string]*Pool
	workers    map[string]*Worker
	containers map[string]*Container
	order      []string // container ids in submission order
	events     []types.EventComputeSchema
	nextID     int
	// tokens holds the pool each machine id may join, until it is revoked.
	tokens      map[string]string
	nextMachine int
	// gaveUp holds the requests a scheduler gave up on. The scheduler's own
	// note of that expires, and a long scenario outlives it.
	gaveUp map[string]bool
}

// New starts a simulation of the pools in configYAML, which has the shape of
// the gateway's config file. Only the pools it names exist.
func New(t testing.TB, configYAML string) *Sim {
	t.Helper()

	server, err := miniredis.Run()
	if err != nil {
		t.Fatalf("schedsim: start redis: %v", err)
	}
	// Redis is reached through memory, with no deadlines and no retries. Over
	// the loopback the round trip is most of what a scenario costs, and a
	// client that gives up on a slow reply asks again: a script that had
	// already taken a request off a queue would lose it, and the scenario
	// would end differently on a busy machine.
	link := bufconn.Listen(1 << 20)
	go func() {
		for {
			conn, err := link.Accept()
			if err != nil {
				return
			}
			server.Server().ServeConn(conn)
		}
	}()
	rdb, err := common.NewRedisClient(
		types.RedisConfig{
			Addrs:        []string{server.Addr()},
			Mode:         types.RedisModeSingle,
			ReadTimeout:  -1,
			WriteTimeout: -1,
			MaxRetries:   -1,
		},
		func(options *redis.UniversalOptions) {
			options.Dialer = func(context.Context, string, string) (net.Conn, error) { return link.Dial() }
		},
	)
	if err != nil {
		t.Fatalf("schedsim: connect to redis: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Start on the wall clock so the few timestamps that still come from it
	// (container state) are not in this clock's future.
	fakeClock := common.NewFakeClock(time.Now().Truncate(time.Second))
	fakeClock.OnStep(func(d time.Duration) {
		server.FastForward(d)
		server.SetTime(fakeClock.Now())
	})

	config := loadConfig(t, configYAML)
	s := &Sim{
		t:             t,
		ctx:           ctx,
		cancel:        cancel,
		Clock:         fakeClock,
		Config:        config,
		redis:         server,
		rdb:           rdb,
		kube:          fake.NewSimpleClientset(),
		backend:       &backend{},
		workerRepo:    repository.NewWorkerRedisRepository(rdb, config.Worker, repository.WithWorkerClock(fakeClock.Now)),
		containerRepo: repository.NewContainerRedisRepository(rdb),
		workspaceRepo: repository.NewWorkspaceRedisRepository(rdb, ""),
		computeRepo:   repository.NewComputeRedisRepository(rdb),
		providers:     map[string]*Provider{},
		machines:      map[string]*machine{},
		deletedPools:  map[string]bool{},
		pools:         map[string]*Pool{},
		workers:       map[string]*Worker{},
		containers:    map[string]*Container{},
		tokens:        map[string]string{},
		gaveUp:        map[string]bool{},
	}
	for name := range config.Worker.Pools {
		s.pools[name] = &Pool{sim: s, Name: name, bootDelay: defaultBootDelay, idleTimeout: defaultIdleTimeout}
	}
	s.addProviders()
	s.kube.PrependReactor("create", "jobs", s.refuseJob)
	t.Cleanup(func() {
		cancel()
		link.Close()
		server.Close()
	})

	s.AddScheduler()
	fakeClock.Every(ctx, workerInterval, s.tickWorkers)
	fakeClock.Every(ctx, failureCheckInterval, s.noteFailures)
	return s
}

// loadConfig lays configYAML over the default config and keeps only the pools
// it names.
func loadConfig(t testing.TB, configYAML string) types.AppConfig {
	t.Helper()

	manager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		t.Fatalf("schedsim: load default config: %v", err)
	}
	if err := manager.LoadConfig(common.YAMLConfigFormat, rawbytes.Provider([]byte(configYAML))); err != nil {
		t.Fatalf("schedsim: load config: %v", err)
	}
	// A misspelt key would quietly turn a scenario into a different one.
	for _, unknown := range manager.UnknownKeys() {
		t.Fatalf("schedsim: config key %q is not recognised", unknown.Key)
	}
	config := manager.GetConfig()

	given := koanf.New(".")
	if err := given.Load(rawbytes.Provider([]byte(configYAML)), yaml.Parser()); err != nil {
		t.Fatalf("schedsim: parse config: %v", err)
	}
	named := map[string]bool{}
	for _, name := range given.MapKeys("worker.pools") {
		named[name] = true
	}
	for name, pool := range config.Worker.Pools {
		if !named[name] {
			delete(config.Worker.Pools, name)
			continue
		}
		// A pool that buys its machines has them join through the agent, as
		// the gateway makes it. Any other is a Kubernetes pool.
		pool.ApplyProvisioning()
		if pool.Mode == "" {
			pool.Mode = types.PoolModeLocal
		}
		config.Worker.Pools[name] = pool
	}
	return config
}

// refuseJob fails the creation of a worker Job in a pool that is refusing
// workers.
func (s *Sim) refuseJob(action k8stesting.Action) (bool, runtime.Object, error) {
	create, ok := action.(k8stesting.CreateAction)
	if !ok {
		return false, nil, nil
	}
	job, ok := create.GetObject().(*batchv1.Job)
	if !ok {
		return false, nil, nil
	}
	if pool, ok := s.pools[job.Labels[scheduler.Beta9WorkerLabelPoolNameKey]]; ok && pool.refusing {
		s.mu.Lock()
		pool.refused++
		s.mu.Unlock()
		return true, nil, errors.New("simulated cluster refused the worker job")
	}
	return false, nil, nil
}

// AddScheduler starts another scheduler replica on the same Redis, clock and
// cluster, and returns it.
func (s *Sim) AddScheduler() *scheduler.Scheduler {
	s.t.Helper()

	replica, err := scheduler.NewScheduler(s.ctx, s.Config, s.rdb, nil, s.backend, s.workspaceRepo,
		scheduler.WithClock(s.Clock),
		scheduler.WithKubernetes(s.kube),
		scheduler.WithInlineBackgroundWork(),
		scheduler.WithComputeEvents(s.recordEvent),
	)
	if err != nil {
		s.t.Fatalf("schedsim: start scheduler: %v", err)
	}
	s.schedulers = append(s.schedulers, replica)
	s.addProvisioner(replica)
	s.Clock.Every(s.ctx, schedulerInterval, func() { replica.ProcessBacklog() })
	return replica
}

// Advance moves time forward by d, running every scheduler, sizer, cleaner and
// worker tick that falls due.
func (s *Sim) Advance(d time.Duration) {
	s.Clock.Advance(d)
}

// Pool returns the handle for a configured pool.
func (s *Sim) Pool(name string) *Pool {
	s.t.Helper()
	pool, ok := s.pools[name]
	if !ok {
		s.t.Fatalf("schedsim: no pool %q", name)
	}
	return pool
}

// Submit sends n copies of the request to the scheduler and returns their
// container ids. Replicas take requests in turn.
func (s *Sim) Submit(spec Request, n int) []string {
	s.t.Helper()

	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		s.mu.Lock()
		s.nextID++
		id := fmt.Sprintf("%s-%04d", spec.kind, s.nextID)
		container := &Container{ID: id, spec: spec, SubmittedAt: s.Clock.Now()}
		s.containers[id] = container
		s.order = append(s.order, id)
		replica := s.schedulers[s.nextID%len(s.schedulers)]
		s.mu.Unlock()

		if err := replica.Run(spec.containerRequest(id)); err != nil {
			s.t.Fatalf("schedsim: submit %s: %v", id, err)
		}
		ids = append(ids, id)
	}
	return ids
}

// Warm starts one idle worker in each named pool and returns once they are
// up; name a pool twice for two workers. It works the way production does: a
// short request pinned to the pool makes the pool add a worker, which then
// sits idle. Warm-up requests are left out of Containers and Placed.
func (s *Sim) Warm(pools ...string) {
	s.t.Helper()

	var ids []string
	var boot time.Duration
	for _, name := range pools {
		pool := s.Pool(name)
		spec := CPU(1000, 1024)
		if gpu := s.Config.Worker.Pools[name].GPUType; gpu != "" {
			spec = Endpoint(gpu)
		}
		spec = spec.InPool(name).For(time.Second)
		spec.warmup = true
		ids = append(ids, s.Submit(spec, 1)...)
		boot = max(boot, pool.bootDelay)
	}
	s.Advance(boot + 5*time.Second)

	for i, id := range ids {
		if len(s.Container(id).Runs) == 0 {
			s.t.Fatalf("schedsim: pool %s did not warm up", pools[i])
		}
	}
}

func (s *Sim) recordEvent(event types.EventComputeSchema) {
	s.mu.Lock()
	defer s.mu.Unlock()
	event.Timestamp = s.Clock.Now()
	s.events = append(s.events, event)
}

// Events returns the compute events the schedulers published with the given
// action, oldest first.
func (s *Sim) Events(action string) []types.EventComputeSchema {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []types.EventComputeSchema
	for _, event := range s.events {
		if event.Action == action {
			out = append(out, event)
		}
	}
	return out
}

// Container returns what the simulation knows about a submitted request.
func (s *Sim) Container(id string) *Container {
	s.t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	container, ok := s.containers[id]
	if !ok {
		s.t.Fatalf("schedsim: no container %q", id)
	}
	return container
}

// Containers returns every submitted request in submission order.
func (s *Sim) Containers() []*Container {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Container, 0, len(s.order))
	for _, id := range s.order {
		if container := s.containers[id]; !container.spec.warmup {
			out = append(out, container)
		}
	}
	return out
}

// PoolsOf returns the pool each container last started on, in the order
// given, with "" for one that has not started.
func (s *Sim) PoolsOf(ids []string) []string {
	s.t.Helper()
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = s.Container(id).Pool()
	}
	return out
}

// Placed counts the containers that are running, or ran, on each pool. A
// container that ran more than once counts for the pool it ran on last.
func (s *Sim) Placed() map[string]int {
	out := map[string]int{}
	for _, container := range s.Containers() {
		if pool := container.Pool(); pool != "" {
			out[pool]++
		}
	}
	return out
}

// Waiting returns the containers that have not started and have not failed.
func (s *Sim) Waiting() []*Container {
	var out []*Container
	for _, container := range s.Containers() {
		if len(container.Runs) == 0 && !s.failed(container.ID) {
			out = append(out, container)
		}
	}
	return out
}

// Failed returns the containers the scheduler gave up on.
func (s *Sim) Failed() []*Container {
	var out []*Container
	for _, container := range s.Containers() {
		if len(container.Runs) == 0 && s.failed(container.ID) {
			out = append(out, container)
		}
	}
	return out
}

func (s *Sim) failed(containerID string) bool {
	s.mu.Lock()
	gaveUp := s.gaveUp[containerID]
	s.mu.Unlock()
	if gaveUp {
		return true
	}

	status, err := s.containerRepo.GetContainerRequestStatus(containerID)
	if err != nil || status != types.ContainerRequestStatusFailed {
		return false
	}
	s.mu.Lock()
	s.gaveUp[containerID] = true
	s.mu.Unlock()
	return true
}

// noteFailures remembers the requests a scheduler has given up on.
func (s *Sim) noteFailures() {
	for _, container := range s.Containers() {
		if len(container.Runs) == 0 {
			s.failed(container.ID)
		}
	}
}

// Requeued returns the containers that a scheduler placed on a worker and
// that then went back to the backlog because the worker was lost.
func (s *Sim) Requeued() []*Container {
	placed := map[string]int{}
	for _, event := range s.Events(types.EventComputeActionContainerPlaced) {
		placed[event.ContainerID]++
	}
	backlog := s.backlog()

	var out []*Container
	for _, container := range s.Containers() {
		if placed[container.ID] > 1 || (placed[container.ID] == 1 && backlog[container.ID]) {
			out = append(out, container)
		}
	}
	return out
}

// backlog returns the ids of the requests waiting in the scheduler's backlog.
func (s *Sim) backlog() map[string]bool {
	out := map[string]bool{}
	for _, key := range []string{common.RedisKeys.SchedulerContainerRequests(), common.RedisKeys.SchedulerBackgroundRequests()} {
		members, err := s.redis.ZMembers(key)
		if err != nil {
			continue // the lane is empty
		}
		for _, member := range members {
			for id := range s.containers {
				if strings.Contains(member, `"container_id":"`+id+`"`) {
					out[id] = true
				}
			}
		}
	}
	return out
}

// Workers returns the pool's worker states as the scheduler sees them, by id.
func (s *Sim) Workers(pool string) []*types.Worker {
	s.t.Helper()
	workers, err := s.workerRepo.GetAllWorkersInPool(pool)
	if err != nil {
		s.t.Fatalf("schedsim: list workers in %s: %v", pool, err)
	}
	sort.Slice(workers, func(i, j int) bool { return workers[i].Id < workers[j].Id })
	return workers
}

func (s *Sim) ExpectPlaced(pool string, n int) {
	s.t.Helper()
	if got := s.Placed()[pool]; got != n {
		s.t.Fatalf("schedsim: %d containers placed on %s, want %d (placed: %v)", got, pool, n, s.Placed())
	}
}

func (s *Sim) ExpectWaiting(n int) {
	s.t.Helper()
	if got := len(s.Waiting()); got != n {
		s.t.Fatalf("schedsim: %d containers waiting, want %d (placed: %v)", got, n, s.Placed())
	}
}

func (s *Sim) ExpectFailed(n int) {
	s.t.Helper()
	if got := len(s.Failed()); got != n {
		s.t.Fatalf("schedsim: %d containers failed, want %d", got, n)
	}
}

func (s *Sim) ExpectRequeued(n int) {
	s.t.Helper()
	if got := len(s.Requeued()); got != n {
		s.t.Fatalf("schedsim: %d containers requeued, want %d", got, n)
	}
}

// ExpectWorkers checks how many workers the scheduler sees in a pool.
func (s *Sim) ExpectWorkers(pool string, n int) {
	s.t.Helper()
	if got := len(s.Workers(pool)); got != n {
		s.t.Fatalf("schedsim: %d workers in %s, want %d", got, pool, n)
	}
}

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

// Pool controls how the simulated workers of one pool behave.
type Pool struct {
	sim  *Sim
	Name string

	bootDelay   time.Duration
	idleTimeout time.Duration
	stalled     bool
	refusing    bool
	refused     int
}

// BootAfter sets how long a new worker takes to start accepting work.
func (p *Pool) BootAfter(d time.Duration) *Pool { p.bootDelay = d; return p }

// IdleAfter sets how long a worker with nothing to run stays up.
func (p *Pool) IdleAfter(d time.Duration) *Pool { p.idleTimeout = d; return p }

// Stall makes the pool's new workers never start: the cluster accepts each
// Job and its pod stays Pending, as when no node has room for it.
func (p *Pool) Stall() *Pool { p.stalled = true; return p }

// Unstall lets pending and new workers start again.
func (p *Pool) Unstall() *Pool { p.stalled = false; return p }

// RefuseWorkers makes the cluster reject the pool's new worker Jobs, so
// adding a worker fails at once.
func (p *Pool) RefuseWorkers() *Pool { p.refusing = true; return p }

// AcceptWorkers undoes RefuseWorkers.
func (p *Pool) AcceptWorkers() *Pool { p.refusing = false; return p }

// Workers returns the pool's simulated workers that have not exited, oldest
// first.
func (p *Pool) Workers() []*Worker {
	p.sim.mu.Lock()
	defer p.sim.mu.Unlock()
	var out []*Worker
	for _, worker := range p.sim.workers {
		if worker.Pool == p.Name && worker.state != workerExited {
			out = append(out, worker)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].createdAt.Equal(out[j].createdAt) {
			return out[i].createdAt.Before(out[j].createdAt)
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// Refused counts the worker Jobs the cluster rejected while the pool was
// refusing workers: each is one failed attempt to add a worker.
func (p *Pool) Refused() int {
	p.sim.mu.Lock()
	defer p.sim.mu.Unlock()
	return p.refused
}

// Started counts every worker the cluster accepted for the pool, whether or
// not it went on to start.
func (p *Pool) Started() int {
	p.sim.mu.Lock()
	defer p.sim.mu.Unlock()
	n := 0
	for _, worker := range p.sim.workers {
		if worker.Pool == p.Name {
			n++
		}
	}
	return n
}

type workerState int

const (
	workerBooting workerState = iota
	workerRunning
	workerVanished
	workerExited
)

// Worker is a simulated worker: a pod in a Kubernetes pool, or the worker of
// a machine that was bought for a pool.
type Worker struct {
	ID   string
	Pool string

	sim     *Sim
	jobName string
	// machine is the bought machine the worker runs on, or nil for a pod.
	// Such a worker lives as long as its machine: it has no Job, and it does
	// not exit when it is idle.
	machine    *machine
	state      workerState
	createdAt  time.Time
	lastWorkAt time.Time
	running    map[string]*run
	// dieOnDelivery makes the worker vanish holding the next requests it
	// takes off its queue.
	dieOnDelivery bool
	// dieOnClaim makes the worker vanish once it has claimed the next
	// request it takes, before the container has started.
	dieOnClaim bool
}

type run struct {
	container *Container
	request   *types.ContainerRequest
	endsAt    time.Time
}

// Vanish stops the worker dead: no more heartbeats, nothing it was running
// finishes, and its pod stays Running. This is a node that stopped answering.
func (w *Worker) Vanish() {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	if w.state != workerExited {
		w.state = workerVanished
	}
}

// LoseRecord drops what the scheduler has on record for the worker, as Redis
// does when it evicts the key. The worker itself is untouched: it finds out
// at its next heartbeat.
func (w *Worker) LoseRecord() {
	w.sim.t.Helper()
	if err := w.sim.rdb.Del(w.sim.ctx, common.RedisKeys.SchedulerWorkerState(w.ID)).Err(); err != nil {
		w.sim.t.Fatalf("schedsim: lose record of worker %s: %v", w.ID, err)
	}
}

// VanishAfterTakingWork makes the worker vanish the moment it has taken its
// next requests off its queue, before it claims them.
func (w *Worker) VanishAfterTakingWork() {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	w.dieOnDelivery = true
}

// VanishAfterClaimingWork makes the worker vanish the moment it has claimed
// its next request, while the container is still starting: a node that is
// lost in the middle of an image pull.
func (w *Worker) VanishAfterClaimingWork() {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	w.dieOnClaim = true
}

// Running reports whether the worker is up and taking work.
func (w *Worker) Running() bool {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	return w.state == workerRunning
}

// Container is a submitted request and what became of it.
type Container struct {
	ID          string
	SubmittedAt time.Time
	// Runs lists each time a worker started the container, oldest first.
	Runs []Run

	spec Request
}

// Run is one start of a container on a worker.
type Run struct {
	Pool      string
	WorkerID  string
	StartedAt time.Time
}

// Pool returns the pool the container last started on, or "".
func (c *Container) Pool() string {
	if len(c.Runs) == 0 {
		return ""
	}
	return c.Runs[len(c.Runs)-1].Pool
}

// tickWorkers is one pass of everything workers, the cluster and bought
// machines do: new Jobs get a pod, pods and machines boot, workers heartbeat,
// take work, finish it and exit when idle.
func (s *Sim) tickWorkers() {
	s.adoptJobs()
	s.tickMachines()

	s.mu.Lock()
	workers := make([]*Worker, 0, len(s.workers))
	for _, worker := range s.workers {
		workers = append(workers, worker)
	}
	s.mu.Unlock()
	sort.Slice(workers, func(i, j int) bool { return workers[i].ID < workers[j].ID })

	for _, worker := range workers {
		worker.tick()
	}
}

// adoptJobs gives every worker Job a pod, as the Job controller would, and a
// simulated worker to run in it.
func (s *Sim) adoptJobs() {
	namespace := s.Config.Worker.Namespace
	jobs, err := s.kube.BatchV1().Jobs(namespace).List(s.ctx, metav1.ListOptions{})
	if err != nil {
		s.t.Fatalf("schedsim: list jobs: %v", err)
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		workerID := job.Labels[scheduler.Beta9WorkerLabelIDKey]
		poolName := job.Labels[scheduler.Beta9WorkerLabelPoolNameKey]

		s.mu.Lock()
		_, known := s.workers[workerID]
		s.mu.Unlock()
		if known || workerID == "" {
			continue
		}

		now := s.Clock.Now()
		labels := map[string]string{"job-name": job.Name}
		for key, value := range job.Labels {
			labels[key] = value
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: job.Name, Namespace: namespace, Labels: labels, CreationTimestamp: metav1.NewTime(now)},
			Status:     corev1.PodStatus{Phase: corev1.PodPending},
		}
		if _, err := s.kube.CoreV1().Pods(namespace).Create(s.ctx, pod, metav1.CreateOptions{}); err != nil {
			s.t.Fatalf("schedsim: create pod for %s: %v", job.Name, err)
		}

		s.mu.Lock()
		s.workers[workerID] = &Worker{ID: workerID, Pool: poolName, sim: s, jobName: job.Name,
			createdAt: now, lastWorkAt: now, running: map[string]*run{}}
		s.mu.Unlock()
	}
}

func (w *Worker) tick() {
	s := w.sim
	pool := s.pools[w.Pool]
	now := s.Clock.Now()

	s.mu.Lock()
	state := w.state
	s.mu.Unlock()
	if state == workerExited || state == workerVanished {
		return
	}

	onMachine := w.machine != nil
	if !onMachine {
		// The cleaner deletes the Job of a worker it gave up on; the pod
		// goes with it.
		if _, err := s.kube.BatchV1().Jobs(s.Config.Worker.Namespace).Get(s.ctx, w.jobName, metav1.GetOptions{}); apierrors.IsNotFound(err) {
			w.exit(false)
			return
		}
	}

	if state == workerBooting {
		// A machine has booted by the time it runs a worker.
		if !onMachine && (pool == nil || pool.stalled || now.Sub(w.createdAt) < pool.bootDelay) {
			return
		}
		// A worker becomes available when it opens its request stream.
		if err := s.workerRepo.RecoverPendingContainerRequests(w.ID); err != nil {
			s.t.Fatalf("schedsim: worker %s recover requests: %v", w.ID, err)
		}
		if err := s.workerRepo.ToggleWorkerAvailable(w.ID, ""); err != nil {
			var notFound *types.ErrWorkerNotFound
			if errors.As(err, &notFound) {
				// A pod without a worker record is done for. A machine
				// waits for its pool to give it one.
				if !onMachine {
					w.exit(false)
				}
				return
			}
			s.t.Fatalf("schedsim: worker %s start: %v", w.ID, err)
		}
		w.setPodPhase(corev1.PodRunning)
		s.mu.Lock()
		w.state = workerRunning
		w.lastWorkAt = now
		s.mu.Unlock()
	}

	if err := s.workerRepo.SetWorkerKeepAlive(w.ID, types.WorkerKeepAlive{RequestStreamOpen: true, CPUTierProtocol: types.CPUTierProtocolVersion}); err != nil {
		// The scheduler removed this worker. A real worker cannot heartbeat
		// either and shuts down; on a machine, the agent starts another.
		if onMachine {
			w.restart()
		} else {
			w.exit(false)
		}
		return
	}

	w.finishContainers(now)
	w.startContainers(now)
	if !w.Running() {
		return
	}

	if len(w.running) > 0 {
		s.mu.Lock()
		w.lastWorkAt = now
		s.mu.Unlock()
		return
	}
	if !onMachine && pool != nil && now.Sub(w.lastWorkAt) > pool.idleTimeout && !w.holdsHeadroom() {
		w.exit(true)
	}
}

// restart is a worker process that died and is started again on its
// machine. What it was running is gone.
func (w *Worker) restart() {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	w.state = workerBooting
	w.running = map[string]*run{}
}

// die ends the worker with its machine: nothing is cleaned up, and nothing
// it was running finishes.
func (w *Worker) die() {
	w.sim.mu.Lock()
	defer w.sim.mu.Unlock()
	w.state = workerExited
	w.running = map[string]*run{}
}

func (w *Worker) holdsHeadroom() bool {
	state, err := w.sim.workerRepo.GetWorkerById(w.ID)
	if err != nil {
		return true
	}
	return w.sim.schedulers[0].WorkerHoldsPoolHeadroom(state)
}

func (w *Worker) startContainers(now time.Time) {
	s := w.sim
	requests, err := s.workerRepo.GetNextContainerRequests(w.ID, 64)
	if err != nil {
		s.t.Fatalf("schedsim: worker %s take requests: %v", w.ID, err)
	}
	if w.dieOnDelivery && len(requests) > 0 {
		w.Vanish()
		return
	}
	for _, request := range requests {
		if err := s.workerRepo.AddContainerToWorker(w.ID, request.ContainerId, request.DeliveryToken); err != nil {
			// The request was cancelled or moved while it was on its way.
			continue
		}
		if w.dieOnClaim {
			// The container is this worker's now, and starting.
			w.Vanish()
			return
		}
		if err := s.containerRepo.UpdateContainerStatus(request.ContainerId, types.ContainerStatusRunning, types.ContainerStateTtlS); err != nil {
			s.t.Fatalf("schedsim: worker %s start %s: %v", w.ID, request.ContainerId, err)
		}

		s.mu.Lock()
		container, ok := s.containers[request.ContainerId]
		if !ok {
			container = &Container{ID: request.ContainerId, SubmittedAt: now, spec: Request{runFor: defaultRunFor}}
			s.containers[request.ContainerId] = container
			s.order = append(s.order, request.ContainerId)
		}
		container.Runs = append(container.Runs, Run{Pool: w.Pool, WorkerID: w.ID, StartedAt: now})
		w.running[request.ContainerId] = &run{container: container, request: request, endsAt: now.Add(container.spec.runFor)}
		s.mu.Unlock()
	}
}

func (w *Worker) finishContainers(now time.Time) {
	s := w.sim
	ids := make([]string, 0, len(w.running))
	for id := range w.running {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		current := w.running[id]
		if now.Before(current.endsAt) {
			// Keep the container's lease, as a running worker does.
			_ = s.containerRepo.UpdateContainerStatus(id, types.ContainerStatusRunning, types.ContainerStateTtlS)
			continue
		}
		delete(w.running, id)
		_ = s.containerRepo.DeleteContainerState(id)
		_ = s.workerRepo.RemoveContainerFromWorker(w.ID, id)
		_ = s.workerRepo.UpdateWorkerCapacity(&types.Worker{Id: w.ID}, current.request, types.AddCapacity)
	}
}

// exit ends the worker. A clean exit removes its own state and completes its
// pod, which the cleaner then collects; otherwise the pod is simply gone.
func (w *Worker) exit(clean bool) {
	s := w.sim
	if clean {
		_ = s.workerRepo.RemoveWorker(w.ID)
		w.setPodPhase(corev1.PodSucceeded)
	} else {
		_ = s.kube.CoreV1().Pods(s.Config.Worker.Namespace).Delete(s.ctx, w.jobName, metav1.DeleteOptions{})
	}
	s.mu.Lock()
	w.state = workerExited
	w.running = map[string]*run{}
	s.mu.Unlock()
}

func (w *Worker) setPodPhase(phase corev1.PodPhase) {
	if w.machine != nil {
		return
	}
	s := w.sim
	pods := s.kube.CoreV1().Pods(s.Config.Worker.Namespace)
	pod, err := pods.Get(s.ctx, w.jobName, metav1.GetOptions{})
	if err != nil {
		return
	}
	pod.Status.Phase = phase
	if _, err := pods.UpdateStatus(context.Background(), pod, metav1.UpdateOptions{}); err != nil {
		s.t.Fatalf("schedsim: update pod %s: %v", w.jobName, err)
	}
}

// Jobs returns the worker Jobs that exist in the simulated cluster.
func (s *Sim) Jobs() []batchv1.Job {
	jobs, err := s.kube.BatchV1().Jobs(s.Config.Worker.Namespace).List(s.ctx, metav1.ListOptions{})
	if err != nil {
		s.t.Fatalf("schedsim: list jobs: %v", err)
	}
	return jobs.Items
}

// backend answers the few Postgres queries the scheduler makes. Any other
// query panics on the nil embedded interface, which is how a scenario finds
// out that the scheduler started to depend on one.
type backend struct {
	repository.BackendRepository

	mu sync.Mutex
	// tokens and revoked count the worker tokens made and revoked.
	tokens  int
	revoked int
}

func (b *backend) GetAdminWorkspace(ctx context.Context) (*types.Workspace, error) {
	return &types.Workspace{Id: 1, ExternalId: "admin", Name: "admin"}, nil
}

func (b *backend) CreateWorkerToken(ctx context.Context, workspaceId uint, tokenType string, workerId string) (types.Token, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens++
	return types.Token{Key: fmt.Sprintf("worker-token-%d", b.tokens)}, nil
}

func (b *backend) BindWorkerToken(ctx context.Context, tokenKey, workerId string) (bool, error) {
	return true, nil
}

// RevokeTokenByExternalId is what a pool asks for a worker it could not add.
func (b *backend) RevokeTokenByExternalId(ctx context.Context, externalId string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.revoked++
	return "", nil
}

// workerTokens returns how many worker tokens were made, and how many of
// them were revoked.
func (b *backend) workerTokens() (made, revoked int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.tokens, b.revoked
}

// GetConcurrencyLimitByWorkspaceId reports no limit, so the platform default
// applies.
func (b *backend) GetConcurrencyLimitByWorkspaceId(ctx context.Context, workspaceId string) (*types.ConcurrencyLimit, error) {
	return nil, sql.ErrNoRows
}

func (b *backend) GetImageCredentials(ctx context.Context, workspace *types.Workspace, imageId string) (string, error) {
	return "", nil
}

func (b *backend) GetLatestCheckpointByStubId(ctx context.Context, stubExternalId string, compatibilityKey ...string) (*types.Checkpoint, error) {
	return nil, nil
}

// Pools with a provisioning block buy their machines. In a simulation they
// buy from fake providers through the production provisioner, and what they
// buy boots, joins its pool and runs work the way a real machine would.

const (
	// adminWorkspace is the workspace that owns shared pools.
	adminWorkspace = "admin"
	// simOwner is this control plane's name on the instances it buys.
	simOwner = "sim"
	// poolSyncInterval is how often the gateway reconciles its pools.
	poolSyncInterval = 30 * time.Second

	defaultMachineCPU      = 8000
	defaultMachineMemoryMB = 32 * 1024
	defaultMachineCents    = 100
)

// Provider controls a simulated provider: what it sells, and what becomes of
// the machines bought from it.
type Provider struct {
	sim  *Sim
	Name string
	fake *providers.Fake

	products []*product
	extra    []providers.Offer
	// spot is whether a product the scenario adds is sold as spot. It is
	// what the provider's first pool asks for.
	spot bool

	bootDelay    time.Duration
	neverJoin    bool
	failNextBoot bool
}

// product is one kind of machine the provider sells.
type product struct {
	gpu           string
	spot          bool
	gpuCount      uint32
	cpuMillicores int64
	memoryMB      int64
	cents         int64
	// stock is how many are left to sell, counted from soldBefore sales of
	// the product. Negative means it never runs out.
	stock      int
	soldBefore int
}

func (p *product) offerID() string {
	name := "cpu"
	if p.gpu != "" {
		name = strings.ToLower(p.gpu)
	}
	if p.spot {
		return name + "-spot"
	}
	return name + "-on-demand"
}

// addProviders gives every provider a pool buys from a fake, selling one
// kind of machine for each of its pools: the pool's GPU type, one GPU, at a
// dollar an hour, and never out of stock.
func (s *Sim) addProviders() {
	names := make([]string, 0, len(s.Config.Worker.Pools))
	for name, pool := range s.Config.Worker.Pools {
		if pool.Buys() {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	var fakes []providers.Provider
	for _, name := range names {
		pool := s.Config.Worker.Pools[name]
		for _, from := range pool.Provisioning.Providers {
			provider, ok := s.providers[from]
			if !ok {
				provider = &Provider{
					sim:       s,
					Name:      from,
					fake:      providers.NewFake(from, simOwner),
					spot:      pool.Provisioning.Spot,
					bootDelay: defaultBootDelay,
				}
				s.providers[provider.Name] = provider
				fakes = append(fakes, provider.fake)
			}
			provider.product(pool.GPUType, pool.Provisioning.Spot)
			if pool.Provisioning.OnDemand {
				provider.product(pool.GPUType, false)
			}
			provider.apply()
		}
	}
	s.registry = providers.RegistryOf(fakes...)
}

// Provider returns the handle for a provider that a pool buys from.
func (s *Sim) Provider(name string) *Provider {
	s.t.Helper()
	provider, ok := s.providers[name]
	if !ok {
		s.t.Fatalf("schedsim: no pool buys from provider %q", name)
	}
	return provider
}

// product returns the provider's product for this GPU type, adding it if
// the provider did not sell it.
func (p *Provider) product(gpu string, spot bool) *product {
	for _, existing := range p.products {
		if existing.gpu == gpu && existing.spot == spot {
			return existing
		}
	}
	added := &product{gpu: gpu, spot: spot, cpuMillicores: defaultMachineCPU, memoryMB: defaultMachineMemoryMB, cents: defaultMachineCents, stock: -1}
	if gpu != "" {
		added.gpuCount = 1
	}
	p.products = append(p.products, added)
	return added
}

// productsOf returns the provider's products with this GPU type. A type the
// provider did not sell is added, sold the way its pools buy.
func (p *Provider) productsOf(gpu string) []*product {
	var out []*product
	for _, existing := range p.products {
		if existing.gpu == gpu {
			out = append(out, existing)
		}
	}
	if len(out) == 0 {
		out = append(out, p.product(gpu, p.spot))
	}
	return out
}

// sold counts the machines bought from an offer so far.
func (p *Provider) sold(offerID string) int {
	n := 0
	for _, request := range p.fake.Creates() {
		if request.Offer.ID == offerID {
			n++
		}
	}
	return n
}

// apply makes the fake sell what the products say.
func (p *Provider) apply() {
	offers := append([]providers.Offer(nil), p.extra...)
	for _, product := range p.products {
		offer := providers.Offer{
			ID:               product.offerID(),
			Cloud:            p.Name,
			InstanceType:     product.offerID(),
			Region:           "sim-1",
			GPU:              product.gpu,
			GPUCount:         product.gpuCount,
			CPUMillicores:    product.cpuMillicores,
			MemoryMB:         product.memoryMB,
			HourlyCostMicros: product.cents * 10_000,
			Spot:             product.spot,
		}
		if product.stock >= 0 {
			left := product.stock - (p.sold(offer.ID) - product.soldBefore)
			if left <= 0 {
				continue
			}
			offer.Available = uint32(left)
		}
		offers = append(offers, offer)
	}
	p.fake.SetOffers(offers...)
}

// Stock sets how many more machines with this GPU type the provider has.
// None means it does not list the type at all. A negative number means it
// never runs out, which is how a provider starts.
func (p *Provider) Stock(gpu string, n int) *Provider {
	for _, product := range p.productsOf(gpu) {
		product.stock = n
		product.soldBefore = p.sold(product.offerID())
	}
	p.apply()
	return p
}

// Price sets what a machine with this GPU type costs an hour, in cents.
func (p *Provider) Price(gpu string, cents int64) *Provider {
	for _, product := range p.productsOf(gpu) {
		product.cents = cents
	}
	p.apply()
	return p
}

// Machine sets the size of the machines the provider sells from now on:
// GPUs, CPU in millicores and memory in MB.
func (p *Provider) Machine(gpus uint32, cpuMillicores, memoryMB int64) *Provider {
	for _, product := range p.products {
		if product.gpu != "" {
			product.gpuCount = gpus
		}
		product.cpuMillicores, product.memoryMB = cpuMillicores, memoryMB
	}
	p.apply()
	return p
}

// Sell adds an offer exactly as given, next to what the provider sells
// already.
func (p *Provider) Sell(offer providers.Offer) *Provider {
	p.extra = append(p.extra, offer)
	p.apply()
	return p
}

// BootAfter sets how long a machine bought from now on takes to join its
// pool.
func (p *Provider) BootAfter(d time.Duration) *Provider { p.bootDelay = d; return p }

// RefuseCreates makes the provider refuse every purchase with err, whatever
// it lists, until it is called with nil.
func (p *Provider) RefuseCreates(err error) *Provider { p.fake.RefuseCreates(err); return p }

// LoseNextAnswer makes the provider carry out the next purchase without the
// buyer hearing of it: the request times out after the machine was created.
func (p *Provider) LoseNextAnswer() *Provider { p.fake.LoseAnswers(1); return p }

// NeverJoin makes machines bought from now on start and never join their
// pool: a broken image, or a network that cannot reach the gateway.
func (p *Provider) NeverJoin() *Provider { p.neverJoin = true; return p }

// JoinAgain undoes NeverJoin for machines bought from now on.
func (p *Provider) JoinAgain() *Provider { p.neverJoin = false; return p }

// FailNextBoot makes the provider fail to start the next machine bought.
func (p *Provider) FailNextBoot() *Provider { p.failNextBoot = true; return p }

// Reclaim makes the provider take back n of the machines it sold, oldest
// first, without warning. Whatever they were running is gone.
func (p *Provider) Reclaim(n int) *Provider {
	p.sim.discoverMachines()
	for _, machine := range p.sim.machinesOf(p) {
		if n == 0 {
			break
		}
		if machine.state == machineGone {
			continue
		}
		p.fake.SetStatus(machine.instanceID, providers.StatusInterrupted, "reclaimed")
		machine.stop()
		n--
	}
	return p
}

// Cordon cordons n of the machines the provider sold, oldest first, as an
// operator does with a machine that is about to go. The machines run on.
func (p *Provider) Cordon(n int) *Provider {
	p.sim.t.Helper()
	p.sim.discoverMachines()
	for _, machine := range p.sim.machinesOf(p) {
		if n == 0 {
			break
		}
		if machine.state != machineJoined || machine.cordoned {
			continue
		}
		machine.cordon()
		n--
	}
	if n > 0 {
		p.sim.t.Fatalf("schedsim: %s has %d fewer machines in a pool than it was to cordon", p.Name, n)
	}
	return p
}

// Orphan puts an instance at the provider that carries a pool machine's tag
// and that nothing has a record of, as a gateway that died in the middle of
// a purchase leaves behind. It returns the instance's ID.
func (p *Provider) Orphan() string {
	return p.fake.Plant(simOwner, providers.StatusActive, map[string]string{provisioner.MachineTag: p.sim.newMachineID()})
}

// ForeignMachine puts an instance at the provider that another control plane
// on the same account bought for one of its pools.
func (p *Provider) ForeignMachine() string {
	return p.fake.Plant("another-control-plane", providers.StatusActive, map[string]string{provisioner.MachineTag: "machine-of-another-control-plane"})
}

// TenantMachine puts an instance at the provider that this control plane
// bought for a tenant's reservation, not for a pool.
func (p *Provider) TenantMachine() string {
	return p.fake.Plant(simOwner, providers.StatusActive, map[string]string{"beam-machine": "tenant-machine"})
}

// Bought counts every machine the provider sold to a pool.
func (p *Provider) Bought() int { return len(p.fake.Creates()) }

// ProviderMachine is a machine that exists at a provider.
type ProviderMachine struct {
	// ID is the provider's name for the machine. MachineID is the name it
	// was bought under, which it carries as a tag.
	ID        string
	MachineID string
	GPU       string
	GPUCount  uint32
	Spot      bool
	// HourlyCents is the price the machine was bought at.
	HourlyCents int64
	// BoughtAt is when the machine was bought, and Joined whether it has
	// joined its pool. Both are zero for an instance nobody bought.
	BoughtAt time.Time
	Joined   bool
}

// Machines returns the machines this control plane has at the provider, in
// the order it got them: what it pays for. A machine the provider took back
// or is deleting is not among them.
func (p *Provider) Machines() []ProviderMachine {
	p.sim.discoverMachines()
	instances, err := p.fake.List(context.Background())
	if err != nil {
		p.sim.t.Fatalf("schedsim: list machines at %s: %v", p.Name, err)
	}
	out := []ProviderMachine{}
	for _, instance := range instances {
		if gone(instance.Status) {
			continue
		}
		listed := ProviderMachine{ID: instance.ID, MachineID: instance.Tags[provisioner.MachineTag]}
		if bought, ok := p.sim.machines[instance.ID]; ok {
			listed.GPU, listed.GPUCount, listed.Spot = bought.offer.GPU, bought.offer.GPUCount, bought.offer.Spot
			listed.HourlyCents = bought.quoted.HourlyCostMicros / 10_000
			listed.BoughtAt, listed.Joined = bought.boughtAt, bought.state == machineJoined
		}
		out = append(out, listed)
	}
	return out
}

// InstanceIDs returns the ID of every instance at the provider, whoever it
// belongs to.
func (p *Provider) InstanceIDs() []string {
	ids := []string{}
	for _, instance := range p.fake.Everything() {
		if !gone(instance.Status) {
			ids = append(ids, instance.ID)
		}
	}
	return ids
}

// gone reports whether an instance in this status is no machine any more:
// the provider is deleting it or took it back.
func gone(status providers.Status) bool {
	return status == providers.StatusDeleted || status == providers.StatusInterrupted
}

// HourlyCents is what the machines at every provider cost an hour together.
func (s *Sim) HourlyCents() int64 {
	total := int64(0)
	for _, provider := range s.providers {
		for _, machine := range provider.Machines() {
			total += machine.HourlyCents
		}
	}
	return total
}

// SpentCents is what every machine bought has cost so far, at the price it
// was bought at, for as long as its provider had it.
func (s *Sim) SpentCents() float64 {
	s.discoverMachines()
	total := 0.0
	for _, machine := range s.machines {
		until := machine.goneAt
		if machine.state != machineGone {
			until = s.Clock.Now()
		}
		total += float64(machine.quoted.HourlyCostMicros) / 10_000 * until.Sub(machine.boughtAt).Hours()
	}
	return total
}

// MachineRecords returns the record the provisioner holds for every machine.
func (s *Sim) MachineRecords() []provisioner.Machine {
	s.t.Helper()
	if s.store == nil {
		return nil
	}
	records, err := s.store.List(s.ctx)
	if err != nil {
		s.t.Fatalf("schedsim: read machine records: %v", err)
	}
	return records
}

// DeletePool removes a pool that buys its machines, as deleting it through
// the pools API does. Its machines are the provisioner's to give back.
func (s *Sim) DeletePool(name string) {
	s.t.Helper()
	if !s.Config.Worker.Pools[name].Buys() {
		s.t.Fatalf("schedsim: pool %q does not buy machines", name)
	}
	s.deletedPools[name] = true
	for _, replica := range s.schedulers {
		replica.DeleteManagedAgentPool(name, s.poolState(name).ManagedInstanceID)
	}
}

// addProvisioner gives a scheduler replica its own provisioner, as every
// gateway has, on the Redis they share, and loads the pools that buy.
func (s *Sim) addProvisioner(replica *scheduler.Scheduler) {
	if len(s.providers) == 0 {
		return
	}
	if s.store == nil {
		s.store = provisioner.NewStore(s.rdb)
	}
	buyer := provisioner.New(provisioner.Options{
		Providers:    s.registry,
		ControlPlane: controlPlane{s},
		Workers:      s.workerRepo,
		Store:        s.store,
		Limits: provisioner.Limits{
			MaxHourlyCents: s.Config.Providers.MaxHourlyCents,
			MaxDailyCents:  s.Config.Providers.MaxDailyCents,
		},
		Clock: s.Clock,
		NewID: s.newMachineID,
	})
	replica.SetProvisioner(buyer)

	s.syncPools(replica)
	s.Clock.Every(s.ctx, poolSyncInterval, func() { s.syncPools(replica) })
	buyer.Start(s.ctx)
}

func (s *Sim) newMachineID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextMachine++
	return fmt.Sprintf("machine-%04d", s.nextMachine)
}

// syncPools is the gateway's reconcile of the pools that join through the
// agent: it loads each into the scheduler and has it look over its machines.
func (s *Sim) syncPools(replica *scheduler.Scheduler) {
	for _, name := range s.buyingPools() {
		if err := replica.EnsureAgentPool(adminWorkspace, s.poolState(name)); err != nil {
			s.t.Fatalf("schedsim: load pool %s: %v", name, err)
		}
	}
}

// buyingPools lists the pools that buy machines and have not been deleted.
func (s *Sim) buyingPools() []string {
	var names []string
	for name, pool := range s.Config.Worker.Pools {
		if pool.Buys() && !s.deletedPools[name] {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names
}

func (s *Sim) poolState(name string) *compute.PoolState {
	config := s.Config.Worker.Pools[name]
	return &compute.PoolState{
		WorkspaceID:       adminWorkspace,
		Name:              name,
		Selector:          name,
		Mode:              string(types.PoolModeExternal),
		Priority:          config.Priority,
		ManagementSource:  types.WorkerPoolManagementSourceConfig,
		ManagedInstanceID: "sim-" + name,
		WorkerConfig:      &config,
	}
}

// controlPlane is the gateway's side of bought machines: the credential a
// machine joins with, and what is cleaned up when it is given back.
type controlPlane struct{ sim *Sim }

func (c controlPlane) BuyingPools(ctx context.Context) (map[string]types.WorkerPoolConfig, map[string]error, error) {
	pools := map[string]types.WorkerPoolConfig{}
	for _, name := range c.sim.buyingPools() {
		pools[name] = c.sim.Config.Worker.Pools[name]
	}
	return pools, nil, nil
}

func (c controlPlane) JoinScript(ctx context.Context, pool, machineID string) (string, string, error) {
	c.sim.mu.Lock()
	defer c.sim.mu.Unlock()
	c.sim.tokens[machineID] = pool
	return joinScript(pool, machineID), "token-" + machineID, nil
}

func (c controlPlane) ForgetMachine(ctx context.Context, pool, machineID, tokenHash string) error {
	s := c.sim
	s.mu.Lock()
	delete(s.tokens, machineID)
	s.mu.Unlock()

	workerID := compute.AgentMachineWorkerID(machineID)
	if worker, err := s.workerRepo.GetWorkerById(workerID); err == nil && worker.MachineId == machineID && worker.PoolName == pool {
		if err := s.workerRepo.RemoveWorker(workerID); err != nil {
			return err
		}
	}
	return s.computeRepo.DeleteAgentMachineState(ctx, adminWorkspace, pool, machineID)
}

func joinScript(pool, machineID string) string {
	return "sim-join " + pool + " " + machineID
}

type machineState int

const (
	// machineBooting: bought, and not up yet.
	machineBooting machineState = iota
	// machineUp: running, and not part of its pool.
	machineUp
	// machineJoined: part of its pool, with a worker.
	machineJoined
	// machineGone: taken back, deleted, or never started.
	machineGone
)

// machine is a simulated machine that a pool bought.
type machine struct {
	sim        *Sim
	provider   *Provider
	instanceID string
	// id and pool are what the machine was told to join as, by the script
	// it was started with.
	id   string
	pool string

	// offer is what the provider sold, and quoted what the buyer asked for:
	// the same machine, at the price the buyer last saw.
	offer     providers.Offer
	quoted    providers.Offer
	boughtAt  time.Time
	joinedAt  time.Time
	goneAt    time.Time
	bootDelay time.Duration
	joins     bool
	failsBoot bool

	state  machineState
	worker *Worker
	// cordoned is what the gateway has on record for the machine once its
	// agent has told it that the machine is about to be taken back.
	cordoned bool
}

// discoverMachines gives every instance that was bought since the last call
// a simulated machine.
func (s *Sim) discoverMachines() {
	names := make([]string, 0, len(s.providers))
	for name := range s.providers {
		names = append(names, name)
	}
	sort.Strings(names)

	now := s.Clock.Now()
	for _, name := range names {
		provider := s.providers[name]
		requests := map[string]providers.CreateRequest{}
		for _, request := range provider.fake.Creates() {
			requests[request.Tags[provisioner.MachineTag]] = request
		}
		for _, instance := range provider.fake.Everything() {
			request, wasBought := requests[instance.Tags[provisioner.MachineTag]]
			if _, known := s.machines[instance.ID]; known || !wasBought {
				continue
			}
			// The machine knows nothing but its startup script.
			told := strings.Fields(request.StartupScript)
			if len(told) != 3 || told[0] != "sim-join" {
				s.t.Fatalf("schedsim: machine %s was started with %q, which is not a join script", instance.ID, request.StartupScript)
			}
			bought := &machine{
				sim:        s,
				provider:   provider,
				instanceID: instance.ID,
				pool:       told[1],
				id:         told[2],
				offer:      provider.fake.Sold(instance.ID),
				quoted:     request.Offer,
				boughtAt:   now,
				bootDelay:  provider.bootDelay,
				joins:      !provider.neverJoin,
				failsBoot:  provider.failNextBoot,
			}
			provider.failNextBoot = false
			s.machines[instance.ID] = bought
			s.machineOrder = append(s.machineOrder, instance.ID)
		}
	}
}

// machinesOf returns the provider's machines, oldest first.
func (s *Sim) machinesOf(provider *Provider) []*machine {
	var out []*machine
	for _, id := range s.machineOrder {
		if machine := s.machines[id]; machine.provider == provider {
			out = append(out, machine)
		}
	}
	return out
}

// tickMachines is one pass of everything bought machines do: boot, join
// their pool, and keep telling the gateway that they are there.
func (s *Sim) tickMachines() {
	s.discoverMachines()
	now := s.Clock.Now()
	for _, id := range s.machineOrder {
		s.machines[id].tick(now)
	}
}

func (m *machine) tick(now time.Time) {
	if m.state == machineGone {
		return
	}
	fake := m.provider.fake
	instance, err := fake.Get(m.sim.ctx, m.instanceID)
	if err != nil || gone(instance.Status) {
		// Its provider deleted it, asked to or not.
		m.stop()
		return
	}

	if m.state == machineBooting {
		if now.Sub(m.boughtAt) < m.bootDelay {
			return
		}
		if m.failsBoot {
			fake.SetStatus(m.instanceID, providers.StatusFailed, "the machine did not start")
			m.stop()
			return
		}
		fake.SetStatus(m.instanceID, providers.StatusActive, "")
		m.state = machineUp
	}

	if m.state == machineUp {
		if m.joins {
			m.join(now)
		}
		return
	}
	m.heartbeat(now)
}

// admitted reports whether the gateway still accepts the machine's token.
func (m *machine) admitted() bool {
	m.sim.mu.Lock()
	defer m.sim.mu.Unlock()
	pool, ok := m.sim.tokens[m.id]
	return ok && pool == m.pool
}

// join is the machine's agent joining its pool, and the gateway telling the
// scheduler about it. Nothing joins a pool that is gone.
func (m *machine) join(now time.Time) {
	s := m.sim
	if !m.admitted() || s.deletedPools[m.pool] {
		return
	}
	if err := s.computeRepo.SaveAgentTokenState(s.ctx, m.agentState(now, now), 0); err != nil {
		s.t.Fatalf("schedsim: machine %s join: %v", m.id, err)
	}
	if err := s.schedulers[0].EnsureAgentMachine(adminWorkspace, s.poolState(m.pool), m.id); err != nil {
		s.t.Fatalf("schedsim: machine %s join pool %s: %v", m.id, m.pool, err)
	}

	m.worker = &Worker{ID: compute.AgentMachineWorkerID(m.id), Pool: m.pool, sim: s, machine: m,
		createdAt: now, lastWorkAt: now, running: map[string]*run{}}
	s.mu.Lock()
	s.workers[m.worker.ID] = m.worker
	s.mu.Unlock()
	m.state = machineJoined
	m.joinedAt = now
}

// heartbeat keeps the machine connected. A machine whose token was revoked
// is turned away, and its worker with it.
func (m *machine) heartbeat(now time.Time) {
	s := m.sim
	if !m.admitted() {
		m.worker.die()
		m.worker = nil
		m.state = machineUp
		m.joins = false
		return
	}
	if s.deletedPools[m.pool] {
		// The gateway has no pool to count the machine in. Its worker runs
		// on until the machine is given back.
		return
	}
	if err := s.computeRepo.SaveAgentTokenState(s.ctx, m.agentState(m.joinedAt, now), 0); err != nil {
		s.t.Fatalf("schedsim: machine %s heartbeat: %v", m.id, err)
	}
}

// agentState is what the machine's agent reports about it.
func (m *machine) agentState(joinedAt, now time.Time) *compute.AgentTokenState {
	state := &compute.AgentTokenState{
		TokenHash:             "agent-" + m.id,
		WorkspaceID:           adminWorkspace,
		PoolName:              m.pool,
		Mode:                  string(types.PoolModeExternal),
		ManagedPoolInstanceID: m.sim.poolState(m.pool).ManagedInstanceID,
		MachineID:             m.id,
		Hostname:              m.instanceID,
		CPUCount:              uint32(m.offer.CPUMillicores / 1000),
		CPUMillicores:         m.offer.CPUMillicores,
		MemoryMB:              uint64(m.offer.MemoryMB),
		GPUCount:              m.offer.GPUCount,
		Executor:              types.DefaultAgentWorkerContainerMode,
		Schedulable:           true,
		Cordoned:              m.cordoned,
		CreatedAt:             joinedAt,
		LastJoinAt:            joinedAt,
		LastHeartbeatAt:       now,
	}
	if m.offer.GPU != "" {
		state.GPUs = []string{m.offer.GPU}
	}
	return state
}

// cordon is what the gateway does when the machine is cordoned: the cordon
// goes on the machine's record and on its worker.
func (m *machine) cordon() {
	s := m.sim
	m.cordoned = true
	if err := s.computeRepo.SaveAgentTokenState(s.ctx, m.agentState(m.joinedAt, s.Clock.Now()), 0); err != nil {
		s.t.Fatalf("schedsim: machine %s record: %v", m.id, err)
	}
	if err := s.workerRepo.SetWorkerCordon(m.worker.ID, true); err != nil {
		s.t.Fatalf("schedsim: machine %s cordon: %v", m.id, err)
	}
}

// stop ends the machine. The gateway is told nothing.
func (m *machine) stop() {
	m.state, m.goneAt = machineGone, m.sim.Clock.Now()
	if m.worker != nil {
		m.worker.die()
	}
}

// The scenarios follow. Each builds a Sim from a config, as a gateway reads
// it, and plays time forward.

const onePool = `
worker:
  pools:
    a10g:
      gpuType: A10G
      priority: 10
`

func TestPoolGrowsForARequestAndShrinksWhenIdle(t *testing.T) {
	sim := New(t, onePool)
	sim.Pool("a10g").BootAfter(30 * time.Second).IdleAfter(5 * time.Minute)

	sim.Submit(Endpoint("A10G").For(time.Minute), 1)

	sim.Advance(10 * time.Second)
	sim.ExpectWorkers("a10g", 1)
	sim.ExpectPlaced("a10g", 0)
	sim.ExpectWaiting(1)

	sim.Advance(30 * time.Second)
	sim.ExpectPlaced("a10g", 1)
	sim.ExpectWaiting(0)

	// The container finishes, the worker idles out, and the cleaner removes
	// its Job.
	sim.Advance(10 * time.Minute)
	sim.ExpectWorkers("a10g", 0)
	if jobs := sim.Jobs(); len(jobs) != 0 {
		t.Fatalf("%d worker jobs left after the pool went idle, want 0", len(jobs))
	}
	if started := sim.Pool("a10g").Started(); started != 1 {
		t.Fatalf("pool started %d workers for one request, want 1", started)
	}
}

// These scenarios pin where requests go when a GPU type has fallback pools.
// They were written against scheduling.failover.chains, before alsoServes
// replaced it, and are kept as they were: a config that still has chains
// must route as it did.

// chainPools has two A10G pools, and two pools of other GPU types that A10G
// requests may fall back to. The fallback order matches the pools'
// priorities, as it does in production.
const chainPools = `
worker:
  pools:
    a10g:
      gpuType: A10G
      priority: 0
    a10g-reserve:
      gpuType: A10G
      priority: -2
    rtx4090:
      gpuType: RTX4090
      priority: 5
    l40s:
      gpuType: L40S
      priority: 3
      requiresPoolSelector: true
    t4:
      gpuType: T4
scheduling:
  failover:
    enabled: true
    chains:
      A10G:
        pools: [rtx4090, l40s]
`

func started(sim *Sim, pools ...string) map[string]int {
	out := map[string]int{}
	for _, name := range pools {
		out[name] = sim.Pool(name).Started()
	}
	return out
}

func TestRequestFillsItsOwnPoolsThenItsFallbacksInOrder(t *testing.T) {
	sim := New(t, chainPools)
	sim.Warm("l40s", "rtx4090", "a10g-reserve", "a10g")

	var ids []string
	for i := 0; i < 4; i++ {
		ids = append(ids, sim.Submit(Endpoint("A10G").For(10*time.Minute), 1)...)
		sim.Advance(2 * time.Second)
	}

	// Its own GPU type first, by priority, although both fallbacks have a
	// higher priority. Then the fallbacks in their order.
	require.Equal(t, []string{"a10g", "a10g-reserve", "rtx4090", "l40s"}, sim.PoolsOf(ids))
}

func TestRequestsInOneBatchSpreadDownTheRoute(t *testing.T) {
	sim := New(t, chainPools)
	sim.Warm("l40s", "rtx4090", "a10g-reserve", "a10g")

	ids := sim.Submit(Endpoint("A10G").For(10*time.Minute), 4)
	sim.Advance(2 * time.Second)

	require.Equal(t, []string{"a10g", "a10g-reserve", "rtx4090", "l40s"}, sim.PoolsOf(ids))
}

func TestReadyFallbackCapacityBeatsGrowingTheRequestsOwnPool(t *testing.T) {
	sim := New(t, chainPools)
	sim.Warm("rtx4090")

	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(2 * time.Second)

	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
	require.Equal(t, map[string]int{"a10g": 0, "a10g-reserve": 0}, started(sim, "a10g", "a10g-reserve"))
}

func TestRequestGrowsItsOwnPoolFirst(t *testing.T) {
	sim := New(t, chainPools)

	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"a10g"}, sim.PoolsOf(ids))
	require.Equal(t,
		map[string]int{"a10g": 1, "a10g-reserve": 0, "rtx4090": 0, "l40s": 0},
		started(sim, "a10g", "a10g-reserve", "rtx4090", "l40s"))
}

func TestGrowthMovesDownTheRouteWhenAPoolRefuses(t *testing.T) {
	t.Run("to the next pool of the same GPU type", func(t *testing.T) {
		sim := New(t, chainPools)
		sim.Pool("a10g").RefuseWorkers()

		ids := sim.Submit(Endpoint("A10G"), 1)
		sim.Advance(2 * time.Minute)

		require.Equal(t, []string{"a10g-reserve"}, sim.PoolsOf(ids))
		require.Equal(t, map[string]int{"rtx4090": 0, "l40s": 0}, started(sim, "rtx4090", "l40s"))
	})

	t.Run("then to the first fallback", func(t *testing.T) {
		sim := New(t, chainPools)
		sim.Pool("a10g").RefuseWorkers()
		sim.Pool("a10g-reserve").RefuseWorkers()

		ids := sim.Submit(Endpoint("A10G"), 1)
		sim.Advance(2 * time.Minute)

		require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
		require.Equal(t, 0, sim.Pool("l40s").Started())
	})
}

func TestFallbackPoolBehindASelectorTakesOnlyFallbackRequests(t *testing.T) {
	sim := New(t, chainPools)
	sim.Warm("l40s", "l40s")

	fallback := sim.Submit(Endpoint("A10G").For(10*time.Minute), 1)
	direct := sim.Submit(Endpoint("L40S"), 1)
	named := sim.Submit(Endpoint("L40S").InPool("l40s"), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"l40s"}, sim.PoolsOf(fallback))
	require.Equal(t, []string{"l40s"}, sim.PoolsOf(named))
	// A request for the pool's own GPU type still has to name the pool.
	require.Equal(t, []string{""}, sim.PoolsOf(direct))
	sim.ExpectFailed(1)
}

func TestRequestThatNamesAPoolStaysThere(t *testing.T) {
	sim := New(t, chainPools)
	sim.Warm("rtx4090", "a10g-reserve")
	sim.Pool("a10g").RefuseWorkers()

	ids := sim.Submit(Endpoint("A10G").InPool("a10g"), 1)
	sim.Advance(2 * time.Minute)

	require.Equal(t, []string{""}, sim.PoolsOf(ids))
	sim.ExpectFailed(1)
}

func TestGPUTypeWithoutFallbacksStaysOnItsOwnPools(t *testing.T) {
	sim := New(t, chainPools)
	sim.Warm("rtx4090", "l40s", "a10g")
	sim.Pool("t4").RefuseWorkers()

	ids := sim.Submit(Endpoint("T4"), 1)
	sim.Advance(2 * time.Minute)

	require.Equal(t, []string{""}, sim.PoolsOf(ids))
	sim.ExpectFailed(1)
}

func TestRequestListingSeveralGPUTypes(t *testing.T) {
	t.Run("uses the fallbacks of the first type that has any", func(t *testing.T) {
		sim := New(t, chainPools)
		sim.Warm("l40s")

		ids := sim.Submit(Endpoint("H100", "A10G"), 1)
		sim.Advance(2 * time.Second)

		require.Equal(t, []string{"l40s"}, sim.PoolsOf(ids))
	})

	t.Run("treats a pool of a listed type as its own, not as a fallback", func(t *testing.T) {
		sim := New(t, chainPools)
		sim.Warm("rtx4090", "a10g")

		// RTX4090 is listed, so the rtx4090 pool competes on priority with
		// the a10g pool instead of waiting behind it.
		ids := sim.Submit(Endpoint("RTX4090", "A10G"), 1)
		sim.Advance(2 * time.Second)

		require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
	})
}

const preemptableFallback = `
worker:
  pools:
    a10g:
      gpuType: A10G
    rtx4090:
      gpuType: RTX4090
      priority: 5
      preemptable: true
    l40s:
      gpuType: L40S
      priority: 3
scheduling:
  failover:
    enabled: true
    chains:
      A10G:
        pools: [rtx4090, l40s]
`

func TestOnlyWorkThatAcceptsInterruptionUsesAPreemptableFallback(t *testing.T) {
	sim := New(t, preemptableFallback)
	sim.Warm("rtx4090", "l40s")

	function := sim.Submit(Function("A10G").For(10*time.Minute), 1)
	sim.Advance(2 * time.Second)
	endpoint := sim.Submit(Endpoint("A10G").For(10*time.Minute), 1)
	sim.Advance(2 * time.Second)

	require.Equal(t, []string{"l40s"}, sim.PoolsOf(function))
	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(endpoint))
}

func TestTwoSchedulersPlaceEachRequestOnce(t *testing.T) {
	sim := New(t, chainPools)
	sim.AddScheduler()

	ids := sim.Submit(Endpoint("A10G").For(10*time.Minute), 12)
	sim.Advance(3 * time.Minute)

	for _, id := range ids {
		require.Len(t, sim.Container(id).Runs, 1, "container %s", id)
	}
	sim.ExpectPlaced("a10g", 12)
	// One worker per request and no more: a replica must not add a worker
	// for a request the other replica is already adding one for.
	require.Equal(t, 12, sim.Pool("a10g").Started())
}

// These scenarios cover fallbacks configured on the pool with alsoServes.
// The ones above cover the same routes configured through the old
// scheduling.failover.chains, which is converted to alsoServes at startup.

// alsoServesPools is chainPools written the new way.
const alsoServesPools = `
worker:
  pools:
    a10g:
      gpuType: A10G
      priority: 0
    a10g-reserve:
      gpuType: A10G
      priority: -2
    rtx4090:
      gpuType: RTX4090
      priority: 5
      alsoServes: [A10G]
    l40s:
      gpuType: L40S
      priority: 3
      requiresPoolSelector: true
      alsoServes: [A10G]
    t4:
      gpuType: T4
`

// Invariant 1: a request is placed on the first pool in its route that has
// room for it now.
func TestRequestIsPlacedOnTheFirstPoolInItsRouteWithRoom(t *testing.T) {
	sim := New(t, alsoServesPools)
	sim.Warm("l40s", "rtx4090", "a10g-reserve", "a10g", "t4")

	var ids []string
	for i := 0; i < 5; i++ {
		ids = append(ids, sim.Submit(Endpoint("A10G").For(10*time.Minute), 1)...)
		sim.Advance(2 * time.Second)
	}

	// Pools of the requested GPU type by priority, then the pools that also
	// serve it by priority. T4 is not in the route, so the fifth request adds
	// a worker to the first pool instead of using the idle T4 one.
	require.Equal(t, []string{"a10g", "a10g-reserve", "rtx4090", "l40s"}, sim.PoolsOf(ids[:4]))
	sim.Advance(time.Minute)
	require.Equal(t, []string{"a10g"}, sim.PoolsOf(ids[4:]))
	sim.ExpectPlaced("t4", 0)
}

// A placement reports how far down its route it landed: 0 on a pool of a
// type the request asked for, counting up through the pools that only also
// serve it.
func TestPlacementReportsItsRouteRank(t *testing.T) {
	sim := New(t, alsoServesPools)
	sim.Warm("l40s", "rtx4090", "a10g")

	var ids []string
	for i := 0; i < 3; i++ {
		ids = append(ids, sim.Submit(Endpoint("A10G").For(10*time.Minute), 1)...)
		sim.Advance(2 * time.Second)
	}
	// A request for any GPU asks for every pool it can use, so landing on
	// the RTX4090 pool is not overflow.
	sim.Warm("rtx4090")
	ids = append(ids, sim.Submit(Endpoint("any").For(10*time.Minute), 1)...)
	sim.Advance(2 * time.Second)
	require.Equal(t, []string{"a10g", "rtx4090", "l40s", "rtx4090"}, sim.PoolsOf(ids))

	type placement struct{ pool, requested, placed, rank string }
	placements := map[string]placement{}
	for _, event := range sim.Events(types.EventComputeActionContainerPlaced) {
		require.NotContains(t, event.Attrs, "failover")
		require.NotContains(t, event.Attrs, "chain")
		if !slices.Contains(ids, event.ContainerID) {
			continue // Warming a pool places a request of its own.
		}
		placements[event.ContainerID] = placement{
			pool:      event.PoolName,
			requested: event.Attrs[types.EventComputeAttrRequestedGPU],
			placed:    event.Attrs[types.EventComputeAttrPlacedGPU],
			rank:      event.Attrs[types.EventComputeAttrRouteRank],
		}
	}
	require.Equal(t, map[string]placement{
		ids[0]: {pool: "a10g", requested: "A10G", placed: "A10G", rank: "0"},
		ids[1]: {pool: "rtx4090", requested: "A10G", placed: "RTX4090", rank: "1"},
		ids[2]: {pool: "l40s", requested: "A10G", placed: "L40S", rank: "2"},
		ids[3]: {pool: "rtx4090", requested: "any", placed: "RTX4090", rank: "0"},
	}, placements)
}

func TestGrowthFollowsTheRoute(t *testing.T) {
	sim := New(t, alsoServesPools)
	sim.Pool("a10g").RefuseWorkers()
	sim.Pool("a10g-reserve").RefuseWorkers()

	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(2 * time.Minute)

	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
	require.Equal(t, 0, sim.Pool("l40s").Started())
}

func TestPoolThatAlsoServesAGPUTypeStillServesItsOwn(t *testing.T) {
	sim := New(t, alsoServesPools)

	ids := sim.Submit(Endpoint("RTX4090"), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
}

func TestAlsoServesGoesOneWay(t *testing.T) {
	sim := New(t, alsoServesPools)
	sim.Warm("a10g")

	// RTX4090 requests do not run on A10G pools just because the RTX4090
	// pool takes A10G overflow: the request waits for a worker of its own.
	ids := sim.Submit(Endpoint("RTX4090"), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
	sim.ExpectPlaced("a10g", 0)
}

func TestGPUTypeWithNoPoolOfItsOwnRunsOnPoolsThatAlsoServeIt(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    l40s:
      gpuType: L40S
      alsoServes: [A6000]
`)

	ids := sim.Submit(Endpoint("A6000"), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"l40s"}, sim.PoolsOf(ids))
}

func TestFallbackPoolsWithEqualPriorityAreTriedByName(t *testing.T) {
	const pools = `
worker:
  pools:
    a10g:
      gpuType: A10G
    spot-c:
      gpuType: L40S
      alsoServes: [A10G]
    spot-a:
      gpuType: L40S
      alsoServes: [A10G]
    spot-b:
      gpuType: RTX4090
      alsoServes: [A10G]
`
	// The order is the same on every run and every replica. It used to
	// follow map iteration.
	for run := 0; run < 5; run++ {
		sim := New(t, pools)
		sim.Warm("spot-c", "spot-b", "spot-a")

		var ids []string
		for i := 0; i < 3; i++ {
			ids = append(ids, sim.Submit(Endpoint("A10G").For(10*time.Minute), 1)...)
			sim.Advance(2 * time.Second)
		}
		require.Equal(t, []string{"spot-a", "spot-b", "spot-c"}, sim.PoolsOf(ids))
	}
}

func TestRequestListingSeveralGPUTypesUsesTheFallbacksOfAll(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    a10g:
      gpuType: A10G
    t4:
      gpuType: T4
    l40s:
      gpuType: L40S
      priority: -1
      alsoServes: [A10G]
    rtx4090:
      gpuType: RTX4090
      priority: -2
      alsoServes: [T4]
`)
	sim.Warm("rtx4090", "l40s")
	sim.Pool("a10g").RefuseWorkers()
	sim.Pool("t4").RefuseWorkers()

	var ids []string
	for i := 0; i < 2; i++ {
		ids = append(ids, sim.Submit(Endpoint("A10G", "T4").For(10*time.Minute), 1)...)
		sim.Advance(2 * time.Second)
	}

	require.Equal(t, []string{"l40s", "rtx4090"}, sim.PoolsOf(ids))
}

func TestOldChainsAndAlsoServesCombine(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    a10g:
      gpuType: A10G
    rtx4090:
      gpuType: RTX4090
      priority: -1
    l40s:
      gpuType: L40S
      priority: -2
      alsoServes: [A10G]
scheduling:
  failover:
    enabled: true
    chains:
      A10G:
        pools: [rtx4090]
`)
	sim.Warm("l40s", "rtx4090")
	sim.Pool("a10g").RefuseWorkers()

	var ids []string
	for i := 0; i < 2; i++ {
		ids = append(ids, sim.Submit(Endpoint("A10G").For(10*time.Minute), 1)...)
		sim.Advance(2 * time.Second)
	}

	require.Equal(t, []string{"rtx4090", "l40s"}, sim.PoolsOf(ids))
}

// These scenarios cover which pool adds a worker when nothing in a request's
// route has room.

// growPools gives A10G requests a route of four pools: a10g, a10g-reserve,
// then rtx4090 and l40s, which also serve A10G.
const growPools = `
worker:
  pools:
    a10g:
      gpuType: A10G
    a10g-reserve:
      gpuType: A10G
      priority: -2
    rtx4090:
      gpuType: RTX4090
      priority: 5
      alsoServes: [A10G]
    l40s:
      gpuType: L40S
      priority: 3
      alsoServes: [A10G]
`

func refused(sim *Sim, pools ...string) map[string]int {
	out := map[string]int{}
	for _, name := range pools {
		out[name] = sim.Pool(name).Refused()
	}
	return out
}

// Invariant 2, first half: a pool is asked to grow only when no pool in the
// route has room or pending capacity for the request.
func TestPoolGrowsOnlyWhenNothingInTheRouteHasRoomOrIsStarting(t *testing.T) {
	// Each a10g worker has two GPUs, so one worker has room for two requests.
	const pools = `
worker:
  pools:
    a10g:
      gpuType: A10G
      poolSizing:
        defaultWorkerCPU: 4000m
        defaultWorkerMemory: 8Gi
        defaultWorkerGpuCount: "2"
    rtx4090:
      gpuType: RTX4090
      alsoServes: [A10G]
`
	t.Run("a starting worker with room is waited for", func(t *testing.T) {
		sim := New(t, pools)

		first := sim.Submit(Endpoint("A10G").For(10*time.Minute), 1)
		sim.Advance(5 * time.Second)
		require.Equal(t, 1, sim.Pool("a10g").Started())

		// The worker is still starting. It will have a GPU to spare, so the
		// second request waits for it instead of growing either pool.
		second := sim.Submit(Endpoint("A10G").For(10*time.Minute), 1)
		sim.Advance(time.Minute)

		require.Equal(t, []string{"a10g", "a10g"}, sim.PoolsOf(append(first, second...)))
		require.Equal(t, map[string]int{"a10g": 1, "rtx4090": 0}, started(sim, "a10g", "rtx4090"))
	})

	t.Run("a worker with room is used", func(t *testing.T) {
		sim := New(t, pools)
		sim.Warm("rtx4090")

		ids := sim.Submit(Endpoint("A10G").For(10*time.Minute), 1)
		sim.Advance(time.Minute)

		require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(ids))
		require.Equal(t, map[string]int{"a10g": 0, "rtx4090": 1}, started(sim, "a10g", "rtx4090"))
	})

	t.Run("with neither, the first pool in the route grows", func(t *testing.T) {
		sim := New(t, pools)

		ids := sim.Submit(Endpoint("A10G").For(10*time.Minute), 3)
		sim.Advance(time.Minute)

		// Two workers of two GPUs for three requests, all in the first pool.
		require.Equal(t, []string{"a10g", "a10g", "a10g"}, sim.PoolsOf(ids))
		require.Equal(t, map[string]int{"a10g": 2, "rtx4090": 0}, started(sim, "a10g", "rtx4090"))
	})
}

// The attempt limit is per pool. Counted per request, three pools that
// refuse would use it up and the fourth would never be asked.
func TestRequestReachesTheEndOfALongRouteOfRefusingPools(t *testing.T) {
	sim := New(t, growPools)
	sim.Pool("a10g").RefuseWorkers()
	sim.Pool("a10g-reserve").RefuseWorkers()
	sim.Pool("rtx4090").RefuseWorkers()

	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(2 * time.Minute)

	require.Equal(t, []string{"l40s"}, sim.PoolsOf(ids))
	sim.ExpectFailed(0)
}

func TestRequestAsksEachPoolInItsRouteForThreeWorkersThenFails(t *testing.T) {
	sim := New(t, growPools)
	pools := []string{"a10g", "a10g-reserve", "rtx4090", "l40s"}
	for _, name := range pools {
		sim.Pool(name).RefuseWorkers()
	}

	sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(2 * time.Minute)

	sim.ExpectFailed(1)
	require.Equal(t, map[string]int{"a10g": 3, "a10g-reserve": 3, "rtx4090": 3, "l40s": 3}, refused(sim, pools...))

	// Nothing is asked again once the request has failed.
	sim.Advance(5 * time.Minute)
	require.Equal(t, map[string]int{"a10g": 3, "a10g-reserve": 3, "rtx4090": 3, "l40s": 3}, refused(sim, pools...))
}

// A pool that just failed to add a worker is left alone for a moment, so a
// queue of waiting requests does not hammer a cluster that is refusing Jobs.
func TestRefusingPoolIsNotAskedForEveryWaitingRequestAtOnce(t *testing.T) {
	const pools = `
worker:
  pools:
    t4:
      gpuType: T4
`
	sim := New(t, pools)
	sim.Pool("t4").RefuseWorkers()

	sim.Submit(Endpoint("T4"), 20)
	sim.Advance(4 * time.Second)

	require.Equal(t, 1, sim.Pool("t4").Refused())
}

// stallPools has an A10G pool that gives up on workers that have not started
// after two minutes, and an RTX4090 pool that A10G requests may overflow to.
const stallPools = `
worker:
  pools:
    a10g:
      gpuType: A10G
      stalledAfter: 2m
    rtx4090:
      gpuType: RTX4090
      alsoServes: [A10G]
`

func TestStalledPoolOverflowsToAPoolThatAlsoServesTheRequest(t *testing.T) {
	sim := New(t, stallPools)
	sim.Pool("a10g").Stall()

	first := sim.Submit(Endpoint("A10G").For(30*time.Minute), 1)
	sim.Advance(90 * time.Second)

	// Until stalledAfter the request waits for the worker it asked for.
	require.Equal(t, []string{""}, sim.PoolsOf(first))
	require.Equal(t, map[string]int{"a10g": 1, "rtx4090": 0}, started(sim, "a10g", "rtx4090"))

	sim.Advance(90 * time.Second)
	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(first))

	// The pool still has a worker that never started, so the next request
	// passes over it at once instead of waiting two minutes of its own.
	second := sim.Submit(Endpoint("A10G").For(30*time.Minute), 1)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"rtx4090"}, sim.PoolsOf(second))
	require.Equal(t, map[string]int{"a10g": 1, "rtx4090": 2}, started(sim, "a10g", "rtx4090"))
}

// Invariant 2, second half: a pool that cannot grow is skipped without
// spending an attempt. The request below passes over its stalled pool on
// every scheduling pass for minutes and is neither failed nor given a second
// worker.
func TestPoolThatCannotGrowIsSkippedWithoutSpendingAnAttempt(t *testing.T) {
	const pools = `
worker:
  pools:
    t4:
      gpuType: T4
      stalledAfter: 1m
`
	sim := New(t, pools)
	sim.Pool("t4").Stall()

	ids := sim.Submit(Endpoint("T4"), 1)
	sim.Advance(8 * time.Minute)

	sim.ExpectFailed(0)
	sim.ExpectWaiting(1)
	require.Equal(t, 1, sim.Pool("t4").Started())

	// When the cluster finds room the worker starts, and the request that
	// was waiting on the pool runs there.
	sim.Pool("t4").Unstall()
	sim.Advance(10 * time.Second)

	require.Equal(t, []string{"t4"}, sim.PoolsOf(ids))
	require.Equal(t, 1, sim.Pool("t4").Started())
}

func TestPoolGrowsAgainOnceItsWorkersStart(t *testing.T) {
	sim := New(t, stallPools)
	sim.Pool("a10g").Stall()

	sim.Submit(Endpoint("A10G").For(30*time.Minute), 1)
	sim.Advance(3 * time.Minute)
	sim.ExpectPlaced("rtx4090", 1)

	sim.Pool("a10g").Unstall()
	sim.Advance(10 * time.Second)

	// The worker that was stuck is idle now and takes the next request; the
	// one after that makes the pool add a worker again.
	ids := sim.Submit(Endpoint("A10G").For(30*time.Minute), 2)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"a10g", "a10g"}, sim.PoolsOf(ids))
	require.Equal(t, map[string]int{"a10g": 2, "rtx4090": 1}, started(sim, "a10g", "rtx4090"))
}

// How long a worker has been starting is read from its shared state, so a
// scheduler replica that did not add the worker passes over the pool too.
func TestEverySchedulerReplicaSeesAStalledPool(t *testing.T) {
	sim := New(t, stallPools)
	sim.AddScheduler()
	sim.Pool("a10g").Stall()

	sim.Submit(Endpoint("A10G").For(30*time.Minute), 1)
	sim.Advance(3 * time.Minute)
	sim.ExpectPlaced("rtx4090", 1)

	// Requests alternate between the replicas.
	ids := sim.Submit(Endpoint("A10G").For(30*time.Minute), 2)
	sim.Advance(time.Minute)

	require.Equal(t, []string{"rtx4090", "rtx4090"}, sim.PoolsOf(ids))
	require.Equal(t, 1, sim.Pool("a10g").Started())
}

// stalledAfter is opt-in. Without it a pool that cannot get nodes keeps its
// requests until the scheduling limit, as it always has.
func TestPoolWithoutStalledAfterKeepsItsRequests(t *testing.T) {
	const pools = `
worker:
  pools:
    a10g:
      gpuType: A10G
    rtx4090:
      gpuType: RTX4090
      alsoServes: [A10G]
`
	sim := New(t, pools)
	sim.Pool("a10g").Stall()

	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(19 * time.Minute)

	require.Equal(t, []string{""}, sim.PoolsOf(ids))
	require.Equal(t, 0, sim.Pool("rtx4090").Started())
	sim.ExpectFailed(0)

	sim.Advance(2 * time.Minute)
	sim.ExpectFailed(1)
}

// Rule 3: every pool keeps its poolSizing.minFree* free.

// floorPools has an A10G pool of one-GPU workers that keeps two GPUs free.
const floorPools = `
worker:
  pools:
    a10g:
      gpuType: A10G
      poolSizing:
        defaultWorkerCPU: 4000m
        defaultWorkerMemory: 8Gi
        defaultWorkerGpuCount: "1"
        minFreeGPU: "2"
`

// freeGPUs counts the GPUs on the pool's workers that are ready and unused.
func freeGPUs(sim *Sim, pool string) int {
	free := 0
	for _, worker := range sim.Workers(pool) {
		if worker.Status == types.WorkerStatusAvailable {
			free += int(worker.FreeGpuCount)
		}
	}
	return free
}

func TestPoolReachesItsFloorFromZeroAndHoldsIt(t *testing.T) {
	sim := New(t, floorPools)
	sim.Pool("a10g").IdleAfter(time.Minute)

	sim.Advance(2 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	require.Equal(t, 2, sim.Pool("a10g").Started())

	// The workers holding the floor have nothing to run for a quarter of an
	// hour, far past their idle timeout. They stay, and none is replaced.
	sim.Advance(15 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	require.Equal(t, 2, sim.Pool("a10g").Started())
}

func TestPoolKeepsItsFloorFreeUnderLoadAndReturnsToItAfter(t *testing.T) {
	sim := New(t, floorPools)
	sim.Pool("a10g").IdleAfter(time.Minute)
	sim.Advance(2 * time.Minute)

	// Three containers take the two free GPUs and one more. The pool adds
	// workers until two GPUs are free again.
	ids := sim.Submit(Endpoint("A10G").For(10*time.Minute), 3)
	sim.Advance(3 * time.Minute)

	require.Equal(t, []string{"a10g", "a10g", "a10g"}, sim.PoolsOf(ids))
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	require.Equal(t, 5, sim.Pool("a10g").Started())

	// The load ends. Workers above the floor idle out; the two that hold it
	// stay, so nothing has to be started to replace them.
	sim.Advance(20 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	sim.ExpectWorkers("a10g", 2)
	require.Equal(t, 5, sim.Pool("a10g").Started())
}

// The floor is best effort. A pool that cannot get workers holds what it has
// and keeps trying; when the cluster has room again it reaches the floor.
func TestPoolThatCannotGetWorkersReachesItsFloorOnceItCan(t *testing.T) {
	sim := New(t, floorPools)
	sim.Pool("a10g").RefuseWorkers()

	sim.Advance(time.Minute)
	require.Equal(t, 0, freeGPUs(sim, "a10g"))
	require.Equal(t, 0, sim.Pool("a10g").Started())
	require.Positive(t, sim.Pool("a10g").Refused())

	sim.Pool("a10g").AcceptWorkers()
	sim.Advance(2 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	require.Equal(t, 2, sim.Pool("a10g").Started())
}

// A pool that says how many GPUs to keep free, and nothing about its
// workers, adds workers with a GPU each. A worker without one would not
// count towards the floor, and the pool would add another every second.
func TestGPUFloorAloneAddsWorkersWithAGPU(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    a10g:
      gpuType: A10G
      poolSizing:
        minFreeGPU: "2"
`)

	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "a10g"))
	require.Equal(t, 2, sim.Pool("a10g").Started())
}

// A pool with no gpuType has no GPUs to keep free, so it adds no worker for
// a GPU floor. Its other floors hold.
func TestGPUFloorOnAPoolWithoutGPUsAddsNothing(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    cpu:
      poolSizing:
        minFreeGPU: "1"
    cpu-floor:
      poolSizing:
        defaultWorkerCPU: 4000m
        minFreeCPU: 4000m
        minFreeGPU: "1"
`)

	sim.Advance(5 * time.Minute)
	require.Zero(t, sim.Pool("cpu").Started())
	require.Equal(t, 1, sim.Pool("cpu-floor").Started())
}

// A worker that stops without warning leaves requests behind: the ones the
// scheduler queued on it before it noticed. They must return to the backlog
// and run somewhere else.

func TestRequestsQueuedOnALostWorkerRunElsewhere(t *testing.T) {
	sim := New(t, onePool)
	sim.Warm("a10g")

	worker := sim.Pool("a10g").Workers()[0]
	worker.Vanish()

	// For up to a minute the scheduler still believes the worker is there
	// and places work on it.
	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(5 * time.Second)
	require.Len(t, sim.Events("container.placed"), 2, "the request and the warm-up should both have been placed")
	require.Equal(t, []string{""}, sim.PoolsOf(ids), "nothing can have started on a dead worker")

	// The worker's state expires, the cleaner removes its Job, and the
	// request is placed again on a new worker.
	sim.Advance(5 * time.Minute)
	sim.ExpectRequeued(1)
	require.Equal(t, []string{"a10g"}, sim.PoolsOf(ids))
	require.NotEqual(t, worker.ID, sim.Container(ids[0]).Runs[0].WorkerID)
}

func TestRequestsDeliveredToALostWorkerRunElsewhere(t *testing.T) {
	sim := New(t, onePool)
	sim.Warm("a10g")

	// The worker takes the request off its queue and dies before starting
	// the container.
	worker := sim.Pool("a10g").Workers()[0]
	worker.VanishAfterTakingWork()
	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(5 * time.Second)
	require.False(t, worker.Running())
	require.Equal(t, []string{""}, sim.PoolsOf(ids))

	sim.Advance(5 * time.Minute)
	sim.ExpectRequeued(1)
	require.Equal(t, []string{"a10g"}, sim.PoolsOf(ids))
	require.NotEqual(t, worker.ID, sim.Container(ids[0]).Runs[0].WorkerID)
}

// A worker claims a request before it starts the container, and from then on
// the request is nowhere but with the worker. A worker that is lost between
// the two, in the middle of an image pull say, leaves a container that says
// it is starting and never will. The scheduler cannot place it again, so it
// ends it: whoever asked for the container sees that it is gone, and asks
// for another.

const claimedLossPool = `
worker:
  pools:
    a10g:
      gpuType: A10G
`

// countedByItsOwner reports whether whoever asked for the container still
// counts on it: an endpoint's autoscaler counts the containers that are
// starting or running, and starts one for each that it is short of.
func countedByItsOwner(s *Sim, containerID string) bool {
	state, err := s.containerRepo.GetContainerState(containerID)
	if err != nil {
		return false
	}
	return state.Status == types.ContainerStatusPending || state.Status == types.ContainerStatusRunning
}

func TestAContainerClaimedByAWorkerThatDiedBeforeStartingItIsReplaced(t *testing.T) {
	sim := New(t, claimedLossPool)
	sim.Warm("a10g")

	// The worker claims the request and dies before the container starts.
	worker := sim.Pool("a10g").Workers()[0]
	worker.VanishAfterClaimingWork()
	lost := sim.Submit(Endpoint("A10G"), 1)[0]
	sim.Advance(5 * time.Second)
	require.False(t, worker.Running())
	require.True(t, countedByItsOwner(sim, lost), "the container looks like one that is starting")
	require.Empty(t, sim.backlog(), "and its request is with the worker alone")

	// The worker's record expires, and with it goes the container.
	sim.Advance(2 * time.Minute)
	require.False(t, countedByItsOwner(sim, lost))
	sim.ExpectRequeued(0)

	// Its owner starts another, which runs on a new worker.
	replacement := sim.Submit(Endpoint("A10G"), 1)[0]
	sim.Advance(2 * time.Minute)
	require.Equal(t, []string{"a10g"}, sim.PoolsOf([]string{replacement}))
	require.NotEqual(t, worker.ID, sim.Container(replacement).Runs[0].WorkerID)
	require.Empty(t, sim.Container(lost).Runs, "the container that was claimed never ran")
}

// A Kubernetes pool makes a token for each worker before it asks the cluster
// for the worker's Job. When the cluster refuses the Job, the pool revokes
// the token: a pool that goes on failing to add workers would otherwise
// leave a working credential behind for each one it tried.

func TestAPoolRevokesTheTokenOfEachWorkerItsClusterRefused(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    t4:
      gpuType: T4
`)

	// The request asks the pool for a worker as often as it may, and the
	// cluster refuses each one.
	sim.Pool("t4").RefuseWorkers()
	sim.Submit(Endpoint("T4"), 1)
	sim.Advance(2 * time.Minute)
	sim.ExpectFailed(1)
	refused := sim.Pool("t4").Refused()
	require.Positive(t, refused)
	made, revoked := sim.backend.workerTokens()
	require.Equal(t, refused, made, "each attempt made a token")
	require.Equal(t, refused, revoked, "and each token was revoked")

	// A worker the cluster accepts keeps its token.
	sim.Pool("t4").AcceptWorkers()
	ids := sim.Submit(Endpoint("T4"), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []string{"t4"}, sim.PoolsOf(ids))
	made, revoked = sim.backend.workerTokens()
	require.Equal(t, refused+1, made)
	require.Equal(t, refused, revoked)
}

// A managed pool can hold machines that nobody bought: an operator starts the
// agent on each one and it joins. The simulation has no such machines of its
// own, so these tests play the operator, the machine's agent and the
// gateway's reconcile of the pool.

// joinedPool is a managed pool of A10G machines that are joined by hand.
const joinedPool = `
worker:
  pools:
    joined-a10g:
      mode: external
      gpuType: A10G
`

// reconcilePool loads the pool into every scheduler, now and every
// poolSyncInterval from now on, as the gateway does with its managed pools.
func reconcilePool(s *Sim, name string) {
	s.t.Helper()
	reconcile := func() {
		for _, replica := range s.schedulers {
			if err := replica.EnsureAgentPool(adminWorkspace, s.poolState(name)); err != nil {
				s.t.Fatalf("schedsim: reconcile pool %s: %v", name, err)
			}
		}
	}
	reconcile()
	s.Clock.Every(s.ctx, poolSyncInterval, reconcile)
}

// joinedMachine is a machine that an operator joined to a pool.
type joinedMachine struct {
	*machine
	agentStopped bool
}

// joinByHand adds a machine to the pool the way an operator does: the machine
// is given a token for the pool, its agent joins with it, and then heartbeats
// for as long as it runs.
func joinByHand(s *Sim, pool, id string) *joinedMachine {
	s.t.Helper()
	s.mu.Lock()
	s.tokens[id] = pool
	s.mu.Unlock()

	joined := &joinedMachine{machine: &machine{sim: s, id: id, pool: pool, instanceID: id, offer: providers.Offer{
		GPU: s.Config.Worker.Pools[pool].GPUType, GPUCount: 1, CPUMillicores: defaultMachineCPU, MemoryMB: defaultMachineMemoryMB,
	}}}
	joined.join(s.Clock.Now())
	if joined.state != machineJoined {
		s.t.Fatalf("schedsim: machine %s did not join pool %s", id, pool)
	}
	s.Clock.Every(s.ctx, workerInterval, func() {
		if joined.state == machineJoined && !joined.agentStopped {
			joined.heartbeat(s.Clock.Now())
		}
	})
	return joined
}

// stopAgent ends the machine's agent: the gateway hears no more heartbeats
// from the machine. Its worker runs on.
func (m *joinedMachine) stopAgent() { m.agentStopped = true }

// A machine that dies leaves behind the requests the scheduler queued on its
// worker before it noticed. The gateway goes on reconciling the pool, and
// that must not keep the dead worker on record: its requests go back to the
// backlog only once its record has expired.

func TestRequestsQueuedOnAJoinedMachineThatDiedRunElsewhere(t *testing.T) {
	sim := New(t, joinedPool)
	reconcilePool(sim, "joined-a10g")
	lost := joinByHand(sim, "joined-a10g", "machine-a")
	sim.Advance(5 * time.Second)
	require.True(t, lost.worker.Running())

	// The machine shuts down: its agent stops first and its worker a little
	// later. The gateway counts the machine as gone a minute after the
	// agent's last heartbeat. That is before the worker's record expires, so
	// a reconcile in between disables the worker, and every reconcile after
	// it finds the machine gone again.
	lost.stopAgent()
	sim.Advance(poolSyncInterval + 10*time.Second)
	lost.stop()

	// Until then the scheduler believes the worker is there and places work
	// on it.
	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(5 * time.Second)
	require.Len(t, sim.Events(types.EventComputeActionContainerPlaced), 1)
	require.Equal(t, []string{""}, sim.PoolsOf(ids), "nothing can have started on a dead machine")

	// The operator joins another machine, and the request runs there.
	replacement := joinByHand(sim, "joined-a10g", "machine-b")
	sim.Advance(5 * time.Minute)
	sim.ExpectRequeued(1)
	require.Equal(t, replacement.worker.ID, sim.Container(ids[0]).Runs[0].WorkerID)
}

func TestRequestsQueuedOnAWorkerThatDiedOnAJoinedMachineRunElsewhere(t *testing.T) {
	sim := New(t, joinedPool)
	reconcilePool(sim, "joined-a10g")
	lost := joinByHand(sim, "joined-a10g", "machine-a")
	sim.Advance(5 * time.Second)
	require.True(t, lost.worker.Running())

	// The worker dies and its machine stays up: the agent keeps heartbeating,
	// so every reconcile finds a machine to look after and saves its worker.
	lost.worker.Vanish()

	ids := sim.Submit(Endpoint("A10G"), 1)
	sim.Advance(5 * time.Second)
	require.Len(t, sim.Events(types.EventComputeActionContainerPlaced), 1)
	require.Equal(t, []string{""}, sim.PoolsOf(ids), "nothing can have started on a dead worker")

	replacement := joinByHand(sim, "joined-a10g", "machine-b")
	sim.Advance(5 * time.Minute)
	sim.ExpectRequeued(1)
	require.Equal(t, replacement.worker.ID, sim.Container(ids[0]).Runs[0].WorkerID)
}

// A pool with a provisioning block buys its machines from one provider. These
// scenarios hold what such a pool must and must not do.

// spotPool is one pool that buys L40S machines from verda, up to four of
// them. Every machine has one GPU and costs a dollar an hour.
const spotPool = `
worker:
  pools:
    spot-l40s:
      gpuType: L40S
      provisioning:
        providers: [verda]
        spot: true
        maxNodes: 4
        idleTimeout: 10m
        bootTimeout: 15m
`

// spotPoolWithFloor is the same pool keeping two GPUs free.
const spotPoolWithFloor = spotPool + `
      poolSizing:
        minFreeGPU: "2"
`

// machines counts what exists at the provider, which is what is paid for.
func machines(sim *Sim, provider string) int {
	return len(sim.Provider(provider).Machines())
}

func TestSpotPoolBuysAMachineForARequestAndRunsItThere(t *testing.T) {
	sim := New(t, spotPool)
	sim.Provider("verda").BootAfter(3 * time.Minute)

	ids := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(time.Minute)

	// The machine is bought at once and is still booting. The request waits
	// for it.
	require.Equal(t, 1, machines(sim, "verda"))
	sim.ExpectWaiting(1)

	sim.Advance(3 * time.Minute)
	require.Equal(t, []string{"spot-l40s"}, sim.PoolsOf(ids))
	require.Equal(t, 1, sim.Provider("verda").Bought())
}

// Invariant 3: a spot pool buys only from its provider and only its GPU type.
func TestSpotPoolBuysOnlyItsGPUTypeFromItsProvider(t *testing.T) {
	sim := New(t, spotPool+`
    spot-a10g:
      gpuType: A10G
      provisioning:
        providers: [nebius]
        spot: true
        maxNodes: 4
`)
	// Both providers sell both GPU types as spot, and each is the cheaper
	// one for the GPU type that the other's pool buys.
	sim.Provider("verda").Stock("A10G", 10).Price("A10G", 50)
	sim.Provider("nebius").Stock("L40S", 10).Price("L40S", 10)

	sim.Submit(Endpoint("L40S").For(time.Hour), 2)
	sim.Submit(Endpoint("A10G").For(time.Hour), 1)
	sim.Advance(5 * time.Minute)

	sim.ExpectPlaced("spot-l40s", 2)
	sim.ExpectPlaced("spot-a10g", 1)
	for _, machine := range sim.Provider("verda").Machines() {
		require.Equal(t, "L40S", machine.GPU)
	}
	for _, machine := range sim.Provider("nebius").Machines() {
		require.Equal(t, "A10G", machine.GPU)
	}
	require.Equal(t, 2, machines(sim, "verda"))
	require.Equal(t, 1, machines(sim, "nebius"))
	// Each paid its own provider's price, not the lower one next door.
	require.Equal(t, int64(2*100+100), sim.HourlyCents())
}

// A pool that asks for spot machines never buys one that is not.
func TestSpotPoolDoesNotBuyOnDemandMachines(t *testing.T) {
	sim := New(t, spotPool)
	// verda has no spot L40S. It has the same machine on demand, for less
	// than its spot machines usually cost.
	sim.Provider("verda").Stock("L40S", 0).Sell(providers.Offer{
		ID: "l40s-on-demand", Cloud: "verda", Region: "fin-01", GPU: "L40S", GPUCount: 1,
		CPUMillicores: 8000, MemoryMB: 32 * 1024, HourlyCostMicros: 500_000,
	})

	sim.Submit(Endpoint("L40S"), 1)
	sim.Advance(5 * time.Minute)

	require.Zero(t, sim.Provider("verda").Bought())
	sim.ExpectWaiting(1)
}

// Invariant 3: it never holds more than maxNodes machines.
func TestSpotPoolNeverHoldsMoreThanMaxNodes(t *testing.T) {
	sim := New(t, spotPoolWithFloor)
	sim.Provider("verda").BootAfter(2 * time.Minute)

	sim.Submit(Endpoint("L40S").For(time.Hour), 9)
	for range 60 {
		sim.Advance(10 * time.Second)
		require.LessOrEqual(t, machines(sim, "verda"), 4)
	}

	// Four machines run four containers. The rest wait: the pool has no room
	// and may not buy any.
	sim.ExpectPlaced("spot-l40s", 4)
	sim.ExpectWaiting(5)
	require.Equal(t, 4, sim.Provider("verda").Bought())
}

// Invariant 3: it never exceeds the spend cap, which covers every pool.
func TestPoolsThatBuyStayUnderTheSpendCap(t *testing.T) {
	sim := New(t, spotPool+`
    spot-a10g:
      gpuType: A10G
      provisioning:
        providers: [nebius]
        spot: true
        maxNodes: 4
providers:
  maxHourlyCents: 250
`)
	sim.Provider("verda").Price("L40S", 100)
	sim.Provider("nebius").Price("A10G", 100)

	sim.Submit(Endpoint("L40S").For(time.Hour), 3)
	sim.Submit(Endpoint("A10G").For(time.Hour), 3)
	for range 30 {
		sim.Advance(10 * time.Second)
		require.LessOrEqual(t, sim.HourlyCents(), int64(250))
	}

	// $2.50 an hour buys two machines at a dollar each, whichever pool asks.
	require.Equal(t, 2, machines(sim, "verda")+machines(sim, "nebius"))
	sim.ExpectWaiting(4)

	// A machine that leaves makes room under the cap for another. The pool
	// that plans first bought both.
	sim.Provider("nebius").Reclaim(1)
	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, machines(sim, "verda")+machines(sim, "nebius"))
	require.Equal(t, 3, sim.Provider("verda").Bought()+sim.Provider("nebius").Bought())
}

// A machine dearer than maxNodeHourlyCents is not bought.
func TestSpotPoolDoesNotPayMoreThanItsPriceLimit(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    spot-l40s:
      gpuType: L40S
      provisioning:
        providers: [verda]
        spot: true
        maxNodes: 4
        maxNodeHourlyCents: 80
`)
	sim.Provider("verda").Price("L40S", 100)
	sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(3 * time.Minute)
	require.Zero(t, sim.Provider("verda").Bought())

	// The price falls under the limit, and the pool buys.
	sim.Provider("verda").Price("L40S", 70)
	sim.Advance(3 * time.Minute)
	require.Equal(t, 1, sim.Provider("verda").Bought())
	sim.ExpectPlaced("spot-l40s", 1)
}

// Invariant 3: booting machines count as capacity.
func TestBootingMachinesCountTowardsTheFloor(t *testing.T) {
	sim := New(t, spotPoolWithFloor)
	// The machines take longer to boot than the ten minutes a worker that
	// is starting is otherwise given.
	sim.Provider("verda").BootAfter(12 * time.Minute)

	// Two machines cover the floor of two GPUs. While they boot the pool is
	// under its floor every second, and buys nothing more.
	sim.Advance(11 * time.Minute)
	require.Equal(t, 2, sim.Provider("verda").Bought())
	require.Equal(t, 0, freeGPUs(sim, "spot-l40s"))
	sim.ExpectWorkers("spot-l40s", 2)

	sim.Advance(3 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, sim.Provider("verda").Bought())
}

func TestRequestsWaitForABootingMachineThatHasRoomForThem(t *testing.T) {
	sim := New(t, spotPool)
	sim.Provider("verda").BootAfter(6*time.Minute).Machine(4, 16_000, 64*1024)

	// One machine has four GPUs. Three requests ask for one each, a minute
	// apart, while it boots.
	ids := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(time.Minute)
	ids = append(ids, sim.Submit(Endpoint("L40S").For(time.Hour), 1)...)
	sim.Advance(time.Minute)
	ids = append(ids, sim.Submit(Endpoint("L40S").For(time.Hour), 1)...)
	sim.Advance(6 * time.Minute)

	require.Equal(t, []string{"spot-l40s", "spot-l40s", "spot-l40s"}, sim.PoolsOf(ids))
	require.Equal(t, 1, sim.Provider("verda").Bought())
}

// Invariant 4: a spot pool with a floor converges to it when stock allows,
// and never releases a machine that holds the floor.
func TestSpotPoolReachesItsFloorFromZeroAndHoldsIt(t *testing.T) {
	sim := New(t, spotPoolWithFloor)

	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	held := sim.Provider("verda").Machines()
	require.Len(t, held, 2)

	// The two machines have nothing to run for an hour, far past the idle
	// timeout. They are the floor: neither is released, so neither has to be
	// bought again.
	sim.Advance(time.Hour)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, held, sim.Provider("verda").Machines())
	require.Equal(t, 2, sim.Provider("verda").Bought())
}

func TestSpotPoolKeepsItsFloorFreeUnderLoadAndReturnsToItAfter(t *testing.T) {
	sim := New(t, spotPoolWithFloor)
	sim.Advance(5 * time.Minute)

	// Two containers take the two free GPUs. The pool buys two more machines
	// to have two GPUs free again, which is all maxNodes allows.
	ids := sim.Submit(Endpoint("L40S").For(20*time.Minute), 2)
	sim.Advance(5 * time.Minute)
	require.Equal(t, []string{"spot-l40s", "spot-l40s"}, sim.PoolsOf(ids))
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 4, machines(sim, "verda"))

	// The load ends. The machines above the floor are released once they
	// have been idle for ten minutes; the floor stays.
	sim.Advance(40 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, machines(sim, "verda"))
	require.Equal(t, 4, sim.Provider("verda").Bought())
}

func TestSpotPoolWithoutAFloorReleasesEveryIdleMachine(t *testing.T) {
	sim := New(t, spotPool)

	sim.Submit(Endpoint("L40S").For(5*time.Minute), 2)
	sim.Advance(4 * time.Minute)
	require.Equal(t, 2, machines(sim, "verda"))

	// Idle for less than the timeout: kept, in case more work comes.
	sim.Advance(9 * time.Minute)
	require.Equal(t, 2, machines(sim, "verda"))

	sim.Advance(5 * time.Minute)
	require.Zero(t, machines(sim, "verda"))
	sim.ExpectWorkers("spot-l40s", 0)
}

// The floor is best effort: without stock the pool holds what it has and
// keeps trying.
func TestSpotPoolWithoutStockReachesItsFloorOnceThereIsSome(t *testing.T) {
	sim := New(t, spotPoolWithFloor)
	sim.Provider("verda").Stock("L40S", 1)

	sim.Advance(10 * time.Minute)
	require.Equal(t, 1, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 1, machines(sim, "verda"))

	sim.Provider("verda").Stock("L40S", 5)
	sim.Advance(10 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, machines(sim, "verda"))
}

// Invariant 2: a pool that cannot grow is skipped without spending an
// attempt, so demand spreads over pools as each runs out.
func TestDemandSpreadsOverSpotPoolsAsEachRunsOutOfStock(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    spot-verda-l40s:
      gpuType: L40S
      priority: 20
      provisioning: {providers: [verda], spot: true, maxNodes: 8}
    spot-nebius-l40s:
      gpuType: L40S
      priority: 10
      provisioning: {providers: [nebius], spot: true, maxNodes: 8}
`)
	sim.Provider("verda").Stock("L40S", 2)
	sim.Provider("nebius").Stock("L40S", 8)

	sim.Submit(Endpoint("L40S").For(time.Hour), 6)
	sim.Advance(6 * time.Minute)

	sim.ExpectPlaced("spot-verda-l40s", 2)
	sim.ExpectPlaced("spot-nebius-l40s", 4)
	sim.ExpectWaiting(0)
	sim.ExpectFailed(0)

	// All six waited for verda at first. Nothing was bought at nebius for
	// the two that verda had a machine for.
	require.Equal(t, 2, sim.Provider("verda").Bought())
	require.Equal(t, 4, sim.Provider("nebius").Bought())
}

// A pool whose provider refuses the purchase is left alone for a while, and
// the request moves on.
func TestRequestMovesOnWhenTheProviderRefusesToSell(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    spot-verda-l40s:
      gpuType: L40S
      priority: 20
      provisioning: {providers: [verda], spot: true, maxNodes: 8}
    spot-nebius-l40s:
      gpuType: L40S
      priority: 10
      provisioning: {providers: [nebius], spot: true, maxNodes: 8}
`)
	// verda lists the machine and then has none: the catalogue was stale.
	sim.Provider("verda").RefuseCreates(providers.ErrOutOfStock)

	ids := sim.Submit(Endpoint("L40S").For(time.Hour), 2)
	sim.Advance(2 * time.Minute)

	require.Equal(t, []string{"spot-nebius-l40s", "spot-nebius-l40s"}, sim.PoolsOf(ids))
	require.Zero(t, machines(sim, "verda"))

	// Two minutes on, verda lists the machine as before. It refused so
	// recently that it is not asked again.
	ids = append(ids, sim.Submit(Endpoint("L40S").For(time.Hour), 1)...)
	sim.Advance(2 * time.Minute)
	require.Equal(t, "spot-nebius-l40s", sim.Container(ids[2]).Pool())
}

// A10G requests overflow into a spot pool of another GPU type that serves
// them, once the A10G pool cannot take them.
func TestRequestsOverflowIntoASpotPoolThatAlsoServesThem(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    a10g:
      gpuType: A10G
    spot-l40s:
      gpuType: L40S
      alsoServes: [A10G]
      provisioning: {providers: [verda], spot: true, maxNodes: 4}
`)
	sim.Pool("a10g").RefuseWorkers()

	ids := sim.Submit(Endpoint("A10G").For(time.Hour), 2)
	sim.Advance(6 * time.Minute)
	require.Equal(t, []string{"spot-l40s", "spot-l40s"}, sim.PoolsOf(ids))
}

// A spot pool takes only work that opted in to interruption.
func TestSpotPoolTakesOnlyWorkThatMayBeInterrupted(t *testing.T) {
	sim := New(t, spotPool)

	sim.Submit(Function("L40S"), 1)
	sim.Advance(5 * time.Minute)

	require.Zero(t, sim.Provider("verda").Bought())
	sim.ExpectPlaced("spot-l40s", 0)
}

// Invariant 5: every record is polled, so a machine the provider took back
// is found, forgotten and replaced.
func TestReclaimedMachineIsFoundAndReplaced(t *testing.T) {
	sim := New(t, spotPoolWithFloor)
	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))

	sim.Provider("verda").Reclaim(1)

	// Well inside the minute that a dead worker's state takes to expire, the
	// machine's worker is gone and its replacement is on the way.
	sim.Advance(20 * time.Second)
	require.Equal(t, 1, freeGPUs(sim, "spot-l40s"))
	sim.ExpectWorkers("spot-l40s", 2)
	require.Equal(t, 2, machines(sim, "verda"))

	sim.Advance(2 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 3, sim.Provider("verda").Bought())
	require.Len(t, sim.MachineRecords(), 2)
}

// Invariant 7, for a machine that was bought: a request that was on its way
// to a machine the provider took back returns to the backlog and runs on the
// machine that replaces it.
func TestRequestQueuedOnAReclaimedMachineReturnsToTheBacklog(t *testing.T) {
	sim := New(t, spotPool)
	// One machine has room for two containers.
	sim.Provider("verda").Machine(2, 16_000, 64*1024)
	first := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []string{"spot-l40s"}, sim.PoolsOf(first))

	// The machine takes the next request off its queue and is gone before it
	// starts it.
	workers := sim.Pool("spot-l40s").Workers()
	require.Len(t, workers, 1)
	workers[0].VanishAfterTakingWork()
	second := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(5 * time.Second)
	require.False(t, workers[0].Running())
	require.Empty(t, sim.Container(second[0]).Runs)
	sim.Provider("verda").Reclaim(1)

	sim.Advance(6 * time.Minute)
	sim.ExpectRequeued(1)
	runs := sim.Container(second[0]).Runs
	require.Len(t, runs, 1)
	require.Equal(t, "spot-l40s", runs[0].Pool)
	require.NotEqual(t, workers[0].ID, runs[0].WorkerID)
	require.Equal(t, 2, sim.Provider("verda").Bought())
}

// A machine that is cordoned because it is about to be taken back no longer
// counts as room in its pool, which buys its replacement while the machine
// is still there. When the provider takes it, nothing is missing.
func TestCordonedMachineIsReplacedBeforeItIsTaken(t *testing.T) {
	sim := New(t, spotPoolWithFloor)
	verda := sim.Provider("verda")
	sim.Advance(5 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, verda.Bought())

	verda.Cordon(1)
	sim.Advance(20 * time.Second)
	require.Equal(t, 1, freeGPUs(sim, "spot-l40s"), "the machine that is going is not room")
	require.Equal(t, 3, verda.Bought(), "its replacement is on the way")

	sim.Advance(time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 3, machines(sim, "verda"), "the machine is still there")

	verda.Reclaim(1)
	sim.Advance(time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, machines(sim, "verda"))
	require.Len(t, sim.MachineRecords(), 2)
	require.Equal(t, 3, verda.Bought(), "nothing more had to be bought")
}

// A cordoned machine is given no new work, and goes on running what it has.
func TestCordonedMachineTakesNoNewWork(t *testing.T) {
	sim := New(t, spotPool)
	// One machine has room for two containers.
	verda := sim.Provider("verda").Machine(2, 16_000, 64*1024)
	first := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(2 * time.Minute)
	require.Equal(t, []string{"spot-l40s"}, sim.PoolsOf(first))
	going := sim.Pool("spot-l40s").Workers()[0]

	verda.Cordon(1)
	second := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(3 * time.Minute)

	// The second container did not take the room the machine has left. It
	// runs on a machine that was bought for it.
	require.Equal(t, 2, verda.Bought())
	runs := sim.Container(second[0]).Runs
	require.Len(t, runs, 1)
	require.NotEqual(t, going.ID, runs[0].WorkerID)
	require.True(t, going.Running(), "the machine runs on until it is taken")
	require.Len(t, sim.Container(first[0]).Runs, 1)
	require.Equal(t, going.ID, sim.Container(first[0]).Runs[0].WorkerID)
	sim.ExpectFailed(0)

	// The next container takes the room that is left on the machine that
	// stays, as it would have on the first.
	third := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(time.Minute)
	require.Len(t, sim.Container(third[0]).Runs, 1)
	require.Equal(t, runs[0].WorkerID, sim.Container(third[0]).Runs[0].WorkerID)
	require.Equal(t, 2, verda.Bought())
}

// The cordon is kept on the machine, not only on its worker: a worker that
// is started again on a cordoned machine is given no work either.
func TestCordonedMachineStaysOutOfUseWhenItsWorkerStartsAgain(t *testing.T) {
	sim := New(t, spotPool)
	verda := sim.Provider("verda").Machine(2, 16_000, 64*1024)
	sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(2 * time.Minute)
	going := sim.Pool("spot-l40s").Workers()[0]

	verda.Cordon(1)
	going.LoseRecord()
	sim.Advance(2 * time.Minute)
	require.True(t, going.Running(), "the worker is up again")

	second := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(3 * time.Minute)
	runs := sim.Container(second[0]).Runs
	require.Len(t, runs, 1)
	require.NotEqual(t, going.ID, runs[0].WorkerID)
	require.Equal(t, 2, verda.Bought())
}

// Invariant 5: tagged instances with no record are terminated.
func TestInstanceNobodyRecordedIsTerminated(t *testing.T) {
	sim := New(t, spotPool)
	orphan := sim.Provider("verda").Orphan()
	// Another control plane on the same account, and a machine this one
	// bought for a tenant, are not the pool's to delete.
	foreign := sim.Provider("verda").ForeignMachine()
	tenant := sim.Provider("verda").TenantMachine()

	sim.Advance(2 * time.Minute)

	left := sim.Provider("verda").InstanceIDs()
	require.NotContains(t, left, orphan)
	require.Contains(t, left, foreign)
	require.Contains(t, left, tenant)
}

// Invariant 6: a machine that has not joined by its boot timeout is
// terminated.
func TestMachineThatNeverJoinsIsTerminatedAtItsBootTimeout(t *testing.T) {
	sim := New(t, `
worker:
  pools:
    spot-l40s:
      gpuType: L40S
      provisioning: {providers: [verda], spot: true, maxNodes: 4, bootTimeout: 5m}
`)
	sim.Provider("verda").NeverJoin()

	sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(4*time.Minute + 30*time.Second)
	first := sim.Provider("verda").InstanceIDs()
	require.Len(t, first, 1)
	// The machine counts as capacity for as long as it may still join, so
	// nothing else is bought for the request.
	sim.ExpectWorkers("spot-l40s", 1)
	require.Equal(t, 1, sim.Provider("verda").Bought())

	// Machines join again from here on, so whatever is bought next works.
	sim.Provider("verda").JoinAgain()
	sim.Advance(time.Minute)
	require.NotContains(t, sim.Provider("verda").InstanceIDs(), first[0])

	// The request did not go down with the machine: another is bought for it.
	sim.Advance(5 * time.Minute)
	sim.ExpectPlaced("spot-l40s", 1)
	require.Equal(t, 2, sim.Provider("verda").Bought())
	require.Equal(t, 1, machines(sim, "verda"))
}

// Invariant 5: the record is written before the provider is asked, so a
// machine whose creation was never confirmed is not lost. It is found by
// its tag, counted, and used.
func TestMachineWhoseCreationWasNeverConfirmedIsFoundAndUsed(t *testing.T) {
	sim := New(t, spotPool)
	sim.Provider("verda").BootAfter(2 * time.Minute).LoseNextAnswer()

	ids := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(time.Minute)
	// The purchase failed as far as the scheduler can tell, and the machine
	// exists. It has a record, and a worker that requests can wait for.
	require.Equal(t, 1, machines(sim, "verda"))
	records := sim.MachineRecords()
	require.Len(t, records, 1)
	require.Equal(t, sim.Provider("verda").Machines()[0].ID, records[0].InstanceID)
	sim.ExpectWorkers("spot-l40s", 1)

	sim.Advance(3 * time.Minute)
	require.Equal(t, []string{"spot-l40s"}, sim.PoolsOf(ids))
	require.Equal(t, 1, sim.Provider("verda").Bought(), "the request ran on the machine that was already bought")
}

// Invariant 5, when two things go wrong with one purchase: its answer is lost,
// and the provider then fails to start the machine. The record still leads
// to the instance, which is deleted through it. At no time is there an
// instance without a record.
func TestMachineWhoseCreationWasNeverConfirmedAndThatFailedIsDeleted(t *testing.T) {
	sim := New(t, spotPool)
	sim.Provider("verda").BootAfter(5 * time.Second).LoseNextAnswer().FailNextBoot()

	sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	for range 96 {
		sim.Advance(5 * time.Second)
		require.Empty(t, unrecorded(sim, "verda"))
	}

	// The failed machine is gone, and the request runs on the next one,
	// which is bought once the offer that failed is tried again.
	require.Equal(t, 2, sim.Provider("verda").Bought())
	require.Len(t, sim.Provider("verda").InstanceIDs(), 1)
	require.Len(t, sim.MachineRecords(), 1)
	sim.ExpectPlaced("spot-l40s", 1)
}

// unrecorded returns the instances at the provider that no record accounts
// for.
func unrecorded(sim *Sim, provider string) []string {
	recorded := map[string]bool{}
	for _, record := range sim.MachineRecords() {
		recorded[record.ID] = true
	}
	var ids []string
	for _, machine := range sim.Provider(provider).Machines() {
		if !recorded[machine.MachineID] {
			ids = append(ids, machine.ID)
		}
	}
	return ids
}

// A machine the provider fails to start is deleted there, not left to cost
// money.
func TestMachineTheProviderFailedToStartIsDeleted(t *testing.T) {
	sim := New(t, spotPool)
	sim.Provider("verda").FailNextBoot()

	sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(8 * time.Minute)

	require.Equal(t, 2, sim.Provider("verda").Bought())
	require.Equal(t, 1, machines(sim, "verda"))
	require.Len(t, sim.Provider("verda").InstanceIDs(), 1)
	sim.ExpectPlaced("spot-l40s", 1)
}

// What the provider failed to start is not bought again at once. The next
// machine is the next cheapest the provider sells, so the request does not
// wait while the pool buys the same machine over and over.
func TestPoolBuysSomethingElseAfterAMachineFailsToStart(t *testing.T) {
	sim := New(t, spotPool)
	verda := sim.Provider("verda")
	verda.Sell(providers.Offer{
		ID: "l40s-spot", Cloud: "verda", Region: "sim-2", GPU: "L40S", GPUCount: 1,
		CPUMillicores: 8000, MemoryMB: 32 * 1024, HourlyCostMicros: 1_500_000, Spot: true,
	})
	verda.FailNextBoot()

	sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(2 * time.Minute)

	sim.ExpectPlaced("spot-l40s", 1)
	require.Equal(t, 2, verda.Bought())
	bought := verda.Machines()
	require.Len(t, bought, 1)
	require.EqualValues(t, 150, bought[0].HourlyCents, "the machine in the other region")
}

// A request that waits for a machine to boot stops waiting when the machine
// is taken back. Its provider has none to spare, so the pool, which buys
// nothing else, buys the same again after five minutes and not at once.
func TestRequestWaitingForAMachineThatIsReclaimedWhileBootingGetsAnother(t *testing.T) {
	sim := New(t, spotPool)
	verda := sim.Provider("verda").BootAfter(2 * time.Minute)

	ids := sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(30 * time.Second)
	require.Equal(t, 1, verda.Bought())
	verda.Reclaim(1)

	// The next check of the machines finds it gone, and the request waits
	// for the pool to buy again, not for the time a machine has to boot.
	sim.Advance(4 * time.Minute)
	require.Equal(t, 1, verda.Bought())
	require.Zero(t, machines(sim, "verda"))
	sim.Advance(2 * time.Minute)
	require.Equal(t, 2, verda.Bought())
	require.Equal(t, 1, machines(sim, "verda"))

	sim.Advance(3 * time.Minute)
	sim.ExpectPlaced("spot-l40s", 1)
	sim.ExpectFailed(0)
	require.Equal(t, 2, verda.Bought())
	require.Len(t, sim.Container(ids[0]).Runs, 1)
}

// A pool that is deleted has its machines released.
func TestMachinesOfADeletedPoolAreReleased(t *testing.T) {
	sim := New(t, spotPoolWithFloor)
	sim.Submit(Endpoint("L40S").For(time.Hour), 1)
	sim.Advance(5 * time.Minute)
	require.Equal(t, 3, machines(sim, "verda"))

	sim.DeletePool("spot-l40s")

	// Nothing is given back for five minutes: a gateway that is restarting
	// with a new config looks the same as a pool that was deleted.
	sim.Advance(4 * time.Minute)
	require.Equal(t, 3, machines(sim, "verda"))

	// Then the idle machines go. The busy one is given ten minutes for its
	// container to finish, and goes too.
	sim.Advance(2 * time.Minute)
	require.Equal(t, 1, machines(sim, "verda"))
	sim.Advance(10 * time.Minute)
	require.Zero(t, machines(sim, "verda"))
	require.Equal(t, 3, sim.Provider("verda").Bought())
	require.Empty(t, sim.MachineRecords())
	sim.ExpectWorkers("spot-l40s", 0)
}

// Two scheduler replicas, each with its own provisioner, share the records.
func TestTwoSchedulersNeverBuyMoreThanMaxNodesBetweenThem(t *testing.T) {
	sim := New(t, spotPoolWithFloor)
	sim.AddScheduler()
	sim.Provider("verda").BootAfter(2 * time.Minute)

	sim.Submit(Endpoint("L40S").For(time.Hour), 9)
	for range 60 {
		sim.Advance(10 * time.Second)
		require.LessOrEqual(t, machines(sim, "verda"), 4)
	}
	sim.ExpectPlaced("spot-l40s", 4)
	require.Equal(t, 4, sim.Provider("verda").Bought())
}

func TestTwoSchedulersBuyTheFloorOnce(t *testing.T) {
	sim := New(t, spotPoolWithFloor)
	sim.AddScheduler()

	sim.Advance(30 * time.Minute)
	require.Equal(t, 2, freeGPUs(sim, "spot-l40s"))
	require.Equal(t, 2, sim.Provider("verda").Bought())
}

// Seeded random runs. Load, stock, prices, boot times, refusals and reclaims
// change at random for half an hour, and what a pool that buys must never do
// is checked every ten seconds. A failure names its seed, which replays it.
// An hour of simulated time takes about half a minute, so the runs share the
// machine's cores.

const randomPools = `
worker:
  pools:
    spot-l40s:
      gpuType: L40S
      provisioning: {providers: [verda], spot: true, maxNodes: 4, idleTimeout: 2m, bootTimeout: 3m}
      poolSizing:
        minFreeGPU: "1"
    spot-a10g:
      gpuType: A10G
      provisioning: {providers: [nebius], spot: true, maxNodes: 3, idleTimeout: 2m, bootTimeout: 3m}
providers:
  maxHourlyCents: 500
`

const (
	randomStep = 10 * time.Second
	// tickSlack is how long the provisioner may take to act on something:
	// two of its passes.
	tickSlack = 30 * time.Second
	// launchGrace is how long a purchase the provider never answered is
	// waited for.
	launchGrace = 2 * time.Minute
)

// boughtFrom says what each provider's pool may hold.
var boughtFrom = map[string]struct {
	pool        string
	gpu         string
	maxNodes    int
	bootTimeout time.Duration
}{
	"verda":  {pool: "spot-l40s", gpu: "L40S", maxNodes: 4, bootTimeout: 3 * time.Minute},
	"nebius": {pool: "spot-a10g", gpu: "A10G", maxNodes: 3, bootTimeout: 3 * time.Minute},
}

func TestSpotPoolsKeepTheirInvariantsUnderRandomLoadStockAndReclaims(t *testing.T) {
	// Two runs by default, one of them with two gateways. SCHEDSIM_SEEDS=40
	// runs seeds 1 to 40.
	seeds := 2
	if given := os.Getenv("SCHEDSIM_SEEDS"); given != "" {
		var err error
		seeds, err = strconv.Atoi(given)
		require.NoError(t, err, "SCHEDSIM_SEEDS")
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed %d", seed), func(t *testing.T) {
			t.Parallel() // each run has its own Redis, clock and schedulers
			run := newRandomRun(t, seed)
			for range 180 {
				run.act()
				run.step()
			}

			// Everything calms down: stock, no reclaims, machines that boot.
			for name, bought := range boughtFrom {
				run.sim.Provider(name).Stock(bought.gpu, -1).Price(bought.gpu, 100).
					BootAfter(30 * time.Second).JoinAgain().RefuseCreates(nil)
			}
			// Invariant 4: within half an hour the floor is met by exactly
			// what it takes and nothing else is paid for, and it stays so.
			sim := run.sim
			settled := func() bool {
				return len(sim.Waiting()) == 0 && freeGPUs(sim, "spot-l40s") == 1 &&
					machines(sim, "verda") == 1 && machines(sim, "nebius") == 0
			}
			for i := 0; i < 180 && !settled(); i++ {
				run.step()
			}
			for range 30 {
				run.step()
			}
			sim.ExpectWaiting(0)
			require.Equal(t, 1, freeGPUs(sim, "spot-l40s"))
			require.Equal(t, 1, machines(sim, "verda"))
			require.Zero(t, machines(sim, "nebius"))
			require.Len(t, sim.MachineRecords(), 1)
			sim.ExpectWorkers("spot-l40s", 1)
			sim.ExpectWorkers("spot-a10g", 0)
		})
	}
}

type randomRun struct {
	t   *testing.T
	sim *Sim
	rng *rand.Rand

	// orphans are the instances planted without a record, and when.
	orphans map[string]time.Time
	// unbacked is since when a record has had no instance at its provider.
	unbacked map[string]time.Time
	// trail is what was done to the world, for the report of a failure.
	trail []string
}

func newRandomRun(t *testing.T, seed int64) *randomRun {
	run := &randomRun{
		t:        t,
		sim:      New(t, randomPools),
		rng:      rand.New(rand.NewSource(seed)),
		orphans:  map[string]time.Time{},
		unbacked: map[string]time.Time{},
	}
	// Every other run has two gateways.
	if seed%2 == 0 {
		run.sim.AddScheduler()
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed %d, the last things done:\n%s", seed, strings.Join(run.trail[max(0, len(run.trail)-25):], "\n"))
		}
	})
	return run
}

func (r *randomRun) did(format string, args ...any) {
	at := r.sim.Clock.Now().Format("15:04:05")
	r.trail = append(r.trail, at+" "+fmt.Sprintf(format, args...))
}

// act changes one thing about the world, or nothing.
func (r *randomRun) act() {
	name := []string{"verda", "nebius"}[r.rng.Intn(2)]
	provider, gpu := r.sim.Provider(name), boughtFrom[name].gpu

	switch roll := r.rng.Intn(100); {
	case roll < 30:
		// About three GPUs' worth of work on average, against the five the
		// spend cap allows: sometimes idle, sometimes more than fits.
		runFor := 30*time.Second + time.Duration(r.rng.Intn(180))*time.Second
		n := 1 + r.rng.Intn(2)
		r.sim.Submit(Endpoint(gpu).For(runFor), n)
		r.did("%d %s requests of %s", n, gpu, runFor)
	case roll < 38:
		stock := r.rng.Intn(6)
		provider.Stock(gpu, stock)
		r.did("%s has %d in stock", name, stock)
	case roll < 43:
		provider.Stock(gpu, -1)
		r.did("%s has endless stock", name)
	case roll < 51:
		provider.Reclaim(1)
		r.did("%s reclaims a machine", name)
	case roll < 57:
		boot := []time.Duration{10 * time.Second, 30 * time.Second, 2 * time.Minute, 4 * time.Minute}[r.rng.Intn(4)]
		provider.BootAfter(boot)
		r.did("%s machines boot in %s", name, boot)
	case roll < 62:
		cents := int64(50 + r.rng.Intn(101))
		provider.Price(gpu, cents)
		r.did("%s charges %d cents", name, cents)
	case roll < 67:
		refusal := []error{providers.ErrOutOfStock, providers.ErrQuota, errors.New("the provider is down")}[r.rng.Intn(3)]
		provider.RefuseCreates(refusal)
		r.did("%s refuses creates: %v", name, refusal)
	case roll < 73:
		provider.RefuseCreates(nil)
		r.did("%s accepts creates", name)
	case roll < 76:
		provider.NeverJoin()
		r.did("%s machines never join", name)
	case roll < 81:
		provider.JoinAgain()
		r.did("%s machines join", name)
	case roll < 84:
		provider.LoseNextAnswer()
		r.did("%s loses its next answer", name)
	case roll < 87:
		provider.FailNextBoot()
		r.did("%s fails its next boot", name)
	case roll < 90:
		orphan := provider.Orphan()
		r.orphans[orphan] = r.sim.Clock.Now()
		r.did("%s has %s, which nobody recorded", name, orphan)
	}
}

// step moves time on and checks every invariant that holds at all times.
func (r *randomRun) step() {
	r.sim.Advance(randomStep)
	t, sim, now := r.t, r.sim, r.sim.Clock.Now()

	recorded := map[string]bool{}
	for _, record := range sim.MachineRecords() {
		recorded[record.ID] = true
	}
	atProvider := map[string]bool{} // by the name the machine was bought under

	for name, bought := range boughtFrom {
		held := 0
		for _, machine := range sim.Provider(name).Machines() {
			atProvider[machine.MachineID] = true
			if planted, ok := r.orphans[machine.ID]; ok {
				// Invariant 5: a tagged instance with no record is deleted.
				require.LessOrEqual(t, now.Sub(planted), tickSlack, "%s has no record and still exists", machine.ID)
				continue
			}
			held++

			// Invariant 3: only its GPU type, and only what it asked for.
			require.Equal(t, bought.gpu, machine.GPU, "%s bought %s", bought.pool, machine.ID)
			require.True(t, machine.Spot, "%s bought %s on demand", bought.pool, machine.ID)
			// Invariant 5: whatever exists has a record.
			require.True(t, recorded[machine.MachineID], "%s (%s) exists and has no record", machine.ID, machine.MachineID)
			// Invariant 6: it has joined, or its boot timeout is not over.
			if !machine.Joined {
				require.LessOrEqual(t, now.Sub(machine.BoughtAt), bought.bootTimeout+tickSlack,
					"%s was bought %s ago and has not joined", machine.ID, now.Sub(machine.BoughtAt))
			}
		}
		// Invariant 3: never more than maxNodes.
		require.LessOrEqual(t, held, bought.maxNodes, "%s holds too many machines", bought.pool)
	}
	// Invariant 3: never over the spend cap.
	require.LessOrEqual(t, sim.HourlyCents(), int64(500))

	// Invariant 5: every record is polled, so one whose machine is gone
	// does not stay.
	for _, record := range sim.MachineRecords() {
		if atProvider[record.ID] {
			delete(r.unbacked, record.ID)
			continue
		}
		since, ok := r.unbacked[record.ID]
		if !ok {
			r.unbacked[record.ID] = now
			continue
		}
		allowed := tickSlack
		if record.InstanceID == "" {
			allowed += launchGrace
		}
		require.LessOrEqual(t, now.Sub(since), allowed, "the record of %s has had no machine for %s", record.ID, now.Sub(since))
	}
}

// A scenario is deterministic because of what the fake clock promises: timers
// run in time order, each at its own due time, on the goroutine that moved
// the clock. The tests below hold the clock to that.

var clockStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestFakeClockRunsTimersInTimeOrder(t *testing.T) {
	c := common.NewFakeClock(clockStart)
	var fired []string
	c.AfterFunc(3*time.Second, func() { fired = append(fired, "c") })
	c.AfterFunc(time.Second, func() { fired = append(fired, "a") })
	c.AfterFunc(time.Second, func() { fired = append(fired, "b") })

	c.Advance(2 * time.Second)
	require.Equal(t, []string{"a", "b"}, fired)
	require.Equal(t, clockStart.Add(2*time.Second), c.Now())

	c.Advance(time.Second)
	require.Equal(t, []string{"a", "b", "c"}, fired)
}

func TestFakeClockTimerSeesItsOwnDueTime(t *testing.T) {
	c := common.NewFakeClock(clockStart)
	var at time.Time
	c.AfterFunc(time.Second, func() { at = c.Now() })
	c.Advance(time.Minute)
	require.Equal(t, clockStart.Add(time.Second), at)
}

func TestFakeClockEveryTicksUntilCancelled(t *testing.T) {
	c := common.NewFakeClock(clockStart)
	ctx, cancel := context.WithCancel(context.Background())
	ticks := 0
	c.Every(ctx, time.Second, func() { ticks++ })

	c.Advance(3500 * time.Millisecond)
	require.Equal(t, 3, ticks)

	cancel()
	c.Advance(time.Minute)
	require.Equal(t, 3, ticks)
}

func TestFakeClockStop(t *testing.T) {
	c := common.NewFakeClock(clockStart)
	fired := false
	timer := c.AfterFunc(time.Second, func() { fired = true })
	require.True(t, timer.Stop())
	require.False(t, timer.Stop())
	c.Advance(time.Minute)
	require.False(t, fired)
}

func TestFakeClockRunsTimersAddedByCallbacks(t *testing.T) {
	c := common.NewFakeClock(clockStart)
	var fired []time.Duration
	c.AfterFunc(time.Second, func() {
		fired = append(fired, c.Now().Sub(clockStart))
		c.AfterFunc(time.Second, func() { fired = append(fired, c.Now().Sub(clockStart)) })
	})
	c.Advance(5 * time.Second)
	require.Equal(t, []time.Duration{time.Second, 2 * time.Second}, fired)
}

func TestFakeClockOnStepSeesEveryMove(t *testing.T) {
	c := common.NewFakeClock(clockStart)
	var total time.Duration
	c.OnStep(func(d time.Duration) { total += d })
	c.AfterFunc(time.Second, func() {})
	c.Advance(10 * time.Second)
	require.Equal(t, 10*time.Second, total)
}
