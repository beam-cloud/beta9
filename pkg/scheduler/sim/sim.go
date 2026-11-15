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
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/clock"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/provisioner"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc/test/bufconn"
	batchv1 "k8s.io/api/batch/v1"
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

	Clock  *clock.Fake
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
	fakeClock := clock.NewFake(time.Now().Truncate(time.Second))
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
