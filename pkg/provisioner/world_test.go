package provisioner

import (
	"bytes"
	"cmp"
	"context"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/clock"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// world is a provisioner with everything around it in memory: Redis, a fake
// clock, the fake provider verda, and stand-ins for the gateway and the
// pools' workers. It starts with one pool, spot-l40s, and one offer.
type world struct {
	t     *testing.T
	ctx   context.Context
	clock *clock.Fake
	rdb   *common.RedisClient
	store *Store

	verda   *providers.Fake
	control *fakeControl
	workers *fakeWorkers

	*Provisioner
}

const testOwner = "test"

func newWorld(t *testing.T, change ...func(*Options)) *world {
	t.Helper()

	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)

	w := &world{
		t:       t,
		ctx:     context.Background(),
		clock:   clock.NewFake(bought),
		rdb:     rdb,
		store:   NewStore(rdb),
		verda:   providers.NewFake("verda", testOwner),
		control: &fakeControl{pools: map[string]types.WorkerPoolConfig{"spot-l40s": spotL40S()}},
		workers: &fakeWorkers{workers: map[string]*types.Worker{}},
	}
	w.verda.SetOffers(l40s())
	w.Provisioner = w.replica(change...)
	return w
}

// replica returns another provisioner on the same Redis, clock and provider,
// as a second gateway would run.
func (w *world) replica(change ...func(*Options)) *Provisioner {
	opts := Options{
		Providers:    providers.RegistryOf(w.verda),
		ControlPlane: w.control,
		Workers:      w.workers,
		Store:        w.store,
		Clock:        w.clock,
	}
	for _, apply := range change {
		apply(&opts)
	}
	return New(opts)
}

// after moves time on and runs a tick.
func (w *world) after(d time.Duration) {
	w.t.Helper()
	w.clock.Advance(d)
	w.Tick(w.ctx)
}

// every ticks every interval until d has passed.
func (w *world) every(interval, d time.Duration) {
	w.t.Helper()
	for passed := time.Duration(0); passed < d; passed += interval {
		w.after(interval)
	}
}

// buy has the pool buy a machine, which must work, and returns its record.
func (w *world) buy(pool string) Machine {
	w.t.Helper()
	bought, err := w.Provision(w.ctx, pool, scheduler.MachineNeed{})
	require.NoError(w.t, err)
	return w.record(bought.MachineID)
}

// record returns a machine's record, which must exist.
func (w *world) record(id string) Machine {
	w.t.Helper()
	for _, machine := range w.records() {
		if machine.ID == id {
			return machine
		}
	}
	w.t.Fatalf("machine %s has no record", id)
	return Machine{}
}

func (w *world) records() []Machine {
	w.t.Helper()
	machines, err := w.store.List(w.ctx)
	require.NoError(w.t, err)
	return machines
}

// instances returns the IDs of the provider's instances that exist, whoever
// they belong to.
func (w *world) instances() []string {
	ids := []string{}
	for _, instance := range w.verda.Everything() {
		ids = append(ids, instance.ID)
	}
	return ids
}

// logged collects what is logged from now until the test ends.
func logged(t *testing.T) *bytes.Buffer {
	t.Helper()
	var lines bytes.Buffer
	original := log.Logger
	log.Logger = zerolog.New(&lines)
	t.Cleanup(func() { log.Logger = original })
	return &lines
}

// failWrites makes every write of a record fail, as it does when Redis
// cannot be written to, until the func it returns is called. Records can
// still be read. Something else holds the store's write lock, and a writer
// that finds it taken gives up at once instead of waiting.
func (w *world) failWrites() (resume func()) {
	w.t.Helper()
	patient := writeLock
	writeLock.Retries = 0
	w.t.Cleanup(func() { writeLock = patient })

	lock := common.NewRedisLock(w.rdb)
	key := common.RedisKeys.ComputeProvisionedLock()
	require.NoError(w.t, lock.Acquire(w.ctx, key, common.RedisLockOptions{TtlS: 3600}))
	return func() {
		w.t.Helper()
		require.NoError(w.t, lock.Release(key))
		writeLock = patient
	}
}

func (w *world) spentCents() float64 {
	w.t.Helper()
	cents, err := w.store.Spent(w.ctx, w.clock.Now())
	require.NoError(w.t, err)
	return cents
}

// spend counts cents as spent at a time, as a machine that is long gone did.
func (w *world) spend(at time.Time, cents float64) {
	w.t.Helper()
	_, err := w.rdb.TxPipelined(w.ctx, func(tx redis.Pipeliner) error {
		addSpend(w.ctx, tx, at, cents)
		return nil
	})
	require.NoError(w.t, err)
}

// fakeControl stands in for the gateway.
type fakeControl struct {
	mu    sync.Mutex
	pools map[string]types.WorkerPoolConfig
	// unloaded are the pools that cannot be loaded, and why.
	unloaded map[string]error
	// poolsErr is why no pool can be read at all.
	poolsErr  error
	forgetErr error
	// forgotten are the machines the gateway was told to forget, in order.
	forgotten []string
	// tokens are the join tokens that have not been revoked, by machine.
	tokens map[string]string
	// beforeJoinScript runs before a join script is made, if set.
	beforeJoinScript func()
}

func (c *fakeControl) BuyingPools(ctx context.Context) (map[string]types.WorkerPoolConfig, map[string]error, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.poolsErr != nil {
		return nil, nil, c.poolsErr
	}
	return maps.Clone(c.pools), maps.Clone(c.unloaded), nil
}

func (c *fakeControl) JoinScript(ctx context.Context, pool, machineID string) (string, string, error) {
	c.mu.Lock()
	before := c.beforeJoinScript
	c.mu.Unlock()
	if before != nil {
		before()
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tokens == nil {
		c.tokens = map[string]string{}
	}
	c.tokens[machineID] = "hash-of-" + machineID
	return "join " + pool + " as " + machineID, c.tokens[machineID], nil
}

func (c *fakeControl) ForgetMachine(ctx context.Context, pool, machineID, tokenHash string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.forgetErr != nil {
		return c.forgetErr
	}
	c.forgotten = append(c.forgotten, machineID)
	if c.tokens[machineID] == tokenHash {
		delete(c.tokens, machineID)
	}
	return nil
}

func (c *fakeControl) set(change func(*fakeControl)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	change(c)
}

func (c *fakeControl) forgot(machineID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Contains(c.forgotten, machineID)
}

func (c *fakeControl) mayJoin(machineID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.tokens[machineID]
	return ok
}

// fakeWorkers stands in for the worker repository.
type fakeWorkers struct {
	mu      sync.Mutex
	workers map[string]*types.Worker
	err     error
}

func (f *fakeWorkers) GetAllWorkersInPool(pool string) ([]*types.Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	var workers []*types.Worker
	for _, worker := range f.workers {
		if worker.PoolName == pool {
			found := *worker
			workers = append(workers, &found)
		}
	}
	slices.SortFunc(workers, func(a, b *types.Worker) int { return cmp.Compare(a.Id, b.Id) })
	return workers, nil
}

func (f *fakeWorkers) CordonWorkerIfIdle(id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	worker, ok := f.workers[id]
	switch {
	case f.err != nil:
		return false, f.err
	case !ok:
		return false, &types.ErrWorkerNotFound{WorkerId: id}
	case !unused(worker):
		return false, nil
	}
	worker.CordonRequested, worker.Status = true, types.WorkerStatusDisabled
	return true, nil
}

func (f *fakeWorkers) SetWorkerCordon(id string, cordoned bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	worker, ok := f.workers[id]
	if !ok {
		return &types.ErrWorkerNotFound{WorkerId: id}
	}
	worker.CordonRequested = cordoned
	if cordoned {
		worker.Status = types.WorkerStatusDisabled
	}
	return nil
}

// join brings up the machine's worker, with nothing to run.
func (f *fakeWorkers) join(machine Machine) {
	f.mu.Lock()
	defer f.mu.Unlock()
	// A worker with GPUs has its pool's GPU type. Here, which does not
	// matter: only that it has one.
	gpu := ""
	if machine.GPUCount > 0 {
		gpu = "L40S"
	}
	f.workers[machine.workerID()] = &types.Worker{
		Id: machine.workerID(), PoolName: machine.Pool, MachineId: machine.ID, Status: types.WorkerStatusAvailable,
		Gpu:      gpu,
		TotalCpu: machine.CPUMillicores, FreeCpu: machine.CPUMillicores,
		TotalMemory: usableMemoryMB(machine.MemoryMB), FreeMemory: usableMemoryMB(machine.MemoryMB),
		TotalGpuCount: machine.GPUCount, FreeGpuCount: machine.GPUCount,
	}
}

// work gives the machine's worker something to run, or takes it away.
func (f *fakeWorkers) work(machine Machine, busy bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	worker := f.workers[machine.workerID()]
	worker.FreeGpuCount = worker.TotalGpuCount
	if busy {
		worker.FreeGpuCount = 0
	}
}

func (f *fakeWorkers) lose(machine Machine) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.workers, machine.workerID())
}

func (f *fakeWorkers) of(machine Machine) *types.Worker {
	f.mu.Lock()
	defer f.mu.Unlock()
	if worker, ok := f.workers[machine.workerID()]; ok {
		found := *worker
		return &found
	}
	return nil
}

func (f *fakeWorkers) fail(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// troubled is a provider whose calls can be made to fail or be watched.
type troubled struct {
	*providers.Fake
	mu           sync.Mutex
	listErr      error
	getErr       error
	extendErr    error
	beforeCreate func(providers.CreateRequest)
}

func (p *troubled) List(ctx context.Context) ([]providers.Instance, error) {
	p.mu.Lock()
	err := p.listErr
	p.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return p.Fake.List(ctx)
}

func (p *troubled) Create(ctx context.Context, req providers.CreateRequest) (providers.Instance, error) {
	if p.beforeCreate != nil {
		p.beforeCreate(req)
	}
	return p.Fake.Create(ctx, req)
}

func (p *troubled) Get(ctx context.Context, id string) (providers.Instance, error) {
	p.mu.Lock()
	err := p.getErr
	p.mu.Unlock()
	if err != nil {
		return providers.Instance{}, err
	}
	return p.Fake.Get(ctx, id)
}

func (p *troubled) failLists(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.listErr = err
}

func (p *troubled) Extend(ctx context.Context, id string, until time.Time) error {
	p.mu.Lock()
	err := p.extendErr
	p.mu.Unlock()
	if err != nil {
		return err
	}
	return p.Fake.Extend(ctx, id, until)
}

func (p *troubled) failGets(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.getErr = err
}

func (p *troubled) failExtends(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.extendErr = err
}
