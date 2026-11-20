package provisioner

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/providers/watch"
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
	clock *common.FakeClock
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
		clock:   common.NewFakeClock(bought),
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

var anyMachine = scheduler.MachineNeed{}

func TestProvisionBuysTheCheapestMachineThatFits(t *testing.T) {
	w := newWorld(t)
	w.verda.SetOffers(
		l40s(costing(120), named("1L40S.40V")),
		l40s(costing(90)),
		l40s(costing(60), named("1A10G.8V"), func(o *providers.Offer) { o.GPU = "A10G" }),
	)
	w.Tick(w.ctx)

	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	got, err := w.Provision(w.ctx, "spot-l40s", scheduler.MachineNeed{CPU: 8000, Memory: 32_000, GPUCount: 1})
	require.NoError(t, err)

	// What the scheduler is told: the machine's ID, known before it boots,
	// and the capacity that can be counted on.
	require.Equal(t, scheduler.ProvisionedMachine{MachineID: got.MachineID, CPU: 20_000, Memory: 54_000, GPUCount: 1, Spot: true}, *got)

	// What the provider was asked for.
	creates := w.verda.Creates()
	require.Len(t, creates, 1)
	require.Equal(t, "1L40S.20V", creates[0].Offer.ID)
	require.Equal(t, got.MachineID, creates[0].Name)
	require.Equal(t, map[string]string{MachineTag: got.MachineID}, creates[0].Tags)
	require.Equal(t, "join spot-l40s as "+got.MachineID, creates[0].StartupScript)
	require.Equal(t, bought.Add(leaseFor), creates[0].Until)
	require.Zero(t, creates[0].MaxHourlyCostMicros, "the pool sets no price limit")

	// What is on record.
	record := w.record(got.MachineID)
	require.Equal(t, Machine{
		ID: got.MachineID, Pool: "spot-l40s", InstanceID: "verda-1", TokenHash: "hash-of-" + got.MachineID,
		offerKey: offerKey{Provider: "verda", OfferID: "1L40S.20V", Region: "FIN-01", Spot: true},
		GPU:      "L40S", GPUCount: 1, CPUMillicores: 20_000, MemoryMB: 60_000, HourlyMicros: 90 * microsPerCent,
		CreatedAt: bought, Until: bought.Add(leaseFor),
	}, record)

	booting, err := w.Booting(w.ctx, "spot-l40s")
	require.NoError(t, err)
	require.Equal(t, []scheduler.ProvisionedMachine{*got}, booting)
	booting, err = w.Booting(w.ctx, "spot-a10g")
	require.NoError(t, err)
	require.Empty(t, booting)

	// The machine is known for what it was bought as for as long as the
	// pool holds it, also once it has joined.
	w.workers.join(record)
	w.workers.work(record, true)
	w.after(tickInterval)
	booting, err = w.Booting(w.ctx, "spot-l40s")
	require.NoError(t, err)
	require.Empty(t, booting)
	held, err := w.Machine(w.ctx, got.MachineID)
	require.NoError(t, err)
	require.Equal(t, got, held)
	held, err = w.Machine(w.ctx, "machine-of-another-kind")
	require.NoError(t, err)
	require.Nil(t, held)
}

// Invariant 3: a pool buys only from its own provider, whatever another one
// sells for less.
func TestAPoolBuysOnlyFromItsProvider(t *testing.T) {
	nebius := providers.NewFake("nebius", testOwner)
	nebius.SetOffers(l40s(costing(10), named("gpu-l40s-a")))
	w := newWorld(t, func(opts *Options) {
		verda, _ := opts.Providers.Get("verda")
		opts.Providers = providers.RegistryOf(verda, nebius)
	})
	// Another pool buys from nebius, so what nebius sells is known too.
	w.control.set(func(c *fakeControl) {
		c.pools["spot-nebius-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Providers = []string{"nebius"} })
	})
	w.Tick(w.ctx)

	machine := w.buy("spot-l40s")
	require.Equal(t, "verda", machine.Provider)
	require.Equal(t, int64(100*microsPerCent), machine.HourlyMicros)
	require.Len(t, w.verda.Creates(), 1)
	require.Empty(t, nebius.Creates())

	machine = w.buy("spot-nebius-l40s")
	require.Equal(t, "nebius", machine.Provider)
	require.Equal(t, int64(10*microsPerCent), machine.HourlyMicros)
	require.Len(t, w.verda.Creates(), 1)
	require.Len(t, nebius.Creates(), 1)
}

// The pool's price limit goes with the purchase, for providers that sell
// spot by bid or by price limit.
func TestThePoolsPriceLimitIsPassedToTheProvider(t *testing.T) {
	w := newWorld(t)
	w.control.set(func(c *fakeControl) {
		c.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxNodeHourlyCents = 150 })
	})
	w.Tick(w.ctx)

	w.buy("spot-l40s")
	require.Equal(t, int64(1_500_000), w.verda.Creates()[0].MaxHourlyCostMicros)
}

// Invariant 5: every instance has a record written before the create call.
func TestTheRecordIsWrittenBeforeTheProviderIsAsked(t *testing.T) {
	var w *world
	provider := &troubled{Fake: providers.NewFake("verda", testOwner)}
	provider.SetOffers(l40s())
	w = newWorld(t, func(opts *Options) { opts.Providers = providers.RegistryOf(provider) })
	w.Tick(w.ctx)

	asked := false
	provider.beforeCreate = func(req providers.CreateRequest) {
		asked = true
		records := w.records()
		require.Len(t, records, 1)
		require.Equal(t, req.Tags[MachineTag], records[0].ID)
		require.Empty(t, records[0].InstanceID)
		require.Equal(t, req.Until, records[0].Until)
		require.EqualValues(t, 100*microsPerCent, records[0].HourlyMicros)
	}
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.NoError(t, err)
	require.True(t, asked)
}

func TestAPoolThatCannotBuy(t *testing.T) {
	twoPools := func(w *world) {
		w.control.pools["spot-a10g"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.GPUType = "A10G" })
		w.verda.SetOffers(l40s(), l40s(named("1A10G.8V"), func(o *providers.Offer) { o.GPU = "A10G" }))
	}

	tests := []struct {
		name    string
		options func(*Options)
		arrange func(*world)
		need    scheduler.MachineNeed
		// wantErr is why it cannot, or wantWhy what it says about it.
		wantErr error
		wantWhy string
	}{
		{
			name:    "a pool nobody told it about",
			arrange: func(w *world) { delete(w.control.pools, "spot-l40s") },
			wantErr: errNotBuying,
		},
		{
			name:    "its provider has nothing in stock",
			arrange: func(w *world) { w.verda.SetOffers() },
			wantWhy: "verda is not known to have anything in stock",
		},
		{
			name:    "its provider has only other GPUs in stock",
			arrange: func(w *world) { w.verda.SetOffers(l40s(func(o *providers.Offer) { o.GPU = "A10G" })) },
			wantWhy: "verda has no L40S in stock",
		},
		{
			name:    "its provider has only on-demand machines in stock",
			arrange: func(w *world) { w.verda.SetOffers(l40s(func(o *providers.Offer) { o.Spot = false })) },
			wantWhy: "verda sells L40S only on demand: remove provisioning.spot",
		},
		{
			name:    "nothing in stock is big enough",
			need:    scheduler.MachineNeed{GPUCount: 2},
			wantWhy: "no L40S that verda has for the pool has room for what is needed (cpu 0m, memory 0 MiB, gpus 2)",
		},
		{
			name:    "nothing in stock is on demand, for work that may not be interrupted",
			need:    scheduler.MachineNeed{GPUCount: 1, OnDemand: true},
			wantWhy: "no L40S that verda has for the pool has room for what is needed (cpu 0m, memory 0 MiB, gpus 1, on demand)",
		},
		{
			name: "it holds maxNodes machines",
			arrange: func(w *world) {
				w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxNodes = 2 })
				w.Tick(w.ctx)
				w.buy("spot-l40s")
				w.buy("spot-l40s")
			},
			wantErr: errAtMaxNodes,
		},
		{
			name: "it holds maxGPUs GPUs",
			arrange: func(w *world) {
				w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxGPUs = 2 })
				w.Tick(w.ctx)
				w.buy("spot-l40s")
				w.buy("spot-l40s")
			},
			wantErr: errAtMaxGPUs,
		},
		{
			name:    "the hourly cap is reached by another pool's machines",
			options: func(opts *Options) { opts.Limits.MaxHourlyCents = 250 },
			arrange: func(w *world) {
				twoPools(w)
				w.Tick(w.ctx)
				w.buy("spot-a10g")
				w.buy("spot-a10g")
			},
			wantErr: errHourlyCap,
		},
		{
			name:    "the daily cap is spent",
			options: func(opts *Options) { opts.Limits.MaxDailyCents = 1000 },
			arrange: func(w *world) {
				w.spend(w.clock.Now().Add(-23*time.Hour), 1000)
			},
			wantErr: errDailyCap,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := []func(*Options){}
			if test.options != nil {
				options = append(options, test.options)
			}
			w := newWorld(t, options...)
			if test.arrange != nil {
				test.arrange(w)
			}
			w.after(time.Minute)
			records, creates := len(w.records()), len(w.verda.Creates())

			require.False(t, w.CanProvision(w.ctx, "spot-l40s", test.need))
			_, err := w.Provision(w.ctx, "spot-l40s", test.need)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
			} else {
				require.EqualError(t, err, `pool "spot-l40s" cannot buy a machine: `+test.wantWhy)
			}

			// Nothing was bought, written down or left able to join.
			require.Len(t, w.records(), records)
			require.Len(t, w.verda.Creates(), creates)
			for machineID := range w.control.tokens {
				_ = w.record(machineID)
			}
		})
	}
}

func TestAPoolCannotBuyBeforeTheFirstTick(t *testing.T) {
	w := newWorld(t)
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))

	w.Tick(w.ctx)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
}

func TestAPoolWhoseProviderIsNotConfiguredDoesNotBuy(t *testing.T) {
	w := newWorld(t)
	w.control.pools["spot-a10g"] = spotL40S(func(pool *types.WorkerPoolConfig) {
		pool.GPUType, pool.Provisioning.Providers = "A10G", []string{"nebius"}
	})
	w.Tick(w.ctx)

	require.False(t, w.CanProvision(w.ctx, "spot-a10g", anyMachine))
	_, err := w.Provision(w.ctx, "spot-a10g", anyMachine)
	require.ErrorContains(t, err, `buys from provider "nebius", which is not configured`)
	require.Empty(t, w.records())

	// The pool next to it is not affected.
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
}

func TestOutOfStockAtPurchaseSkipsTheOfferForAWhile(t *testing.T) {
	w := newWorld(t)
	w.verda.SetOffers(l40s(), l40s(in("FIN-02"), costing(140)))
	w.Tick(w.ctx)

	// The provider lists the cheapest offer, and has none.
	w.verda.RefuseCreates(providers.ErrOutOfStock)
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.ErrorIs(t, err, providers.ErrOutOfStock)
	require.Empty(t, w.records(), "nothing was bought, so nothing is on record")
	require.Empty(t, w.control.tokens, "and nothing can join")

	// The pool can still grow: by the other offer.
	w.verda.RefuseCreates(nil)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	require.Equal(t, "FIN-02", w.buy("spot-l40s").Region)

	// The refused offer is passed over for soldOutFor, though still listed.
	w.every(time.Minute, soldOutFor-2*time.Minute)
	require.Equal(t, "FIN-02", w.buy("spot-l40s").Region)
	w.after(2 * time.Minute)
	require.Equal(t, "FIN-01", w.buy("spot-l40s").Region)
}

func TestOutOfStockEverywhereMeansThePoolCannotGrow(t *testing.T) {
	w := newWorld(t)
	w.Tick(w.ctx)

	w.verda.RefuseCreates(providers.ErrOutOfStock)
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.ErrorIs(t, err, providers.ErrOutOfStock)

	w.verda.RefuseCreates(nil)
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	w.every(time.Minute, soldOutFor-time.Minute)
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	w.after(time.Minute)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
}

func TestQuotaRefusalStopsThePoolBuyingForAWhile(t *testing.T) {
	w := newWorld(t)
	w.control.pools["on-demand-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Spot = false })
	w.verda.SetOffers(l40s(), l40s(in("FIN-02")), l40s(named("1L40S.20V.od"), func(o *providers.Offer) { o.Spot = false }))
	w.Tick(w.ctx)

	w.verda.RefuseCreates(providers.ErrQuota)
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.ErrorIs(t, err, providers.ErrQuota)
	require.Empty(t, w.records())
	require.Empty(t, w.control.tokens)
	require.Len(t, w.verda.Creates(), 0)

	// Quota is not about one offer: the pool stops asking altogether.
	w.verda.RefuseCreates(nil)
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	_, err = w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.ErrorIs(t, err, errBackingOff)
	require.True(t, w.CanProvision(w.ctx, "on-demand-l40s", anyMachine), "another pool is not held back")

	w.every(time.Minute, quotaBackoff-time.Minute)
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	w.after(time.Minute)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
}

func TestAPurchaseThatFailedForNoKnownReasonIsNotAssumedToHaveFailed(t *testing.T) {
	w := newWorld(t)
	w.Tick(w.ctx)

	// The provider is down. Whether it created anything is not known.
	w.verda.RefuseCreates(errors.New("502 bad gateway"))
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.ErrorContains(t, err, "502 bad gateway")
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine), "the pool waits before it asks again")

	// The record stays, counts against the limits, and may still join.
	records := w.records()
	require.Len(t, records, 1)
	require.Empty(t, records[0].InstanceID)
	require.True(t, w.control.mayJoin(records[0].ID))

	w.verda.RefuseCreates(nil)
	w.after(errorBackoff)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))

	// The provider never lists it: after the launch grace it is given up.
	w.every(tickInterval, launchGrace)
	require.Empty(t, w.records())
	require.False(t, w.control.mayJoin(records[0].ID))
	require.Empty(t, w.verda.Deletes(), "there was nothing to delete")
}

func TestAPurchaseWhoseAnswerWasLostIsPickedUpByItsTag(t *testing.T) {
	w := newWorld(t)
	w.Tick(w.ctx)

	w.verda.LoseAnswers(1)
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.Error(t, err)
	require.Equal(t, []string{"verda-1"}, w.instances(), "the machine exists all the same")

	record := w.records()[0]
	require.Empty(t, record.InstanceID)
	booting, err := w.Booting(w.ctx, "spot-l40s")
	require.NoError(t, err)
	require.Empty(t, booting)

	w.after(tickInterval)
	record = w.record(record.ID)
	require.Equal(t, "verda-1", record.InstanceID)
	require.Equal(t, bought.Add(leaseFor), record.Until, "what it was bought with is on record")
	require.Nil(t, record.Release)
	booting, err = w.Booting(w.ctx, "spot-l40s")
	require.NoError(t, err)
	require.Len(t, booting, 1)
	require.Empty(t, w.verda.Deletes())
}

// A lost answer for a machine that then fails to start: the instance is
// deleted when the record is, not left for the sweep of unrecorded instances.
func TestAPurchaseWhoseAnswerWasLostAndThatFailedIsDeleted(t *testing.T) {
	w := newWorld(t)
	w.Tick(w.ctx)

	w.verda.LoseAnswers(1)
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.Error(t, err)
	w.verda.SetStatus("verda-1", providers.StatusFailed, "no capacity")

	w.after(tickInterval)
	require.Empty(t, w.records())
	require.Empty(t, w.instances())
	require.Equal(t, []string{"verda-1"}, w.verda.Deletes())
}

func TestStockIsCountedDownBetweenListings(t *testing.T) {
	w := newWorld(t)
	w.verda.SetOffers(l40s(func(o *providers.Offer) { o.Available = 2 }))
	w.Tick(w.ctx)

	w.buy("spot-l40s")
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	w.buy("spot-l40s")
	// The list is not fetched again yet; the pool knows it bought the last.
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))

	w.verda.SetOffers(l40s(func(o *providers.Offer) { o.Available = 1 }))
	w.after(offersMaxAge)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
}

func TestAStaleListIsNotBoughtFrom(t *testing.T) {
	provider := &troubledOffers{Fake: providers.NewFake("verda", testOwner)}
	provider.SetOffers(l40s())
	w := newWorld(t, func(opts *Options) { opts.Providers = providers.RegistryOf(provider) })
	w.Tick(w.ctx)

	// The provider cannot be asked what it sells. Its last list is used.
	provider.err.Store(true)
	w.every(time.Minute, offersStaleAfter-time.Minute)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))

	// Until it is too old to mean anything.
	w.after(time.Minute)
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))

	provider.err.Store(false)
	w.after(time.Minute)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
}

// Invariant 3 across replicas: gateways buying at the same moment hold
// maxNodes, maxGPUs and the spend cap between them.
func TestReplicasBuyingAtOnceStayWithinTheLimits(t *testing.T) {
	tests := []struct {
		name    string
		maxGPUs int
		limits  Limits
		want    int
	}{
		{name: "maxNodes", want: 4},
		{name: "maxGPUs", maxGPUs: 3, want: 3},
		{name: "the hourly cap", limits: Limits{MaxHourlyCents: 250}, want: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := newWorld(t)
			w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxGPUs = test.maxGPUs })
			replicas := []*Provisioner{}
			for range 3 {
				replica := w.replica(func(opts *Options) { opts.Limits = test.limits })
				replica.Tick(w.ctx)
				replicas = append(replicas, replica)
			}

			var bought atomic.Int32
			var wg sync.WaitGroup
			for _, replica := range replicas {
				for range 4 {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if _, err := replica.Provision(w.ctx, "spot-l40s", anyMachine); err == nil {
							bought.Add(1)
						}
					}()
				}
			}
			wg.Wait()

			require.EqualValues(t, test.want, bought.Load())
			require.Len(t, w.records(), test.want)
			require.Len(t, w.instances(), test.want)
		})
	}
}

// troubledOffers is a provider whose list of offers can be made to fail.
type troubledOffers struct {
	*providers.Fake
	err atomic.Bool
}

func (p *troubledOffers) Offers(ctx context.Context, filter providers.OfferFilter) ([]providers.Offer, error) {
	if p.err.Load() {
		return nil, errors.New("the provider is down")
	}
	return p.Fake.Offers(ctx, filter)
}

// A pool's config can change while a purchase is under way. What is bought,
// from whom and for how much at most all come from one reading of it: the
// one the machine was chosen with.
func TestAPurchaseIsMadeWithTheConfigItsMachineWasChosenWith(t *testing.T) {
	nebius := providers.NewFake("nebius", testOwner)
	nebius.SetOffers(l40s(named("gpu-l40s-a"), costing(80)))
	w := newWorld(t, func(opts *Options) {
		verda, _ := opts.Providers.Get("verda")
		opts.Providers = providers.RegistryOf(verda, nebius)
	})
	w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxNodeHourlyCents = 150 })
	// Another pool buys from nebius, so what nebius sells is known.
	w.control.pools["spot-nebius-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Providers = []string{"nebius"} })
	w.Tick(w.ctx)

	// While the machine is being prepared to join, the pool is moved to
	// nebius with another price limit, and this replica reads it.
	w.control.beforeJoinScript = func() {
		w.control.set(func(c *fakeControl) {
			c.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) {
				pool.Provisioning.Providers, pool.Provisioning.MaxNodeHourlyCents = []string{"nebius"}, 90
			})
		})
		w.Tick(w.ctx)
	}
	machine := w.buy("spot-l40s")

	require.Equal(t, "nebius", machine.Provider)
	require.Empty(t, w.verda.Creates(), "verda is not asked for what nebius sells")
	creates := nebius.Creates()
	require.Len(t, creates, 1)
	require.Equal(t, "gpu-l40s-a", creates[0].Offer.ID)
	require.EqualValues(t, 90*microsPerCent, creates[0].MaxHourlyCostMicros)
}

// What becomes of a purchase when its record cannot take the instance the
// provider answered with.

func newTroubledWorld(t *testing.T) (*world, *troubled) {
	t.Helper()
	provider := &troubled{Fake: providers.NewFake("verda", testOwner)}
	provider.SetOffers(l40s())
	w := newWorld(t, func(opts *Options) {
		opts.Providers = providers.RegistryOf(provider)
		opts.NewID = func() string { return "machine-1" }
	})
	w.Tick(w.ctx)
	return w, provider
}

// A provider that takes longer to answer than the launch grace: a pass finds
// nothing of the purchase at it and gives the purchase up. The instance that
// comes of it after all has no record, and the next pass deletes it.
func TestAPurchaseGivenUpOnBeforeItsProviderAnsweredLeavesNoMachine(t *testing.T) {
	w, provider := newTroubledWorld(t)

	provider.beforeCreate = func(providers.CreateRequest) { w.after(launchGrace) }
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.EqualError(t, err, `pool "spot-l40s": machine machine-1 was given up on before its provider answered`)

	require.Empty(t, w.records())
	require.False(t, w.control.mayJoin("machine-1"))
	require.Len(t, provider.Everything(), 1, "the provider created it all the same")

	w.after(tickInterval)
	require.Empty(t, provider.Everything())
	require.Equal(t, []string{"verda-1"}, provider.Deletes())
}

// The provider answered and the record could not be written. The machine is
// bought all the same: the scheduler is told so, and the next pass ties the
// instance to its record by its tag.
func TestAMachineWhoseInstanceCouldNotBeRecordedIsBoughtAllTheSame(t *testing.T) {
	w, provider := newTroubledWorld(t)

	var resume func()
	provider.beforeCreate = func(providers.CreateRequest) { resume = w.failWrites() }
	bought, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.NoError(t, err)
	require.Equal(t, scheduler.ProvisionedMachine{MachineID: "machine-1", CPU: 20_000, Memory: 54_000, GPUCount: 1, Spot: true}, *bought)
	require.Empty(t, w.record("machine-1").InstanceID)
	require.True(t, w.control.mayJoin("machine-1"))

	resume()
	w.after(tickInterval)
	record := w.record("machine-1")
	require.Equal(t, "verda-1", record.InstanceID)
	require.Nil(t, record.Release)
	require.Len(t, provider.Everything(), 1)
	require.Empty(t, provider.Deletes())
}

// Why a pool cannot buy is said when it is asked to and cannot: once for
// each reason, however often it is asked, and again only after it has bought
// a machine in between.
func TestWhyAPoolCannotBuyIsSaidOnceForEachReason(t *testing.T) {
	w := newWorld(t)
	w.verda.SetOffers(l40s(func(o *providers.Offer) { o.Spot = false }))
	lines := logged(t)
	said := func(reason string) int {
		count := 0
		for _, line := range strings.Split(lines.String(), "\n") {
			if strings.Contains(line, "provisioner: the pool cannot buy a machine") && strings.Contains(line, reason) {
				count++
			}
		}
		return count
	}
	asked := func(times int, want bool) {
		t.Helper()
		for range times {
			require.Equal(t, want, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
			w.after(tickInterval)
		}
	}

	w.Tick(w.ctx)
	asked(5, false)
	require.Equal(t, 1, said("verda sells L40S only on demand: remove provisioning.spot"))
	require.Contains(t, lines.String(), `"level":"warn"`)
	require.Contains(t, lines.String(), `"pool_name":"spot-l40s"`)

	// Another reason is said as well, and the first is not said again.
	w.verda.SetOffers()
	w.after(offersMaxAge)
	asked(5, false)
	require.Equal(t, 1, said("verda is not known to have anything in stock"))
	require.Equal(t, 1, said("only on demand"))

	// The pool buys a machine. That it cannot buy another is news again.
	w.verda.SetOffers(l40s())
	w.after(offersMaxAge)
	asked(1, true)
	w.buy("spot-l40s")
	w.verda.SetOffers()
	w.after(offersMaxAge)
	asked(5, false)
	require.Equal(t, 2, said("verda is not known to have anything in stock"))
	require.Equal(t, 1, said("only on demand"))
}

// A pool is asked for machines of many sizes. That it can buy one of them
// does not make what it said about another news.
func TestWhyAPoolCannotBuyIsNotSaidAgainBecauseItCanBuyForAnotherNeed(t *testing.T) {
	w := newWorld(t)
	lines := logged(t)
	w.Tick(w.ctx)

	twoGPUs := scheduler.MachineNeed{GPUCount: 2}
	for range 5 {
		require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
		require.False(t, w.CanProvision(w.ctx, "spot-l40s", twoGPUs))
		w.after(tickInterval)
	}
	require.Equal(t, 1, strings.Count(lines.String(), "provisioner: the pool cannot buy a machine"))
	require.Contains(t, lines.String(), "no L40S that verda has for the pool has room for what is needed (cpu 0m, memory 0 MiB, gpus 2)")
}

// What stands in the way is said once however it stands: for each size the
// pool is asked for, and at each price its provider asks. What a pool has
// said does not grow with them.
func TestWhyAPoolCannotBuyIsSaidOnceWhateverSizeIsAskedForAndWhateverThePrice(t *testing.T) {
	w := newWorld(t)
	lines := logged(t)
	said := func() int { return strings.Count(lines.String(), "provisioner: the pool cannot buy a machine") }
	w.Tick(w.ctx)

	for gpus := uint32(2); gpus < 10; gpus++ {
		require.False(t, w.CanProvision(w.ctx, "spot-l40s", scheduler.MachineNeed{GPUCount: gpus}))
	}
	require.Equal(t, 1, said())
	require.Contains(t, lines.String(), "no L40S that verda has for the pool has room for what is needed (cpu 0m, memory 0 MiB, gpus 2)")

	w.control.set(func(c *fakeControl) {
		c.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxNodeHourlyCents = 50 })
	})
	for price := int64(100); price < 108; price++ {
		w.verda.SetOffers(l40s(costing(price)))
		w.after(offersMaxAge)
		require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	}
	require.Equal(t, 2, said())
	require.Contains(t, lines.String(), "verda has no L40S within provisioning.maxNodeHourlyCents 50: the cheapest costs 100 cents an hour")
	require.Len(t, w.said["spot-l40s"], 2)
}

// Each of the other reasons a pool cannot buy for is said too.
func TestEveryReasonAPoolCannotBuyForIsSaid(t *testing.T) {
	tests := []struct {
		name    string
		options func(*Options)
		arrange func(*world)
		want    string
	}{
		{
			name: "a provider that is not configured",
			arrange: func(w *world) {
				w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Providers = []string{"nebius"} })
			},
			want: `the pool buys from provider \"nebius\", which is not configured under providers`,
		},
		{
			name:    "a provider that is disabled",
			options: func(opts *Options) { opts.Providers, _ = newDisabledVast(t) },
			arrange: func(w *world) {
				w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Providers = []string{disabledVastName} })
			},
			want: `the pool buys from provider \"` + disabledVastName + `\", which is disabled under providers.disabled`,
		},
		{
			name: "a purchase that just failed",
			arrange: func(w *world) {
				w.Tick(w.ctx)
				w.verda.RefuseCreates(providers.ErrQuota)
				_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
				require.ErrorIs(w.t, err, providers.ErrQuota)
			},
			want: "a purchase for the pool failed a moment ago",
		},
		{
			name: "maxNodes",
			arrange: func(w *world) {
				w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxNodes = 1 })
				w.Tick(w.ctx)
				w.buy("spot-l40s")
			},
			want: "the pool holds as many machines as provisioning.maxNodes allows",
		},
		{
			name: "maxGPUs",
			arrange: func(w *world) {
				w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxGPUs = 1 })
				w.Tick(w.ctx)
				w.buy("spot-l40s")
			},
			want: "another machine would exceed provisioning.maxGPUs",
		},
		{
			name:    "the hourly cap",
			options: func(opts *Options) { opts.Limits.MaxHourlyCents = 150 },
			arrange: func(w *world) {
				w.Tick(w.ctx)
				w.buy("spot-l40s")
			},
			want: "one more machine would take the hourly cost of all machines over providers.maxHourlyCents",
		},
		{
			name:    "the daily cap",
			options: func(opts *Options) { opts.Limits.MaxDailyCents = 1000 },
			arrange: func(w *world) { w.spend(w.clock.Now(), 1000) },
			want:    "providers.maxDailyCents has been spent in the last 24 hours",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := []func(*Options){}
			if test.options != nil {
				options = append(options, test.options)
			}
			w := newWorld(t, options...)
			test.arrange(w)
			w.after(tickInterval)
			lines := logged(t)

			for range 3 {
				require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
			}
			require.Equal(t, 1, strings.Count(lines.String(), "provisioner: the pool cannot buy a machine"))
			require.Contains(t, lines.String(), test.want)
		})
	}
}

// What a replica has yet to read, it has nothing to say about: a pool made
// since its last tick, or a provider whose offers it has yet to list. The
// next tick reads them, and says so itself when it cannot. A pool that keeps
// capacity free asks every second whether it can buy, so anything said here
// would be said of every new pool, and of every pool when a gateway starts.
func TestAPoolThisReplicaHasYetToReadIsNotSaidToBeUnableToBuy(t *testing.T) {
	provider := &troubledOffers{Fake: providers.NewFake("verda", testOwner)}
	provider.SetOffers(l40s())
	w := newWorld(t, func(opts *Options) { opts.Providers = providers.RegistryOf(provider) })
	lines := logged(t)
	said := func() int { return strings.Count(lines.String(), "provisioner: the pool cannot buy a machine") }

	// Before its first tick it knows no pool.
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	require.Zero(t, said())

	// It reads the pool, and cannot list what the pool's provider sells.
	provider.err.Store(true)
	w.Tick(w.ctx)
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	require.Zero(t, said())
	require.Contains(t, lines.String(), "provisioner: could not list what the provider sells")

	// A pool is made after that tick.
	w.control.set(func(c *fakeControl) {
		c.pools["spot-a10g"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.GPUType = "A10G" })
	})
	require.False(t, w.CanProvision(w.ctx, "spot-a10g", anyMachine))
	require.Zero(t, said())

	// Once it has read both, what stands in a pool's way is said.
	provider.err.Store(false)
	w.after(tickInterval)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	require.False(t, w.CanProvision(w.ctx, "spot-a10g", anyMachine))
	require.Equal(t, 1, said())
	require.Contains(t, lines.String(), "verda has no A10G in stock")
}

// spotL40S is a pool that buys spot L40S machines from verda.
func spotL40S(change ...func(*types.WorkerPoolConfig)) types.WorkerPoolConfig {
	pool := types.WorkerPoolConfig{
		GPUType:      "L40S",
		Provisioning: types.WorkerPoolProvisioningConfig{Providers: []string{"verda"}, Spot: true, MaxNodes: 4},
	}
	for _, apply := range change {
		apply(&pool)
	}
	return pool
}

// l40s is a spot machine with one L40S for a dollar an hour.
func l40s(change ...func(*providers.Offer)) providers.Offer {
	offer := providers.Offer{
		ID: "1L40S.20V", Provider: "verda", Region: "FIN-01",
		GPU: "L40S", GPUCount: 1, CPUMillicores: 20_000, MemoryMB: 60_000,
		HourlyCostMicros: 100 * microsPerCent, Spot: true,
	}
	for _, apply := range change {
		apply(&offer)
	}
	return offer
}

func costing(cents int64) func(*providers.Offer) {
	return func(offer *providers.Offer) { offer.HourlyCostMicros = cents * microsPerCent }
}

func in(region string) func(*providers.Offer) {
	return func(offer *providers.Offer) { offer.Region = region }
}

func named(id string) func(*providers.Offer) {
	return func(offer *providers.Offer) { offer.ID = id }
}

func TestSells(t *testing.T) {
	cpuPool := spotL40S(func(pool *types.WorkerPoolConfig) { pool.GPUType = "" })
	cpuOffer := l40s(func(offer *providers.Offer) { offer.GPU, offer.GPUCount = "", 0 })

	tests := []struct {
		name  string
		offer providers.Offer
		pool  types.WorkerPoolConfig
		want  bool
	}{
		{"the pool's GPU, spot as configured", l40s(), spotL40S(), true},
		{"GPU names compare without case", l40s(func(o *providers.Offer) { o.GPU = "l40s" }), spotL40S(), true},
		{"on-demand for a spot pool", l40s(func(o *providers.Offer) { o.Spot = false }), spotL40S(), false},
		{"spot for an on-demand pool", l40s(), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.Spot = false }), false},
		{"on-demand for an on-demand pool", l40s(func(o *providers.Offer) { o.Spot = false }), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.Spot = false }), true},
		{"another GPU", l40s(func(o *providers.Offer) { o.GPU = "A10G" }), spotL40S(), false},
		{"a GPU type with no GPUs", l40s(func(o *providers.Offer) { o.GPUCount = 0 }), spotL40S(), false},
		{"a CPU machine for a GPU pool", cpuOffer, spotL40S(), false},
		{"a CPU machine for a CPU pool", cpuOffer, cpuPool, true},
		{"a GPU machine for a CPU pool", l40s(), cpuPool, false},
		{"GPUs with no type for a CPU pool", l40s(func(o *providers.Offer) { o.GPU = "" }), cpuPool, false},
		{"in one of the pool's regions", l40s(in("FIN-02")), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.Regions = []string{"FIN-01", "FIN-02"} }), true},
		{"outside the pool's regions", l40s(in("ICE-01")), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.Regions = []string{"FIN-01", "FIN-02"} }), false},
		{"without a price", l40s(costing(0)), spotL40S(), false},
		{"at the pool's price limit", l40s(costing(150)), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.MaxNodeHourlyCents = 150 }), true},
		{"over the pool's price limit", l40s(costing(151)), spotL40S(func(p *types.WorkerPoolConfig) { p.Provisioning.MaxNodeHourlyCents = 150 }), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, sells(test.offer, test.pool))
		})
	}
}

func TestHolds(t *testing.T) {
	offer := l40s() // 20 cores, 60000 MB of which 54000 can be counted on, 1 GPU

	tests := []struct {
		name string
		need scheduler.MachineNeed
		want bool
	}{
		{"nothing in particular", scheduler.MachineNeed{}, true},
		{"all of its cores", scheduler.MachineNeed{CPU: 20_000}, true},
		{"more cores than it has", scheduler.MachineNeed{CPU: 20_001}, false},
		{"the memory that can be counted on", scheduler.MachineNeed{Memory: 54_000}, true},
		{"memory the kernel keeps", scheduler.MachineNeed{Memory: 54_001}, false},
		{"its GPU", scheduler.MachineNeed{GPUCount: 1}, true},
		{"more GPUs than it has", scheduler.MachineNeed{GPUCount: 2}, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, holds(offer, test.need))
		})
	}
}

func TestChoose(t *testing.T) {
	// held returns n records of machines in the pool, at cents an hour each.
	held := func(pool string, n int, cents int64) []Machine {
		machines := make([]Machine, n)
		for i := range machines {
			machines[i] = Machine{Pool: pool, HourlyMicros: cents * microsPerCent}
		}
		return machines
	}
	releasing := held("spot-l40s", 1, 100)
	releasing[0].Release = &Release{Reason: "it had nothing to run"}

	tests := []struct {
		name    string
		buy     purchase
		want    string // the offer's ID and region
		wantErr error
	}{
		{
			name: "the cheapest that fits",
			buy:  purchase{offers: []providers.Offer{l40s(costing(120), named("big")), l40s(costing(90), named("small")), l40s(costing(100), named("medium"))}},
			want: "small in FIN-01",
		},
		{
			name: "the same price: by region, then by name",
			buy:  purchase{offers: []providers.Offer{l40s(named("b"), in("FIN-02")), l40s(named("b")), l40s(named("a"), in("FIN-02")), l40s(named("c"))}},
			want: "b in FIN-01",
		},
		{
			name: "a cheaper machine that is too small is passed over",
			buy: purchase{
				needs:  []scheduler.MachineNeed{{GPUCount: 2}},
				offers: []providers.Offer{l40s(costing(90)), l40s(costing(190), named("2L40S"), func(o *providers.Offer) { o.GPUCount = 2 })},
			},
			want: "2L40S in FIN-01",
		},
		{
			name: "a cheaper machine the pool does not buy is passed over",
			buy:  purchase{offers: []providers.Offer{l40s(costing(40), named("on-demand"), func(o *providers.Offer) { o.Spot = false }), l40s(costing(90))}},
			want: "1L40S.20V in FIN-01",
		},
		{
			name: "one below maxNodes",
			buy:  purchase{held: held("spot-l40s", 3, 100), offers: []providers.Offer{l40s()}},
			want: "1L40S.20V in FIN-01",
		},
		{
			name:    "at maxNodes",
			buy:     purchase{held: held("spot-l40s", 4, 100), offers: []providers.Offer{l40s()}},
			wantErr: errAtMaxNodes,
		},
		{
			name:    "a machine being given back counts until its record is gone",
			buy:     purchase{held: append(held("spot-l40s", 3, 100), releasing...), offers: []providers.Offer{l40s()}},
			wantErr: errAtMaxNodes,
		},
		{
			name: "other pools' machines do not count towards maxNodes",
			buy:  purchase{held: held("spot-a10g", 9, 100), offers: []providers.Offer{l40s()}},
			want: "1L40S.20V in FIN-01",
		},
		{
			name: "exactly the hourly cap",
			buy:  purchase{limits: Limits{MaxHourlyCents: 400}, held: held("spot-l40s", 3, 100), offers: []providers.Offer{l40s()}},
			want: "1L40S.20V in FIN-01",
		},
		{
			name:    "a cent over the hourly cap",
			buy:     purchase{limits: Limits{MaxHourlyCents: 399}, held: held("spot-l40s", 3, 100), offers: []providers.Offer{l40s()}},
			wantErr: errHourlyCap,
		},
		{
			name:    "the hourly cap covers every pool's machines",
			buy:     purchase{limits: Limits{MaxHourlyCents: 400}, held: append(held("spot-a10g", 2, 150), held("spot-l40s", 1, 100)...), offers: []providers.Offer{l40s()}},
			wantErr: errHourlyCap,
		},
		{
			name: "under the daily cap",
			buy:  purchase{limits: Limits{MaxDailyCents: 1000}, spentToday: 999.5, offers: []providers.Offer{l40s()}},
			want: "1L40S.20V in FIN-01",
		},
		{
			name:    "the daily cap is spent",
			buy:     purchase{limits: Limits{MaxDailyCents: 1000}, spentToday: 1000, offers: []providers.Offer{l40s()}},
			wantErr: errDailyCap,
		},
		{
			name: "no cap",
			buy:  purchase{spentToday: 1e9, held: held("spot-a10g", 50, 1000), offers: []providers.Offer{l40s()}},
			want: "1L40S.20V in FIN-01",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.buy.pool, test.buy.config = "spot-l40s", spotL40S()

			offer, err := choose(test.buy)
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, offer.ID+" in "+offer.Region)
		})
	}
}

// maxGPUs caps the GPUs of all the pool's machines together, whatever
// machines they are on.
func TestChooseKeepsThePoolWithinMaxGPUs(t *testing.T) {
	// held returns records of machines in the pool, one for each GPU count.
	held := func(gpus ...uint32) []Machine {
		machines := make([]Machine, len(gpus))
		for i, count := range gpus {
			machines[i] = Machine{Pool: "spot-l40s", GPUCount: count}
		}
		return machines
	}
	// offer is a machine with that many GPUs, at cents an hour.
	offer := func(gpus uint32, cents int64) providers.Offer {
		return l40s(named(fmt.Sprintf("%dL40S", gpus)), costing(cents), func(o *providers.Offer) { o.GPUCount = gpus })
	}
	releasing := held(4)
	releasing[0].Release = &Release{Reason: "it had nothing to run"}

	tests := []struct {
		name    string
		maxGPUs int
		held    []Machine
		offers  []providers.Offer
		want    uint32 // the GPUs of the machine chosen
		wantErr error
	}{
		{name: "no cap", held: held(8, 8), offers: []providers.Offer{offer(8, 100)}, want: 8},
		{name: "one machine with as many GPUs as the cap", maxGPUs: 8, offers: []providers.Offer{offer(8, 100)}, want: 8},
		{name: "the last GPU under the cap", maxGPUs: 8, held: held(1, 2, 4), offers: []providers.Offer{offer(1, 100)}, want: 1},
		{name: "at the cap", maxGPUs: 8, held: held(1, 1, 2, 4), offers: []providers.Offer{offer(1, 100)}, wantErr: errAtMaxGPUs},
		{name: "at the cap, whatever is in stock", maxGPUs: 8, held: held(8), wantErr: errAtMaxGPUs},
		{name: "cheaper machines that would pass the cap are passed over", maxGPUs: 8, held: held(2, 4), offers: []providers.Offer{offer(8, 30), offer(4, 50), offer(2, 100)}, want: 2},
		{name: "every machine in stock would pass the cap", maxGPUs: 8, held: held(7), offers: []providers.Offer{offer(2, 50)}, wantErr: errAtMaxGPUs},
		{name: "a machine with more GPUs than the cap", maxGPUs: 8, offers: []providers.Offer{offer(16, 50)}, wantErr: errAtMaxGPUs},
		{name: "a machine being given back counts until its record is gone", maxGPUs: 8, held: append(held(4), releasing...), offers: []providers.Offer{offer(1, 100)}, wantErr: errAtMaxGPUs},
		{name: "other pools' machines do not count towards maxGPUs", maxGPUs: 8, held: []Machine{{Pool: "spot-a10g", GPUCount: 32}}, offers: []providers.Offer{offer(8, 100)}, want: 8},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := spotL40S(func(pool *types.WorkerPoolConfig) {
				pool.Provisioning.MaxNodes, pool.Provisioning.MaxGPUs = 32, test.maxGPUs
			})

			got, err := choose(purchase{pool: "spot-l40s", config: config, held: test.held, offers: test.offers})
			if test.wantErr != nil {
				require.ErrorIs(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, got.GPUCount)
		})
	}
}

// A pool that finds nothing to buy says what stands in the way: the first
// thing it asks for that nothing in stock has.
func TestChooseSaysWhatLeftNoOffer(t *testing.T) {
	onDemand := func(o *providers.Offer) { o.Spot = false }
	a10g := func(o *providers.Offer) { o.GPU = "A10G" }
	cpu := func(o *providers.Offer) { o.GPU, o.GPUCount = "", 0 }

	onDemandPool := func(pool *types.WorkerPoolConfig) { pool.Provisioning.Spot = false }
	cpuPool := func(pool *types.WorkerPoolConfig) { pool.GPUType = "" }
	inRegions := func(regions ...string) func(*types.WorkerPoolConfig) {
		return func(pool *types.WorkerPoolConfig) { pool.Provisioning.Regions = regions }
	}
	payingAtMost := func(cents int64) func(*types.WorkerPoolConfig) {
		return func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxNodeHourlyCents = cents }
	}

	tests := []struct {
		name   string
		pool   []func(*types.WorkerPoolConfig)
		need   scheduler.MachineNeed
		offers []providers.Offer
		want   string
	}{
		{
			name: "nothing in stock",
			want: "verda is not known to have anything in stock",
		},
		{
			name:   "a GPU type the provider does not sell",
			offers: []providers.Offer{l40s(a10g), l40s(cpu)},
			want:   "verda has no L40S in stock",
		},
		{
			name:   "a GPU type listed without a price",
			offers: []providers.Offer{l40s(costing(0))},
			want:   "verda has no L40S in stock",
		},
		{
			name:   "no machine without a GPU",
			pool:   []func(*types.WorkerPoolConfig){cpuPool},
			offers: []providers.Offer{l40s()},
			want:   "verda has no CPU machine in stock",
		},
		{
			name:   "spot is not asked for, and the provider sells nothing else",
			pool:   []func(*types.WorkerPoolConfig){onDemandPool},
			offers: []providers.Offer{l40s(), l40s(a10g, onDemand)},
			want:   "verda sells L40S only as spot: set provisioning.spot",
		},
		{
			name:   "spot is asked for, and the provider sells none",
			offers: []providers.Offer{l40s(onDemand)},
			want:   "verda sells L40S only on demand: remove provisioning.spot",
		},
		{
			name:   "machines without a GPU, sold only as spot",
			pool:   []func(*types.WorkerPoolConfig){cpuPool, onDemandPool},
			offers: []providers.Offer{l40s(cpu)},
			want:   "verda sells CPU machines only as spot: set provisioning.spot",
		},
		{
			name:   "regions the provider does not offer",
			pool:   []func(*types.WorkerPoolConfig){inRegions("FIN-03", "ICE-01")},
			offers: []providers.Offer{l40s(in("FIN-02")), l40s(), l40s(named("2L40S.40V")), l40s(in("ICE-01"), onDemand), l40s(in("FIN-03"), a10g)},
			want:   "verda offers no L40S in regions [FIN-03 ICE-01]; it offers them in FIN-01, FIN-02",
		},
		{
			name:   "a price limit under the cheapest",
			pool:   []func(*types.WorkerPoolConfig){payingAtMost(100)},
			offers: []providers.Offer{l40s(costing(150)), l40s(costing(135), named("1L40S.10V")), l40s(costing(90), onDemand)},
			want:   "verda has no L40S within provisioning.maxNodeHourlyCents 100: the cheapest costs 135 cents an hour",
		},
		{
			name:   "a price limit under the cheapest in the pool's regions",
			pool:   []func(*types.WorkerPoolConfig){payingAtMost(100), inRegions("FIN-02")},
			offers: []providers.Offer{l40s(costing(90)), l40s(in("FIN-02"), func(o *providers.Offer) { o.HourlyCostMicros = 1_353_700 })},
			want:   "verda has no L40S within provisioning.maxNodeHourlyCents 100: the cheapest costs 135.37 cents an hour",
		},
		{
			name:   "nothing the pool buys is big enough",
			need:   scheduler.MachineNeed{CPU: 8000, Memory: 32_000, GPUCount: 2},
			offers: []providers.Offer{l40s(), l40s(named("2L40S.40V"), onDemand, func(o *providers.Offer) { o.GPUCount = 2 })},
			want:   "no L40S that verda has for the pool has room for what is needed (cpu 8000m, memory 32000 MiB, gpus 2)",
		},

		// The first thing that stands in the way is the one that is said.
		{
			name:   "sold the other way, and elsewhere, and for too much",
			pool:   []func(*types.WorkerPoolConfig){onDemandPool, inRegions("FIN-03"), payingAtMost(50)},
			offers: []providers.Offer{l40s()},
			want:   "verda sells L40S only as spot: set provisioning.spot",
		},
		{
			name:   "elsewhere, and for too much",
			pool:   []func(*types.WorkerPoolConfig){inRegions("FIN-03"), payingAtMost(50)},
			offers: []providers.Offer{l40s()},
			want:   "verda offers no L40S in regions [FIN-03]; it offers them in FIN-01",
		},
		{
			name:   "for too much, and too small",
			pool:   []func(*types.WorkerPoolConfig){payingAtMost(50)},
			need:   scheduler.MachineNeed{GPUCount: 2},
			offers: []providers.Offer{l40s()},
			want:   "verda has no L40S within provisioning.maxNodeHourlyCents 50: the cheapest costs 100 cents an hour",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := choose(purchase{pool: "spot-l40s", config: spotL40S(test.pool...), needs: []scheduler.MachineNeed{test.need}, offers: test.offers})
			require.EqualError(t, err, test.want)
		})
	}
}

// started returns a world whose pool has been loaded and has bought n
// machines, each of which has joined and has work.
func started(t *testing.T, n int, change ...func(*Options)) (*world, []Machine) {
	t.Helper()
	w := newWorld(t, change...)
	w.Tick(w.ctx)

	machines := make([]Machine, n)
	for i := range machines {
		machines[i] = w.buy("spot-l40s")
		w.workers.join(machines[i])
		w.workers.work(machines[i], true)
	}
	w.after(tickInterval)
	return w, machines
}

// gone checks that nothing is left of a machine: no instance, no record, no
// way to join.
func (w *world) gone(machine Machine) {
	w.t.Helper()
	require.NotContains(w.t, w.instances(), machine.InstanceID, "the instance still exists")
	for _, record := range w.records() {
		require.NotEqual(w.t, machine.ID, record.ID, "the record still exists")
	}
	require.True(w.t, w.control.forgot(machine.ID), "the gateway was not told to forget the machine")
	require.False(w.t, w.control.mayJoin(machine.ID), "the machine can still join")
}

// Invariant 6: a machine that has not joined by its boot timeout is
// terminated.
func TestAMachineThatDoesNotJoinIsDeletedAtItsBootTimeout(t *testing.T) {
	w := newWorld(t)
	w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.BootTimeout = types.Duration(5 * time.Minute) })
	w.Tick(w.ctx)
	machine := w.buy("spot-l40s")

	w.every(tickInterval, 5*time.Minute-tickInterval)
	require.Nil(t, w.record(machine.ID).Release)
	require.Equal(t, []string{machine.InstanceID}, w.instances())

	w.after(tickInterval)
	w.gone(machine)
	require.Equal(t, []string{machine.InstanceID}, w.verda.Deletes())
}

func TestAMachineThatJoinsIsKept(t *testing.T) {
	w := newWorld(t)
	w.Tick(w.ctx)
	machine := w.buy("spot-l40s")

	w.after(3 * time.Minute)
	w.workers.join(machine)
	w.workers.work(machine, true)
	w.after(tickInterval)
	require.Equal(t, w.clock.Now(), w.record(machine.ID).ReadyAt)
	booting, err := w.Booting(w.ctx, "spot-l40s")
	require.NoError(t, err)
	require.Empty(t, booting)

	w.every(time.Minute, 2*time.Hour)
	require.Nil(t, w.record(machine.ID).Release)
	require.Empty(t, w.verda.Deletes())
}

func TestAnIdleMachineIsGivenBackAfterTheIdleTimeout(t *testing.T) {
	w, machines := started(t, 1)
	machine := machines[0]

	// Work comes and goes: the timeout starts over.
	w.workers.work(machine, false)
	w.every(time.Minute, 9*time.Minute)
	w.workers.work(machine, true)
	w.after(time.Minute)
	w.workers.work(machine, false)
	w.every(time.Minute, 9*time.Minute)
	require.Nil(t, w.record(machine.ID).Release)

	w.every(time.Minute, 2*time.Minute)
	w.gone(machine)
}

// The machine is found idle, and gets work before it is cordoned: it stays.
func TestAMachineThatGetsWorkAsItsIdleTimeoutEndsIsKept(t *testing.T) {
	w, machines := started(t, 1)
	machine := machines[0]

	w.workers.work(machine, false)
	w.every(time.Minute, 9*time.Minute)

	// What the pass reads says idle; by the time it cordons, there is work.
	racing := &lateWork{fakeWorkers: w.workers, machine: machine}
	late := w.replica(func(opts *Options) { opts.Workers = racing })
	late.Tick(w.ctx)
	w.clock.Advance(2 * time.Minute)
	late.Tick(w.ctx)

	require.True(t, racing.raced, "the cordon was never attempted")
	record := w.record(machine.ID)
	require.Nil(t, record.Release)
	require.True(t, record.IdleSince.IsZero(), "the idle timeout starts over")
	require.Equal(t, types.WorkerStatusAvailable, w.workers.of(machine).Status)
}

// lateWork gives a machine work between the read that finds it idle and the
// attempt to cordon it.
type lateWork struct {
	*fakeWorkers
	machine Machine
	raced   bool
}

func (l *lateWork) CordonWorkerIfIdle(id string) (bool, error) {
	l.raced = true
	l.fakeWorkers.work(l.machine, true)
	return l.fakeWorkers.CordonWorkerIfIdle(id)
}

// Invariant 4: a machine that holds the floor is never released for being
// idle.
func TestIdleMachinesThatHoldTheFloorAreKept(t *testing.T) {
	w := newWorld(t)
	w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.PoolSizing.MinFreeGPU = "2" })
	w.Tick(w.ctx)

	machines := []Machine{w.buy("spot-l40s"), w.buy("spot-l40s"), w.buy("spot-l40s"), w.buy("spot-l40s")}
	for _, machine := range machines {
		w.workers.join(machine)
	}
	// The floor is held by the ready workers that come first by ID.
	slices.SortFunc(machines, func(a, b Machine) int { return compareStrings(a.workerID(), b.workerID()) })

	w.every(time.Minute, time.Hour)
	require.Len(t, w.records(), 2)
	_ = w.record(machines[0].ID)
	_ = w.record(machines[1].ID)
	w.gone(machines[2])
	w.gone(machines[3])

	// One of them gets work, so the next in line would hold the floor; with
	// nothing else in the pool, the two stay for as long as they like.
	w.workers.work(machines[0], true)
	w.every(time.Minute, time.Hour)
	require.Len(t, w.records(), 2)
}

func compareStrings(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func TestAFloorIsNotPaidForOnceTheDailyBudgetIsSpent(t *testing.T) {
	w := newWorld(t, func(opts *Options) { opts.Limits.MaxDailyCents = 150 })
	w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.PoolSizing.MinFreeGPU = "1" })
	w.Tick(w.ctx)
	machine := w.buy("spot-l40s")
	w.workers.join(machine)

	// A dollar an hour: it holds the floor for an hour and a half.
	w.every(time.Minute, 85*time.Minute)
	require.Nil(t, w.record(machine.ID).Release)

	w.every(time.Minute, 10*time.Minute)
	w.gone(machine)
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	require.InDelta(t, 150, w.spentCents(), 3)
}

// A machine its provider took back is forgotten in the pass that sees it,
// which returns its requests to the backlog: nothing waits for its worker's
// state to expire. What the provider left of it is deleted.
func TestAMachineItsProviderTookBackIsForgottenAtOnce(t *testing.T) {
	tests := []struct {
		name string
		take func(*world, Machine)
	}{
		{"it was interrupted", func(w *world, m Machine) { w.verda.SetStatus(m.InstanceID, providers.StatusInterrupted, "preempted") }},
		{"it is being deleted", func(w *world, m Machine) { w.verda.SetStatus(m.InstanceID, providers.StatusDeleted, "") }},
		{"it is gone", func(w *world, m Machine) { w.verda.Forget(m.InstanceID) }},
		{"it failed", func(w *world, m Machine) { w.verda.SetStatus(m.InstanceID, providers.StatusFailed, "host error") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w, machines := started(t, 2)
			// Past the launch grace, a machine its provider does not have
			// is not one the provider has yet to show.
			w.after(launchGrace)

			test.take(w, machines[0])
			w.after(tickInterval)

			w.gone(machines[0])
			require.Nil(t, w.record(machines[1].ID).Release, "the other machine is not touched")
			require.False(t, w.control.forgot(machines[1].ID))
		})
	}
}

// What a machine was bought as is not bought again at once after its
// provider failed to start it: the next machine comes from another offer, if
// there is one. A pool would otherwise buy the same machine, and wait for it
// to fail, for as long as it is the cheapest.
func TestAnOfferWhoseMachineFailedToStartIsPassedOverForAWhile(t *testing.T) {
	w := newWorld(t)
	// The same machine type in two clouds of an aggregator, and in another
	// region.
	w.verda.SetOffers(
		l40s(func(o *providers.Offer) { o.Cloud = "hyperstack" }),
		l40s(func(o *providers.Offer) { o.Cloud = "massedcompute" }, costing(110)),
		l40s(in("FIN-02"), costing(140)),
	)
	other := w.replica()
	w.Tick(w.ctx)
	other.Tick(w.ctx)
	machine := w.buy("spot-l40s")
	require.Equal(t, "hyperstack", machine.Cloud)

	w.verda.SetStatus(machine.InstanceID, providers.StatusFailed, "no_capacity")
	w.after(tickInterval)
	w.gone(machine)

	// The replica that saw it fail buys the next cheapest.
	require.Equal(t, "massedcompute", w.buy("spot-l40s").Cloud)

	// So does a replica that did not, once it has looked.
	other.Tick(w.ctx)
	bought, err := other.Provision(w.ctx, "spot-l40s", anyMachine)
	require.NoError(t, err)
	require.Equal(t, "massedcompute", w.record(bought.MachineID).Cloud)

	// The offer is bought again after soldOutFor.
	w.verda.SetOffers(l40s(func(o *providers.Offer) { o.Cloud = "hyperstack" }), l40s(in("FIN-02"), costing(140)))
	w.every(tickInterval, soldOutFor-2*tickInterval)
	require.Equal(t, "FIN-02", w.buy("spot-l40s").Region)
	w.every(tickInterval, 2*tickInterval)
	require.Equal(t, "hyperstack", w.buy("spot-l40s").Cloud)
}

// A machine that ran and was then taken back is replaced at once, by the
// same offer if the provider still lists it: a provider that has no more
// says so when it is asked.
func TestAnOfferWhoseMachineWasTakenBackIsBoughtAgain(t *testing.T) {
	tests := []struct {
		name string
		take func(*world, Machine)
	}{
		{"it was interrupted", func(w *world, m Machine) { w.verda.SetStatus(m.InstanceID, providers.StatusInterrupted, "preempted") }},
		{"it is being deleted", func(w *world, m Machine) { w.verda.SetStatus(m.InstanceID, providers.StatusDeleted, "") }},
		{"it is gone", func(w *world, m Machine) { w.verda.Forget(m.InstanceID) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := newWorld(t)
			w.verda.SetOffers(l40s(), l40s(in("FIN-02"), costing(140)))
			w.Tick(w.ctx)
			machine := w.buy("spot-l40s")
			w.workers.join(machine)
			w.after(launchGrace)

			test.take(w, machine)
			w.after(tickInterval)
			w.gone(machine)

			require.Equal(t, "FIN-01", w.buy("spot-l40s").Region)
		})
	}
}

// A provider that has no spot machine to spare does not always say so when
// it is asked: it sells one, and takes it back before it has come up. What
// that machine was bought as is passed over for a while, like an offer the
// provider refused to sell, so that the pool does not buy it over and over
// while what waits for it waits.
func TestAnOfferWhoseMachineWasTakenBackBeforeItRanIsPassedOverForAWhile(t *testing.T) {
	taken := func(offers ...providers.Offer) *world {
		w := newWorld(t)
		w.verda.SetOffers(offers...)
		w.Tick(w.ctx)
		machine := w.buy("spot-l40s")
		w.after(launchGrace)
		w.verda.SetStatus(machine.InstanceID, providers.StatusInterrupted, "preempted")
		w.after(tickInterval)
		w.gone(machine)
		return w
	}

	// The next machine is bought as something else.
	w := taken(l40s(), l40s(in("FIN-02"), costing(140)))
	require.Equal(t, "FIN-02", w.buy("spot-l40s").Region)

	// A pool that buys nothing else has nothing to buy meanwhile, and says
	// so to what would wait for it.
	w = taken(l40s())
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	w.every(tickInterval, soldOutFor)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
}

// A machine that is given back for a reason of the pool's own says nothing
// about the offer. One that never came up does: the wait for the next is
// expected to be twice as long, which an offer that costs a little more is
// worth paying to avoid.
func TestAnOfferWhoseMachineThePoolGaveBackIsBoughtAgain(t *testing.T) {
	for _, test := range []struct {
		name, next string
		joins      bool
	}{
		{name: "it had nothing to run", joins: true, next: "FIN-01"},
		{name: "it never joined", next: "FIN-02"},
	} {
		t.Run(test.name, func(t *testing.T) {
			w := newWorld(t)
			w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.BootTimeout = types.Duration(5 * time.Minute) })
			w.verda.SetOffers(l40s(), l40s(in("FIN-02"), costing(140)))
			w.Tick(w.ctx)

			machine := w.buy("spot-l40s")
			if test.joins {
				w.workers.join(machine)
			}
			w.every(time.Minute, 12*time.Minute)
			w.gone(machine)

			require.Equal(t, test.next, w.buy("spot-l40s").Region)
		})
	}
}

// What one replica is refused for lack of stock, the others pass over too,
// once they have looked.
func TestAnOfferOneReplicaWasRefusedIsPassedOverByTheOthers(t *testing.T) {
	w := newWorld(t)
	w.verda.SetOffers(l40s(), l40s(in("FIN-02"), costing(140)))
	other := w.replica()
	w.Tick(w.ctx)
	other.Tick(w.ctx)

	w.verda.RefuseCreates(providers.ErrOutOfStock)
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.ErrorIs(t, err, providers.ErrOutOfStock)
	w.verda.RefuseCreates(nil)

	w.clock.Advance(tickInterval)
	other.Tick(w.ctx)
	bought, err := other.Provision(w.ctx, "spot-l40s", anyMachine)
	require.NoError(t, err)
	require.Equal(t, "FIN-02", w.record(bought.MachineID).Region)

	// Both buy it again after soldOutFor from the refusal.
	w.clock.Advance(soldOutFor - tickInterval)
	other.Tick(w.ctx)
	bought, err = other.Provision(w.ctx, "spot-l40s", anyMachine)
	require.NoError(t, err)
	require.Equal(t, "FIN-01", w.record(bought.MachineID).Region)
}

func TestNothingIsGivenUpWhileTheProviderCannotBeAsked(t *testing.T) {
	provider := &troubled{Fake: providers.NewFake("verda", testOwner)}
	provider.SetOffers(l40s())
	w, machines := started(t, 1, func(opts *Options) { opts.Providers = providers.RegistryOf(provider) })

	provider.failLists(errors.New("the provider is down"))
	w.every(time.Minute, time.Hour)
	require.Nil(t, w.record(machines[0].ID).Release)
	require.Empty(t, provider.Deletes())

	// A list that is behind: the instance is asked for before it is
	// believed gone.
	provider.failLists(nil)
	stale := &staleList{troubled: provider}
	behind := w.replica(func(opts *Options) { opts.Providers = providers.RegistryOf(stale) })
	behind.Tick(w.ctx)
	w.clock.Advance(tickInterval)
	behind.Tick(w.ctx)
	require.Nil(t, w.record(machines[0].ID).Release)
}

// A purchase whose answer was lost has a record and no instance to ask
// for. While its provider cannot be asked, the machine may well exist, so
// its record stays however long that lasts.
func TestAPurchaseNeverAnsweredIsNotGivenUpWhileTheProviderCannotBeAsked(t *testing.T) {
	provider := &troubled{Fake: providers.NewFake("verda", testOwner)}
	provider.SetOffers(l40s())
	w := newWorld(t, func(opts *Options) { opts.Providers = providers.RegistryOf(provider) })
	w.Tick(w.ctx)

	provider.LoseAnswers(1)
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.Error(t, err)

	provider.failLists(errors.New("the provider is down"))
	w.every(time.Minute, time.Hour)
	records := w.records()
	require.Len(t, records, 1)
	require.Empty(t, records[0].InstanceID)
	require.Nil(t, records[0].Release)

	// The provider answers again. The machine is found by its tag and, long
	// past its boot timeout by now, deleted through its record.
	provider.failLists(nil)
	w.after(tickInterval)
	require.Equal(t, []string{"verda-1"}, provider.Deletes())
	require.Empty(t, provider.Everything())
	require.Empty(t, w.records())
}

// staleList is a provider whose list has nothing in it yet.
type staleList struct{ *troubled }

func (p *staleList) List(ctx context.Context) ([]providers.Instance, error) {
	return []providers.Instance{}, nil
}

func TestNothingIsGivenUpWhileThePoolsWorkersCannotBeRead(t *testing.T) {
	w := newWorld(t)
	w.Tick(w.ctx)
	booting := w.buy("spot-l40s")
	idle := w.buy("spot-l40s")
	w.workers.join(idle)
	w.after(tickInterval)

	w.workers.fail(errors.New("redis is down"))
	w.every(time.Minute, time.Hour)
	require.Nil(t, w.record(booting.ID).Release, "it may have joined")
	require.Nil(t, w.record(idle.ID).Release, "it may have work")
	require.Empty(t, w.verda.Deletes())
}

func TestAMachineWhoseWorkerDoesNotComeBackIsGivenBack(t *testing.T) {
	w, machines := started(t, 1)
	machine := machines[0]

	// Its worker is gone for a while, and comes back.
	w.workers.lose(machine)
	w.every(time.Minute, 19*time.Minute)
	w.workers.join(machine)
	w.workers.work(machine, true)
	w.after(time.Minute)
	require.True(t, w.record(machine.ID).DownSince.IsZero())

	// It is gone for the boot timeout.
	w.workers.lose(machine)
	w.every(time.Minute, 20*time.Minute)
	require.Nil(t, w.record(machine.ID).Release)
	w.after(time.Minute)
	w.gone(machine)
}

func TestTheRecordStaysUntilTheProviderHasDeletedTheMachine(t *testing.T) {
	w, machines := started(t, 1)
	machine := machines[0]
	w.workers.work(machine, false)

	w.verda.FailDeletes(errors.New("the provider is down"))
	w.every(time.Minute, 30*time.Minute)

	record := w.record(machine.ID)
	require.NotNil(t, record.Release)
	require.Equal(t, "it had nothing to run for 10m0s", record.Release.Reason)
	require.Equal(t, []string{machine.InstanceID}, w.instances())
	require.Greater(t, len(w.verda.Deletes()), 10, "the delete is tried again on every pass")
	// It still counts against maxNodes and the caps, and is still paid for.
	require.InDelta(t, 100*w.clock.Now().Sub(bought).Hours(), w.spentCents(), 3)

	w.verda.FailDeletes(nil)
	w.after(time.Minute)
	w.gone(machine)
}

func TestAMachineIsNotDeletedBeforeTheGatewayHasForgottenIt(t *testing.T) {
	w, machines := started(t, 1)
	machine := machines[0]
	w.workers.work(machine, false)

	// Its requests could not be returned to the backlog: it is not deleted
	// from under them.
	w.control.set(func(c *fakeControl) { c.forgetErr = errors.New("redis is down") })
	w.every(time.Minute, 30*time.Minute)
	require.NotNil(t, w.record(machine.ID).Release)
	require.Empty(t, w.verda.Deletes())

	w.control.set(func(c *fakeControl) { c.forgetErr = nil })
	w.after(time.Minute)
	w.gone(machine)
}

// Invariant 5: tagged instances with no record are terminated.
func TestInstancesNobodyRecordedAreDeleted(t *testing.T) {
	w, machines := started(t, 1)

	orphan := w.verda.Plant(testOwner, providers.StatusActive, map[string]string{MachineTag: "machine-of-a-gateway-that-died"})
	// Not bought for a pool: a tenant's reservation, say.
	untagged := w.verda.Plant(testOwner, providers.StatusActive, map[string]string{"reservation": "r-1"})
	// Bought for a pool by another deployment that shares the account.
	foreign := w.verda.Plant("staging", providers.StatusActive, map[string]string{MachineTag: "machine-of-staging"})
	// Already on its way out.
	deleting := w.verda.Plant(testOwner, providers.StatusDeleted, map[string]string{MachineTag: "machine-on-its-way-out"})

	w.after(tickInterval)
	require.ElementsMatch(t, []string{machines[0].InstanceID, untagged, foreign, deleting}, w.instances())
	require.Equal(t, []string{orphan}, w.verda.Deletes())
	require.Nil(t, w.record(machines[0].ID).Release)
}

// A provider that no pool buys from any more can still hold what a pool
// bought before its config changed. It is searched, less often.
func TestProvidersNoPoolBuysFromAreSweptLessOften(t *testing.T) {
	nebius := providers.NewFake("nebius", testOwner)
	w := newWorld(t)
	p := w.replica(func(opts *Options) { opts.Providers = providers.RegistryOf(w.verda, nebius) })

	first := nebius.Plant(testOwner, providers.StatusActive, map[string]string{MachineTag: "machine-1"})
	p.Tick(w.ctx)
	require.Equal(t, []string{first}, nebius.Deletes())

	second := nebius.Plant(testOwner, providers.StatusActive, map[string]string{MachineTag: "machine-2"})
	for passed := time.Duration(0); passed < sweepAllEvery-time.Minute; passed += time.Minute {
		w.clock.Advance(time.Minute)
		p.Tick(w.ctx)
	}
	require.Equal(t, []string{first}, nebius.Deletes())

	w.clock.Advance(time.Minute)
	p.Tick(w.ctx)
	require.Equal(t, []string{first, second}, nebius.Deletes())
}

// What the disabled Vast of these tests is configured with, and the
// instances it has: one that a machine's record names, one that a purchase
// without an answer left, one that nobody recorded, and another control
// plane's.
const (
	disabledVastName = "vast"
	disabledVastKey  = "synthetic-key"

	vastInstancesPath = "/api/v1/instances/"
	vastInstancePath  = "/api/v0/instances/"

	vastRecorded   = "101"
	vastUnanswered = "102"
	vastOrphan     = "103"
	vastForeign    = "104"

	recordedMachine   = "existing"
	unansweredMachine = "unanswered"
)

// disabledVast stands in for Vast behind a registry that disables it, and
// counts what the registry's real client asks of it.
type disabledVast struct {
	mu                           sync.Mutex
	lists, gets, offers, creates int
	deleted                      []string
}

// newDisabledVast returns a registry whose one provider is a disabled Vast,
// and the stand-in its client talks to.
func newDisabledVast(t *testing.T) (*providers.Registry, *disabledVast) {
	t.Helper()
	api := &disabledVast{}
	instance := func(id, machine, owner string) map[string]any {
		return map[string]any{"id": json.Number(id), "actual_status": "running", "is_bid": true,
			"label": fmt.Sprintf("owned-vm %s=%s %s=%s", providers.OwnerTag, owner, MachineTag, machine)}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		api.mu.Lock()
		defer api.mu.Unlock()
		if r.Header.Get("Authorization") != "Bearer "+disabledVastKey {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		switch {
		case r.Method == http.MethodGet && r.URL.Path == vastInstancesPath:
			api.lists++
			// The recorded instance is not in the list yet, so that it is
			// asked for by its ID. The purchase without an answer is found
			// by its tag alone.
			_ = json.NewEncoder(w).Encode(map[string]any{"instances": []any{
				instance(vastUnanswered, unansweredMachine, testOwner),
				instance(vastOrphan, "orphan", testOwner),
				instance(vastForeign, "foreign", "another-owner"),
			}})
		case r.Method == http.MethodGet && r.URL.Path == vastInstancePath+vastRecorded+"/":
			api.gets++
			_ = json.NewEncoder(w).Encode(map[string]any{"instances": instance(vastRecorded, recordedMachine, testOwner)})
		case r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, vastInstancePath):
			api.deleted = append(api.deleted, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, vastInstancePath), "/"))
			_ = json.NewEncoder(w).Encode(map[string]bool{"success": true})
		case r.Method == http.MethodPost:
			// A search for offers.
			api.offers++
			w.WriteHeader(http.StatusBadRequest)
		case r.Method == http.MethodPut:
			// A bid for a machine.
			api.creates++
			w.WriteHeader(http.StatusBadRequest)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	registry := providers.NewRegistry(types.ProviderConfig{
		Disabled: []string{disabledVastName}, Vast: types.VastProviderConfig{ApiKey: disabledVastKey, BaseURL: server.URL},
	}, testOwner)
	require.Empty(t, registry.Names(), "this is a genuinely disabled registry client")
	require.Equal(t, []string{disabledVastName}, registry.ConfiguredNames())
	return registry, api
}

func TestDisabledProviderLifecycleStillObservesExistingAndUnansweredPurchases(t *testing.T) {
	registry, api := newDisabledVast(t)
	ctx, now := context.Background(), time.Now()
	p := &Provisioner{providers: registry, sweptAll: now}
	existing := Machine{ID: recordedMachine, InstanceID: vastRecorded}
	existing.Provider = disabledVastName
	unanswered := Machine{ID: unansweredMachine}
	unanswered.Provider = disabledVastName
	listed := p.listInstances(ctx, snapshot{}, []Machine{existing, unanswered}, now)
	require.Contains(t, listed, disabledVastName, "disabled providers with existing records still need lifecycle inspection")
	state, known := p.instanceOf(ctx, existing, listed)
	require.Equal(t, present, state)
	require.Equal(t, vastRecorded, known.ID, "a stale list must still reach the real disabled provider's Get")
	state, found := p.instanceOf(ctx, unanswered, listed)
	require.Equal(t, present, state)
	require.Equal(t, vastUnanswered, found.ID, "an unanswered purchase is rediscovered by its machine tag")
	p.deleteUnrecorded(ctx, listed, map[string]string{existing.ID: existing.InstanceID, unanswered.ID: ""})
	client, ok := registry.Get(disabledVastName)
	require.True(t, ok)
	// Once the rediscovered purchase is released, its disabled client can
	// still delete it; the orphan sweep already deleted the unrecorded VM.
	require.NoError(t, client.Delete(ctx, found.ID))
	offers, err := client.Offers(ctx, providers.OfferFilter{})
	require.NoError(t, err)
	require.Empty(t, offers)
	_, err = client.Create(ctx, providers.CreateRequest{})
	require.ErrorIs(t, err, providers.ErrDisabled)
	api.mu.Lock()
	defer api.mu.Unlock()
	require.Equal(t, 1, api.lists)
	require.Equal(t, 1, api.gets)
	require.ElementsMatch(t, []string{vastOrphan, vastUnanswered}, api.deleted)
	require.Zero(t, api.offers)
	require.Zero(t, api.creates)
}

func TestDisabledProviderWithoutPoolsIsStillSweptForOrphans(t *testing.T) {
	registry, api := newDisabledVast(t)
	p := &Provisioner{providers: registry}
	ctx := context.Background()
	listed := p.listInstances(ctx, snapshot{}, nil, time.Now())
	p.deleteUnrecorded(ctx, listed, nil)
	api.mu.Lock()
	defer api.mu.Unlock()
	require.Equal(t, 1, api.lists, "the periodic full sweep includes disabled providers even without records or pools")
	require.ElementsMatch(t, []string{vastUnanswered, vastOrphan}, api.deleted, "foreign-owner instances never reach cleanup")
	require.Zero(t, api.offers)
	require.Zero(t, api.creates)
}

func TestMachinesOfAPoolThatNoLongerBuysAreDrained(t *testing.T) {
	w, machines := started(t, 3)
	idle, short, long := machines[0], machines[1], machines[2]
	w.workers.work(idle, false)

	// The pool is deleted, or its provisioning block is removed.
	w.control.set(func(c *fakeControl) { delete(c.pools, "spot-l40s") })
	w.every(time.Minute, poolGoneGrace)
	require.Len(t, w.records(), 3, "replicas may disagree about pools for a while; nothing happens yet")
	for _, machine := range machines {
		require.Nil(t, w.record(machine.ID).Release)
	}

	// After the grace, the idle machine goes at once and the others are
	// cordoned, so they get no new work.
	w.after(time.Minute)
	w.gone(idle)
	for _, machine := range []Machine{short, long} {
		record := w.record(machine.ID)
		require.Equal(t, "its pool no longer buys machines", record.Release.Reason)
		require.Equal(t, w.clock.Now().Add(drainFor), record.Release.DrainUntil)
		require.True(t, w.workers.of(machine).CordonRequested)
		require.False(t, w.control.forgot(machine.ID))
	}

	// One finishes its work and goes.
	w.workers.work(short, false)
	w.after(time.Minute)
	w.gone(short)
	require.Len(t, w.records(), 1)

	// The other is given drainFor, and no longer.
	w.every(time.Minute, drainFor-2*time.Minute)
	require.Len(t, w.records(), 1)
	w.after(time.Minute)
	w.gone(long)
}

func TestAPoolThatChangedProviderNoLongerBuysItsOldMachines(t *testing.T) {
	w, machines := started(t, 1)
	w.workers.work(machines[0], false)

	w.control.set(func(c *fakeControl) {
		c.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Providers = []string{"nebius"} })
	})
	w.every(time.Minute, poolGoneGrace+time.Minute)
	w.gone(machines[0])
}

// A pool that could not be loaded may still exist, so its machines are kept.
// It does not hold up the machines of a pool that is gone.
func TestMachinesAreKeptWhileThePoolsCannotAllBeLoaded(t *testing.T) {
	w := newWorld(t)
	w.control.pools["spot-a10g"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.GPUType = "A10G" })
	w.verda.SetOffers(l40s(), l40s(named("1A10G.8V"), func(o *providers.Offer) { o.GPU = "A10G" }))
	w.Tick(w.ctx)
	kept, drained := w.buy("spot-l40s"), w.buy("spot-a10g")
	for _, machine := range []Machine{kept, drained} {
		w.workers.join(machine)
		w.workers.work(machine, true)
	}
	w.after(tickInterval)

	// One pool did not load, and the gateway said which. The other was
	// deleted.
	w.control.set(func(c *fakeControl) {
		delete(c.pools, "spot-l40s")
		delete(c.pools, "spot-a10g")
		c.unloaded = map[string]error{"spot-l40s": errors.New("unsupported container runtime \"kata\"")}
	})
	w.every(time.Minute, time.Hour)

	record := w.record(kept.ID)
	require.Nil(t, record.Release)
	require.True(t, record.PoolGoneSince.IsZero())
	w.gone(drained)
}

// During a rolling deployment one gateway has a new pool and the other does
// not. The one that has it keeps the other from draining its machines.
func TestAReplicaThatHasThePoolKeepsItsMachines(t *testing.T) {
	w, machines := started(t, 1)

	old := w.replica(func(opts *Options) {
		opts.ControlPlane = &fakeControl{pools: map[string]types.WorkerPoolConfig{}}
	})
	for passed := time.Duration(0); passed < time.Hour; passed += tickInterval {
		w.clock.Advance(tickInterval)
		// Whichever ticks first runs the pass; the other only refreshes.
		old.Tick(w.ctx)
		w.Tick(w.ctx)
	}
	require.Nil(t, w.record(machines[0].ID).Release)

	// The replica that had the pool is gone for good.
	for passed := time.Duration(0); passed <= poolGoneGrace; passed += tickInterval {
		w.clock.Advance(tickInterval)
		old.Tick(w.ctx)
	}
	require.NotNil(t, w.record(machines[0].ID).Release)
}

func TestOnlyOneReplicaChecksTheMachinesPerInterval(t *testing.T) {
	w, _ := started(t, 1)
	other := w.replica()
	w.verda.FailDeletes(errors.New("the provider is down"))
	w.verda.Plant(testOwner, providers.StatusActive, map[string]string{MachineTag: "machine-nobody-recorded"})

	w.clock.Advance(tickInterval)
	w.Tick(w.ctx)
	other.Tick(w.ctx)
	require.Len(t, w.verda.Deletes(), 1, "one pass, so one attempt")

	// The replica that did not run the pass still knows what can be bought.
	require.True(t, other.CanProvision(w.ctx, "spot-l40s", anyMachine))

	w.clock.Advance(tickInterval)
	other.Tick(w.ctx)
	w.Tick(w.ctx)
	require.Len(t, w.verda.Deletes(), 2)
}

func TestTheProvidersOwnDeadlineIsPutOffWhileTheMachineIsHeld(t *testing.T) {
	w, machines := started(t, 1)
	id := machines[0].InstanceID
	require.Equal(t, bought.Add(leaseFor), w.verda.Until(id))

	// Not before it comes close.
	w.every(time.Minute, leaseFor-renewLeaseWithin-2*time.Minute)
	require.Equal(t, bought.Add(leaseFor), w.verda.Until(id))

	w.every(time.Minute, 3*time.Minute)
	until := w.verda.Until(id)
	require.True(t, until.After(bought.Add(leaseFor)))
	require.Equal(t, until, w.record(machines[0].ID).Until)

	// A day on, the provider's deadline is still ahead.
	w.every(5*time.Minute, 24*time.Hour)
	require.GreaterOrEqual(t, w.verda.Until(id).Sub(w.clock.Now()), renewLeaseWithin)
}

func TestWhatMachinesCostIsAddedUp(t *testing.T) {
	w := newWorld(t)
	w.verda.SetOffers(l40s(costing(120)))
	w.Tick(w.ctx)

	// Two machines at $1.20 an hour, with work for 90 minutes.
	machines := []Machine{w.buy("spot-l40s"), w.buy("spot-l40s")}
	for _, machine := range machines {
		w.workers.join(machine)
		w.workers.work(machine, true)
	}
	w.every(time.Minute, 90*time.Minute)
	require.InDelta(t, 2*120*1.5, w.spentCents(), 1)

	// Given back after ten idle minutes, and billed to the minute they went.
	for _, machine := range machines {
		w.workers.work(machine, false)
	}
	w.every(time.Minute, 30*time.Minute)
	require.Empty(t, w.records())
	require.InDelta(t, 2*120*(100.0/60), w.spentCents(), 6)

	// Nothing more is spent on machines that are gone.
	spent := w.spentCents()
	w.every(time.Minute, time.Hour)
	require.Equal(t, spent, w.spentCents())
}

func TestAPurchaseNeverAnsweredCostsNothingUntilItIsFound(t *testing.T) {
	w := newWorld(t)
	w.Tick(w.ctx)
	w.verda.RefuseCreates(errors.New("502 bad gateway"))
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.Error(t, err)

	w.every(tickInterval, 10*time.Minute)
	require.Empty(t, w.records())
	require.Zero(t, w.spentCents())
}

// A gateway that starts while the pools cannot be read has never seen one.
// That is no reason to take every machine for one whose pool is gone.
func TestNothingIsDrainedBeforeThePoolsHaveBeenRead(t *testing.T) {
	w, machines := started(t, 1)

	restarted := w.replica(func(opts *Options) {
		opts.ControlPlane = &fakeControl{poolsErr: errors.New("invalid managed pool state")}
	})
	for passed := time.Duration(0); passed < time.Hour; passed += tickInterval {
		w.clock.Advance(tickInterval)
		restarted.Tick(w.ctx)
	}

	record := w.record(machines[0].ID)
	require.Nil(t, record.Release)
	require.True(t, record.PoolGoneSince.IsZero())
	require.False(t, restarted.CanProvision(w.ctx, "spot-l40s", anyMachine), "nor does it buy for a pool it has not seen")
}

func TestWhatWasLastReadOfThePoolsStandsWhileTheyCannotBeRead(t *testing.T) {
	w, machines := started(t, 1)
	unreadable := func(c *fakeControl) { c.poolsErr = errors.New("redis is down") }

	// A pool that was there is still taken to be.
	w.control.set(unreadable)
	w.every(time.Minute, time.Hour)
	record := w.record(machines[0].ID)
	require.Nil(t, record.Release)
	require.True(t, record.PoolGoneSince.IsZero())
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))

	// A pool that was gone is still taken to be: its machines go.
	w.control.set(func(c *fakeControl) {
		c.poolsErr = nil
		delete(c.pools, "spot-l40s")
	})
	w.after(time.Minute)
	w.control.set(unreadable)
	w.every(time.Minute, poolGoneGrace+drainFor)
	w.gone(machines[0])
}

func TestAPoolThatCouldNotBeLoadedIsReportedOnceForEachReason(t *testing.T) {
	w := newWorld(t)
	lines := logged(t)
	reported := func() int { return strings.Count(lines.String(), "a pool could not be loaded") }

	w.control.set(func(c *fakeControl) {
		c.unloaded = map[string]error{"spot-t4": errors.New("provisioning.maxNodes must be at least 1")}
	})
	w.every(tickInterval, time.Minute)
	require.Equal(t, 1, reported())
	require.Contains(t, lines.String(), `"pool_name":"spot-t4"`)
	require.Contains(t, lines.String(), "provisioning.maxNodes must be at least 1")

	w.control.set(func(c *fakeControl) {
		c.unloaded = map[string]error{"spot-t4": errors.New(`unsupported container runtime "kata"`)}
	})
	w.every(tickInterval, time.Minute)
	require.Equal(t, 2, reported())
}

// What a pass cannot read, it cannot act on: while that lasts, machines are
// not given back. It says so, once for each pass.

func TestATickSaysWhenItCannotReadTheMachinesToVouchFor(t *testing.T) {
	w := newWorld(t)
	lines := logged(t)
	said := func() int { return strings.Count(lines.String(), "could not read the machines held to vouch for") }

	w.every(tickInterval, time.Minute)
	require.Zero(t, said())

	// One record that cannot be read fails the list of them.
	require.NoError(t, w.rdb.HSet(w.ctx, common.RedisKeys.ComputeProvisionedMachines(), "machine-1", "{").Err())
	w.every(tickInterval, time.Minute)
	require.Equal(t, 4, said(), "once for each tick")
	require.Contains(t, lines.String(), "read the record of machine machine-1")
}

func TestAPassSaysWhenItCannotReadAPoolsWorkers(t *testing.T) {
	w, _ := started(t, 2)
	lines := logged(t)
	said := func() int { return strings.Count(lines.String(), "could not read a pool's workers") }

	w.every(tickInterval, time.Minute)
	require.Zero(t, said())

	w.workers.fail(errors.New("redis is down"))
	w.every(tickInterval, time.Minute)
	require.Equal(t, 4, said(), "once for each pass, however many machines the pool has")
	require.Contains(t, lines.String(), `"pool_name":"spot-l40s"`)
	require.Contains(t, lines.String(), "redis is down")
}

func TestAPassSaysWhenItCannotAskAProviderAboutAMachineItDoesNotList(t *testing.T) {
	provider := &troubled{Fake: providers.NewFake("verda", testOwner)}
	provider.SetOffers(l40s())
	w, machines := started(t, 1, func(opts *Options) { opts.Providers = providers.RegistryOf(provider) })
	lines := logged(t)
	said := func() int {
		return strings.Count(lines.String(), "could not ask the provider about a machine it does not list")
	}

	// The provider's list is behind, and it answers for the machine itself.
	behind := w.replica(func(opts *Options) { opts.Providers = providers.RegistryOf(&staleList{troubled: provider}) })
	pass := func() {
		w.clock.Advance(tickInterval)
		behind.Tick(w.ctx)
	}
	pass()
	require.Zero(t, said())

	// It no longer does.
	provider.failGets(errors.New("503 service unavailable"))
	for range 4 {
		pass()
	}
	require.Equal(t, 4, said(), "once for each pass")
	require.Contains(t, lines.String(), `"machine_id":"`+machines[0].ID+`"`)
	require.Contains(t, lines.String(), "503 service unavailable")
}

// That another replica has the pass is no error. That Redis cannot say when
// the last one ran is: no pass runs, and the tick says so.
func TestAPassThatCannotBeStartedIsAnErrorUnlessAnotherReplicaHasIt(t *testing.T) {
	w := newWorld(t)
	lines := logged(t)
	passes := 0
	pass := func(context.Context) { passes++ }

	// The time of the last pass is not something that can be read as one.
	require.NoError(t, w.rdb.HSet(w.ctx, common.RedisKeys.ComputeProvisionedReconcile(), "not", "a time").Err())
	require.Error(t, w.store.Reconcile(w.ctx, w.clock.Now(), tickInterval, pass))
	require.Zero(t, passes)

	w.Tick(w.ctx)
	require.Equal(t, 1, strings.Count(lines.String(), "provisioner: could not check the machines held"))

	// Another replica holds the pass: nothing is said.
	require.NoError(t, w.rdb.Del(w.ctx, common.RedisKeys.ComputeProvisionedReconcile()).Err())
	lock := common.NewRedisLock(w.rdb)
	require.NoError(t, lock.Acquire(w.ctx, common.RedisKeys.ComputeProvisionedReconcileLock(), common.RedisLockOptions{TtlS: 60}))
	require.NoError(t, w.store.Reconcile(w.ctx, w.clock.Now(), tickInterval, pass))
	require.Zero(t, passes)

	w.Tick(w.ctx)
	require.Equal(t, 1, strings.Count(lines.String(), "provisioner: could not check the machines held"))
}

// slowToShow is a provider that does not know, while it is hiding them, the
// instances it has sold: EC2 answers that there is no such instance for the
// first moments after it has returned the instance's ID.
type slowToShow struct {
	*providers.Fake
	hiding atomic.Bool
}

func (p *slowToShow) List(ctx context.Context) ([]providers.Instance, error) {
	if p.hiding.Load() {
		return []providers.Instance{}, nil
	}
	return p.Fake.List(ctx)
}

func (p *slowToShow) Get(ctx context.Context, id string) (providers.Instance, error) {
	if p.hiding.Load() {
		return providers.Instance{}, fmt.Errorf("%w: %s has no instance %s yet", providers.ErrNotFound, p.Name(), id)
	}
	return p.Fake.Get(ctx, id)
}

func newSlowToShowWorld(t *testing.T) (*world, *slowToShow) {
	t.Helper()
	provider := &slowToShow{Fake: providers.NewFake("verda", testOwner)}
	provider.SetOffers(l40s())
	w := newWorld(t, func(opts *Options) { opts.Providers = providers.RegistryOf(provider) })
	w.Tick(w.ctx)
	return w, provider
}

func TestAMachineItsProviderCannotFindYetIsNotGivenUp(t *testing.T) {
	w, provider := newSlowToShowWorld(t)
	machine := w.buy("spot-l40s")

	provider.hiding.Store(true)
	w.every(tickInterval, launchGrace-tickInterval)
	require.Nil(t, w.record(machine.ID).Release)
	require.True(t, w.control.mayJoin(machine.ID))

	// The provider has caught up: the machine is the pool's as before.
	provider.hiding.Store(false)
	w.every(tickInterval, launchGrace)
	require.Nil(t, w.record(machine.ID).Release)
	require.Empty(t, provider.Deletes())
	require.Len(t, provider.Everything(), 1)
}

func TestAMachineItsProviderStillCannotFindAfterTheLaunchGraceIsGivenUp(t *testing.T) {
	w, provider := newSlowToShowWorld(t)
	machine := w.buy("spot-l40s")

	provider.hiding.Store(true)
	w.every(tickInterval, launchGrace)
	require.Empty(t, w.records())
	require.True(t, w.control.forgot(machine.ID))
	require.False(t, w.control.mayJoin(machine.ID))
}

// A provider whose list does not have a machine is asked for the machine
// itself. While that fails, it has not said that the machine is gone: the
// machine is kept, however long it lasts.
func TestAMachineIsKeptWhileItsProviderCannotBeAskedAboutIt(t *testing.T) {
	provider := &troubled{Fake: providers.NewFake("verda", testOwner)}
	provider.SetOffers(l40s())
	w, machines := started(t, 1, func(opts *Options) { opts.Providers = providers.RegistryOf(provider) })
	machine := machines[0]

	behind := w.replica(func(opts *Options) { opts.Providers = providers.RegistryOf(&staleList{troubled: provider}) })
	pass := func() {
		w.clock.Advance(time.Minute)
		behind.Tick(w.ctx)
	}

	provider.failGets(errors.New("503 service unavailable"))
	for range 60 {
		pass()
	}
	require.Nil(t, w.record(machine.ID).Release)
	require.False(t, w.control.forgot(machine.ID))
	require.Empty(t, provider.Deletes())

	// It answers again, and has no such machine.
	provider.failGets(nil)
	provider.Forget(machine.InstanceID)
	pass()
	w.gone(machine)
}

// A machine at a provider this gateway is no longer configured for cannot
// be deleted there. It may still be running, so its record stays, and what
// it costs is still counted, until a gateway that has the provider deletes
// it.
func TestAMachineStaysOnRecordWhileItsProviderIsNotConfigured(t *testing.T) {
	w, machines := started(t, 1)
	machine := machines[0]
	lines := logged(t)

	// It has nothing to run, and is to be given back.
	w.workers.work(machine, false)
	without := w.replica(func(opts *Options) { opts.Providers = providers.RegistryOf() })
	for range 30 {
		w.clock.Advance(time.Minute)
		without.Tick(w.ctx)
	}
	require.NotNil(t, w.record(machine.ID).Release)
	require.True(t, w.control.forgot(machine.ID))
	require.Equal(t, []string{machine.InstanceID}, w.instances())
	require.Contains(t, lines.String(), "cannot delete a machine at a provider that is not configured; it may still be running")
	require.InDelta(t, 100*w.clock.Now().Sub(bought).Hours(), w.spentCents(), 3)

	w.after(time.Minute)
	w.gone(machine)
	require.Equal(t, []string{machine.InstanceID}, w.verda.Deletes())
}

// A machine that ran and then failed was started: what it was bought as can
// be run. It is given back as a machine that failed, and the next one is
// bought from the same offer.
func TestAnOfferWhoseMachineFailedAfterItHadRunIsBoughtAgain(t *testing.T) {
	w := newWorld(t)
	w.verda.SetOffers(l40s(), l40s(in("FIN-02"), costing(140)))
	lines := logged(t)
	w.Tick(w.ctx)

	machine := w.buy("spot-l40s")
	w.workers.join(machine)
	w.workers.work(machine, true)
	w.after(tickInterval)
	require.False(t, w.record(machine.ID).ReadyAt.IsZero())

	w.verda.SetStatus(machine.InstanceID, providers.StatusFailed, "host error")
	w.after(tickInterval)
	w.gone(machine)
	require.Contains(t, lines.String(), `"reason":"its provider says it failed"`)
	require.NotContains(t, lines.String(), "failed to start it")

	require.Equal(t, "FIN-01", w.buy("spot-l40s").Region)
}

// Why a provider failed a machine or took it back is known only to the
// provider. What it said is logged when the machine is given back.
func TestWhatTheProviderSaidOfAMachineItFailedOrTookIsLogged(t *testing.T) {
	tests := []struct {
		name   string
		status providers.Status
		said   string
	}{
		{"it took the machine back", providers.StatusInterrupted, "Server.SpotInstanceTermination"},
		{"it failed the machine", providers.StatusFailed, "host error"},
		{"it is deleting the machine", providers.StatusDeleted, "the lease ran out"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w, machines := started(t, 1)
			lines := logged(t)

			w.verda.SetStatus(machines[0].InstanceID, test.status, test.said)
			w.after(tickInterval)
			w.gone(machines[0])
			require.Contains(t, lines.String(), "provisioner: giving a machine back")
			require.Contains(t, lines.String(), `"status_detail":"`+test.said+`"`)
		})
	}
}

// A machine that is given back for a reason of the pool's own is not one
// its provider had anything to say about.
func TestWhatTheProviderSaysOfAMachineThePoolGivesBackIsNotLogged(t *testing.T) {
	w, machines := started(t, 1)
	lines := logged(t)

	w.verda.SetStatus(machines[0].InstanceID, providers.StatusActive, "running")
	w.workers.work(machines[0], false)
	w.every(time.Minute, 12*time.Minute)
	w.gone(machines[0])
	require.Contains(t, lines.String(), "provisioner: giving a machine back")
	require.NotContains(t, lines.String(), "status_detail")
}

// cannotCordon reads workers and fails to cordon one.
type cannotCordon struct {
	*fakeWorkers
}

func (c *cannotCordon) CordonWorkerIfIdle(id string) (bool, error) {
	return false, errors.New("redis is down")
}

// Whether a machine still has nothing to run is asked as it is cordoned.
// While that cannot be asked, the machine may have work: it is kept, and
// asked about again on the next pass.
func TestAMachineWhoseIdlenessCannotBeCheckedIsKept(t *testing.T) {
	w, machines := started(t, 1)
	machine := machines[0]
	w.workers.work(machine, false)

	failing := w.replica(func(opts *Options) { opts.Workers = &cannotCordon{fakeWorkers: w.workers} })
	for range 30 {
		w.clock.Advance(time.Minute)
		failing.Tick(w.ctx)
	}
	record := w.record(machine.ID)
	require.Nil(t, record.Release)
	require.False(t, record.IdleSince.IsZero(), "it is still known to have been idle")
	require.False(t, w.workers.of(machine).CordonRequested)
	require.Empty(t, w.verda.Deletes())

	w.after(time.Minute)
	w.gone(machine)
}

// draining returns a world whose pool is gone, with one machine that has
// work and has just been told to finish it.
func draining(t *testing.T) (*world, Machine) {
	t.Helper()
	w, machines := started(t, 1)
	w.control.set(func(c *fakeControl) { delete(c.pools, "spot-l40s") })
	w.every(time.Minute, poolGoneGrace+time.Minute)

	record := w.record(machines[0].ID)
	require.NotNil(t, record.Release)
	require.Equal(t, w.clock.Now().Add(drainFor), record.Release.DrainUntil)
	require.False(t, w.control.forgot(record.ID))
	return w, record
}

// A machine that is being drained and has no worker has no work left to
// wait for.
func TestADrainingMachineWhoseWorkerIsGoneIsGivenBackAtOnce(t *testing.T) {
	w, machine := draining(t)

	w.workers.lose(machine)
	w.after(time.Minute)
	w.gone(machine)
}

// While the worker of a machine that is being drained cannot be read, the
// machine may still have work. It is waited for, for as long as any other.
func TestADrainingMachineWhoseWorkerCannotBeReadIsWaitedFor(t *testing.T) {
	w, machine := draining(t)

	// Its work ends, which nobody can see.
	w.workers.work(machine, false)
	w.workers.fail(errors.New("redis is down"))
	w.every(time.Minute, drainFor-time.Minute)
	require.NotNil(t, w.record(machine.ID).Release)
	require.False(t, w.control.forgot(machine.ID))
	require.Empty(t, w.verda.Deletes())

	w.after(time.Minute)
	w.gone(machine)
}

// A provider that cannot be told to put off a machine's deletion is told
// again on the next pass. Until it has been, the record keeps the time the
// provider has.
func TestADeadlineThatCannotBePutOffIsPutOffOnALaterPass(t *testing.T) {
	provider := &troubled{Fake: providers.NewFake("verda", testOwner)}
	provider.SetOffers(l40s())
	w, machines := started(t, 1, func(opts *Options) { opts.Providers = providers.RegistryOf(provider) })
	machine := machines[0]

	provider.failExtends(errors.New("the provider is down"))
	w.every(time.Minute, leaseFor-renewLeaseWithin+10*time.Minute)
	record := w.record(machine.ID)
	require.Nil(t, record.Release)
	require.Equal(t, bought.Add(leaseFor), record.Until)
	require.Equal(t, bought.Add(leaseFor), provider.Until(machine.InstanceID))

	provider.failExtends(nil)
	w.after(time.Minute)
	require.Equal(t, w.clock.Now().Add(leaseFor), provider.Until(machine.InstanceID))
	require.Equal(t, w.clock.Now().Add(leaseFor), w.record(machine.ID).Until)
}

// An instance's tag says which machine it is. When a provider carries a
// create out twice, two instances carry one machine's tag: the record names
// one of them, and nothing would ever delete the other.
func TestASecondInstanceCarryingARecordedMachinesTagIsDeleted(t *testing.T) {
	w, machines := started(t, 1)
	twin := w.verda.Plant(testOwner, providers.StatusActive, map[string]string{MachineTag: machines[0].ID})

	w.after(tickInterval)
	require.Equal(t, []string{twin}, w.verda.Deletes())
	require.Equal(t, []string{machines[0].InstanceID}, w.instances())
	require.Nil(t, w.record(machines[0].ID).Release)
}

// A record that has yet to learn its instance takes one that carries its
// tag. Of two, it takes one and the other is deleted.
func TestOfTwoInstancesCarryingTheTagOfAPurchaseNeverAnsweredOneIsKept(t *testing.T) {
	w := newWorld(t)
	w.Tick(w.ctx)
	w.verda.LoseAnswers(1)
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.Error(t, err)
	record := w.records()[0]
	twin := w.verda.Plant(testOwner, providers.StatusPending, map[string]string{MachineTag: record.ID})

	w.after(tickInterval)
	require.Equal(t, "verda-1", w.record(record.ID).InstanceID)
	w.after(tickInterval)
	require.Equal(t, []string{twin}, w.verda.Deletes())
	require.Equal(t, []string{"verda-1"}, w.instances())
	require.Nil(t, w.record(record.ID).Release)
}

var bought = time.Date(2026, 11, 13, 12, 0, 0, 0, time.UTC)

// machine is the record of a machine bought at `bought` that its provider
// has answered for.
func machine(change ...func(*Machine)) Machine {
	m := Machine{
		ID: "machine-1", Pool: "spot-l40s", InstanceID: "verda-1",
		offerKey: offerKey{Provider: "verda", OfferID: "1L40S.20V", Spot: true},
		GPUCount: 1, HourlyMicros: 100 * microsPerCent,
		CreatedAt: bought,
	}
	for _, apply := range change {
		apply(&m)
	}
	return m
}

// joined is a machine whose worker came up a minute after it was bought.
func joined(change ...func(*Machine)) Machine {
	return machine(append([]func(*Machine){func(m *Machine) { m.ReadyAt = bought.Add(time.Minute) }}, change...)...)
}

// seen is what a pass sees `after` the machine was bought: the provider has
// it running, its pool still buys it, and it has no worker.
func seen(after time.Duration, change ...func(*sighting)) sighting {
	pool := spotL40S()
	s := sighting{
		now:          bought.Add(after),
		provider:     present,
		instance:     providers.Instance{ID: "verda-1", Status: providers.StatusActive},
		workersKnown: true,
		pool:         &pool,
		poolKnown:    true,
	}
	for _, apply := range change {
		apply(&s)
	}
	return s
}

func providerHas(status providers.Status) func(*sighting) {
	return func(s *sighting) { s.instance.Status = status }
}

func providerSaid(says providerSays) func(*sighting) {
	return func(s *sighting) { s.provider, s.instance = says, providers.Instance{} }
}

func worker(status types.WorkerStatus, busy bool, change ...func(*types.Worker)) func(*sighting) {
	return func(s *sighting) {
		s.worker = &types.Worker{
			Id: "worker-1", Status: status,
			TotalCpu: 20_000, FreeCpu: 20_000, TotalMemory: 54_000, FreeMemory: 54_000, TotalGpuCount: 1, FreeGpuCount: 1,
		}
		if busy {
			s.worker.FreeGpuCount = 0
		}
		for _, apply := range change {
			apply(s.worker)
		}
	}
}

func cordoned(w *types.Worker) { w.CordonRequested = true }

func noPool(s *sighting) { s.pool = nil }

func TestDecide(t *testing.T) {
	const (
		busy = true
		idle = false
	)
	at := func(after time.Duration) time.Time { return bought.Add(after) }
	// released is the release of a machine for reason, which the market's
	// history names end.
	released := func(reason, end string) func(*Machine) {
		return func(m *Machine) { m.Release = &Release{Reason: reason, End: end} }
	}
	// failed is the release of a machine its provider could not start.
	failed := func(m *Machine) {
		m.Release = &Release{Reason: "its provider failed to start it", End: watch.ResultFailed, SoldOut: true}
	}

	tests := []struct {
		name string
		was  Machine
		seen sighting
		// want is the record afterwards; nil means unchanged.
		want          func(*Machine)
		releaseIfIdle bool
	}{
		// A machine that is already being given back.
		{
			name: "being given back: nothing more is decided",
			was:  machine(released("it had nothing to run for 10m0s", watch.ResultReleased)),
			seen: seen(2*time.Hour, providerSaid(gone)),
		},

		// What the provider says.
		{
			name: "its provider no longer has it",
			was:  joined(),
			seen: seen(time.Hour, providerSaid(gone), worker(types.WorkerStatusAvailable, busy)),
			want: released("its provider no longer has it", watch.ResultGone),
		},
		{
			// A provider can take a moment to know what it has just sold.
			name: "its provider cannot find it yet",
			was:  machine(),
			seen: seen(launchGrace-time.Second, providerSaid(gone)),
		},
		{
			// Nor is anything made of what else is seen of it meanwhile.
			name: "its provider cannot find it yet, whatever its worker and its pool",
			was:  machine(),
			seen: seen(launchGrace-time.Second, providerSaid(gone), noPool, worker(types.WorkerStatusAvailable, idle)),
		},
		{
			name: "its provider still cannot find it after the launch grace",
			was:  machine(),
			seen: seen(launchGrace, providerSaid(gone)),
			want: released("its provider no longer has it", watch.ResultGone),
		},
		{
			name: "its provider is deleting it",
			was:  joined(),
			seen: seen(time.Hour, providerHas(providers.StatusDeleted), worker(types.WorkerStatusAvailable, busy)),
			want: released("its provider is deleting it", watch.ResultGone),
		},
		{
			// A spot machine, taken back in the middle of its work. Its
			// worker still looks alive: its state has not expired yet.
			name: "its provider took it back",
			was:  joined(),
			seen: seen(time.Hour, providerHas(providers.StatusInterrupted), worker(types.WorkerStatusAvailable, busy)),
			want: released("its provider took it back", watch.ResultInterrupted),
		},
		{
			// A spot machine its provider could not spare: one bought as the
			// same would go the same way.
			name: "its provider took it back before it ran",
			was:  machine(),
			seen: seen(5*time.Minute, providerHas(providers.StatusInterrupted)),
			want: func(m *Machine) {
				m.Release = &Release{Reason: "its provider took it back before it ran", End: watch.ResultInterrupted, SoldOut: true}
			},
		},
		{
			name: "its provider failed to start it",
			was:  machine(),
			seen: seen(time.Minute, providerHas(providers.StatusFailed)),
			want: failed,
		},
		{
			// It ran, so what it was bought as can be run: its failing says
			// nothing about the offer.
			name: "its provider says it failed after it had run",
			was:  joined(),
			seen: seen(time.Hour, providerHas(providers.StatusFailed), worker(types.WorkerStatusAvailable, busy)),
			want: released("its provider says it failed", watch.ResultFailed),
		},
		{
			name: "its provider cannot be asked: it is not given up",
			was:  joined(),
			seen: seen(time.Hour, providerSaid(notAsked), worker(types.WorkerStatusAvailable, busy)),
		},

		// A purchase whose answer never arrived.
		{
			name: "never answered, not listed yet",
			was:  machine(func(m *Machine) { m.InstanceID = "" }),
			seen: seen(launchGrace-time.Second, providerSaid(gone)),
		},
		{
			name: "never answered, not listed after the launch grace",
			was:  machine(func(m *Machine) { m.InstanceID = "" }),
			seen: seen(launchGrace, providerSaid(gone)),
			want: released("its provider never created it", ""),
		},
		{
			name: "never answered, and its provider cannot be asked",
			was:  machine(func(m *Machine) { m.InstanceID = "" }),
			seen: seen(time.Hour, providerSaid(notAsked)),
		},
		{
			name: "never answered, found by its tag",
			was:  machine(func(m *Machine) { m.InstanceID = "" }),
			seen: seen(time.Minute, providerHas(providers.StatusPending)),
			want: func(m *Machine) { m.InstanceID = "verda-1" },
		},
		{
			name: "never answered, found failed: its instance is recorded so that it is deleted",
			was:  machine(func(m *Machine) { m.InstanceID = "" }),
			seen: seen(time.Minute, providerHas(providers.StatusFailed)),
			want: func(m *Machine) {
				m.InstanceID = "verda-1"
				failed(m)
			},
		},
		{
			name: "never answered, found taken back",
			was:  machine(func(m *Machine) { m.InstanceID = "" }),
			seen: seen(time.Minute, providerHas(providers.StatusDeleted)),
			want: func(m *Machine) {
				m.InstanceID = "verda-1"
				released("its provider is deleting it", watch.ResultGone)(m)
			},
		},

		// Whether it joined.
		{
			name: "booting, inside its boot timeout",
			was:  machine(),
			seen: seen(20*time.Minute-time.Second, worker(types.WorkerStatusPending, idle)),
		},
		{
			name: "not joined by the boot timeout",
			was:  machine(),
			seen: seen(20*time.Minute, worker(types.WorkerStatusPending, idle)),
			want: released("it did not join its pool within 20m0s", watch.ResultNeverUp),
		},
		{
			name: "not joined by the learned default boot timeout",
			was:  machine(),
			seen: seen(2*time.Minute, func(s *sighting) { s.usualBoot = time.Minute }),
			want: released("it did not join its pool within 2m0s", watch.ResultNeverUp),
		},
		{
			name: "still booting past the learned estimate within its configured timeout",
			was:  machine(),
			seen: seen(4*time.Minute, func(s *sighting) {
				s.usualBoot = time.Minute
				s.pool.Provisioning.BootTimeout = types.Duration(5 * time.Minute)
			}),
		},
		{
			name: "not joined by its pool's own boot timeout",
			was:  machine(),
			seen: seen(5*time.Minute, func(s *sighting) {
				s.usualBoot = time.Minute
				s.pool.Provisioning.BootTimeout = types.Duration(5 * time.Minute)
			}),
			want: released("it did not join its pool within 5m0s", watch.ResultNeverUp),
		},
		{
			name: "not joined, and its pool's workers cannot be read",
			was:  machine(),
			seen: seen(time.Hour, func(s *sighting) { s.workersKnown = false }),
		},
		{
			name: "its worker is up",
			was:  machine(),
			seen: seen(3*time.Minute, worker(types.WorkerStatusAvailable, busy)),
			want: func(m *Machine) { m.ReadyAt = at(3 * time.Minute) },
		},

		// Whether it stayed.
		{
			name: "its worker is gone",
			was:  joined(),
			seen: seen(time.Hour),
			want: func(m *Machine) { m.DownSince = at(time.Hour) },
		},
		{
			name: "its worker is disabled, and not on purpose",
			was:  joined(),
			seen: seen(time.Hour, worker(types.WorkerStatusDisabled, busy)),
			want: func(m *Machine) { m.DownSince = at(time.Hour) },
		},
		{
			name: "its worker is back",
			was:  joined(func(m *Machine) { m.DownSince = at(50 * time.Minute) }),
			seen: seen(time.Hour, worker(types.WorkerStatusAvailable, busy)),
			want: func(m *Machine) { m.DownSince = time.Time{} },
		},
		{
			name: "its worker is cordoned, which is on purpose",
			was:  joined(func(m *Machine) { m.DownSince = at(50 * time.Minute) }),
			seen: seen(time.Hour, worker(types.WorkerStatusDisabled, busy, cordoned)),
			want: func(m *Machine) { m.DownSince = time.Time{} },
		},
		{
			name: "without a worker for less than the boot timeout",
			was:  joined(func(m *Machine) { m.DownSince = at(40 * time.Minute) }),
			seen: seen(time.Hour - time.Second),
		},
		{
			name: "without a worker for the boot timeout",
			was:  joined(func(m *Machine) { m.DownSince = at(40 * time.Minute) }),
			seen: seen(time.Hour),
			want: released("it has had no worker for 20m0s", watch.ResultLost),
		},

		// Whether its pool still buys it.
		{
			name: "no pool buys it",
			was:  joined(),
			seen: seen(time.Hour, noPool, worker(types.WorkerStatusAvailable, busy)),
			want: func(m *Machine) { m.PoolGoneSince = at(time.Hour) },
		},
		{
			name: "no pool buys it, but its pool could not be loaded",
			was:  joined(),
			seen: seen(time.Hour, noPool, worker(types.WorkerStatusAvailable, busy), func(s *sighting) { s.poolKnown = false }),
		},
		{
			name: "its pool is back",
			was:  joined(func(m *Machine) { m.PoolGoneSince = at(58 * time.Minute) }),
			seen: seen(time.Hour, worker(types.WorkerStatusAvailable, busy)),
			want: func(m *Machine) { m.PoolGoneSince = time.Time{} },
		},
		{
			name: "no pool has bought it for less than the grace",
			was:  joined(func(m *Machine) { m.PoolGoneSince = at(time.Hour - poolGoneGrace + time.Second) }),
			seen: seen(time.Hour, noPool, worker(types.WorkerStatusAvailable, busy)),
		},
		{
			name: "no pool has bought it for the grace: its work is waited for",
			was:  joined(func(m *Machine) { m.PoolGoneSince = at(time.Hour - poolGoneGrace) }),
			seen: seen(time.Hour, noPool, worker(types.WorkerStatusAvailable, busy)),
			want: func(m *Machine) {
				m.Release = &Release{Reason: "its pool no longer buys machines", End: watch.ResultReleased, DrainUntil: at(time.Hour + drainFor)}
			},
		},
		{
			name: "idle without a pool: the grace applies, not the idle timeout",
			was:  joined(func(m *Machine) { m.PoolGoneSince = at(58 * time.Minute); m.IdleSince = at(10 * time.Minute) }),
			seen: seen(time.Hour, noPool, worker(types.WorkerStatusAvailable, idle)),
		},

		// Whether it has anything to run.
		{
			name: "nothing to run",
			was:  joined(),
			seen: seen(time.Hour, worker(types.WorkerStatusAvailable, idle)),
			want: func(m *Machine) { m.IdleSince = at(time.Hour) },
		},
		{
			name: "nothing to run for less than the idle timeout",
			was:  joined(func(m *Machine) { m.IdleSince = at(50*time.Minute + time.Second) }),
			seen: seen(time.Hour, worker(types.WorkerStatusAvailable, idle)),
		},
		{
			name:          "nothing to run for the idle timeout",
			was:           joined(func(m *Machine) { m.IdleSince = at(50 * time.Minute) }),
			seen:          seen(time.Hour, worker(types.WorkerStatusAvailable, idle)),
			releaseIfIdle: true,
		},
		{
			name: "nothing to run for its pool's own idle timeout",
			was:  joined(func(m *Machine) { m.IdleSince = at(58 * time.Minute) }),
			seen: seen(time.Hour, worker(types.WorkerStatusAvailable, idle), func(s *sighting) {
				s.pool.Provisioning.IdleTimeout = types.Duration(2 * time.Minute)
			}),
			releaseIfIdle: true,
		},
		{
			name: "nothing to run, but it holds the floor",
			was:  joined(func(m *Machine) { m.IdleSince = at(10 * time.Minute) }),
			seen: seen(time.Hour, worker(types.WorkerStatusAvailable, idle), func(s *sighting) { s.holdsFloor = true }),
		},
		{
			name: "it holds the floor of a pool whose budget for the day is spent",
			was:  joined(func(m *Machine) { m.IdleSince = at(10 * time.Minute) }),
			seen: seen(time.Hour, worker(types.WorkerStatusAvailable, idle), func(s *sighting) {
				s.holdsFloor, s.overBudget = true, true
			}),
			releaseIfIdle: true,
		},
		{
			name: "it has work again",
			was:  joined(func(m *Machine) { m.IdleSince = at(10 * time.Minute) }),
			seen: seen(time.Hour, worker(types.WorkerStatusAvailable, busy)),
			want: func(m *Machine) { m.IdleSince = time.Time{} },
		},
		{
			name:          "cordoned with nothing to run",
			was:           joined(func(m *Machine) { m.IdleSince = at(10 * time.Minute) }),
			seen:          seen(time.Hour, worker(types.WorkerStatusDisabled, idle, cordoned)),
			releaseIfIdle: true,
		},
		{
			name: "nothing to run, and its pool's workers cannot be read",
			was:  joined(func(m *Machine) { m.IdleSince = at(10 * time.Minute) }),
			seen: seen(time.Hour, func(s *sighting) { s.workersKnown = false }),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			want := test.was
			if test.want != nil {
				test.want(&want)
			}

			next, releaseIfIdle := decide(test.was, test.seen)
			require.Equal(t, want, next)
			require.Equal(t, test.releaseIfIdle, releaseIfIdle)
		})
	}
}

// A machine keeps the instance its purchase recorded while a pass, which
// read the record before the answer arrived, writes what it found.
func TestTakeKeepsTheInstanceAPurchaseRecordedMeanwhile(t *testing.T) {
	stored := machine()                                                            // the purchase has recorded verda-1
	found := machine(func(m *Machine) { m.InstanceID = ""; m.IdleSince = bought }) // the pass read it before that

	stored.take(found)
	require.Equal(t, "verda-1", stored.InstanceID)
	require.Equal(t, bought, stored.IdleSince)

	// A pass that found the instance by its tag records it.
	stored = machine(func(m *Machine) { m.InstanceID = "" })
	stored.take(machine())
	require.Equal(t, "verda-1", stored.InstanceID)
}

func TestStoreKeepsRecords(t *testing.T) {
	w := newWorld(t)
	ctx := w.ctx

	// Nothing is added when the decision fails.
	refused := errors.New("no")
	_, err := w.store.Reserve(ctx, func(held []Machine) (Machine, error) { return machine(), refused })
	require.ErrorIs(t, err, refused)
	require.Empty(t, w.records())

	// Each decision sees the records before it.
	for _, id := range []string{"machine-b", "machine-a"} {
		_, err := w.store.Reserve(ctx, func(held []Machine) (Machine, error) {
			return machine(func(m *Machine) { m.ID = id; m.HourlyMicros = int64(len(held)) }), nil
		})
		require.NoError(t, err)
	}
	records := w.records()
	require.Len(t, records, 2)
	require.Equal(t, "machine-a", records[0].ID, "records are listed by ID")
	require.EqualValues(t, 1, records[0].HourlyMicros, "the second decision saw the first record")
	require.True(t, bought.Equal(records[0].CreatedAt))

	// A record is changed in place, and comes back as it is now.
	updated, err := w.store.Update(ctx, "machine-a", func(m *Machine) { m.InstanceID = "verda-7" })
	require.NoError(t, err)
	require.Equal(t, "verda-7", updated.InstanceID)
	require.Equal(t, "verda-7", w.record("machine-a").InstanceID)

	// A record that is gone cannot be changed, and is not brought back.
	_, err = w.store.Update(ctx, "machine-c", func(m *Machine) { m.InstanceID = "verda-8" })
	require.ErrorIs(t, err, errNoRecord)
	require.Len(t, w.records(), 2)

	// Removing is safe to repeat.
	require.NoError(t, w.store.Remove(ctx, "machine-a", bought))
	require.NoError(t, w.store.Remove(ctx, "machine-a", bought))
	require.Len(t, w.records(), 1)
}

func TestStoreKeepsWhatIsSoldOutUntilItLapses(t *testing.T) {
	w := newWorld(t)
	ctx := w.ctx
	now := w.clock.Now()
	spot := keyOf(l40s())
	onDemand := keyOf(l40s(func(o *providers.Offer) { o.Spot = false }))
	elsewhere := keyOf(l40s(in("FIN-02"), func(o *providers.Offer) { o.Cloud = "hyperstack" }))

	marks, err := w.store.SoldOut(ctx, now)
	require.NoError(t, err)
	require.Empty(t, marks)

	require.NoError(t, w.store.MarkSoldOut(ctx, spot, now.Add(5*time.Minute)))
	require.NoError(t, w.store.MarkSoldOut(ctx, onDemand, now.Add(time.Minute)))
	require.NoError(t, w.store.MarkSoldOut(ctx, elsewhere, now.Add(3*time.Minute)))
	// A later mark replaces an earlier one.
	require.NoError(t, w.store.MarkSoldOut(ctx, elsewhere, now.Add(4*time.Minute)))

	marks, err = w.store.SoldOut(ctx, now.Add(time.Minute-time.Second))
	require.NoError(t, err)
	require.Len(t, marks, 3)
	require.True(t, marks[spot].Equal(now.Add(5*time.Minute)))
	require.True(t, marks[onDemand].Equal(now.Add(time.Minute)))
	require.True(t, marks[elsewhere].Equal(now.Add(4*time.Minute)))

	// A mark is gone from the moment it lapses, and is not kept.
	marks, err = w.store.SoldOut(ctx, now.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, marks, 2)
	require.NotContains(t, marks, onDemand)
	marks, err = w.store.SoldOut(ctx, now)
	require.NoError(t, err)
	require.Len(t, marks, 2, "the lapsed mark was removed")

	marks, err = w.store.SoldOut(ctx, now.Add(time.Hour))
	require.NoError(t, err)
	require.Empty(t, marks)
}

// Invariant 3 across replicas: a limit checked while a record is reserved
// holds however many gateways reserve at once.
func TestReserveHoldsALimitAcrossReplicas(t *testing.T) {
	w := newWorld(t)
	const limit = 5

	full := errors.New("full")
	var reserved atomic.Int32
	var wg sync.WaitGroup
	for replica := range 4 {
		store := NewStore(w.rdb) // each gateway has its own
		for i := range 6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, err := store.Reserve(w.ctx, func(held []Machine) (Machine, error) {
					if len(held) >= limit {
						return Machine{}, full
					}
					return machine(func(m *Machine) { m.ID = fmt.Sprintf("machine-%d-%d", replica, i) }), nil
				})
				if err == nil {
					reserved.Add(1)
				} else if !errors.Is(err, full) {
					t.Errorf("reserve: %v", err)
				}
			}()
		}
	}
	wg.Wait()

	require.EqualValues(t, limit, reserved.Load())
	require.Len(t, w.records(), limit)
}

func TestReconcileRunsOnePassPerInterval(t *testing.T) {
	w := newWorld(t)
	now := w.clock.Now()
	passes := 0
	pass := func(context.Context) { passes++ }

	require.NoError(t, w.store.Reconcile(w.ctx, now, 14*time.Second, pass))
	require.Equal(t, 1, passes)

	// Another replica's tick, a moment later.
	require.NoError(t, NewStore(w.rdb).Reconcile(w.ctx, now.Add(time.Second), 14*time.Second, pass))
	require.Equal(t, 1, passes)

	require.NoError(t, w.store.Reconcile(w.ctx, now.Add(14*time.Second), 14*time.Second, pass))
	require.Equal(t, 2, passes)
}

func TestReconcileNeverRunsTwoPassesAtOnce(t *testing.T) {
	w := newWorld(t)
	now := w.clock.Now()

	started, finish := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- w.store.Reconcile(w.ctx, now, time.Second, func(context.Context) {
			close(started)
			<-finish
		})
	}()
	<-started

	// A pass is due, by the clock, but one is running: another replica has
	// it, which is no more an error than one that ran a moment ago.
	require.NoError(t, NewStore(w.rdb).Reconcile(w.ctx, now.Add(time.Minute), time.Second, func(context.Context) {
		t.Error("a second pass ran while the first was running")
	}))

	close(finish)
	require.NoError(t, <-done)
}

// A record is one flat object in Redis. The key of the offer its machine was
// bought as is part of it, under names of its own: the offer's ID is not the
// machine's.
func TestARecordKeepsTheKeyOfItsOfferNextToItsOwnID(t *testing.T) {
	w := newWorld(t)
	w.Tick(w.ctx)
	machine := w.buy("spot-l40s")

	stored, err := w.rdb.HGet(w.ctx, common.RedisKeys.ComputeProvisionedMachines(), machine.ID).Result()
	require.NoError(t, err)
	var fields map[string]any
	require.NoError(t, json.Unmarshal([]byte(stored), &fields))
	require.Equal(t, machine.ID, fields["id"])
	require.Equal(t, "verda", fields["provider"])
	require.Equal(t, "1L40S.20V", fields["offer_id"])
	require.Equal(t, "FIN-01", fields["region"])
	require.Equal(t, true, fields["spot"])

	require.Equal(t, keyOf(l40s()), w.record(machine.ID).offerKey)
}

func TestSpentAddsUpTheLastDay(t *testing.T) {
	w := newWorld(t)
	now := w.clock.Now() // noon

	w.spend(now, 10)
	w.spend(now.Add(-30*time.Minute), 5)
	w.spend(now.Add(-23*time.Hour), 100)
	w.spend(now.Add(-24*time.Hour), 1000)
	require.Equal(t, 115.0, w.spentCents(), "the hour of now and the 23 before it")

	w.clock.Advance(time.Hour)
	require.Equal(t, 15.0, w.spentCents())

	// An hour's spend is not kept for ever.
	kept, err := w.rdb.TTL(w.ctx, spendKey(now)).Result()
	require.NoError(t, err)
	require.Equal(t, spendKept, kept)
}

func TestStoreCountsWhatARecordIsBilledForAsItWritesTheRecord(t *testing.T) {
	w := newWorld(t)
	_, err := w.store.Reserve(w.ctx, func([]Machine) (Machine, error) { return machine(), nil }) // a dollar an hour
	require.NoError(t, err)
	spent := func() float64 {
		cents, err := w.store.Spent(w.ctx, bought.Add(2*time.Hour))
		require.NoError(t, err)
		return cents
	}
	require.Zero(t, spent())

	_, err = w.store.Update(w.ctx, "machine-1", func(m *Machine) { m.BilledUntil = bought.Add(30 * time.Minute) })
	require.NoError(t, err)
	require.InDelta(t, 50, spent(), 0.001)

	// A change that bills nothing counts nothing.
	_, err = w.store.Update(w.ctx, "machine-1", func(m *Machine) { m.IdleSince = bought.Add(40 * time.Minute) })
	require.NoError(t, err)
	require.InDelta(t, 50, spent(), 0.001)

	// What is counted runs from where the record was billed to, not from
	// where the caller last saw it billed to.
	_, err = w.store.Update(w.ctx, "machine-1", func(m *Machine) { m.BilledUntil = bought.Add(45 * time.Minute) })
	require.NoError(t, err)
	require.InDelta(t, 75, spent(), 0.001)

	// Removing the record counts the rest, and removing it again nothing.
	require.NoError(t, w.store.Remove(w.ctx, "machine-1", bought.Add(time.Hour)))
	require.InDelta(t, 100, spent(), 0.001)
	require.NoError(t, w.store.Remove(w.ctx, "machine-1", bought.Add(2*time.Hour)))
	require.InDelta(t, 100, spent(), 0.001)
}

// A record without an instance is of a machine that is not known to have
// been bought.
func TestStoreCountsNothingForAMachineWithoutAnInstance(t *testing.T) {
	w := newWorld(t)
	_, err := w.store.Reserve(w.ctx, func([]Machine) (Machine, error) {
		return machine(func(m *Machine) { m.InstanceID = "" }), nil
	})
	require.NoError(t, err)

	_, err = w.store.Update(w.ctx, "machine-1", func(m *Machine) { m.BilledUntil = bought.Add(30 * time.Minute) })
	require.NoError(t, err)
	require.NoError(t, w.store.Remove(w.ctx, "machine-1", bought.Add(time.Hour)))

	cents, err := w.store.Spent(w.ctx, bought.Add(time.Hour))
	require.NoError(t, err)
	require.Zero(t, cents)
}

// A Redis cluster runs a transaction, and one read of many keys, only on
// keys of one slot, and a key's slot is that of its hash tag.
func TestARecordAndWhatItCostAreInOneClusterSlot(t *testing.T) {
	tag := func(key string) string {
		_, rest, _ := strings.Cut(key, "{")
		tag, _, closed := strings.Cut(rest, "}")
		require.True(t, closed, "%s has no hash tag", key)
		return tag
	}
	records := tag(common.RedisKeys.ComputeProvisionedMachines())
	require.NotEmpty(t, records)
	require.Equal(t, records, tag(spendKey(bought)))
	require.Equal(t, records, tag(spendKey(bought.Add(-23*time.Hour))))
}

// What a machine cost is counted once, whatever goes wrong while it is
// written down: a pass that could not write the record has counted nothing,
// and the next pass counts the whole of it.
func TestWhatAMachineCostIsCountedOnceWhenItsRecordCannotBeWritten(t *testing.T) {
	w, _ := started(t, 1) // a dollar an hour, with work
	owed := func() float64 { return 100 * w.clock.Now().Sub(bought).Hours() }
	w.every(time.Minute, 10*time.Minute)
	require.InDelta(t, owed(), w.spentCents(), 0.01)

	resume := w.failWrites()
	spent := w.spentCents()
	w.every(time.Minute, 3*time.Minute)
	require.Equal(t, spent, w.spentCents(), "nothing is counted while the record cannot say that it was")

	resume()
	w.after(time.Minute)
	require.InDelta(t, owed(), w.spentCents(), 0.01)
}

func TestTheLastOfWhatAMachineCostIsCountedOnceWhenItsRecordCannotBeRemoved(t *testing.T) {
	w, machines := started(t, 1)
	machine := machines[0]
	owed := func() float64 { return 100 * w.clock.Now().Sub(bought).Hours() }

	// It is given back, and its provider cannot delete it for a while.
	w.workers.work(machine, false)
	w.verda.FailDeletes(errors.New("the provider is down"))
	w.every(time.Minute, 15*time.Minute)
	require.NotNil(t, w.record(machine.ID).Release)
	w.verda.FailDeletes(nil)

	// The provider deletes it, and its record cannot be removed.
	resume := w.failWrites()
	w.every(20*time.Second, 80*time.Second)
	require.Empty(t, w.instances())
	require.Len(t, w.records(), 1)

	// It is paid for until its record is gone, and no part of that twice.
	resume()
	w.after(20 * time.Second)
	require.Empty(t, w.records())
	spent := w.spentCents()
	require.InDelta(t, owed(), spent, 0.01)

	// Nothing more is spent on a machine that is gone.
	w.every(time.Minute, 10*time.Minute)
	require.Equal(t, spent, w.spentCents())
}

// recorded is what a provisioner told the market's history became of its
// machines, oldest first.
type recorded struct {
	log      *watch.MemoryLog
	recorder *watch.Recorder
}

// recordingOutcomes is a world whose provisioner writes what becomes of its
// machines to the market's history.
func recordingOutcomes(t *testing.T) (*world, *recorded) {
	t.Helper()
	log := watch.NewMemoryLog()
	told := &recorded{log: log}
	var w *world
	w = newWorld(t, func(opts *Options) {
		told.recorder = watch.NewRecorder(log, watch.Streams{}, opts.Clock)
		opts.Outcomes = told.recorder
	})
	return w, told
}

func (r *recorded) outcomes(t *testing.T) []watch.Outcome {
	t.Helper()
	r.recorder.Flush(context.Background())
	var outcomes []watch.Outcome
	for _, record := range r.log.All("outcomes") {
		var outcome watch.Outcome
		require.NoError(t, json.Unmarshal(record.Body, &outcome))
		outcomes = append(outcomes, outcome)
	}
	return outcomes
}

// A provider that names a price and not its stock is known to have had a
// machine only from being asked for one. Every machine a pool buys is
// written to the market's history: how the purchase went, how long the
// machine took to join, and how long it was kept before it ended, and how.
func TestWhatBecomesOfAMachineAPoolBuysIsWrittenToTheMarketsHistory(t *testing.T) {
	w, told := recordingOutcomes(t)
	w.Tick(w.ctx)
	machine := w.buy("spot-l40s")

	w.after(3 * time.Minute)
	require.Len(t, told.outcomes(t), 1, "it has not joined yet")
	w.workers.join(machine)
	w.workers.work(machine, true)
	w.after(tickInterval)
	up := w.clock.Now()
	w.every(tickInterval, time.Hour)

	w.verda.SetStatus(machine.InstanceID, providers.StatusInterrupted, "preempted")
	w.after(tickInterval)
	ended := w.clock.Now()
	w.every(tickInterval, 10*time.Minute)

	outcomes := told.outcomes(t)
	require.Len(t, outcomes, 3, "each is written once")
	create, active, end := outcomes[0], outcomes[1], outcomes[2]

	require.Equal(t, watch.Outcome{
		At: bought, Event: watch.OutcomeCreate, Result: watch.ResultOK, Source: watch.SourcePool,
		Provider: "verda", Cloud: "verda", Region: "FIN-01", Geo: "eu-north", Offer: "1L40S.20V", Spot: true,
		GPU: "L40S", GPUs: 1, HourlyMicros: 100 * microsPerCent,
	}, create)

	require.Equal(t, watch.OutcomeActive, active.Event)
	require.Equal(t, up, active.At)
	require.Equal(t, up.Sub(bought), active.Took, "how long it took to join, from when it was bought")

	require.Equal(t, watch.OutcomeEnd, end.Event)
	require.Equal(t, watch.ResultInterrupted, end.Result)
	require.Equal(t, ended, end.At)
	require.Equal(t, ended.Sub(bought), end.Took, "how long the machine was had")
	for _, later := range []watch.Outcome{active, end} {
		later.At, later.Event, later.Result, later.Took = create.At, create.Event, create.Result, create.Took
		require.Equal(t, create, later, "every outcome of a machine says the same of what it was bought as")
	}
}

func TestAPurchaseAProviderRefusesIsWrittenToTheMarketsHistoryWithWhy(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		result string
	}{
		{"it has none left", fmt.Errorf("%w: no capacity in FIN-01", providers.ErrOutOfStock), watch.ResultOutOfStock},
		{"the account may buy no more", providers.ErrQuota, watch.ResultQuota},
		{"it failed", errors.New("internal server error"), watch.ResultError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w, told := recordingOutcomes(t)
			w.Tick(w.ctx)
			w.verda.RefuseCreates(test.err)
			_, err := w.Provision(w.ctx, "spot-l40s", scheduler.MachineNeed{})
			require.Error(t, err)
			w.every(tickInterval, time.Hour)

			outcomes := told.outcomes(t)
			require.Len(t, outcomes, 1, "a machine that was never made has no end")
			require.Equal(t, watch.OutcomeCreate, outcomes[0].Event)
			require.Equal(t, test.result, outcomes[0].Result)
			require.Equal(t, test.err.Error(), outcomes[0].Err)
			require.Equal(t, "L40S", outcomes[0].GPU)
		})
	}
}

// How a machine ended says whether it is its provider that is to be held to
// it: one that never joined, and one that was given back for want of work,
// are told apart.
func TestHowAMachineEndedIsWrittenToTheMarketsHistory(t *testing.T) {
	t.Run("it never joined", func(t *testing.T) {
		w, told := recordingOutcomes(t)
		w.Tick(w.ctx)
		w.buy("spot-l40s")
		w.every(tickInterval, time.Hour)

		outcomes := told.outcomes(t)
		require.Len(t, outcomes, 2)
		require.Equal(t, watch.OutcomeEnd, outcomes[1].Event)
		require.Equal(t, watch.ResultNeverUp, outcomes[1].Result)
	})
	t.Run("it had nothing to run", func(t *testing.T) {
		w, told := recordingOutcomes(t)
		w.Tick(w.ctx)
		machine := w.buy("spot-l40s")
		w.workers.join(machine)
		w.workers.work(machine, false)
		w.every(tickInterval, time.Hour)

		outcomes := told.outcomes(t)
		require.Len(t, outcomes, 3)
		require.Equal(t, watch.OutcomeActive, outcomes[1].Event)
		require.Equal(t, watch.OutcomeEnd, outcomes[2].Event)
		require.Equal(t, watch.ResultReleased, outcomes[2].Result)
	})
}

// endOf is the end the market's history has of a machine, if it has one.
func endOf(t *testing.T, told *recorded) (watch.Outcome, bool) {
	t.Helper()
	var ends []watch.Outcome
	for _, outcome := range told.outcomes(t) {
		if outcome.Event == watch.OutcomeEnd {
			ends = append(ends, outcome)
		}
	}
	require.LessOrEqual(t, len(ends), 1, "a machine ends once")
	if len(ends) == 0 {
		return watch.Outcome{}, false
	}
	return ends[0], true
}

// A machine that is given back is had, and paid for, until its provider
// has deleted it. That is when the market's history says it ended, and not
// when it was decided to give it back.
func TestAMachineThatIsGivenBackEndsWhenItHasBeenDeleted(t *testing.T) {
	t.Run("after its work was waited for", func(t *testing.T) {
		w, told := recordingOutcomes(t)
		w.Tick(w.ctx)
		machine := w.buy("spot-l40s")
		w.workers.join(machine)
		w.workers.work(machine, true)
		w.after(tickInterval)

		// Its pool is deleted, and its work is waited for.
		w.control.set(func(c *fakeControl) { delete(c.pools, "spot-l40s") })
		w.every(time.Minute, poolGoneGrace+time.Minute)
		require.NotNil(t, w.record(machine.ID).Release)
		w.every(time.Minute, 5*time.Minute)
		_, ended := endOf(t, told)
		require.False(t, ended, "it is still running, and still has its work")

		w.workers.work(machine, false)
		w.after(time.Minute)
		w.gone(machine)
		end, ended := endOf(t, told)
		require.True(t, ended)
		require.Equal(t, watch.ResultReleased, end.Result)
		require.Equal(t, w.clock.Now(), end.At)
		require.Equal(t, w.clock.Now().Sub(bought), end.Took, "how long it was had, with the time its work was waited for")
	})

	t.Run("however long its provider takes to delete it", func(t *testing.T) {
		w, told := recordingOutcomes(t)
		w.Tick(w.ctx)
		machine := w.buy("spot-l40s")
		w.workers.join(machine)
		w.workers.work(machine, false)
		w.verda.FailDeletes(errors.New("the provider is down"))
		w.every(time.Minute, 30*time.Minute)
		require.NotNil(t, w.record(machine.ID).Release)
		_, ended := endOf(t, told)
		require.False(t, ended, "its provider still has it")

		w.verda.FailDeletes(nil)
		w.after(time.Minute)
		w.gone(machine)
		end, ended := endOf(t, told)
		require.True(t, ended)
		require.Equal(t, watch.ResultReleased, end.Result)
		require.Equal(t, w.clock.Now(), end.At)
		require.Equal(t, w.clock.Now().Sub(bought), end.Took)

		w.every(time.Minute, 10*time.Minute)
		endOf(t, told)
	})

	t.Run("that never joined its pool", func(t *testing.T) {
		w, told := recordingOutcomes(t)
		w.Tick(w.ctx)
		machine := w.buy("spot-l40s")
		w.verda.FailDeletes(errors.New("the provider is down"))
		w.every(time.Minute, time.Hour)
		require.NotNil(t, w.record(machine.ID).Release)
		_, ended := endOf(t, told)
		require.False(t, ended)

		w.verda.FailDeletes(nil)
		w.after(time.Minute)
		end, ended := endOf(t, told)
		require.True(t, ended)
		require.Equal(t, watch.ResultNeverUp, end.Result)
		require.Equal(t, w.clock.Now(), end.At)
	})
}

// A machine its provider took back ended when that was seen, however long
// what is left of it takes to delete.
func TestAMachineItsProviderTookBackEndedWhenThatWasSeen(t *testing.T) {
	w, told := recordingOutcomes(t)
	w.Tick(w.ctx)
	machine := w.buy("spot-l40s")
	w.workers.join(machine)
	w.workers.work(machine, true)
	w.every(tickInterval, time.Hour)

	w.verda.SetStatus(machine.InstanceID, providers.StatusInterrupted, "preempted")
	w.verda.FailDeletes(errors.New("the provider is down"))
	w.after(tickInterval)
	seen := w.clock.Now()
	end, ended := endOf(t, told)
	require.True(t, ended)
	require.Equal(t, watch.ResultInterrupted, end.Result)
	require.Equal(t, seen, end.At)
	require.Equal(t, seen.Sub(bought), end.Took)

	w.every(time.Minute, 20*time.Minute)
	w.verda.FailDeletes(nil)
	w.after(time.Minute)
	w.gone(machine)
	end, _ = endOf(t, told)
	require.Equal(t, seen, end.At, "it is written once")
}

// Only what a provider did to a machine ended it before it was given back.
func TestOnlyAMachineItsProviderEndedHasEndedBeforeItIsDeleted(t *testing.T) {
	for end, atProvider := range map[string]bool{
		watch.ResultInterrupted: true,
		watch.ResultFailed:      true,
		watch.ResultGone:        true,
		watch.ResultNeverUp:     false,
		watch.ResultLost:        false,
		watch.ResultReleased:    false,
		"":                      false,
	} {
		require.Equal(t, atProvider, endedAtProvider(end), end)
	}
	// Whatever a provider's status makes of a machine is its provider's
	// doing.
	for status, end := range lostEnd {
		require.True(t, endedAtProvider(end), status)
	}
}

// A pool with several providers buys from whichever is expected to cost
// least. One that refuses holds back only what the pool buys from it.
func TestAPoolBuysFromWhicheverOfItsProvidersCostsLeast(t *testing.T) {
	w := newWorld(t)
	nebius := providers.NewFake("nebius", testOwner)
	nebius.SetOffers(l40s(costing(60), in("eu-north1")))
	w.Provisioner = w.replica(func(opts *Options) { opts.Providers = providers.RegistryOf(w.verda, nebius) })
	w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Providers = []string{"verda", "nebius"} })
	w.Tick(w.ctx)
	require.Equal(t, "nebius", w.buy("spot-l40s").Provider)

	nebius.RefuseCreates(providers.ErrQuota)
	_, err := w.Provision(w.ctx, "spot-l40s", anyMachine)
	require.ErrorIs(t, err, providers.ErrQuota)
	require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	require.Equal(t, "verda", w.buy("spot-l40s").Provider)
}

// A pool that buys both ways buys a spot machine when it is worth its risk
// for the work that waits: its price, and how often its provider takes such
// machines back.
func TestChooseWeighsSpotAgainstTheWorkLostWithIt(t *testing.T) {
	both := spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.OnDemand = true })
	offers := []providers.Offer{l40s(), l40s(named("on-demand"), costing(110), func(o *providers.Offer) { o.Spot = false })}
	// Ten machines, each taken back after half an hour.
	oftenTaken := watch.Estimates{}
	for range 10 {
		oftenTaken.Learn(watch.OutcomeOf(offers[0]).Ended(watch.ResultInterrupted, 30*time.Minute))
	}

	for _, test := range []struct {
		name      string
		need      scheduler.MachineNeed
		estimates watch.Estimates
		spot      bool
	}{
		{name: "work that loses little", need: scheduler.MachineNeed{HoursLost: 0.5}, spot: true},
		{name: "work that loses a day", need: scheduler.MachineNeed{HoursLost: 8}},
		{name: "work that may not be interrupted", need: scheduler.MachineNeed{OnDemand: true, HoursLost: 0.5}},
		{name: "a provider that often takes machines back", need: scheduler.MachineNeed{HoursLost: 0.5}, estimates: oftenTaken},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := choose(purchase{pool: "spot-l40s", config: both, needs: []scheduler.MachineNeed{test.need}, offers: offers, estimates: test.estimates})
			require.NoError(t, err)
			require.Equal(t, test.spot, got.Spot)
		})
	}
}

// A machine is given twice as long to join as machines bought like it take.
// One that is given back for it earns the next a little longer.
func TestAMachineIsGivenTwiceAsLongToJoinAsThoseBeforeItTook(t *testing.T) {
	w := newWorld(t)
	w.Tick(w.ctx)
	first := w.buy("spot-l40s")
	w.workers.join(first)
	w.after(2 * time.Minute)

	for _, within := range []time.Duration{4 * time.Minute, 5 * time.Minute} {
		late := w.buy("spot-l40s")
		w.every(tickInterval, within-tickInterval)
		require.Nil(t, w.record(late.ID).Release)
		w.after(tickInterval)
		w.gone(late)
	}
}

// What was learned is kept for every replica. A gateway that finds none of
// it starts from the market's history.
func TestWhatWasLearnedIsReadFromTheMarketsHistory(t *testing.T) {
	w, told := recordingOutcomes(t)
	w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.BootTimeout = types.Duration(5 * time.Minute) })
	w.verda.SetOffers(l40s(), l40s(in("FIN-02"), costing(140)))
	w.Tick(w.ctx)
	w.every(time.Minute, 6*time.Minute)
	never := w.buy("spot-l40s")
	w.every(time.Minute, 6*time.Minute)
	w.gone(never)
	told.outcomes(t)

	require.NoError(t, w.rdb.Del(w.ctx, common.RedisKeys.ComputeProvisionedEstimates()).Err())
	other := w.replica(func(opts *Options) { opts.Outcomes = told.recorder })
	other.Tick(w.ctx)
	bought, err := other.Provision(w.ctx, "spot-l40s", anyMachine)
	require.NoError(t, err)
	require.Equal(t, "FIN-02", w.record(bought.MachineID).Region, "the offer whose machine never came up is passed over")
}

// What a pool is asked to expect of its machines is what became of those
// bought before as the one it would buy now: how long they took to come up,
// and how often they were taken back. A pool that can buy no more is known
// by the machines it has.
func TestAPoolSaysWhatToExpectOfItsMachines(t *testing.T) {
	w := newWorld(t, func(opts *Options) { opts.Outcomes = nil })
	w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxNodes = 2 })
	w.verda.SetOffers(l40s(), l40s(in("FIN-02"), costing(300)))
	w.Tick(w.ctx)

	// Nothing has been bought yet: a spot machine is taken to come up in
	// five minutes and to be lost once in twenty hours.
	require.Equal(t, scheduler.MachineOutlook{CanBuy: true, ComeUp: unknownBoot, Taken: 1.0 / 20}, w.Expect(w.ctx, "spot-l40s", anyMachine))
	require.Zero(t, w.Expect(w.ctx, "no-such-pool", anyMachine))

	// Three machines were seen up after three minutes, and each was taken
	// back within the half hour.
	for range 3 {
		machine := w.buy("spot-l40s")
		w.after(2 * time.Minute)
		w.workers.join(machine)
		w.workers.work(machine, true)
		w.every(time.Minute, 20*time.Minute)
		w.verda.SetStatus(machine.InstanceID, providers.StatusInterrupted, "preempted")
		w.after(tickInterval)
		w.gone(machine)
	}
	outlook := w.Expect(w.ctx, "spot-l40s", anyMachine)
	require.True(t, outlook.CanBuy)
	require.InDelta(t, 3, outlook.ComeUp.Minutes(), 0.1)
	require.InDelta(t, 1, outlook.Taken, 0.1, "lost three times in an hour and a bit of its own, and two hours of its provider's")

	// The region where none was lost is expected to keep its machines.
	elsewhere := w.Expect(w.ctx, "spot-l40s", scheduler.MachineNeed{HoursLost: 8})
	require.True(t, elsewhere.CanBuy)
	require.Less(t, elsewhere.Taken, outlook.Taken/2, "work that loses a day is bought the dearer machine that is kept")

	// At maxNodes the pool buys nothing, and is known by what it has.
	first, second := w.buy("spot-l40s"), w.buy("spot-l40s")
	require.Equal(t, []string{"FIN-01", "FIN-01"}, []string{first.Region, second.Region})
	full := w.Expect(w.ctx, "spot-l40s", anyMachine)
	require.False(t, full.CanBuy)
	require.InDelta(t, outlook.Taken, full.Taken, 1e-9)
	require.InDelta(t, outlook.ComeUp.Minutes(), full.ComeUp.Minutes(), 1e-9)

	// A machine that is kept has not ended, and says nothing to the market's
	// history yet. The hours it has run count for its pool all the same.
	for _, machine := range []Machine{first, second} {
		w.workers.join(machine)
		w.workers.work(machine, true)
	}
	w.every(time.Minute, 6*time.Hour)
	kept := w.Expect(w.ctx, "spot-l40s", anyMachine)
	require.False(t, kept.CanBuy)
	require.Less(t, kept.Taken, full.Taken/4, "three were lost in an hour, and two have now run for six")
}

// readLog is a history that does something more once it has been read.
type readLog struct {
	watch.Log
	read func()
}

func (l *readLog) Read(ctx context.Context, stream watch.Stream, from, to time.Time) ([]watch.Record, error) {
	records, err := l.Log.Read(ctx, stream, from, to)
	if read := l.read; read != nil {
		l.read = nil
		read()
	}
	return records, err
}

// Every replica reads the market's history when nothing has been learned
// yet, and several may read it at once. It counts once all the same.
func TestTheMarketsHistoryCountsOnceHoweverManyReplicasReadIt(t *testing.T) {
	w, told := recordingOutcomes(t)
	w.Tick(w.ctx)
	machine := w.buy("spot-l40s")
	w.workers.join(machine)
	w.workers.work(machine, true)
	w.every(tickInterval, time.Hour)
	w.verda.SetStatus(machine.InstanceID, providers.StatusInterrupted, "preempted")
	w.every(tickInterval, 10*time.Minute)
	told.outcomes(t)

	forget := func() { require.NoError(t, w.rdb.Del(w.ctx, common.RedisKeys.ComputeProvisionedEstimates()).Err()) }
	learned := func() watch.Estimates {
		estimates, err := w.store.Estimates(w.ctx)
		require.NoError(t, err)
		return estimates
	}
	replica := func(log watch.Log) *Provisioner {
		return w.replica(func(opts *Options) { opts.Outcomes = watch.NewRecorder(log, watch.Streams{}, opts.Clock) })
	}
	forget()
	replica(told.log).Tick(w.ctx)
	once := learned()
	require.NotEmpty(t, once)

	// One replica has read the history when another reads it and takes it
	// in first.
	forget()
	replica(&readLog{Log: told.log, read: func() { replica(told.log).Tick(w.ctx) }}).Tick(w.ctx)
	require.Equal(t, once, learned())

	// Nor does a replica that reads the history later undo what was learned
	// since.
	_, err := w.store.Learn(w.ctx, machine.outcome(w.clock.Now()).CameUp(time.Minute))
	require.NoError(t, err)
	since := learned()
	require.NotEqual(t, once, since)
	_, err = w.store.StartEstimates(w.ctx, nil)
	require.NoError(t, err)
	require.Equal(t, since, learned())

	// What becomes of a machine before the history is read is in the
	// history, and is not taken in twice either.
	forget()
	_, err = w.store.Learn(w.ctx, machine.outcome(w.clock.Now()).CameUp(time.Minute))
	require.NoError(t, err)
	require.Nil(t, learned())
}

// While a pool buys its last machine, whoever waits for the pool stays.
func TestAPoolThatIsBuyingItsLastMachineIsStillWaitedFor(t *testing.T) {
	w, provider := newTroubledWorld(t)
	w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.MaxNodes = 1 })
	w.Tick(w.ctx)

	provider.beforeCreate = func(providers.CreateRequest) {
		require.True(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
	}
	w.buy("spot-l40s")
	w.clock.Advance(recordsMaxAge)
	require.False(t, w.CanProvision(w.ctx, "spot-l40s", anyMachine))
}
