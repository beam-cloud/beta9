package schedsim

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/provisioner"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

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
		provider, ok := s.providers[pool.Provisioning.Provider]
		if !ok {
			provider = &Provider{
				sim:       s,
				Name:      pool.Provisioning.Provider,
				fake:      providers.NewFake(pool.Provisioning.Provider, simOwner),
				spot:      pool.Provisioning.Spot,
				bootDelay: defaultBootDelay,
			}
			s.providers[provider.Name] = provider
			fakes = append(fakes, provider.fake)
		}
		provider.product(pool.GPUType, pool.Provisioning.Spot)
		provider.apply()
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

// GiveNotice makes the provider tell n of the machines it sold, oldest
// first, that it is about to take them back. The machines run on. Each one's
// agent passes the notice on to the gateway, which cordons the machine.
func (p *Provider) GiveNotice(n int) *Provider {
	p.sim.t.Helper()
	p.sim.discoverMachines()
	for _, machine := range p.sim.machinesOf(p) {
		if n == 0 {
			break
		}
		if machine.state != machineJoined || machine.cordoned {
			continue
		}
		machine.notice()
		n--
	}
	if n > 0 {
		p.sim.t.Fatalf("schedsim: %s has %d fewer machines in a pool than it was to give notice", p.Name, n)
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

// notice is the machine's agent telling the gateway that the machine is
// about to be taken back, and what the gateway does about it: the cordon
// goes on the machine's record and on its worker.
func (m *machine) notice() {
	s := m.sim
	m.cordoned = true
	if err := s.computeRepo.SaveAgentTokenState(s.ctx, m.agentState(m.joinedAt, s.Clock.Now()), 0); err != nil {
		s.t.Fatalf("schedsim: machine %s notice: %v", m.id, err)
	}
	if err := s.workerRepo.SetWorkerCordon(m.worker.ID, true); err != nil {
		s.t.Fatalf("schedsim: machine %s cordon: %v", m.id, err)
	}
}

// stop ends the machine. The gateway is told nothing.
func (m *machine) stop() {
	m.state = machineGone
	if m.worker != nil {
		m.worker.die()
	}
}
