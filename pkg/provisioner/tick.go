package provisioner

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const (
	// tickInterval is how often every replica refreshes what it knows, and
	// how often one of them checks the machines against their providers.
	tickInterval = 15 * time.Second

	// launchGrace is how long after its purchase a machine that its provider
	// does not have is waited for before it is given up. The provider may
	// still be acting on a request it never answered, and may take a moment
	// to know of a machine it has just sold.
	launchGrace = 2 * time.Minute
	// poolGoneGrace is how long no replica may have had a machine's pool
	// before the machine is drained. It covers a rolling deployment, during
	// which replicas disagree about which pools exist.
	poolGoneGrace = 5 * time.Minute
	// drainFor is how long the work on a machine of a pool that is gone is
	// waited for before the machine is given back anyway.
	drainFor = 10 * time.Minute
	// billEvery is how often a machine's cost is added to what was spent.
	billEvery = time.Minute
	// sweepAllEvery is how often providers that no pool buys from are
	// searched for instances that have no record.
	sweepAllEvery = 10 * time.Minute
)

// Start runs Tick now and then every tickInterval, until ctx is done.
func (p *Provisioner) Start(ctx context.Context) {
	p.clock.AfterFunc(0, func() { p.Tick(ctx) })
	p.clock.Every(ctx, tickInterval, func() { p.Tick(ctx) })
}

// Tick refreshes what this replica knows about pools, offers and spend.
// Then, unless another replica did so within the interval, it checks every
// machine against its provider and its worker.
func (p *Provisioner) Tick(ctx context.Context) {
	p.refresh(ctx)
	p.vouch(ctx)

	if err := p.store.Reconcile(ctx, p.clock.Now(), tickInterval-time.Second, p.reconcile); err != nil {
		log.Error().Err(err).Msg("provisioner: could not check the machines held")
	}
}

// refresh loads the pools that buy machines, what their providers sell and
// what has been spent.
func (p *Provisioner) refresh(ctx context.Context) {
	now := p.clock.Now()

	p.refreshPools(ctx)
	for _, provider := range providersOf(p.snapshot().pools) {
		p.refreshOffers(ctx, provider, now)
	}
	p.refreshSoldOut(ctx, now)

	if p.limits.MaxDailyCents > 0 {
		spent, err := p.store.Spent(ctx, now)
		if err != nil {
			log.Warn().Err(err).Msg("provisioner: could not read what was spent on machines")
			return
		}
		p.mu.Lock()
		p.spentToday = spent
		p.mu.Unlock()
	}
}

// refreshPools loads the pools that buy machines. While they cannot be
// read, what was read last stands.
func (p *Provisioner) refreshPools(ctx context.Context) {
	pools, unloaded, err := p.controlPlane.BuyingPools(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("provisioner: could not read the pools that buy machines")
		return
	}
	if pools == nil {
		pools = map[string]types.WorkerPoolConfig{} // read, and there are none
	}
	p.mu.Lock()
	before := p.unloaded
	p.pools, p.unloaded = pools, unloaded
	p.mu.Unlock()

	// Said once for each pool and reason, not on every tick.
	for pool, why := range unloaded {
		if was, ok := before[pool]; !ok || was.Error() != why.Error() {
			log.Warn().Err(why).Str("pool_name", pool).Msg("provisioner: a pool could not be loaded; the machines it may have bought are kept as they are")
		}
	}
}

// providersOf lists the providers the pools buy from, in a fixed order.
func providersOf(pools map[string]types.WorkerPoolConfig) []string {
	var names []string
	for _, config := range pools {
		if name := config.Provisioning.Provider; !slices.Contains(names, name) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names
}

func (p *Provisioner) refreshOffers(ctx context.Context, name string, now time.Time) {
	p.mu.Lock()
	list, fetched := p.offers[name]
	p.mu.Unlock()
	if fetched && now.Sub(list.fetchedAt) < offersMaxAge {
		return
	}

	provider, ok := p.providers.Get(name)
	if !ok {
		p.mu.Lock()
		warned := p.unconfigured[name]
		p.unconfigured[name] = true
		p.mu.Unlock()
		if !warned {
			log.Error().Str("provider", name).Msg("provisioner: a pool buys from a provider that is not configured under providers; it will not grow")
		}
		return
	}
	offers, err := provider.Offers(ctx, providers.OfferFilter{})
	if err != nil {
		// The last list is bought from until it is offersStaleAfter old.
		log.Warn().Err(err).Str("provider", name).Msg("provisioner: could not list what the provider sells")
		return
	}
	p.mu.Lock()
	p.offers[name] = offerList{offers: offers, fetchedAt: now}
	p.mu.Unlock()
}

// refreshSoldOut loads the offers that any replica found sold out.
func (p *Provisioner) refreshSoldOut(ctx context.Context, now time.Time) {
	soldOut, err := p.store.SoldOut(ctx, now)
	if err != nil {
		// What this replica found itself is kept.
		log.Warn().Err(err).Msg("provisioner: could not read which offers are sold out")
		return
	}
	p.mu.Lock()
	p.soldOut = soldOut
	p.mu.Unlock()
}

// vouch clears the mark on machines that another replica found without a
// pool and that this replica has the pool of. Replicas disagree about pools
// while a deployment rolls out; a machine is drained only when none of them
// has had its pool for poolGoneGrace.
func (p *Provisioner) vouch(ctx context.Context) {
	machines, err := p.store.List(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("provisioner: could not read the machines held to vouch for those whose pool exists")
		return
	}
	known := p.snapshot()
	for _, machine := range machines {
		if machine.PoolGoneSince.IsZero() || machine.Release != nil {
			continue
		}
		if _, buys := known.buys(machine); !buys {
			continue
		}
		if _, err := p.store.Update(ctx, machine.ID, func(record *Machine) { record.PoolGoneSince = time.Time{} }); err != nil && !errors.Is(err, errNoRecord) {
			log.Warn().Err(err).Str("machine_id", machine.ID).Msg("provisioner: could not keep a machine whose pool exists")
		}
	}
}

// reconcile is one pass over every machine.
func (p *Provisioner) reconcile(ctx context.Context) {
	now := p.clock.Now()
	known := p.snapshot()

	// The providers are asked before the records are read. A machine bought
	// while this runs then has its record by the time records are read, and
	// is not taken for an instance nobody recorded.
	recordedBefore, err := p.store.List(ctx)
	if err != nil {
		log.Error().Err(err).Msg("provisioner: could not read the machines held")
		return
	}
	instances := p.listInstances(ctx, known, recordedBefore, now)
	machines, err := p.store.List(ctx)
	if err != nil {
		log.Error().Err(err).Msg("provisioner: could not read the machines held")
		return
	}

	workers := poolWorkers{source: p.workers, byPool: map[string]*workersOfPool{}}
	recorded := make(map[string]string, len(machines))
	for _, machine := range machines {
		recorded[machine.ID] = machine.InstanceID
		p.reconcileMachine(ctx, machine, p.sight(ctx, machine, now, known, instances, &workers))
	}
	p.deleteUnrecorded(ctx, instances, recorded)
	p.forgetRecentRecords()
}

// listInstances asks each provider what it has. A provider that could not be
// asked is left out of the result, which is different from one that has
// nothing. Providers that no pool buys from and no machine is at are only
// asked every sweepAllEvery.
func (p *Provisioner) listInstances(ctx context.Context, known snapshot, machines []Machine, now time.Time) map[string][]providers.Instance {
	inUse := providersOf(known.pools)
	for _, machine := range machines {
		if !slices.Contains(inUse, machine.Provider) {
			inUse = append(inUse, machine.Provider)
		}
	}
	p.mu.Lock()
	sweepAll := now.Sub(p.sweptAll) >= sweepAllEvery
	if sweepAll {
		p.sweptAll = now
	}
	p.mu.Unlock()

	listed := map[string][]providers.Instance{}
	for _, name := range p.providers.Names() {
		if !sweepAll && !slices.Contains(inUse, name) {
			continue
		}
		provider, _ := p.providers.Get(name)
		instances, err := provider.List(ctx)
		if err != nil {
			log.Warn().Err(err).Str("provider", name).Msg("provisioner: could not list the provider's instances")
			continue
		}
		listed[name] = instances
	}
	return listed
}

// providerSays is what a machine's provider says about it.
type providerSays int

const (
	// notAsked: the provider could not be asked, so nothing is known.
	notAsked providerSays = iota
	// present: the provider has the instance.
	present
	// gone: the provider has no such instance.
	gone
)

// sighting is what can be seen of one machine during a pass.
type sighting struct {
	now time.Time

	provider providerSays
	// instance is the provider's account of the machine, when present.
	instance providers.Instance

	// workersKnown is false when the pool's workers could not be read.
	// worker is then nil whether or not the machine has one.
	workersKnown bool
	// worker is the worker that stands for the machine, or nil.
	worker *types.Worker
	// holdsFloor reports whether the worker is one of those keeping the
	// pool's free capacity at its floor.
	holdsFloor bool

	// pool is the config of the machine's pool, or nil when this replica
	// has no pool that buys it.
	pool *types.WorkerPoolConfig
	// poolKnown is false when this replica cannot tell whether the machine's
	// pool exists: the pool could not be loaded, or no pool has been read
	// yet. pool is then nil whether or not a pool buys the machine.
	poolKnown bool
	// overBudget reports whether providers.maxDailyCents has been spent.
	overBudget bool
}

// available reports whether the machine's worker is up and takes work.
func (s sighting) available() bool {
	return s.worker != nil && s.worker.Status == types.WorkerStatusAvailable
}

// bootTimeout is how long the machine has to join its pool, and how long it
// may be without a worker after that. A machine whose pool is gone has the
// timeout of a pool that sets none.
func (s sighting) bootTimeout() time.Duration {
	if s.pool == nil {
		return types.WorkerPoolProvisioningConfig{}.BootWithin()
	}
	return s.pool.Provisioning.BootWithin()
}

func (p *Provisioner) sight(ctx context.Context, machine Machine, now time.Time, known snapshot, instances map[string][]providers.Instance, workers *poolWorkers) sighting {
	s := sighting{now: now, poolKnown: known.knows(machine.Pool), overBudget: known.overBudget}
	if config, buys := known.buys(machine); buys {
		s.pool = &config
	}
	s.provider, s.instance = p.instanceOf(ctx, machine, instances)

	if pool := workers.of(machine.Pool); pool.err == nil {
		s.workersKnown = true
		s.worker = pool.byID[machine.workerID()]
		if s.pool != nil {
			s.holdsFloor = scheduler.HoldsHeadroom(*s.pool, pool.all, s.worker)
		}
	}
	return s
}

// instanceOf finds what the provider says about a machine. A machine whose
// purchase was never answered has no instance ID and is known by its tag.
func (p *Provisioner) instanceOf(ctx context.Context, machine Machine, instances map[string][]providers.Instance) (providerSays, providers.Instance) {
	listed, asked := instances[machine.Provider]
	if !asked {
		return notAsked, providers.Instance{}
	}
	for _, instance := range listed {
		if machine.InstanceID != "" && instance.ID == machine.InstanceID {
			return present, instance
		}
		if machine.InstanceID == "" && instance.Tags[MachineTag] == machine.ID {
			return present, instance
		}
	}
	if machine.InstanceID == "" {
		return gone, providers.Instance{}
	}

	// A list can be behind a purchase. The instance itself is asked for
	// before it is believed gone. Its provider has just listed, so it is one
	// this gateway has.
	provider, _ := p.providers.Get(machine.Provider)
	instance, err := provider.Get(ctx, machine.InstanceID)
	switch {
	case errors.Is(err, providers.ErrNotFound):
		return gone, providers.Instance{}
	case err != nil:
		log.Warn().Err(err).Str("machine_id", machine.ID).Str("provider", machine.Provider).Str("instance_id", machine.InstanceID).
			Msg("provisioner: could not ask the provider about a machine it does not list")
		return notAsked, providers.Instance{}
	}
	return present, instance
}

// poolWorkers reads each pool's workers once per pass.
type poolWorkers struct {
	source Workers
	byPool map[string]*workersOfPool
}

type workersOfPool struct {
	all  []*types.Worker
	byID map[string]*types.Worker
	err  error
}

func (w *poolWorkers) of(pool string) *workersOfPool {
	if loaded, ok := w.byPool[pool]; ok {
		return loaded
	}
	loaded := &workersOfPool{byID: map[string]*types.Worker{}}
	loaded.all, loaded.err = w.source.GetAllWorkersInPool(pool)
	if loaded.err != nil {
		// Whether its machines have a worker, or work, is not known in this
		// pass: none is given back for want of one.
		log.Warn().Err(loaded.err).Str("pool_name", pool).Msg("provisioner: could not read a pool's workers")
	}
	for _, worker := range loaded.all {
		loaded.byID[worker.Id] = worker
	}
	w.byPool[pool] = loaded
	return loaded
}

// decide returns the machine's record as it should be after this sighting.
// A machine that is to be given back comes back with Release set. It is
// asked, in this order, whether its provider still runs it, whether it
// joined its pool and stayed, whether its pool still buys it, and whether it
// has anything to run.
//
// releaseIfIdle is true for a machine that has had nothing to run for the
// pool's idle timeout and does not hold the floor. Whether it still has
// nothing to run only its worker's lock can tell, so the caller asks, and
// releases the machine if so.
//
// decide changes nothing but the record it returns.
func decide(m Machine, s sighting) (next Machine, releaseIfIdle bool) {
	if m.Release != nil {
		return m, false
	}
	if s.provider == gone && s.now.Sub(m.CreatedAt) < launchGrace {
		return m, false // its provider may yet come to know of it
	}
	if reason := atProvider(&m, s); reason != "" {
		m.Release = &Release{Reason: reason, SoldOut: reason == failedToStart}
		return m, false
	}
	if m.InstanceID == "" {
		return m, false // nothing is known to have been bought
	}
	if reason := withoutWorker(&m, s); reason != "" {
		m.Release = &Release{Reason: reason}
		return m, false
	}
	if s.pool == nil {
		m.Release = withoutPool(&m, s)
		return m, false
	}
	m.PoolGoneSince = time.Time{}
	releaseIfIdle = withoutWork(&m, s)
	return m, releaseIfIdle
}

// failedToStart is why a machine is given back that its provider gave up on
// before it ever ran a worker. What it was bought as is then passed over for
// a while.
const failedToStart = "its provider failed to start it"

// lostStatus is why a machine is given back whose provider reports it in
// this status: the provider no longer runs it.
var lostStatus = map[providers.Status]string{
	providers.StatusDeleted:     "its provider is deleting it",
	providers.StatusInterrupted: "its provider took it back",
	providers.StatusFailed:      "its provider says it failed",
}

// atProvider returns why the machine's provider no longer runs it, or "".
// A purchase whose answer never arrived is known by its tag: its instance is
// recorded first, whatever state it is in, so that giving the machine back
// deletes it.
func atProvider(m *Machine, s sighting) string {
	switch s.provider {
	case gone:
		if m.InstanceID == "" {
			return "its provider never created it"
		}
		return "its provider no longer has it"
	case present:
		if m.InstanceID == "" {
			m.InstanceID = s.instance.ID
		}
		// A machine that failed after it had run was started: what it was
		// bought as can be run.
		if s.instance.Status == providers.StatusFailed && m.ReadyAt.IsZero() {
			return failedToStart
		}
		return lostStatus[s.instance.Status]
	}
	return ""
}

// withoutWorker keeps ReadyAt and DownSince, and returns why a machine that
// did not join its pool, or did not stay, is given back, or "".
func withoutWorker(m *Machine, s sighting) string {
	timeout := s.bootTimeout()
	switch {
	case !s.workersKnown:
	case s.available():
		if m.ReadyAt.IsZero() {
			m.ReadyAt = s.now
		}
		m.DownSince = time.Time{}
	case m.ReadyAt.IsZero():
		if s.now.Sub(m.CreatedAt) >= timeout {
			return fmt.Sprintf("it did not join its pool within %s", timeout)
		}
	case heldBack(s.worker):
		m.DownSince = time.Time{}
	case m.DownSince.IsZero():
		m.DownSince = s.now
	case s.now.Sub(m.DownSince) >= timeout:
		return fmt.Sprintf("it has had no worker for %s", timeout)
	}
	return ""
}

// withoutPool keeps PoolGoneSince for a machine that no pool of this replica
// buys, and returns its release once no replica has had its pool for
// poolGoneGrace: the machine is given back when its work is done.
func withoutPool(m *Machine, s sighting) *Release {
	switch {
	case !s.poolKnown:
	case m.PoolGoneSince.IsZero():
		m.PoolGoneSince = s.now
	case s.now.Sub(m.PoolGoneSince) >= poolGoneGrace:
		return &Release{Reason: "its pool no longer buys machines", DrainUntil: s.now.Add(drainFor)}
	}
	return nil
}

// withoutWork keeps IdleSince, and reports whether the machine has had
// nothing to run for its pool's idle timeout and does not hold the floor.
func withoutWork(m *Machine, s sighting) bool {
	switch {
	case !s.workersKnown:
		return false
	case !(s.available() || heldBack(s.worker)) || !unused(s.worker):
		m.IdleSince = time.Time{}
		return false
	case m.IdleSince.IsZero():
		m.IdleSince = s.now
	}
	// A pool that has spent its budget for the day does not pay for a floor.
	return s.now.Sub(m.IdleSince) >= s.pool.Provisioning.IdleAfter() && (!s.holdsFloor || s.overBudget)
}

// heldBack reports whether the worker is kept from taking work on purpose:
// cordoned, or being moved to another worker image. Such a machine is up.
func heldBack(worker *types.Worker) bool {
	return worker != nil && worker.Status == types.WorkerStatusDisabled &&
		(worker.CordonRequested || worker.RolloutGeneration != "")
}

// unused reports whether none of the worker's capacity is taken.
func unused(worker *types.Worker) bool {
	return worker.FreeCpu >= worker.TotalCpu &&
		worker.FreeMemory >= worker.TotalMemory &&
		worker.FreeGpuCount >= worker.TotalGpuCount
}

// reconcileMachine carries out what decide found for one machine.
func (p *Provisioner) reconcileMachine(ctx context.Context, machine Machine, s sighting) {
	next, releaseIfIdle := decide(machine, s)
	if releaseIfIdle {
		// The worker is cordoned in the same step that finds it without
		// work, so nothing can be placed on it from here on.
		idle, err := p.workers.CordonWorkerIfIdle(machine.workerID())
		switch {
		case err != nil:
			log.Warn().Err(err).Str("machine_id", machine.ID).Msg("provisioner: could not check whether a machine is idle")
		case idle:
			next.Release = &Release{Reason: fmt.Sprintf("it had nothing to run for %s", s.pool.Provisioning.IdleAfter())}
		default:
			next.IdleSince = time.Time{}
		}
	}
	next = billed(next, s.now)
	if next.Release == nil {
		next = p.renewLease(ctx, next, s.now)
	}

	if !reflect.DeepEqual(next, machine) {
		if _, err := p.store.Update(ctx, machine.ID, func(record *Machine) { record.take(next) }); err != nil {
			log.Warn().Err(err).Str("machine_id", machine.ID).Msg("provisioner: could not update a machine's record")
			return
		}
		if next.Release != nil && machine.Release == nil {
			giving := log.Info().
				Str("pool_name", machine.Pool).
				Str("machine_id", machine.ID).
				Str("provider", machine.Provider).
				Str("instance_id", next.InstanceID).
				Str("reason", next.Release.Reason)
			if _, lost := lostStatus[s.instance.Status]; lost {
				// Why, only the provider knows: what it said goes with it.
				giving = giving.Str("status_detail", s.instance.StatusDetail)
			}
			giving.Msg("provisioner: giving a machine back")
			if next.Release.SoldOut {
				// The next machine is bought as something else, if anything
				// else is on offer.
				p.markSoldOut(ctx, next.offerKey)
			}
		}
	}
	if next.Release != nil {
		p.finishRelease(ctx, next, s.now)
	}
}

// take records what a pass found out about the machine. The purchase may
// have written the machine's instance in the meantime, which the pass did
// not see and must not undo.
func (m *Machine) take(found Machine) {
	instanceID := m.InstanceID
	*m = found
	if m.InstanceID == "" {
		m.InstanceID = instanceID
	}
}

// finishRelease gives a machine back: the gateway forgets it, its provider
// deletes it, and its record is removed. Each step can be repeated, and the
// record stays until the provider has confirmed the machine gone, so a step
// that fails is tried again on the next pass.
func (p *Provisioner) finishRelease(ctx context.Context, machine Machine, now time.Time) {
	if now.Before(machine.Release.DrainUntil) && p.stillWorking(machine) {
		return
	}
	if !p.forget(ctx, machine) {
		return
	}
	if machine.InstanceID != "" {
		provider, ok := p.providers.Get(machine.Provider)
		if !ok {
			log.Error().Str("machine_id", machine.ID).Str("provider", machine.Provider).Str("instance_id", machine.InstanceID).
				Msg("provisioner: cannot delete a machine at a provider that is not configured; it may still be running")
			return
		}
		if err := provider.Delete(ctx, machine.InstanceID); err != nil {
			log.Warn().Err(err).Str("machine_id", machine.ID).Str("provider", machine.Provider).Str("instance_id", machine.InstanceID).
				Msg("provisioner: could not delete a machine at its provider")
			return
		}
	}
	// The machine is paid for until now. Should its record stay, it is paid
	// for until the record goes: there is no telling since when it is gone.
	if err := p.store.Remove(ctx, machine.ID, now); err != nil {
		log.Warn().Err(err).Str("machine_id", machine.ID).Msg("provisioner: could not remove the record of a machine that is gone")
	}
}

// stillWorking cordons a machine that is being drained and reports whether
// it has work left.
func (p *Provisioner) stillWorking(machine Machine) bool {
	idle, err := p.workers.CordonWorkerIfIdle(machine.workerID())
	var noWorker *types.ErrWorkerNotFound
	switch {
	case errors.As(err, &noWorker):
		return false
	case err != nil:
		return true
	case idle:
		return false
	}
	if err := p.workers.SetWorkerCordon(machine.workerID(), true); err != nil {
		log.Warn().Err(err).Str("machine_id", machine.ID).Msg("provisioner: could not cordon a machine that is being drained")
	}
	return true
}

// billed returns the machine billed up to now, once billEvery has passed
// since it last was. The store counts what that comes to as spent when it
// writes the record, and what is left when it removes it.
func billed(machine Machine, now time.Time) Machine {
	if machine.InstanceID == "" {
		return machine // nothing is known to have been bought
	}
	if now.Sub(machine.billedFrom()) >= billEvery {
		machine.BilledUntil = now
	}
	return machine
}

// renewLease puts off the time at which the provider deletes the machine by
// itself, once that time comes close.
func (p *Provisioner) renewLease(ctx context.Context, machine Machine, now time.Time) Machine {
	if machine.InstanceID == "" || machine.Until.Sub(now) > renewLeaseWithin {
		return machine
	}
	provider, ok := p.providers.Get(machine.Provider)
	if !ok {
		return machine
	}
	until := now.Add(leaseFor)
	if err := provider.Extend(ctx, machine.InstanceID, until); err != nil {
		log.Warn().Err(err).Str("machine_id", machine.ID).Str("instance_id", machine.InstanceID).Msg("provisioner: could not put off the machine's deletion at its provider")
		return machine
	}
	machine.Until = until
	return machine
}

// deleteUnrecorded deletes instances that were bought for a pool and that no
// record accounts for: what a purchase leaves behind when the gateway dies
// in the middle of it, and the second of two instances that carry one
// machine's tag. Without this they would run, and cost, for ever.
//
// recorded is the instance that each machine's record names, by machine ID.
// A record that has yet to learn its instance names none, and accounts for
// whichever carries its tag.
func (p *Provisioner) deleteUnrecorded(ctx context.Context, instances map[string][]providers.Instance, recorded map[string]string) {
	for name, listed := range instances {
		provider, _ := p.providers.Get(name) // it has just listed them
		for _, instance := range listed {
			machineID := instance.Tags[MachineTag]
			if machineID == "" || instance.Status == providers.StatusDeleted {
				continue
			}
			if named, ok := recorded[machineID]; ok && (named == "" || named == instance.ID) {
				continue
			}
			log.Warn().Str("provider", name).Str("instance_id", instance.ID).Str("machine_id", machineID).
				Msg("provisioner: deleting an instance that was bought for a pool and that no record accounts for")
			if err := provider.Delete(ctx, instance.ID); err != nil {
				log.Warn().Err(err).Str("provider", name).Str("instance_id", instance.ID).Msg("provisioner: could not delete an instance that has no record")
			}
		}
	}
}
