// Package provisioner buys machines for the pools that have a provisioning
// block, and looks after each one until its provider has confirmed it gone.
//
// The scheduler asks it three things: whether a pool can buy a machine now
// (CanProvision), to buy one (Provision), and which machines are still
// booting (Booting). Everything else happens on a tick, which compares each
// machine with what its provider and its worker say about it and gives back
// the ones that failed, never joined, have nothing to run, or belong to a
// pool that is gone.
//
// Every machine has a record (Machine), written before the provider is asked
// for the machine and removed after the provider has deleted it. Limits are
// checked while the record is written, under a lock all replicas share, and
// no lock is held while a provider is called. An instance that carries
// MachineTag and that no record accounts for is deleted.
//
// The package knows providers through pkg/providers and pools through their
// config. What a machine needs to join a pool, and what the gateway has to
// clean up when it leaves, is behind ControlPlane.
package provisioner

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/clock"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

const (
	// offersMaxAge is how old a provider's list of offers may be before it
	// is fetched again.
	offersMaxAge = 30 * time.Second
	// offersStaleAfter is how long a list is bought from when the provider
	// cannot be asked for a new one.
	offersStaleAfter = 5 * time.Minute
	// recordsMaxAge is how old the records CanProvision decides on may be.
	// Provision always decides on the records as they are.
	recordsMaxAge = time.Second

	// soldOutFor is how long an offer is passed over, whatever the provider
	// lists, after the provider refused to sell it for lack of stock or
	// failed to start a machine bought as it.
	soldOutFor = 5 * time.Minute
	// quotaBackoff is how long a pool does not buy after the provider
	// refused for quota.
	quotaBackoff = 10 * time.Minute
	// errorBackoff is how long a pool does not buy after a purchase failed
	// for any other reason.
	errorBackoff = time.Minute

	// leaseFor is how long ahead a provider that can delete a machine by
	// itself is told to. Should every gateway be gone, the machines go too.
	leaseFor = 6 * time.Hour
	// renewLeaseWithin is how close that deletion may come before it is put
	// off again.
	renewLeaseWithin = 3 * time.Hour
)

// ControlPlane is what the provisioner needs from the gateway.
type ControlPlane interface {
	// BuyingPools returns the config of every pool that buys its machines,
	// by name, and why each pool that could not be loaded could not. Such a
	// pool may buy machines too. It fails when the pools could not be read
	// at all.
	BuyingPools(ctx context.Context) (pools map[string]types.WorkerPoolConfig, unloaded map[string]error, err error)

	// JoinScript returns the shell script that makes a new machine join the
	// pool as machineID, and the hash of the token in it.
	JoinScript(ctx context.Context, pool, machineID string) (script, tokenHash string, err error)

	// ForgetMachine removes what the gateway holds about a machine: its
	// worker, whose requests go back to the backlog, its agent and its
	// token. A machine may be forgotten twice, or before it ever joined.
	ForgetMachine(ctx context.Context, pool, machineID, tokenHash string) error
}

// Workers is the part of the worker repository the provisioner reads a
// machine's state from and cordons it through.
type Workers interface {
	GetAllWorkersInPool(poolName string) ([]*types.Worker, error)
	CordonWorkerIfIdle(workerId string) (bool, error)
	SetWorkerCordon(workerId string, cordoned bool) error
}

type Options struct {
	Providers    *providers.Registry
	ControlPlane ControlPlane
	Workers      Workers
	Store        *Store
	Limits       Limits
	// Clock is the provisioner's source of time. Nil means the wall clock.
	Clock clock.Clock
	// NewID returns the ID of the next machine. Nil means a random one.
	NewID func() string
}

type Provisioner struct {
	providers    *providers.Registry
	controlPlane ControlPlane
	workers      Workers
	store        *Store
	limits       Limits
	clock        clock.Clock
	newID        func() string

	// What this replica knows. The tick keeps it fresh, and CanProvision
	// answers from it without asking anyone.
	mu sync.Mutex
	// pools are the pools that buy machines, by name. unloaded are the
	// pools that could not be loaded, and why: one of them may buy machines
	// too. Both are as they were last read, and pools is nil until then.
	pools      map[string]types.WorkerPoolConfig
	unloaded   map[string]error
	offers     map[string]offerList // by provider
	spentToday float64              // cents, over the last 24 hours
	recent     recentMachines
	// soldOut are the offers that are not bought for now, and until when.
	// The store has them for every replica, and the tick reads them from
	// it. In between, a replica adds what it finds itself.
	soldOut map[offerKey]time.Time
	backoff map[string]time.Time // by pool, until when
	// said is, by pool, what has stood in the way of buying a machine and
	// been said since the pool last bought one. Each is said once.
	said         map[string][]string
	unconfigured map[string]bool // providers already warned about
	sweptAll     time.Time
}

type offerList struct {
	offers    []providers.Offer
	fetchedAt time.Time
}

// offerKey is what tells a provider which machine to sell. A machine's
// record keeps the key of the offer it was bought as.
type offerKey struct {
	Provider string `json:"provider"`
	Cloud    string `json:"cloud,omitempty"`
	OfferID  string `json:"offer_id"`
	Region   string `json:"region,omitempty"`
	Spot     bool   `json:"spot,omitempty"`
}

func keyOf(offer providers.Offer) offerKey {
	return offerKey{Provider: offer.Provider, Cloud: offer.Cloud, OfferID: offer.ID, Region: offer.Region, Spot: offer.Spot}
}

// recentMachines are the records as they were last read. They have not been
// read while readAt is zero.
type recentMachines struct {
	machines []Machine
	readAt   time.Time
}

func New(opts Options) *Provisioner {
	newID := opts.NewID
	if newID == nil {
		newID = func() string { return "machine-" + uuid.NewString() }
	}
	return &Provisioner{
		providers:    opts.Providers,
		controlPlane: opts.ControlPlane,
		workers:      opts.Workers,
		store:        opts.Store,
		limits:       opts.Limits,
		clock:        clock.OrReal(opts.Clock),
		newID:        newID,
		offers:       map[string]offerList{},
		soldOut:      map[offerKey]time.Time{},
		backoff:      map[string]time.Time{},
		said:         map[string][]string{},
		unconfigured: map[string]bool{},
	}
}

// Why a pool cannot buy a machine, beyond the reasons in choose. The first
// two are of a replica that has yet to read the pool, or to list what the
// pool's provider sells.
var (
	errNotBuying  = errors.New("the pool is not known to buy machines")
	errNotListed  = errors.New("what the pool's provider sells has not been listed")
	errBackingOff = errors.New("a purchase for the pool failed a moment ago")
)

// CanProvision reports whether the pool could buy a machine with room for
// need now. It asks no provider: it answers from the last tick's offers.
//
// When the pool cannot, it says why. It is asked often, for machines of many
// sizes, so it says each thing that stands in the way once, and again only
// after the pool has bought a machine in between. It says nothing of what
// this replica has yet to read: the next tick reads it, and says so itself
// when it cannot.
func (p *Provisioner) CanProvision(ctx context.Context, pool string, need scheduler.MachineNeed) bool {
	held, err := p.recentRecords(ctx)
	if err != nil {
		log.Warn().Err(err).Str("pool_name", pool).Msg("provisioner: cannot read the machines held")
		return false
	}
	_, _, err = p.choose(pool, need, held)
	unread := errors.Is(err, errNotBuying) || errors.Is(err, errNotListed)
	if err != nil && !unread && p.unsaid(pool, err) {
		log.Warn().Err(err).Str("pool_name", pool).Msg("provisioner: the pool cannot buy a machine")
	}
	return err == nil
}

// unsaid reports whether the pool has not yet said that this stands in the
// way of buying, and takes it as said. How it stands this time is left out
// of what is remembered, so that what a pool has said does not grow with the
// sizes it is asked for or the prices it is offered.
func (p *Provisioner) unsaid(pool string, why error) bool {
	what := why.Error()
	if o := (obstacle{}); errors.As(why, &o) {
		what = o.what
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if slices.Contains(p.said[pool], what) {
		return false
	}
	p.said[pool] = append(p.said[pool], what)
	return true
}

// Provision buys one machine for the pool and returns it before it has
// booted.
func (p *Provisioner) Provision(ctx context.Context, pool string, need scheduler.MachineNeed) (*scheduler.ProvisionedMachine, error) {
	machine, request, err := p.reserve(ctx, pool, need)
	if err != nil {
		return nil, err
	}
	// The machine was chosen from what this provider was seen to sell.
	provider, _ := p.providers.Get(machine.Provider)
	instance, err := provider.Create(ctx, request)
	if err != nil {
		p.refused(ctx, machine, err)
		return nil, fmt.Errorf("pool %q: buy %s in %s: %w", pool, machine.OfferID, machine.Region, err)
	}
	p.sold(pool, request.Offer)
	return p.recordInstance(ctx, machine, instance)
}

// reserve writes the record of the machine the pool is about to buy, and
// returns it with what its provider is to be asked for.
//
// The record is written before anything is bought, while every limit is
// checked against the records as they are. It holds everything about the
// purchase but the provider's name for the machine, so that a purchase whose
// answer is lost can be picked up from the record alone.
func (p *Provisioner) reserve(ctx context.Context, pool string, need scheduler.MachineNeed) (Machine, providers.CreateRequest, error) {
	id := p.newID()
	script, tokenHash, err := p.controlPlane.JoinScript(ctx, pool, id)
	if err != nil {
		return Machine{}, providers.CreateRequest{}, fmt.Errorf("pool %q: prepare a machine to join: %w", pool, err)
	}

	request := providers.CreateRequest{
		Name:          id,
		StartupScript: script,
		Tags:          map[string]string{MachineTag: id},
		Until:         p.clock.Now().Add(leaseFor),
	}
	machine, err := p.store.Reserve(ctx, func(held []Machine) (Machine, error) {
		offer, config, err := p.choose(pool, need, held)
		if err != nil {
			return Machine{}, err
		}
		request.Offer = offer
		// The pool's price limit is the most it pays, whatever the provider
		// charges later.
		request.MaxHourlyCostMicros = config.Provisioning.MaxNodeHourlyCents * microsPerCent
		record := newMachine(id, pool, offer, p.clock.Now())
		record.TokenHash = tokenHash
		record.Until = request.Until
		return record, nil
	})
	if err != nil {
		p.forget(ctx, Machine{ID: id, Pool: pool, TokenHash: tokenHash})
		return Machine{}, providers.CreateRequest{}, fmt.Errorf("pool %q cannot buy a machine: %w", pool, err)
	}
	p.forgetRecentRecords()
	return machine, request, nil
}

// refused takes note of why the provider did not sell the machine, so that
// the pool does not ask for the same again at once. A provider that has none
// left, or that lets the account buy no more, has sold nothing: the purchase
// is undone. One that failed in any other way may have created the machine
// all the same: its record stays, and the next tick finds out.
func (p *Provisioner) refused(ctx context.Context, machine Machine, err error) {
	switch {
	case errors.Is(err, providers.ErrOutOfStock):
		p.markSoldOut(ctx, machine.offerKey)
		p.abandon(ctx, machine)
	case errors.Is(err, providers.ErrQuota):
		p.backOff(machine.Pool, quotaBackoff)
		p.abandon(ctx, machine)
	default:
		p.backOff(machine.Pool, errorBackoff)
	}
}

// recordInstance writes down the instance the provider answered with, and
// returns the machine as the scheduler is told about it.
func (p *Provisioner) recordInstance(ctx context.Context, machine Machine, instance providers.Instance) (*scheduler.ProvisionedMachine, error) {
	bought, err := p.store.Update(ctx, machine.ID, func(record *Machine) { record.InstanceID = instance.ID })
	if errors.Is(err, errNoRecord) {
		// The tick gave up on the purchase while it was under way. The
		// instance has no record now, and the next tick deletes it.
		return nil, fmt.Errorf("pool %q: machine %s was given up on before its provider answered", machine.Pool, machine.ID)
	}
	if err != nil {
		// The instance carries the machine's tag, so the next tick ties it
		// to its record.
		log.Warn().Err(err).Str("pool_name", machine.Pool).Str("machine_id", machine.ID).Msg("provisioner: bought a machine but could not record its instance")
		bought = machine
	}

	log.Info().
		Str("pool_name", machine.Pool).
		Str("machine_id", machine.ID).
		Str("provider", machine.Provider).
		Str("instance_id", instance.ID).
		Str("offer", machine.OfferID).
		Str("region", machine.Region).
		Bool("spot", machine.Spot).
		Int64("hourly_micros", machine.HourlyMicros).
		Msg("provisioner: bought a machine")
	provisioned := bought.provisioned()
	return &provisioned, nil
}

// Booting returns the pool's machines that have been bought and have not run
// a worker yet.
func (p *Provisioner) Booting(ctx context.Context, pool string) ([]scheduler.ProvisionedMachine, error) {
	machines, err := p.store.List(ctx)
	if err != nil {
		return nil, err
	}
	var booting []scheduler.ProvisionedMachine
	for _, machine := range machines {
		if machine.Pool == pool && machine.booting() {
			booting = append(booting, machine.provisioned())
		}
	}
	return booting, nil
}

// choose decides what the pool would buy, with what this replica knows and
// the records it is given. It returns the pool's config as it was when it
// decided, which is the one to buy with.
func (p *Provisioner) choose(pool string, need scheduler.MachineNeed, held []Machine) (providers.Offer, types.WorkerPoolConfig, error) {
	now := p.clock.Now()

	p.mu.Lock()
	defer p.mu.Unlock()
	config, ok := p.pools[pool]
	if !ok {
		return providers.Offer{}, config, errNotBuying
	}
	if _, ok := p.providers.Get(config.Provisioning.Provider); !ok {
		return providers.Offer{}, config, fmt.Errorf("the pool buys from provider %q, which is not configured under providers", config.Provisioning.Provider)
	}
	list, listed := p.offers[config.Provisioning.Provider]
	if !listed {
		return providers.Offer{}, config, errNotListed
	}
	if now.Before(p.backoff[pool]) {
		return providers.Offer{}, config, errBackingOff
	}
	offer, err := choose(purchase{
		pool:       pool,
		config:     config,
		need:       need,
		offers:     p.inStockLocked(list, now),
		held:       held,
		limits:     p.limits,
		spentToday: p.spentToday,
	})
	return offer, config, err
}

// inStockLocked returns what the provider was last seen to sell, less the
// offers it has since refused to sell. A list too old to buy from has
// nothing.
func (p *Provisioner) inStockLocked(list offerList, now time.Time) []providers.Offer {
	if now.Sub(list.fetchedAt) >= offersStaleAfter {
		return nil
	}
	inStock := make([]providers.Offer, 0, len(list.offers))
	for _, offer := range list.offers {
		if now.Before(p.soldOut[keyOf(offer)]) {
			continue
		}
		inStock = append(inStock, offer)
	}
	return inStock
}

// markSoldOut has an offer passed over for soldOutFor: by this replica from
// now, and by the others from their next tick.
func (p *Provisioner) markSoldOut(ctx context.Context, offer offerKey) {
	until := p.clock.Now().Add(soldOutFor)
	p.mu.Lock()
	p.soldOut[offer] = until
	p.mu.Unlock()
	if err := p.store.MarkSoldOut(ctx, offer, until); err != nil {
		log.Warn().Err(err).Str("provider", offer.Provider).Str("offer", offer.OfferID).Msg("provisioner: could not tell the other replicas that an offer is sold out")
	}
}

func (p *Provisioner) backOff(pool string, d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.backoff[pool] = p.clock.Now().Add(d)
}

// sold takes one machine off the stock this replica remembers, so that it
// does not buy more of an offer than its provider counted before the list is
// fetched again. The pool that bought it has shown that it can buy: why it
// cannot, should it come to that again, is said anew.
func (p *Provisioner) sold(pool string, offer providers.Offer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.said, pool)
	list := p.offers[offer.Provider]
	for i, listed := range list.offers {
		if keyOf(listed) != keyOf(offer) || listed.Available == 0 {
			continue
		}
		if listed.Available == 1 {
			list.offers = slices.Delete(slices.Clone(list.offers), i, i+1)
		} else {
			list.offers = slices.Clone(list.offers)
			list.offers[i].Available--
		}
		p.offers[offer.Provider] = list
		return
	}
}

// recentRecords returns the records, read at most recordsMaxAge ago.
func (p *Provisioner) recentRecords(ctx context.Context) ([]Machine, error) {
	now := p.clock.Now()
	p.mu.Lock()
	recent := p.recent
	p.mu.Unlock()
	if now.Sub(recent.readAt) < recordsMaxAge {
		return recent.machines, nil
	}

	machines, err := p.store.List(ctx)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	p.recent = recentMachines{machines: machines, readAt: now}
	p.mu.Unlock()
	return machines, nil
}

func (p *Provisioner) forgetRecentRecords() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recent = recentMachines{}
}

// abandon undoes a purchase the provider refused: nothing was bought.
func (p *Provisioner) abandon(ctx context.Context, machine Machine) {
	p.forget(ctx, machine)
	if err := p.store.Remove(ctx, machine.ID, p.clock.Now()); err != nil {
		// The tick removes it once the purchase is old enough to give up on.
		log.Warn().Err(err).Str("pool_name", machine.Pool).Str("machine_id", machine.ID).Msg("provisioner: could not remove the record of a machine that was not bought")
	}
	p.forgetRecentRecords()
}

func (p *Provisioner) forget(ctx context.Context, machine Machine) bool {
	if err := p.controlPlane.ForgetMachine(ctx, machine.Pool, machine.ID, machine.TokenHash); err != nil {
		log.Warn().Err(err).Str("pool_name", machine.Pool).Str("machine_id", machine.ID).Msg("provisioner: could not remove a machine from its pool")
		return false
	}
	return true
}

// snapshot is what the tick decides on. Its maps are the provisioner's own:
// reading the pools again replaces them, and nothing changes one.
type snapshot struct {
	pools      map[string]types.WorkerPoolConfig
	unloaded   map[string]error
	overBudget bool
}

func (p *Provisioner) snapshot() snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return snapshot{
		pools:      p.pools,
		unloaded:   p.unloaded,
		overBudget: p.limits.dailySpent(p.spentToday),
	}
}

// knows reports whether this replica can tell if the pool exists: it has
// read the pools, and this one did not fail to load.
func (s snapshot) knows(pool string) bool {
	_, unloaded := s.unloaded[pool]
	return s.pools != nil && !unloaded
}

// buys reports whether this replica has a pool that buys the machine: one
// of its name, buying from its provider.
func (s snapshot) buys(machine Machine) (types.WorkerPoolConfig, bool) {
	config, ok := s.pools[machine.Pool]
	if !ok || config.Provisioning.Provider != machine.Provider {
		return types.WorkerPoolConfig{}, false
	}
	return config, true
}
