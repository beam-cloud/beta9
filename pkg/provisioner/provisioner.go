// Package provisioner buys machines for the pools that have a provisioning
// block, and looks after each one until its provider has confirmed it gone.
//
// The scheduler asks it three things: whether a pool can buy a machine now
// (CanProvision), to buy one (Provision), and which machines it bought
// (Booting, Machine). Everything else happens on a tick, which compares each
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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/providers/watch"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/redislock"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
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
	// quotaBackoff is how long a pool does not buy from a provider after it
	// refused for quota.
	quotaBackoff = 10 * time.Minute
	// errorBackoff is how long a pool does not buy from a provider after a
	// purchase failed for any other reason.
	errorBackoff = time.Minute

	// buyGrace is how long a machine that its provider has yet to answer
	// for counts as one the pool is still buying.
	buyGrace = 15 * time.Second
	// waitWorth is what an hour of waiting for a machine is worth, in hours
	// of the cheapest machine that would do.
	waitWorth = 10
	// unknownBoot is how long a machine is taken to need to come up before
	// any has.
	unknownBoot = 5 * time.Minute
	// estimatesFrom is how far back the market's history is read when
	// nothing has been learned yet.
	estimatesFrom = 30 * 24 * time.Hour

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
	Clock common.Clock
	// NewID returns the ID of the next machine. Nil means a random one.
	NewID func() string
	// Outcomes takes note of what becomes of the machines that are bought.
	// Nil means nobody does.
	Outcomes *watch.Recorder
}

type Provisioner struct {
	providers    *providers.Registry
	controlPlane ControlPlane
	workers      Workers
	store        *Store
	limits       Limits
	clock        common.Clock
	newID        func() string
	outcomes     *watch.Recorder

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
	backoff map[string]time.Time // by pool and provider, until when
	// estimates are what past purchases say to expect of the next. The
	// store has them for every replica; a map that is in use is not changed.
	estimates watch.Estimates
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
		clock:        common.ClockOrReal(opts.Clock),
		newID:        newID,
		outcomes:     opts.Outcomes,
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
// any of needs now. It asks no provider: it answers from the last tick's
// offers.
//
// When the pool cannot, it says why. It is asked often, for machines of many
// sizes, so it says each thing that stands in the way once, and again only
// after the pool has bought a machine in between. It says nothing of what
// this replica has yet to read: the next tick reads it, and says so itself
// when it cannot.
func (p *Provisioner) CanProvision(ctx context.Context, pool string, needs ...scheduler.MachineNeed) bool {
	held, err := p.recentRecords(ctx)
	if err != nil {
		log.Warn().Err(err).Str("pool_name", pool).Msg("provisioner: cannot read the machines held")
		return false
	}
	// A machine the pool is still buying does not count against it: whoever
	// waits for the pool stays while it is bought.
	held = slices.DeleteFunc(slices.Clone(held), func(m Machine) bool {
		return m.Pool == pool && m.InstanceID == "" && m.Release == nil && p.clock.Now().Sub(m.CreatedAt) < buyGrace
	})
	_, _, err = p.choose(pool, needs, held)
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
// booted: the first of those it would buy for all of needs (see solve).
func (p *Provisioner) Provision(ctx context.Context, pool string, needs ...scheduler.MachineNeed) (*scheduler.ProvisionedMachine, error) {
	machine, request, err := p.reserve(ctx, pool, needs)
	if err != nil {
		return nil, err
	}
	// The machine was chosen from what this provider was seen to sell.
	provider, _ := p.providers.Get(machine.Provider)
	asked := p.clock.Now()
	instance, err := provider.Create(ctx, request)
	answered := p.clock.Now()
	p.outcomes.Record(machine.outcome(answered).Bought(err, answered.Sub(asked)))
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
func (p *Provisioner) reserve(ctx context.Context, pool string, needs []scheduler.MachineNeed) (Machine, providers.CreateRequest, error) {
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
		offer, config, err := p.choose(pool, needs, held)
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
		p.backOff(machine, quotaBackoff)
		p.abandon(ctx, machine)
	default:
		p.backOff(machine, errorBackoff)
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
		machineLog(log.Warn(), machine).Err(err).Msg("provisioner: bought a machine but could not record its instance")
		bought = machine
		bought.InstanceID = instance.ID
	}

	machineLog(log.Info(), bought).
		Str("offer", bought.OfferID).
		Str("region", bought.Region).
		Bool("spot", bought.Spot).
		Int64("hourly_micros", bought.HourlyMicros).
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

// Machine returns a machine that a pool bought and holds, or nil when none
// has that ID.
func (p *Provisioner) Machine(ctx context.Context, id string) (*scheduler.ProvisionedMachine, error) {
	machine, err := p.store.get(ctx, id)
	if errors.Is(err, errNoRecord) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	provisioned := machine.provisioned()
	return &provisioned, nil
}

// choose decides what the pool would buy, with what this replica knows and
// the records it is given. It returns the pool's config as it was when it
// decided, which is the one to buy with.
func (p *Provisioner) choose(pool string, needs []scheduler.MachineNeed, held []Machine) (providers.Offer, types.WorkerPoolConfig, error) {
	now := p.clock.Now()

	p.mu.Lock()
	config, ok := p.pools[pool]
	if !ok {
		p.mu.Unlock()
		return providers.Offer{}, config, errNotBuying
	}
	// What every provider the pool can buy from has in stock, and why the
	// first that it cannot buy from is none of them.
	var inStock []providers.Offer
	var unusable error
	for _, name := range config.Provisioning.Providers {
		list, listed := p.offers[name]
		switch _, configured := p.providers.Get(name); {
		case !configured:
			unusable = cmp.Or(unusable, fmt.Errorf("the pool buys from provider %q, which is not configured under providers", name))
		// A disabled provider lists no offers, which would read as one that
		// has nothing in stock.
		case !p.providers.IsEnabled(name):
			unusable = cmp.Or(unusable, fmt.Errorf("the pool buys from provider %q, which is disabled under providers.disabled", name))
		case !listed:
			unusable = cmp.Or(unusable, errNotListed)
		case now.Before(p.backoff[pool+"/"+name]):
			unusable = cmp.Or(unusable, errBackingOff)
		default:
			inStock = append(inStock, p.inStockLocked(list, now)...)
		}
	}
	buying := purchase{
		pool:       pool,
		config:     config,
		needs:      needs,
		offers:     inStock,
		held:       held,
		limits:     p.limits,
		spentToday: p.spentToday,
		estimates:  p.estimates,
		now:        now,
	}
	// What to buy is worked out with none of this held: it takes a while.
	p.mu.Unlock()
	if len(inStock) == 0 && unusable != nil {
		return providers.Offer{}, config, unusable
	}
	offer, err := choose(buying)
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

func (p *Provisioner) backOff(machine Machine, d time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.backoff[machine.Pool+"/"+machine.Provider] = p.clock.Now().Add(d)
}

// learn writes what became of a machine to the market's history, and takes
// it into what every replica expects of the next.
func (p *Provisioner) learn(ctx context.Context, outcome watch.Outcome) {
	p.outcomes.Record(outcome)
	estimates, err := p.store.Learn(ctx, outcome)
	if err != nil {
		log.Warn().Err(err).Msg("provisioner: could not learn from what became of a machine")
	}
	if estimates == nil {
		return
	}
	p.mu.Lock()
	p.estimates = estimates
	p.mu.Unlock()
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
	i := slices.IndexFunc(list.offers, func(listed providers.Offer) bool {
		return keyOf(listed) == keyOf(offer) && listed.Available > 0
	})
	if i < 0 {
		return // no longer listed, or its provider does not count how many it has
	}
	// The list is the provider's own, so a copy is changed.
	list.offers = slices.Clone(list.offers)
	list.offers[i].Available--
	if list.offers[i].Available == 0 {
		list.offers = slices.Delete(list.offers, i, i+1)
	}
	p.offers[offer.Provider] = list
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
		machineLog(log.Warn(), machine).Err(err).Msg("provisioner: could not remove the record of a machine that was not bought")
	}
	p.forgetRecentRecords()
}

func (p *Provisioner) forget(ctx context.Context, machine Machine) bool {
	if err := p.controlPlane.ForgetMachine(ctx, machine.Pool, machine.ID, machine.TokenHash); err != nil {
		machineLog(log.Warn(), machine).Err(err).Msg("provisioner: could not remove a machine from its pool")
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
	estimates  watch.Estimates
}

func (p *Provisioner) snapshot() snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	return snapshot{
		pools:      p.pools,
		unloaded:   p.unloaded,
		overBudget: p.limits.dailySpent(p.spentToday),
		estimates:  p.estimates,
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
	if !ok || !slices.Contains(config.Provisioning.Providers, machine.Provider) {
		return types.WorkerPoolConfig{}, false
	}
	return config, true
}

// MachineTag is the tag an instance bought for a pool carries at its
// provider. Its value is the machine's ID. An instance with the tag and no
// record is deleted, so nothing else may use the tag.
const MachineTag = "beam-pool-machine"

// Machine is the record of one machine bought for a pool. It is written
// before the provider is asked for the machine and removed once the provider
// has confirmed that the machine is gone, so everything that costs money has
// a record.
type Machine struct {
	// ID is the ID the machine joins its pool with, and its MachineTag.
	ID   string `json:"id"`
	Pool string `json:"pool"`
	// What it was bought as, and from which provider.
	offerKey
	// InstanceID is the provider's ID for the machine. It is empty until the
	// provider has answered the request to create it.
	InstanceID string `json:"instance_id,omitempty"`
	// TokenHash names the credential the machine joins with.
	TokenHash string `json:"token_hash,omitempty"`

	// InstanceType is what its provider calls a machine of its kind, where
	// that is not the offer's ID.
	InstanceType string `json:"instance_type,omitempty"`

	// What it has, and costs.
	GPU           string `json:"gpu,omitempty"`
	GPUCount      uint32 `json:"gpu_count,omitempty"`
	CPUMillicores int64  `json:"cpu_millicores"`
	MemoryMB      int64  `json:"memory_mb"`
	HourlyMicros  int64  `json:"hourly_micros"`

	CreatedAt time.Time `json:"created_at"`
	// ReadyAt is when the machine's worker first became available. It is
	// zero while the machine boots.
	ReadyAt time.Time `json:"ready_at,omitzero"`
	// Until is when a provider that can delete a machine by itself deletes
	// this one, unless that is put off again. It is set, and put off, for
	// every machine: a provider that cannot does nothing with it.
	Until time.Time `json:"until,omitzero"`
	// BilledUntil is the time up to which the machine's cost has been added
	// to what was spent.
	BilledUntil time.Time `json:"billed_until,omitzero"`

	// IdleSince is since when the machine has had nothing to run.
	IdleSince time.Time `json:"idle_since,omitzero"`
	// DownSince is since when a machine that had joined has had no usable
	// worker.
	DownSince time.Time `json:"down_since,omitzero"`
	// PoolGoneSince is since when no gateway replica has had a pool that
	// buys this machine.
	PoolGoneSince time.Time `json:"pool_gone_since,omitzero"`

	// Release is set once the machine is to be given back. It is not unset.
	Release *Release `json:"release,omitempty"`
}

// Release is why a machine is given back, and how.
type Release struct {
	Reason string `json:"reason"`
	// End is how the market's history names this end of a machine: one of
	// its results. It is empty for a machine that was never made.
	End string `json:"end,omitempty"`
	// DrainUntil is how long the machine's work is waited for before it is
	// given back anyway. Zero means it is given back at once.
	DrainUntil time.Time `json:"drain_until,omitzero"`
	// SoldOut is set when the provider could not start what it sold. What
	// the machine was bought as is then passed over for a while, like an
	// offer the provider refused to sell.
	SoldOut bool `json:"sold_out,omitempty"`
}

// newMachine is the record of a machine about to be bought from offer.
func newMachine(id, pool string, offer providers.Offer, now time.Time) Machine {
	return Machine{
		ID:            id,
		Pool:          pool,
		offerKey:      keyOf(offer),
		InstanceType:  offer.InstanceType,
		GPU:           offer.GPU,
		GPUCount:      offer.GPUCount,
		CPUMillicores: offer.CPUMillicores,
		MemoryMB:      offer.MemoryMB,
		HourlyMicros:  offer.HourlyCostMicros,
		CreatedAt:     now,
	}
}

// outcome is what the market's history is told became of the machine at
// now: what it was bought as, where, and for how much.
func (m Machine) outcome(now time.Time) watch.Outcome {
	outcome := watch.OutcomeOf(providers.Offer{
		ID:               m.OfferID,
		Provider:         m.Provider,
		Cloud:            m.Cloud,
		InstanceType:     m.InstanceType,
		Region:           m.Region,
		GPU:              m.GPU,
		GPUCount:         m.GPUCount,
		HourlyCostMicros: m.HourlyMicros,
		Spot:             m.Spot,
	})
	outcome.Source = watch.SourcePool
	outcome.At = now
	return outcome
}

// machineLog adds what tells the machine apart to a log event.
func machineLog(event *zerolog.Event, m Machine) *zerolog.Event {
	return event.
		Str("pool_name", m.Pool).
		Str("machine_id", m.ID).
		Str("provider", m.Provider).
		Str("instance_id", m.InstanceID)
}

// booting reports whether the machine has been bought and has not run a
// worker yet.
func (m Machine) booting() bool {
	return m.InstanceID != "" && m.ReadyAt.IsZero() && m.Release == nil
}

// workerID is the ID of the worker that stands for the machine.
func (m Machine) workerID() string {
	return compute.AgentMachineWorkerID(m.ID)
}

// billedFrom is the time from which the machine's cost has yet to be
// counted as spent.
func (m Machine) billedFrom() time.Time {
	if m.BilledUntil.IsZero() {
		return m.CreatedAt
	}
	return m.BilledUntil
}

// cost is what the machine cost from one time to another, in cents. A
// machine without an instance is not known to have been bought, and has cost
// nothing.
func (m Machine) cost(from, to time.Time) float64 {
	if m.InstanceID == "" || !to.After(from) {
		return 0
	}
	return float64(m.HourlyMicros) / microsPerCent * to.Sub(from).Hours()
}

// usableMemoryShare is how much of the memory a machine is sold with can be
// counted on. A machine reports less than its nominal size, because firmware
// and the kernel keep some.
const usableMemoryShare = 0.9

func usableMemoryMB(nominal int64) int64 {
	return int64(float64(nominal) * usableMemoryShare)
}

// provisioned is the machine as the scheduler is told about it.
func (m Machine) provisioned() scheduler.ProvisionedMachine {
	return scheduler.ProvisionedMachine{
		MachineID: m.ID,
		CPU:       m.CPUMillicores,
		Memory:    usableMemoryMB(m.MemoryMB),
		GPUCount:  m.GPUCount,
		Spot:      m.Spot,
	}
}

// Limits cap what all pools together spend on machines. Zero means no cap.
type Limits struct {
	// MaxHourlyCents is the most the machines held may cost per hour.
	MaxHourlyCents int64
	// MaxDailyCents is the most that may be spent in any 24 hours.
	MaxDailyCents int64
}

// dailySpent reports whether what was spent in the last 24 hours, in cents,
// has reached MaxDailyCents.
func (l Limits) dailySpent(spentToday float64) bool {
	return l.MaxDailyCents > 0 && spentToday >= float64(l.MaxDailyCents)
}

// purchase is everything that decides whether a pool may buy a machine, and
// which.
type purchase struct {
	pool   string
	config types.WorkerPoolConfig
	// needs is what the pool is short of a machine for. None is a machine
	// for nothing in particular.
	needs []scheduler.MachineNeed
	// offers is what the pool's providers have in stock.
	offers []providers.Offer
	// held is every machine there is a record of, for every pool.
	held   []Machine
	limits Limits
	// spentToday is what machines have cost in the last 24 hours, in cents.
	spentToday float64
	// estimates are what past purchases say to expect of one made at now.
	estimates watch.Estimates
	now       time.Time
}

// Why a pool cannot buy a machine. When it is for want of an offer, noOffer
// says what stands in the way.
var (
	errAtMaxNodes = errors.New("the pool holds as many machines as provisioning.maxNodes allows")
	errAtMaxGPUs  = errors.New("another machine would exceed provisioning.maxGPUs")
	errHourlyCap  = errors.New("one more machine would take the hourly cost of all machines over providers.maxHourlyCents")
	errDailyCap   = errors.New("providers.maxDailyCents has been spent in the last 24 hours")
)

// microsPerCent converts the cents limits are written in to the millionths
// of a dollar offers are priced in.
const microsPerCent = 10_000

// choose picks the machine to buy: the first of those that are the best to
// buy for what is needed, of what is in stock that the pool buys (see
// solve). It fails when the pool is at maxNodes or maxGPUs, when a spend cap
// would be passed, and when nothing in stock is a machine for any of it. It
// changes nothing.
func choose(p purchase) (providers.Offer, error) {
	left, err := p.headroom()
	if err != nil {
		return providers.Offer{}, err
	}
	needs := p.needs
	if len(needs) == 0 {
		needs = []scheduler.MachineNeed{{}}
	}
	if buy := solve(needs, p.candidates(), left); len(buy) > 0 {
		return buy[0].Offer, nil
	}

	// Why not, for the hardest of them to buy for. A machine that would
	// take the pool over a limit stands in the way only when no other does.
	hardest := slices.MinFunc(needs, scheduler.MachineNeed.Compare)
	overMaxGPUs, overHourly := false, false
	for _, offer := range selling(p.offers, p.config) {
		if holds(offer, hardest) {
			overMaxGPUs = overMaxGPUs || int(offer.GPUCount) > left.gpus
			overHourly = overHourly || offer.HourlyCostMicros > left.hourlyMicros
		}
	}
	switch {
	case overMaxGPUs:
		return providers.Offer{}, errAtMaxGPUs
	case overHourly:
		return providers.Offer{}, errHourlyCap
	}
	return providers.Offer{}, noOffer(p, hardest)
}

// headroom is how much more the pool may buy. A machine counts against it
// with every GPU it has, and until its record is gone: until then it may
// still exist, and cost money.
func (p purchase) headroom() (headroom, error) {
	buying := p.config.Provisioning
	left := headroom{machines: buying.MaxNodes, gpus: math.MaxInt, hourlyMicros: math.MaxInt64}
	if buying.MaxGPUs > 0 {
		left.gpus = buying.MaxGPUs
	}
	if p.limits.MaxHourlyCents > 0 {
		left.hourlyMicros = p.limits.MaxHourlyCents * microsPerCent
	}
	for _, machine := range p.held {
		left.hourlyMicros -= machine.HourlyMicros
		if machine.Pool == p.pool {
			left.machines--
			left.gpus -= int(machine.GPUCount)
		}
	}
	switch {
	case left.machines <= 0:
		return left, errAtMaxNodes
	case left.gpus <= 0:
		return left, errAtMaxGPUs
	case p.limits.dailySpent(p.spentToday):
		return left, errDailyCap
	}
	return left, nil
}

// candidates are the offers in stock that the pool buys, the cheapest
// first, with what to expect of each. All of that is learned from what
// became of the machines bought before (watch.Estimates).
func (p purchase) candidates() []*candidate {
	offers := selling(p.offers, p.config)
	slices.SortFunc(offers, byPrice)
	// An offer that nothing has come up as is taken to be as fast as the
	// fastest: one taken to be slow would never be tried.
	fastest := unknownBoot
	for _, offer := range offers {
		if boot, _, _ := p.estimates.Of(watch.OutcomeOf(offer), p.now); boot > 0 {
			fastest = min(fastest, boot)
		}
	}
	candidates := make([]*candidate, len(offers))
	for i, offer := range offers {
		boot, odds, taken := p.estimates.Of(watch.OutcomeOf(offer), p.now)
		candidates[i] = &candidate{Offer: offer, comeUp: cmp.Or(boot, fastest).Hours() / odds, taken: taken}
	}
	return candidates
}

// byPrice orders offers by what they cost. Those at one price are always in
// the same order.
func byPrice(a, b providers.Offer) int {
	return cmp.Or(
		cmp.Compare(a.HourlyCostMicros, b.HourlyCostMicros),
		cmp.Compare(a.Region, b.Region),
		cmp.Compare(a.ID, b.ID),
		cmp.Compare(a.Cloud, b.Cloud),
	)
}

// obstacle is what stands between a pool and a machine to buy. The detail is
// how it stands this time: the room that was needed, a price, the regions on
// offer. It differs from one request and one listing to the next, and what
// stands in the way does not.
type obstacle struct {
	what   string
	detail string
}

func (o obstacle) Error() string { return o.what + o.detail }

// noOffer says why nothing in stock is a machine to buy. It lets go of what
// is asked for one thing at a time, from the room that is needed back to
// spot or on-demand, until the provider has something: the last thing let go
// of is what stands in the way.
func noOffer(p purchase, need scheduler.MachineNeed) error {
	buying, from := p.config.Provisioning, strings.Join(p.config.Provisioning.Providers, ", ")
	// What the pool buys, as one of them and as many.
	one, many := strings.TrimSpace(p.config.GPUType), strings.TrimSpace(p.config.GPUType)
	if one == "" {
		one, many = "CPU machine", "CPU machines"
	}

	asked := p.config
	if len(selling(p.offers, asked)) > 0 {
		kind := ""
		if need.OnDemand {
			kind = ", on demand"
		}
		return obstacle{
			what:   fmt.Sprintf("no %s that %s has for the pool has room for what is needed", one, from),
			detail: fmt.Sprintf(" (cpu %dm, memory %d MiB, gpus %d%s)", need.CPU, need.Memory, need.GPUCount, kind),
		}
	}
	asked.Provisioning.MaxNodeHourlyCents = 0
	if atAnyPrice := selling(p.offers, asked); len(atAnyPrice) > 0 {
		return obstacle{
			what:   fmt.Sprintf("%s has no %s within provisioning.maxNodeHourlyCents %d", from, one, buying.MaxNodeHourlyCents),
			detail: fmt.Sprintf(": the cheapest costs %s cents an hour", cents(slices.MinFunc(atAnyPrice, byPrice).HourlyCostMicros)),
		}
	}
	asked.Provisioning.Regions = nil
	if anywhere := selling(p.offers, asked); len(anywhere) > 0 {
		return obstacle{
			what:   fmt.Sprintf("%s offers no %s in regions %v", from, many, buying.Regions),
			detail: "; it offers them in " + strings.Join(regionsOf(anywhere), ", "),
		}
	}
	asked.Provisioning.Spot = !buying.Spot
	switch theOtherWay := selling(p.offers, asked); {
	case len(theOtherWay) > 0 && buying.Spot:
		return fmt.Errorf("%s sells %s only on demand: remove provisioning.spot", from, many)
	case len(theOtherWay) > 0:
		return fmt.Errorf("%s sells %s only as spot: set provisioning.spot", from, many)
	case len(p.offers) > 0:
		return fmt.Errorf("%s has no %s in stock", from, one)
	}
	return fmt.Errorf("%s is not known to have anything in stock", from)
}

// selling returns what of the stock a pool that asks for this buys.
func selling(stock []providers.Offer, asked types.WorkerPoolConfig) []providers.Offer {
	var sold []providers.Offer
	for _, offer := range stock {
		if sells(offer, asked) {
			sold = append(sold, offer)
		}
	}
	return sold
}

// regionsOf lists the regions the offers are in, in a fixed order.
func regionsOf(offers []providers.Offer) []string {
	regions := make([]string, 0, len(offers))
	for _, offer := range offers {
		regions = append(regions, offer.Region)
	}
	slices.Sort(regions)
	return slices.Compact(regions)
}

// cents writes a price in cents, with as many decimals as it has.
func cents(micros int64) string {
	return strconv.FormatFloat(float64(micros)/microsPerCent, 'f', -1, 64)
}

// sells reports whether the offer is something the pool buys: its GPU type,
// spot or on-demand as configured, in its regions, within its price limit.
func sells(offer providers.Offer, pool types.WorkerPoolConfig) bool {
	buying := pool.Provisioning
	return (offer.Spot == buying.Spot || (!offer.Spot && buying.OnDemand)) &&
		strings.EqualFold(offer.GPU, strings.TrimSpace(pool.GPUType)) &&
		(offer.GPUCount > 0) == (offer.GPU != "") &&
		(len(buying.Regions) == 0 || slices.Contains(buying.Regions, offer.Region)) &&
		// Without a price there is no telling what it does to the caps.
		offer.HourlyCostMicros > 0 &&
		(buying.MaxNodeHourlyCents <= 0 || offer.HourlyCostMicros <= buying.MaxNodeHourlyCents*microsPerCent)
}

// holds reports whether a machine bought from the offer has room for need.
// A machine its provider may take back has none for work that may not be
// interrupted.
func holds(offer providers.Offer, need scheduler.MachineNeed) bool {
	return roomOf(offer).fits(sizeOf(need)) > 0 && !(offer.Spot && need.OnDemand)
}

const (
	// tickInterval is how often every replica refreshes what it knows, and
	// how often one of them checks the machines against their providers.
	tickInterval = 15 * time.Second

	// launchGrace is how long after its purchase a machine that its provider
	// does not have is waited for before it is given up. The provider may
	// still be acting on a request it never answered, and may take a moment
	// to know of a machine it has just sold.
	launchGrace = providers.LaunchGrace
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
	p.refreshEstimates(ctx, now)

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
	names := make([]string, 0, len(pools))
	for _, config := range pools {
		names = append(names, config.Provisioning.Providers...)
	}
	slices.Sort(names)
	return slices.Compact(names)
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

// refreshEstimates loads what any replica learned from the machines that
// were bought. When nothing has been learned yet, it starts from what the
// market's history says became of them.
func (p *Provisioner) refreshEstimates(ctx context.Context, now time.Time) {
	estimates, err := p.store.Estimates(ctx)
	if err == nil && estimates == nil {
		var history []watch.Outcome
		if history, err = p.outcomes.Read(ctx, now.Add(-estimatesFrom), now); err == nil {
			estimates, err = p.store.StartEstimates(ctx, history)
		}
	}
	if err != nil {
		log.Warn().Err(err).Msg("provisioner: could not read what was learned from past purchases")
		return
	}
	p.mu.Lock()
	p.estimates = estimates
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
			machineLog(log.Warn(), machine).Err(err).Msg("provisioner: could not keep a machine whose pool exists")
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
	for _, name := range p.providers.ConfiguredNames() {
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
	// usualBoot is how long machines bought like it take to come up, or
	// zero when none has.
	usualBoot time.Duration
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
	s.usualBoot, _, _ = known.estimates.Of(machine.outcome(now), now)
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
	isMachine := func(instance providers.Instance) bool {
		if machine.InstanceID == "" {
			return instance.Tags[MachineTag] == machine.ID
		}
		return instance.ID == machine.InstanceID
	}
	if i := slices.IndexFunc(listed, isMachine); i >= 0 {
		return present, listed[i]
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
		machineLog(log.Warn(), machine).Err(err).Msg("provisioner: could not ask the provider about a machine it does not list")
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
	if reason, end := atProvider(&m, s); reason != "" {
		m.Release = &Release{Reason: reason, End: end, SoldOut: reason == failedToStart}
		return m, false
	}
	if m.InstanceID == "" {
		return m, false // nothing is known to have been bought
	}
	if reason, end := withoutWorker(&m, s); reason != "" {
		m.Release = &Release{Reason: reason, End: end}
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

// lostEnd is how the market's history names the end of a machine whose
// provider reports it in this status.
var lostEnd = map[providers.Status]string{
	providers.StatusDeleted:     watch.ResultGone,
	providers.StatusInterrupted: watch.ResultInterrupted,
	providers.StatusFailed:      watch.ResultFailed,
}

// endedAtProvider reports whether a machine that ended this way had ended
// by the time it was found: its provider took it back, failed it, or no
// longer had it. Any other machine is given back while it runs, and ends
// when its provider has deleted it.
func endedAtProvider(end string) bool {
	switch end {
	case watch.ResultGone, watch.ResultInterrupted, watch.ResultFailed:
		return true
	}
	return false
}

// atProvider returns why the machine's provider no longer runs it, and how
// the market's history names that end, or "". A purchase whose answer never
// arrived is known by its tag: its instance is recorded first, whatever
// state it is in, so that giving the machine back deletes it.
func atProvider(m *Machine, s sighting) (reason, end string) {
	switch s.provider {
	case gone:
		if m.InstanceID == "" {
			// There was no machine, so none ended.
			return "its provider never created it", ""
		}
		return "its provider no longer has it", watch.ResultGone
	case present:
		if m.InstanceID == "" {
			m.InstanceID = s.instance.ID
		}
		// A machine that failed after it had run was started: what it was
		// bought as can be run.
		if s.instance.Status == providers.StatusFailed && m.ReadyAt.IsZero() {
			return failedToStart, watch.ResultFailed
		}
		return lostStatus[s.instance.Status], lostEnd[s.instance.Status]
	}
	return "", ""
}

// withoutWorker keeps ReadyAt and DownSince, and returns why a machine that
// did not join its pool, or did not stay, is given back, and how the
// market's history names that end, or "".
func withoutWorker(m *Machine, s sighting) (reason, end string) {
	timeout := s.bootTimeout()
	switch {
	case !s.workersKnown:
	case s.available():
		if m.ReadyAt.IsZero() {
			m.ReadyAt = s.now
		}
		m.DownSince = time.Time{}
	case m.ReadyAt.IsZero():
		// Twice as long as machines bought like it take is long enough:
		// another is expected to be up sooner.
		if s.usualBoot > 0 {
			timeout = min(timeout, 2*s.usualBoot)
		}
		if s.now.Sub(m.CreatedAt) >= timeout {
			return fmt.Sprintf("it did not join its pool within %s", timeout), watch.ResultNeverUp
		}
	case heldBack(s.worker):
		m.DownSince = time.Time{}
	case m.DownSince.IsZero():
		m.DownSince = s.now
	case s.now.Sub(m.DownSince) >= timeout:
		return fmt.Sprintf("it has had no worker for %s", timeout), watch.ResultLost
	}
	return "", ""
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
		return &Release{Reason: "its pool no longer buys machines", End: watch.ResultReleased, DrainUntil: s.now.Add(drainFor)}
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
		next = p.releaseIfIdle(next, s)
	}
	next = billed(next, s.now)
	if next.Release == nil {
		next = p.renewLease(ctx, next, s.now)
	}

	if !reflect.DeepEqual(next, machine) {
		if _, err := p.store.Update(ctx, machine.ID, func(record *Machine) { record.take(next) }); err != nil {
			machineLog(log.Warn(), machine).Err(err).Msg("provisioner: could not update a machine's record")
			return
		}
	}
	if machine.ReadyAt.IsZero() && !next.ReadyAt.IsZero() {
		p.learn(ctx, next.outcome(s.now).CameUp(next.ReadyAt.Sub(next.CreatedAt)))
	}
	if next.Release == nil {
		return
	}
	if machine.Release == nil {
		p.announceRelease(ctx, next, s)
	}
	p.finishRelease(ctx, next, s.now)
}

// releaseIfIdle releases a machine that has nothing to run. The worker is
// cordoned in the same step that finds it without work, so nothing can be
// placed on it from here on. A machine that has work after all starts its
// idle time anew.
func (p *Provisioner) releaseIfIdle(machine Machine, s sighting) Machine {
	idle, err := p.workers.CordonWorkerIfIdle(machine.workerID())
	switch {
	case err != nil:
		machineLog(log.Warn(), machine).Err(err).Msg("provisioner: could not check whether a machine is idle")
	case idle:
		machine.Release = &Release{
			Reason: fmt.Sprintf("it had nothing to run for %s", s.pool.Provisioning.IdleAfter()),
			End:    watch.ResultReleased,
		}
	default:
		machine.IdleSince = time.Time{}
	}
	return machine
}

// announceRelease says that a machine is given back from now on, and why.
func (p *Provisioner) announceRelease(ctx context.Context, machine Machine, s sighting) {
	giving := machineLog(log.Info(), machine).Str("reason", machine.Release.Reason)
	if _, lost := lostStatus[s.instance.Status]; lost {
		// Why, only the provider knows: what it said goes with it.
		giving = giving.Str("status_detail", s.instance.StatusDetail)
	}
	giving.Msg("provisioner: giving a machine back")
	if endedAtProvider(machine.Release.End) {
		// It ended when its provider ended it, which is no later than now.
		// What is left of it is still to be deleted, which says nothing of
		// how long it was had.
		p.recordEnd(ctx, machine, s.now)
	}
	if machine.Release.SoldOut {
		// The next machine is bought as something else, if anything else is
		// on offer.
		p.markSoldOut(ctx, machine.offerKey)
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

// recordEnd writes to the market's history that the machine ended at now,
// and how. A machine that was never made has no end.
func (p *Provisioner) recordEnd(ctx context.Context, machine Machine, now time.Time) {
	if machine.Release.End == "" {
		return
	}
	p.learn(ctx, machine.outcome(now).Ended(machine.Release.End, now.Sub(machine.CreatedAt)))
}

// finishRelease gives a machine back: the gateway forgets it, its provider
// deletes it, and its record is removed. Each step can be repeated, and the
// record stays until the provider has confirmed the machine gone, so a step
// that fails is tried again on the next pass.
//
// A machine that was given back while it ran is had, and paid for, until
// this is done: its work may be waited for first, and its provider may not
// delete it at once. Its end is written to the market's history with its
// record's removal, which happens once.
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
			machineLog(log.Error(), machine).Msg("provisioner: cannot delete a machine at a provider that is not configured; it may still be running")
			return
		}
		if err := provider.Delete(ctx, machine.InstanceID); err != nil {
			machineLog(log.Warn(), machine).Err(err).Msg("provisioner: could not delete a machine at its provider")
			return
		}
	}
	// The machine is paid for until now. Should its record stay, it is paid
	// for until the record goes: there is no telling since when it is gone.
	if err := p.store.Remove(ctx, machine.ID, now); err != nil {
		machineLog(log.Warn(), machine).Err(err).Msg("provisioner: could not remove the record of a machine that is gone")
		return
	}
	if !endedAtProvider(machine.Release.End) {
		p.recordEnd(ctx, machine, now)
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
		machineLog(log.Warn(), machine).Err(err).Msg("provisioner: could not cordon a machine that is being drained")
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
		machineLog(log.Warn(), machine).Err(err).Msg("provisioner: could not put off the machine's deletion at its provider")
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

// errNoRecord: the machine's record was removed while it was being changed.
var errNoRecord = errors.New("the machine has no record")

// Store keeps the records, and what their machines have cost, in Redis,
// where every gateway replica sees them. Records are written one at a time
// under one lock.
type Store struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewStore(rdb *common.RedisClient) *Store {
	return &Store{rdb: rdb, lock: common.NewRedisLock(rdb)}
}

// Writes are a few Redis commands, so a writer that finds the lock taken
// waits for it rather than giving up.
var writeLock = common.RedisLockOptions{TtlS: 10, Retries: 100, RetryInterval: 50 * time.Millisecond}

// List returns every record, ordered by machine ID.
func (s *Store) List(ctx context.Context) ([]Machine, error) {
	stored, err := s.rdb.HGetAll(ctx, common.RedisKeys.ComputeProvisionedMachines()).Result()
	if err != nil {
		return nil, fmt.Errorf("list bought machines: %w", err)
	}
	machines := make([]Machine, 0, len(stored))
	for id, data := range stored {
		machine, err := readRecord(id, data)
		if err != nil {
			return nil, err
		}
		machines = append(machines, machine)
	}
	slices.SortFunc(machines, func(a, b Machine) int { return strings.Compare(a.ID, b.ID) })
	return machines, nil
}

// Reserve adds the record newRecord returns. newRecord sees every record
// there is and nothing can be added while it looks, so a limit it checks
// holds across replicas. When newRecord fails, nothing is added.
func (s *Store) Reserve(ctx context.Context, newRecord func(held []Machine) (Machine, error)) (Machine, error) {
	var reserved Machine
	err := s.lock.WithLease(ctx, common.RedisKeys.ComputeProvisionedLock(), writeLock, func(ctx context.Context) error {
		held, err := s.List(ctx)
		if err != nil {
			return err
		}
		if reserved, err = newRecord(held); err != nil {
			return err
		}
		return s.save(ctx, reserved, 0)
	})
	return reserved, err
}

// Update changes one record and returns it as it is now. It fails with
// errNoRecord when the record is gone.
//
// A change that moves BilledUntil on counts what the machine cost in between
// as spent. The record and the spend are written in one transaction: a write
// that fails has counted nothing, and one that succeeds has counted it once.
func (s *Store) Update(ctx context.Context, id string, change func(*Machine)) (Machine, error) {
	var updated Machine
	err := s.lock.WithLease(ctx, common.RedisKeys.ComputeProvisionedLock(), writeLock, func(ctx context.Context) error {
		stored, err := s.get(ctx, id)
		if err != nil {
			return err
		}
		updated = stored
		change(&updated)
		return s.save(ctx, updated, updated.cost(stored.billedFrom(), updated.BilledUntil))
	})
	return updated, err
}

// Remove deletes a record and, in the same transaction, counts what its
// machine cost since it was last billed, up to until, as spent. Removing a
// record that is gone is not an error, and counts nothing.
func (s *Store) Remove(ctx context.Context, id string, until time.Time) error {
	return s.lock.WithLease(ctx, common.RedisKeys.ComputeProvisionedLock(), writeLock, func(ctx context.Context) error {
		machine, err := s.get(ctx, id)
		if errors.Is(err, errNoRecord) {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HDel(ctx, common.RedisKeys.ComputeProvisionedMachines(), id)
			addSpend(ctx, pipe, until, machine.cost(machine.billedFrom(), until))
			return nil
		})
		return err
	})
}

func (s *Store) get(ctx context.Context, id string) (Machine, error) {
	data, err := s.rdb.HGet(ctx, common.RedisKeys.ComputeProvisionedMachines(), id).Result()
	if errors.Is(err, redis.Nil) {
		return Machine{}, errNoRecord
	}
	if err != nil {
		return Machine{}, err
	}
	return readRecord(id, data)
}

func readRecord(id, data string) (Machine, error) {
	var machine Machine
	if err := json.Unmarshal([]byte(data), &machine); err != nil {
		return Machine{}, fmt.Errorf("read the record of machine %s: %w", id, err)
	}
	return machine, nil
}

// save writes a record and, in the same transaction, counts cents as spent
// at the time the record is billed to.
func (s *Store) save(ctx context.Context, machine Machine, cents float64) error {
	data, err := json.Marshal(machine)
	if err != nil {
		return err
	}
	_, err = s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, common.RedisKeys.ComputeProvisionedMachines(), machine.ID, data)
		addSpend(ctx, pipe, machine.BilledUntil, cents)
		return nil
	})
	return err
}

const (
	// spendHour names the hour whose spend a moment's cost is added to.
	spendHour = "2006010215"
	// spendHours is how many hours of spend make a day's: the hour of now
	// and the 23 before it.
	spendHours = 24
	// spendKept is how long an hour's spend is kept: an hour longer than it
	// is counted, so that it is still there while it is.
	spendKept = 25 * time.Hour
)

func spendKey(at time.Time) string {
	return common.RedisKeys.ComputeProvisionedSpend(at.UTC().Format(spendHour))
}

// addSpend has the transaction count cents as spent in the hour of at.
func addSpend(ctx context.Context, tx redis.Pipeliner, at time.Time, cents float64) {
	if cents <= 0 {
		return
	}
	tx.IncrByFloat(ctx, spendKey(at), cents)
	tx.Expire(ctx, spendKey(at), spendKept)
}

// Spent returns what the machines have cost in the day up to now, in cents.
func (s *Store) Spent(ctx context.Context, now time.Time) (float64, error) {
	hours := make([]string, spendHours)
	for i := range hours {
		hours[i] = spendKey(now.Add(-time.Duration(i) * time.Hour))
	}
	spent, err := s.rdb.MGet(ctx, hours...).Result()
	if err != nil {
		return 0, fmt.Errorf("read what was spent: %w", err)
	}
	total := 0.0
	for i, value := range spent {
		text, ok := value.(string)
		if !ok {
			continue // nothing was spent in that hour
		}
		cents, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return 0, fmt.Errorf("read what was spent (%s): %w", hours[i], err)
		}
		total += cents
	}
	return total, nil
}

// MarkSoldOut records, for every replica, that an offer is not to be bought
// until the given time.
func (s *Store) MarkSoldOut(ctx context.Context, offer offerKey, until time.Time) error {
	field, err := json.Marshal(offer)
	if err != nil {
		return err
	}
	return s.rdb.HSet(ctx, common.RedisKeys.ComputeProvisionedSoldOut(), string(field), until.UnixNano()).Err()
}

// SoldOut returns the offers that are not to be bought now, and until when.
// It removes the marks that have lapsed.
func (s *Store) SoldOut(ctx context.Context, now time.Time) (map[offerKey]time.Time, error) {
	stored, err := s.rdb.HGetAll(ctx, common.RedisKeys.ComputeProvisionedSoldOut()).Result()
	if err != nil {
		return nil, fmt.Errorf("read what is sold out: %w", err)
	}
	marks := make(map[offerKey]time.Time, len(stored))
	var lapsed []string
	for field, value := range stored {
		var offer offerKey
		nanos, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			err = json.Unmarshal([]byte(field), &offer)
		}
		if until := time.Unix(0, nanos); err == nil && now.Before(until) {
			marks[offer] = until
			continue
		}
		lapsed = append(lapsed, field)
	}
	if len(lapsed) > 0 {
		// Whoever reads next removes what this could not.
		_ = s.rdb.HDel(ctx, common.RedisKeys.ComputeProvisionedSoldOut(), lapsed...).Err()
	}
	return marks, nil
}

// Estimates returns what has been learned from past purchases, or nil when
// nothing has been yet.
func (s *Store) Estimates(ctx context.Context) (watch.Estimates, error) {
	data, err := s.rdb.Get(ctx, common.RedisKeys.ComputeProvisionedEstimates()).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read the estimates: %w", err)
	}
	var estimates watch.Estimates
	return estimates, json.Unmarshal(data, &estimates)
}

// Learn takes an outcome into the estimates, for every replica, and returns
// them as they are now. It adds nothing to estimates that have not been
// started, and returns nil: the outcome is in the history they start from.
func (s *Store) Learn(ctx context.Context, outcome watch.Outcome) (watch.Estimates, error) {
	return s.estimate(ctx, false, outcome)
}

// StartEstimates starts the estimates from the market's history, and returns
// them. Every replica reads the history when it finds no estimates, and
// several may at once: the first to get here starts them, and the others
// get what it wrote.
func (s *Store) StartEstimates(ctx context.Context, history []watch.Outcome) (watch.Estimates, error) {
	return s.estimate(ctx, true, history...)
}

func (s *Store) estimate(ctx context.Context, start bool, outcomes ...watch.Outcome) (estimates watch.Estimates, err error) {
	err = s.lock.WithLease(ctx, common.RedisKeys.ComputeProvisionedLock(), writeLock, func(ctx context.Context) error {
		estimates, err = s.Estimates(ctx)
		if started := estimates != nil; err != nil || started == start {
			return err // they start once, and learn only once started
		}
		if start {
			estimates = watch.Estimates{}
		}
		for _, outcome := range outcomes {
			estimates.Learn(outcome)
		}
		data, err := json.Marshal(estimates)
		if err != nil {
			return err
		}
		return s.rdb.Set(ctx, common.RedisKeys.ComputeProvisionedEstimates(), data, 0).Err()
	})
	return estimates, err
}

// reconcileLock is held by the replica that runs a pass, which renews it for
// as long as the pass runs. Its TTL is how long the other replicas wait
// before one of them runs a pass when that replica is gone.
var reconcileLock = common.RedisLockOptions{TtlS: 60}

// Reconcile runs pass unless another replica is running one, or one started
// less than every ago. Either way another replica has it, which is no error.
// Passes delete machines and add up what was spent, so two must never run at
// once, and there is no use in every replica asking the providers the same
// questions.
func (s *Store) Reconcile(ctx context.Context, now time.Time, every time.Duration, pass func(context.Context)) error {
	err := s.lock.WithLease(ctx, common.RedisKeys.ComputeProvisionedReconcileLock(), reconcileLock, func(ctx context.Context) error {
		last, err := s.rdb.Get(ctx, common.RedisKeys.ComputeProvisionedReconcile()).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		if nanos, err := strconv.ParseInt(last, 10, 64); err == nil && now.Sub(time.Unix(0, nanos)) < every {
			return nil
		}
		if err := s.rdb.Set(ctx, common.RedisKeys.ComputeProvisionedReconcile(), now.UnixNano(), 0).Err(); err != nil {
			return err
		}
		pass(ctx)
		return nil
	})
	if errors.Is(err, redislock.ErrNotObtained) {
		return nil
	}
	return err
}
