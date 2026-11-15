package provisioner

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

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
	require.Equal(t, scheduler.ProvisionedMachine{MachineID: got.MachineID, CPU: 20_000, Memory: 54_000, GPUCount: 1}, *got)

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
		GPUCount: 1, CPUMillicores: 20_000, MemoryMB: 60_000, HourlyMicros: 90 * microsPerCent,
		CreatedAt: bought, Until: bought.Add(leaseFor),
	}, record)

	booting, err := w.Booting(w.ctx, "spot-l40s")
	require.NoError(t, err)
	require.Equal(t, []scheduler.ProvisionedMachine{*got}, booting)
	booting, err = w.Booting(w.ctx, "spot-a10g")
	require.NoError(t, err)
	require.Empty(t, booting)
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
		c.pools["spot-nebius-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Provider = "nebius" })
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
		pool.GPUType, pool.Provisioning.Provider = "A10G", "nebius"
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
// maxNodes and the spend cap between them.
func TestReplicasBuyingAtOnceStayWithinTheLimits(t *testing.T) {
	tests := []struct {
		name   string
		limits Limits
		want   int
	}{
		{"maxNodes", Limits{}, 4},
		{"the hourly cap", Limits{MaxHourlyCents: 250}, 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			w := newWorld(t)
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
	w.control.pools["spot-nebius-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Provider = "nebius" })
	w.Tick(w.ctx)

	// While the machine is being prepared to join, the pool is moved to
	// nebius with another price limit, and this replica reads it.
	w.control.beforeJoinScript = func() {
		w.control.set(func(c *fakeControl) {
			c.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) {
				pool.Provisioning.Provider, pool.Provisioning.MaxNodeHourlyCents = "nebius", 90
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
	require.Equal(t, scheduler.ProvisionedMachine{MachineID: "machine-1", CPU: 20_000, Memory: 54_000, GPUCount: 1}, *bought)
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
				w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Provider = "nebius" })
			},
			want: `the pool buys from provider \"nebius\", which is not configured under providers`,
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
