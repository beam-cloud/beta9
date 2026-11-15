package provisioner

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

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

// A machine its provider took back is replaced at once, by the same offer
// if the provider still lists it: a provider that has no more says so when
// it is asked.
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
			w.after(launchGrace)

			test.take(w, machine)
			w.after(tickInterval)
			w.gone(machine)

			require.Equal(t, "FIN-01", w.buy("spot-l40s").Region)
		})
	}
}

// A machine that is given back for a reason of the pool's own says nothing
// about the offer.
func TestAnOfferWhoseMachineThePoolGaveBackIsBoughtAgain(t *testing.T) {
	w := newWorld(t)
	w.control.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.BootTimeout = types.Duration(5 * time.Minute) })
	w.verda.SetOffers(l40s(), l40s(in("FIN-02"), costing(140)))
	w.Tick(w.ctx)

	// One never joins, and one has nothing to run.
	never := w.buy("spot-l40s")
	w.every(time.Minute, 6*time.Minute)
	w.gone(never)
	idle := w.buy("spot-l40s")
	require.Equal(t, "FIN-01", idle.Region)
	w.workers.join(idle)
	w.every(time.Minute, 12*time.Minute)
	w.gone(idle)

	require.Equal(t, "FIN-01", w.buy("spot-l40s").Region)
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
		c.pools["spot-l40s"] = spotL40S(func(pool *types.WorkerPoolConfig) { pool.Provisioning.Provider = "nebius" })
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
