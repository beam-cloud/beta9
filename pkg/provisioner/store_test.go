package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/stretchr/testify/require"
)

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
