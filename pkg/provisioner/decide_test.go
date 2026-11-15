package provisioner

import (
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

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
	released := func(reason string) func(*Machine) {
		return func(m *Machine) { m.Release = &Release{Reason: reason} }
	}
	// failed is the release of a machine its provider could not start.
	failed := func(m *Machine) {
		m.Release = &Release{Reason: "its provider failed to start it", SoldOut: true}
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
			was:  machine(released("it had nothing to run for 10m0s")),
			seen: seen(2*time.Hour, providerSaid(gone)),
		},

		// What the provider says.
		{
			name: "its provider no longer has it",
			was:  joined(),
			seen: seen(time.Hour, providerSaid(gone), worker(types.WorkerStatusAvailable, busy)),
			want: released("its provider no longer has it"),
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
			want: released("its provider no longer has it"),
		},
		{
			name: "its provider is deleting it",
			was:  joined(),
			seen: seen(time.Hour, providerHas(providers.StatusDeleted), worker(types.WorkerStatusAvailable, busy)),
			want: released("its provider is deleting it"),
		},
		{
			// A spot machine, taken back in the middle of its work. Its
			// worker still looks alive: its state has not expired yet.
			name: "its provider took it back",
			was:  joined(),
			seen: seen(time.Hour, providerHas(providers.StatusInterrupted), worker(types.WorkerStatusAvailable, busy)),
			want: released("its provider took it back"),
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
			want: released("its provider says it failed"),
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
			want: released("its provider never created it"),
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
				released("its provider is deleting it")(m)
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
			want: released("it did not join its pool within 20m0s"),
		},
		{
			name: "not joined by its pool's own boot timeout",
			was:  machine(),
			seen: seen(5*time.Minute, func(s *sighting) { s.pool.Provisioning.BootTimeout = types.Duration(5 * time.Minute) }),
			want: released("it did not join its pool within 5m0s"),
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
			want: released("it has had no worker for 20m0s"),
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
				m.Release = &Release{Reason: "its pool no longer buys machines", DrainUntil: at(time.Hour + drainFor)}
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
