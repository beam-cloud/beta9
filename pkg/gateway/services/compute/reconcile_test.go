package compute

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

// boughtMachine is a pool with one machine bought from a provider, and what a
// test needs to watch the service look after it.
type boughtMachine struct {
	service  *Service
	provider *providers.Fake
	repo     *fakeComputeRepo
	events   *capturedEvents
	state    *model.PoolState
	instance string
	now      time.Time
}

const boughtTokenHash = "join-token-hash"

func newBoughtMachine(t *testing.T) *boughtMachine {
	t.Helper()
	now := time.Now().UTC()
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Mode:        string(types.PoolModePrivate),
		Reservations: []model.Reservation{{
			Name:                  "beam-pool-1-machine-1",
			MachineID:             "machine-1",
			Source:                model.SourceCLIReservation,
			Status:                model.ReservationPending,
			CreatedAt:             now,
			ExpiresAt:             now.Add(6 * time.Hour),
			HourlyCostMicros:      1_000_000,
			RegistrationTokenHash: boughtTokenHash,
		}},
	}
	provider := fakeProvider("shadeform")
	instance := provider.Plant(testOwner, providers.StatusPending, nil)
	state.Reservations[0].ID = instance
	state.Reservations[0].Provider = provider.Name()
	state.Reservations[0].InstanceID = instance

	repo := &fakeComputeRepo{
		pools:      map[string][]*model.PoolState{"workspace-1": {state}},
		joinTokens: map[string]*model.JoinTokenState{boughtTokenHash: {TokenHash: boughtTokenHash, ExpiresAt: now.Add(time.Hour)}},
	}
	events := &capturedEvents{}
	return &boughtMachine{
		service:  &Service{computeRepo: repo, eventRepo: events, providers: providers.RegistryOf(provider)},
		provider: provider,
		repo:     repo,
		events:   events,
		state:    state,
		instance: instance,
		now:      now,
	}
}

// check runs the reconciler over the pool once, after the given time has
// passed, and returns the reservation as it was saved.
func (m *boughtMachine) check(t *testing.T, after time.Duration) model.Reservation {
	t.Helper()
	m.now = m.now.Add(after)
	m.repo.savedPool = false
	require.NoError(t, m.service.reconcilePool(context.Background(), "workspace-1", m.state, m.now))
	return m.repo.pools["workspace-1"][0].Reservations[0]
}

// reported returns the status of every event of the given kind, in order, and
// forgets the events it has seen.
func (m *boughtMachine) reported(action string) []string {
	statuses := []string{}
	for _, event := range m.events.events {
		if event.Action == action {
			statuses = append(statuses, event.Status)
		}
	}
	m.events.events = nil
	return statuses
}

func (m *boughtMachine) tokenRevoked() bool {
	return m.repo.joinTokens[boughtTokenHash].Revoked
}

func TestStatusCheckReportsAChangeOnlyWhenSomethingChanged(t *testing.T) {
	m := newBoughtMachine(t)
	const statusUpdated = types.EventComputeActionReservationStatusUpdated

	// Nothing has changed at the provider. The time of the check is saved, so
	// that the provider is not asked again by the next pass, and nothing is
	// reported.
	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationPending, reservation.Status)
	require.Equal(t, m.now, reservation.LastStatusCheckAt)
	require.True(t, m.repo.savedPool)
	require.Empty(t, m.reported(statusUpdated))

	for range 3 {
		m.check(t, 2*reconcileStatusCheckInterval)
	}
	require.Empty(t, m.reported(statusUpdated), "a machine that is still booting is not news")

	// The machine comes up.
	m.provider.SetStatus(m.instance, providers.StatusActive, "")
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationActive, reservation.Status)
	require.Equal(t, []string{string(model.ReservationActive)}, m.reported(statusUpdated))

	m.check(t, 2*reconcileStatusCheckInterval)
	require.Empty(t, m.reported(statusUpdated), "a machine that is still running is not news")

	// The provider's wording, and the machine's address, are changes too.
	m.provider.SetStatus(m.instance, providers.StatusActive, "Running startup script")
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, "Running startup script", reservation.LastStatusMessage)
	require.Len(t, m.reported(statusUpdated), 1)

	m.provider.SetAddress(m.instance, "203.0.113.7")
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, "203.0.113.7", reservation.PublicIP)
	require.Len(t, m.reported(statusUpdated), 1)

	m.check(t, 2*reconcileStatusCheckInterval)
	require.Empty(t, m.reported(statusUpdated))
}

func TestStatusCheckAsksTheProviderAtMostOncePerInterval(t *testing.T) {
	m := newBoughtMachine(t)
	m.check(t, 0)

	m.provider.SetStatus(m.instance, providers.StatusActive, "")
	reservation := m.check(t, reconcileStatusCheckInterval/2)
	require.Equal(t, model.ReservationPending, reservation.Status, "checked too recently to ask again")

	reservation = m.check(t, reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationActive, reservation.Status)
}

func TestStatusCheckLeavesAMachineAloneWhenItsStatusIsUnknown(t *testing.T) {
	for _, was := range []model.ReservationStatus{model.ReservationPending, model.ReservationActive} {
		t.Run(string(was), func(t *testing.T) {
			m := newBoughtMachine(t)
			m.state.Reservations[0].Status = was
			m.provider.SetStatus(m.instance, providers.StatusUnknown, "resizing")

			reservation := m.check(t, 0)
			require.Equal(t, was, reservation.Status, "a status nobody understands says nothing about the machine")
			require.Empty(t, m.provider.Deletes())
			require.False(t, m.tokenRevoked())
		})
	}
}

func TestStatusCheckClosesAReservationWhoseMachineIsGone(t *testing.T) {
	for name, lose := range map[string]func(*boughtMachine){
		"the provider no longer knows it":  func(m *boughtMachine) { m.provider.Forget(m.instance) },
		"the provider says it was deleted": func(m *boughtMachine) { m.provider.SetStatus(m.instance, providers.StatusDeleted, "") },
	} {
		t.Run(name, func(t *testing.T) {
			m := newBoughtMachine(t)
			m.state.Reservations[0].Status = model.ReservationActive
			lose(m)

			reservation := m.check(t, 0)
			require.Equal(t, model.ReservationDeleted, reservation.Status)
			require.Equal(t, instanceGoneMessage, reservation.LastStatusMessage)
			require.Empty(t, reservation.LastError)
			require.True(t, m.tokenRevoked(), "nothing can join the pool as a machine that is gone")
			require.Equal(t, []string{string(model.ReservationDeleted)}, m.reported(types.EventComputeActionReservationStatusUpdated))
			require.Empty(t, m.provider.Deletes(), "there is nothing left to delete")

			// It is closed: the provider is not asked about it again.
			m.check(t, 2*reconcileStatusCheckInterval)
			require.Empty(t, m.reported(types.EventComputeActionReservationStatusUpdated))
		})
	}
}

func TestStatusCheckKeepsAReservationWhenTheProviderCannotBeAsked(t *testing.T) {
	m := newBoughtMachine(t)
	m.state.Reservations[0].Status = model.ReservationActive
	m.service.providers = providers.RegistryOf()

	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationActive, reservation.Status)
	require.Equal(t, `provider "shadeform" is not configured`, reservation.LastError)
	require.False(t, m.tokenRevoked())
}

func TestStatusCheckDeletesAMachineTheProviderFailedToStart(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusFailed, "no capacity in the zone")

	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationFailed, reservation.Status)
	require.Equal(t, reconcileReasonProviderFailed, reservation.TerminatingReason)
	require.Equal(t, "no capacity in the zone", reservation.LastStatusMessage)
	require.Empty(t, reservation.LastError)
	require.Equal(t, []string{m.instance}, m.provider.Deletes(), "a failed machine is not left at the provider")
	require.True(t, m.tokenRevoked())

	left, err := m.provider.List(context.Background())
	require.NoError(t, err)
	require.Empty(t, left)
}

func TestStatusCheckDeletesWhatIsLeftOfAMachineTheProviderTookBack(t *testing.T) {
	m := newBoughtMachine(t)
	m.state.Reservations[0].Status = model.ReservationActive
	m.provider.SetStatus(m.instance, providers.StatusInterrupted, "")

	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationFailed, reservation.Status)
	require.Equal(t, instanceInterruptedMessage, reservation.LastStatusMessage)
	require.Equal(t, []string{m.instance}, m.provider.Deletes(), "a stopped machine or its disk is not left at the provider")
	require.True(t, m.tokenRevoked())
}

func TestStatusCheckKeepsDeletingAFailedMachineUntilTheProviderLetsItGo(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusFailed, "")
	m.provider.FailDeletes(errors.New("provider is down"))

	reservation := m.check(t, 0)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, reconcileReasonProviderFailed, reservation.TerminatingReason)
	require.Equal(t, instanceFailedMessage, reservation.LastStatusMessage)
	require.Equal(t, "provider is down", reservation.LastError)

	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Len(t, m.provider.Deletes(), 2, "the delete is tried again")

	m.provider.FailDeletes(nil)
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationFailed, reservation.Status, "it ends as the failure it was, not as a release")
	require.Equal(t, instanceFailedMessage, reservation.LastStatusMessage)
	require.Empty(t, reservation.LastError)
	require.Len(t, m.provider.Deletes(), 3)

	left, err := m.provider.List(context.Background())
	require.NoError(t, err)
	require.Empty(t, left)
}

func TestFailedMachineThatHasAlsoExpiredStillEndsAsAFailure(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusFailed, "")
	m.provider.FailDeletes(errors.New("provider is down"))

	// The machine failed and is past the time it was wanted for. One pass
	// asks the provider to delete it once, as the failure it is.
	reservation := m.check(t, 7*time.Hour)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, reconcileReasonProviderFailed, reservation.TerminatingReason)
	require.Len(t, m.provider.Deletes(), 1)

	m.provider.FailDeletes(nil)
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationFailed, reservation.Status)
}

func TestExpiredReservationIsDeletedAtTheProvider(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.SetStatus(m.instance, providers.StatusActive, "")

	reservation := m.check(t, 7*time.Hour)
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Equal(t, reconcileReasonReservationExpired, reservation.TerminatingReason)
	require.Equal(t, []string{m.instance}, m.provider.Deletes())
	require.True(t, m.tokenRevoked())
}

func TestDeletePoolDeletesItsMachinesAtTheProvider(t *testing.T) {
	m := newBoughtMachine(t)

	res, err := m.service.DeletePool(testAuthContext("workspace-1", "owner-token"), &pb.DeletePoolRequest{Name: "pool-1"})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, []string{m.instance}, m.provider.Deletes())
	require.Empty(t, m.repo.pools["workspace-1"])
	require.True(t, m.tokenRevoked())
}

func TestDeletePoolKeepsThePoolWhileAMachineCannotBeDeleted(t *testing.T) {
	m := newBoughtMachine(t)
	m.provider.FailDeletes(errors.New("provider is down"))

	// The pool's record is the only record of the machine. Dropping it would
	// leave the machine running, and charged for, with nothing to find it by.
	res, err := m.service.DeletePool(testAuthContext("workspace-1", "owner-token"), &pb.DeletePoolRequest{Name: "pool-1"})
	require.NoError(t, err)
	require.False(t, res.Ok)
	require.Contains(t, res.ErrMsg, `pool "pool-1" was not deleted`)
	require.Contains(t, res.ErrMsg, "provider is down")
	require.Len(t, m.repo.pools["workspace-1"], 1)
	require.True(t, m.repo.savedPool)
	reservation := m.repo.pools["workspace-1"][0].Reservations[0]
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, reconcileReasonMachineReleased, reservation.TerminatingReason)
	require.True(t, m.tokenRevoked())

	// The reconciler goes on trying, and then the pool can be deleted.
	m.provider.FailDeletes(nil)
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationDeleted, reservation.Status)

	res, err = m.service.DeletePool(testAuthContext("workspace-1", "owner-token"), &pb.DeletePoolRequest{Name: "pool-1"})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Empty(t, m.repo.pools["workspace-1"])
	left, err := m.provider.List(context.Background())
	require.NoError(t, err)
	require.Empty(t, left)
}

func TestDeletePoolDeletesAMachineLeftBehindByAnOlderFailure(t *testing.T) {
	// Reservations that failed before failed machines were deleted may still
	// have a machine at the provider.
	m := newBoughtMachine(t)
	m.state.Reservations[0].Status = model.ReservationFailed

	res, err := m.service.DeletePool(testAuthContext("workspace-1", "owner-token"), &pb.DeletePoolRequest{Name: "pool-1"})
	require.NoError(t, err)
	require.True(t, res.Ok, res.ErrMsg)
	require.Equal(t, []string{m.instance}, m.provider.Deletes())
	require.Empty(t, m.repo.pools["workspace-1"])
}

// hardOfHearing is a provider that, while err is set, cannot be told how long
// a machine is wanted.
type hardOfHearing struct {
	*providers.Fake
	err     error
	extends int
}

func (p *hardOfHearing) Extend(ctx context.Context, id string, until time.Time) error {
	p.extends++
	if p.err != nil {
		return p.err
	}
	return p.Fake.Extend(ctx, id, until)
}

// renewedMachine is a pool with one running machine whose term is about to
// end, in a workspace that can pay for more.
type renewedMachine struct {
	service  *Service
	provider *hardOfHearing
	repo     *fakeComputeRepo
	state    *model.PoolState
	instance string
	start    time.Time
	expiry   time.Time
}

func newRenewedMachine(t *testing.T) *renewedMachine {
	t.Helper()
	start := time.Now().UTC().Truncate(time.Second)
	fake := fakeProvider("shadeform")
	instance := fake.Plant(testOwner, providers.StatusActive, nil)
	provider := &hardOfHearing{Fake: fake}
	expiry := start.Add(90 * time.Second)
	state := &model.PoolState{
		WorkspaceID: "workspace-1",
		Name:        "pool-1",
		Mode:        string(types.PoolModePrivate),
		Reservations: []model.Reservation{{
			ID:               instance,
			InstanceID:       instance,
			Provider:         "shadeform",
			Name:             "beam-pool-1-machine-1",
			MachineID:        "machine-1",
			Source:           model.SourceCLIReservation,
			Status:           model.ReservationActive,
			CreatedAt:        start.Add(-time.Hour),
			BillingCursorAt:  start.Add(-time.Minute),
			ExpiresAt:        expiry,
			HourlyCostMicros: 1_000_000,
		}},
	}
	repo := &fakeComputeRepo{pools: map[string][]*model.PoolState{"workspace-1": {state}}}
	return &renewedMachine{
		service: &Service{
			computeRepo: repo,
			eventRepo:   &capturedEvents{},
			providers:   providers.RegistryOf(provider),
			billing:     &fakeManagedBilling{balanceDecision: billingDecision{OK: true, AvailableCents: 1_000_000}},
		},
		provider: provider,
		repo:     repo,
		state:    state,
		instance: instance,
		start:    start,
		expiry:   expiry,
	}
}

// pass runs the reconciler over the pool once, the given time after the start,
// and returns the reservation as it was saved.
func (m *renewedMachine) pass(t *testing.T, after time.Duration) model.Reservation {
	t.Helper()
	require.NoError(t, m.service.reconcilePool(context.Background(), "workspace-1", m.state, m.start.Add(after)))
	return m.repo.pools["workspace-1"][0].Reservations[0]
}

func TestARenewalStandsWhenTheProviderCannotBeToldOfIt(t *testing.T) {
	m := newRenewedMachine(t)
	m.provider.err = errors.New("shadeform: 503 service unavailable")

	// The term is renewed for the hour the workspace can pay for.
	reservation := m.pass(t, 0)
	require.Equal(t, m.expiry.Add(time.Hour), reservation.ExpiresAt)
	require.Contains(t, reservation.LastError, "503 service unavailable")

	// The provider stays down past the time the term would have ended. The
	// machine is running and paid for, and it is kept.
	for _, after := range []time.Duration{time.Minute, 2 * time.Minute, 3 * time.Minute} {
		reservation = m.pass(t, after)
		require.Equal(t, model.ReservationActive, reservation.Status, "after %s", after)
	}
	require.Empty(t, m.provider.Deletes())
	left, err := m.provider.List(context.Background())
	require.NoError(t, err)
	require.Len(t, left, 1)
}

func TestAProviderThatCouldNotBeToldOfARenewalIsToldOnTheNextPass(t *testing.T) {
	m := newRenewedMachine(t)
	m.provider.err = errors.New("shadeform: 503 service unavailable")
	reservation := m.pass(t, 0)
	require.Equal(t, m.expiry.Add(time.Hour), reservation.ExpiresAt, "the term is renewed all the same")
	require.Equal(t, 1, m.provider.extends)
	require.True(t, m.provider.Until(m.instance).IsZero(), "the provider has not heard of the renewal")

	// It is still down a minute later, and is asked again although the term
	// is not due for another renewal.
	m.pass(t, time.Minute)
	require.Equal(t, 2, m.provider.extends)

	m.provider.err = nil
	reservation = m.pass(t, 2*time.Minute)
	require.Equal(t, 3, m.provider.extends)
	require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), m.provider.Until(m.instance))
	require.Empty(t, reservation.LastError)

	// Once it has been told, it is left alone until the next renewal.
	m.pass(t, 3*time.Minute)
	require.Equal(t, 3, m.provider.extends)
}

func TestARenewalStandsWhenTheProviderIsNotConfigured(t *testing.T) {
	m := newRenewedMachine(t)
	configured := m.service.providers
	m.service.providers = providers.RegistryOf()

	reservation := m.pass(t, 0)
	require.Equal(t, m.expiry.Add(time.Hour), reservation.ExpiresAt)
	reservation = m.pass(t, 3*time.Minute)
	require.Equal(t, model.ReservationActive, reservation.Status)
	require.Contains(t, reservation.LastError, `provider "shadeform" is not configured`)

	// The provider is configured again, and hears of the renewal it missed.
	m.service.providers = configured
	reservation = m.pass(t, 5*time.Minute)
	require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), m.provider.Until(m.instance))
}

// logged is what was logged while a test ran.
type logged struct {
	mu    sync.Mutex
	lines bytes.Buffer
}

func (l *logged) Write(line []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lines.Write(line)
}

// failures returns the entries that say an operation on a reservation failed,
// by operation.
func (l *logged) failures(t *testing.T) map[string]map[string]any {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	failures := map[string]map[string]any{}
	for _, line := range bytes.Split(bytes.TrimSpace(l.lines.Bytes()), []byte("\n")) {
		if len(line) == 0 {
			continue
		}
		entry := map[string]any{}
		require.NoError(t, json.Unmarshal(line, &entry))
		if operation, ok := entry["operation"].(string); ok {
			failures[operation] = entry
		}
	}
	return failures
}

func captureLog(t *testing.T) *logged {
	t.Helper()
	captured := &logged{}
	previous := log.Logger
	log.Logger = zerolog.New(captured)
	t.Cleanup(func() { log.Logger = previous })
	return captured
}

// outOfReach is a provider that cannot be asked about a machine.
type outOfReach struct {
	providers.Provider
	err error
}

func (p outOfReach) Get(context.Context, string) (providers.Instance, error) {
	return providers.Instance{}, p.err
}

func TestAFailureOnAReservationIsLoggedWithWhatItIsAbout(t *testing.T) {
	for name, tc := range map[string]struct {
		// breaks makes something fail for the machine before the pass.
		breaks    func(m *renewedMachine)
		operation string
		problem   string
	}{
		"usage that billing does not take": {
			breaks: func(m *renewedMachine) {
				m.service.billing.(*fakeManagedBilling).usageErr = errors.New("billing callback unavailable")
			},
			operation: "record usage",
			problem:   "billing callback unavailable",
		},
		"a provider that cannot be told of a renewal": {
			breaks:    func(m *renewedMachine) { m.provider.err = errors.New("shadeform: 503 service unavailable") },
			operation: "extend at provider",
			problem:   "503 service unavailable",
		},
		"a provider that cannot be asked about the machine": {
			breaks: func(m *renewedMachine) {
				m.service.providers = providers.RegistryOf(outOfReach{Provider: m.provider, err: errors.New("shadeform: connection reset")})
			},
			operation: "check status",
			problem:   "connection reset",
		},
		"a provider that is not configured": {
			breaks:    func(m *renewedMachine) { m.service.providers = providers.RegistryOf() },
			operation: "check status",
			problem:   `provider "shadeform" is not configured`,
		},
		"a machine that cannot be deleted when its term ends": {
			breaks: func(m *renewedMachine) {
				// The workspace cannot pay for another hour.
				m.service.billing.(*fakeManagedBilling).balanceDecision.AvailableCents = 50
				m.state.Reservations[0].ExpiresAt = m.start.Add(-time.Second)
				m.provider.FailDeletes(errors.New("provider is down"))
			},
			operation: "delete machine",
			problem:   "provider is down",
		},
	} {
		t.Run(name, func(t *testing.T) {
			m := newRenewedMachine(t)
			tc.breaks(m)
			captured := captureLog(t)

			reservation := m.pass(t, 0)

			entry, ok := captured.failures(t)[tc.operation]
			require.True(t, ok, "nothing was logged for %q: %v", tc.operation, captured.failures(t))
			require.Equal(t, "warn", entry["level"])
			require.Equal(t, "workspace-1", entry["workspace_id"])
			require.Equal(t, "pool-1", entry["pool_name"])
			require.Equal(t, m.instance, entry["reservation_id"])
			require.Contains(t, entry["error"], tc.problem)
			require.Contains(t, reservation.LastError, tc.problem, "and the reservation says so too")
		})
	}
}

func TestAFailedExtensionIsLogged(t *testing.T) {
	p := newExtendedPool(t)
	stubborn := &hardOfHearing{Fake: p.provider, err: errors.New("shadeform: 503 service unavailable")}
	p.service.providers = providers.RegistryOf(stubborn)
	captured := captureLog(t)

	res := p.extend(t, &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "48h"})
	require.True(t, res.Ok, res.ErrMsg)

	entry, ok := captured.failures(t)["extend"]
	require.True(t, ok, "nothing was logged: %v", captured.failures(t))
	require.Equal(t, "workspace-1", entry["workspace_id"])
	require.Equal(t, "pool-1", entry["pool_name"])
	require.Equal(t, p.instance, entry["reservation_id"])
	require.Contains(t, entry["error"], "503 service unavailable")
	reservation := p.repo.pools["workspace-1"][0].Reservations[0]
	require.Contains(t, reservation.LastError, "503 service unavailable")
	require.Equal(t, p.expiry, reservation.ExpiresAt, "the machine keeps the term its provider knows")
}

func TestAStatusCheckDoesNotClearWhatBillingRecordedInTheSamePass(t *testing.T) {
	m := newRenewedMachine(t)
	m.service.billing.(*fakeManagedBilling).usageErr = errors.New("billing callback unavailable")

	// Usage cannot be recorded, pass after pass, while the provider answers
	// every status check. The record goes on saying what is wrong.
	for _, after := range []time.Duration{0, time.Minute, 2 * time.Minute} {
		reservation := m.pass(t, after)
		require.Contains(t, reservation.LastError, "billing callback unavailable", "after %s", after)
	}
}

// A provider that deletes machines by itself is a safety net for a machine
// Beam has lost track of. It is told a time a week past the machine's term
// wherever it is told one, so that it never ends a machine Beam still runs.
func TestAProviderIsToldAMachineIsWantedUntilAWeekPastItsTerm(t *testing.T) {
	require.Equal(t, 7*24*time.Hour, reservationDeletionGrace)

	t.Run("when the machine is bought", func(t *testing.T) {
		shadeform := fakeProvider("shadeform", a10gOffer)
		repo := &fakeComputeRepo{}

		res := launchA10Gs(t, testAuthContext("workspace-1", "token-1"), launchService(repo, shadeform), 1)
		require.True(t, res.Ok, res.ErrMsg)
		reservation := repo.pools["workspace-1"][0].Reservations[0]
		require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), shadeform.Creates()[0].Until)
		require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), shadeform.Until(reservation.ID))
	})

	t.Run("when its term is renewed", func(t *testing.T) {
		m := newRenewedMachine(t)

		reservation := m.pass(t, 0)
		require.Equal(t, m.expiry.Add(time.Hour), reservation.ExpiresAt)
		require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), m.provider.Until(m.instance))
	})

	t.Run("when its pool is extended", func(t *testing.T) {
		p := newExtendedPool(t)

		res := p.extend(t, &pb.ExtendPoolCapacityRequest{Name: "pool-1", Ttl: "48h"})
		require.True(t, res.Ok, res.ErrMsg)
		reservation := p.repo.pools["workspace-1"][0].Reservations[0]
		require.True(t, reservation.ExpiresAt.After(p.expiry))
		require.Equal(t, reservation.ExpiresAt.Add(reservationDeletionGrace), p.provider.Until(p.instance))
	})
}
