package compute

import (
	"errors"
	"testing"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// How long a machine has to start at its provider.

// shortBoot is a boot timeout that a config sets in place of the default.
const shortBoot = 3 * time.Minute

// newBootingMachine has a machine that was bought this moment and is still
// starting at its provider, under the boot timeout. Zero is the default.
func newBootingMachine(t *testing.T, bootTimeout time.Duration) *boughtMachine {
	t.Helper()
	m := newBoughtMachine(t)
	m.state.Reservations[0].CreatedAt = m.now
	m.service.appConfig.ManagedCompute.Reservations.BootTimeout = types.Duration(bootTimeout)
	return m
}

func TestPendingReservationClosesAtDefaultProviderBootDeadline(t *testing.T) {
	const within = types.ManagedComputeDefaultReservationBootTimeout
	m := newBootingMachine(t, 0)
	reservation := m.check(t, within-time.Minute)
	require.Equal(t, model.ReservationPending, reservation.Status)
	require.Empty(t, m.provider.Deletes())
	require.False(t, m.tokenRevoked())

	reservation = m.check(t, time.Minute)
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Equal(t, reconcileReasonLaunchFailed, reservation.TerminatingReason)
	require.Contains(t, reservation.LastStatusMessage, within.String())
	require.Equal(t, []string{m.instance}, m.provider.Deletes())
	require.True(t, m.tokenRevoked(), "a queued bid cannot join after its deadline")
	require.Empty(t, m.provider.Everything())
}

func TestPendingReservationUsesConfiguredBootDeadline(t *testing.T) {
	m := newBootingMachine(t, shortBoot)
	require.Equal(t, model.ReservationPending, m.check(t, shortBoot-time.Minute).Status)
	require.False(t, m.tokenRevoked())
	reservation := m.check(t, time.Minute)
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Equal(t, reconcileReasonLaunchFailed, reservation.TerminatingReason)
	require.Len(t, m.provider.Deletes(), 1)
}

func TestProviderBootTimeoutRetainsTerminationUntilDeleteSucceeds(t *testing.T) {
	m := newBootingMachine(t, shortBoot)
	unavailable := errors.New("provider unavailable")
	m.provider.FailDeletes(unavailable)
	reservation := m.check(t, shortBoot)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, reconcileReasonLaunchFailed, reservation.TerminatingReason)
	require.Equal(t, unavailable.Error(), reservation.LastError)
	require.True(t, m.tokenRevoked())
	require.Len(t, m.provider.Deletes(), 1)

	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Len(t, m.provider.Deletes(), 2)
	m.provider.FailDeletes(nil)
	reservation = m.check(t, 2*reconcileStatusCheckInterval)
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Equal(t, reconcileReasonLaunchFailed, reservation.TerminatingReason)
	require.Empty(t, reservation.LastError)
	require.Empty(t, m.provider.Everything())
	require.Len(t, m.provider.Deletes(), 3)
}

func TestProviderActiveAtBootDeadlineIsKeptWithoutWaitingForAgent(t *testing.T) {
	m := newBootingMachine(t, shortBoot)
	m.provider.SetStatus(m.instance, providers.StatusActive, "")
	reservation := m.check(t, shortBoot)
	require.Equal(t, model.ReservationActive, reservation.Status)
	require.Empty(t, m.provider.Deletes())
	require.False(t, m.tokenRevoked())
	// Provider readiness alone ends this deadline; agent readiness is a
	// separate lifecycle concern, even when no agent has registered yet.
	require.Empty(t, m.repo.machines)
	require.Equal(t, model.ReservationActive, m.check(t, time.Hour).Status)
}

func TestAlreadyActiveReservationDoesNotAcquireAnotherBootDeadline(t *testing.T) {
	m := newBootingMachine(t, shortBoot)
	m.state.Reservations[0].Status = model.ReservationActive
	// A provider can briefly report pending during a later reboot. Keep
	// the already-running booking and its registration token in that case.
	reservation := m.check(t, time.Hour)
	require.Equal(t, model.ReservationActive, reservation.Status)
	require.Empty(t, m.provider.Deletes())
	require.False(t, m.tokenRevoked())
	require.Equal(t, model.ReservationActive, m.check(t, time.Hour).Status)
}

func TestUnknownProviderStatusDoesNotProveBootTimedOut(t *testing.T) {
	m := newBootingMachine(t, shortBoot)
	m.provider.SetStatus(m.instance, providers.StatusUnknown, "maintenance status unavailable")
	require.Equal(t, model.ReservationPending, m.check(t, time.Hour).Status)
	require.Empty(t, m.provider.Deletes())
	require.False(t, m.tokenRevoked())
}

func TestUnavailableProviderDoesNotProveBootTimedOut(t *testing.T) {
	m := newBootingMachine(t, shortBoot)
	m.provider.getErr = errors.New("provider status unavailable")
	reservation := m.check(t, time.Hour)
	require.Equal(t, model.ReservationPending, reservation.Status)
	require.Contains(t, reservation.LastError, m.provider.getErr.Error())
	require.Empty(t, m.provider.Deletes())
	require.False(t, m.tokenRevoked())
}
