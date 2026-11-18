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

func initialProviderBoot(t *testing.T, timeout time.Duration) *boughtMachine {
	t.Helper()
	m := boughtMachineAt(time.Date(2026, 11, 18, 16, 0, 0, 0, time.UTC), providers.StatusPending, 6*time.Hour)
	m.state.Reservations[0].CreatedAt = m.now
	m.service.appConfig.ManagedCompute.Reservations.BootTimeout = types.Duration(timeout)
	return m
}

func TestReservationProviderBootTimeoutDefaultsAndOverride(t *testing.T) {
	for _, tc := range []struct {
		configured time.Duration
		want       time.Duration
	}{{0, 20 * time.Minute}, {-time.Minute, 20 * time.Minute}, {3 * time.Minute, 3 * time.Minute}} {
		config := types.ManagedComputeReservationsConfig{BootTimeout: types.Duration(tc.configured)}
		require.Equal(t, tc.want, config.BootTimeoutOrDefault())
	}
}

func TestPendingReservationClosesAtDefaultProviderBootDeadline(t *testing.T) {
	m := initialProviderBoot(t, 0)
	reservation := m.check(t, 19*time.Minute)
	require.Equal(t, model.ReservationPending, reservation.Status)
	require.Empty(t, m.provider.Deletes())
	require.False(t, m.tokenRevoked())

	reservation = m.check(t, time.Minute)
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Equal(t, reconcileReasonLaunchFailed, reservation.TerminatingReason)
	require.Contains(t, reservation.LastStatusMessage, "20m0s")
	require.Equal(t, []string{m.instance}, m.provider.Deletes())
	require.True(t, m.tokenRevoked(), "a queued bid cannot join after its deadline")
	require.Empty(t, m.provider.Everything())
}

func TestPendingReservationUsesConfiguredBootDeadline(t *testing.T) {
	m := initialProviderBoot(t, 3*time.Minute)
	require.Equal(t, model.ReservationPending, m.check(t, 2*time.Minute).Status)
	require.False(t, m.tokenRevoked())
	reservation := m.check(t, time.Minute)
	require.Equal(t, model.ReservationDeleted, reservation.Status)
	require.Equal(t, reconcileReasonLaunchFailed, reservation.TerminatingReason)
	require.Len(t, m.provider.Deletes(), 1)
}

func TestProviderBootTimeoutRetainsTerminationUntilDeleteSucceeds(t *testing.T) {
	m := initialProviderBoot(t, 3*time.Minute)
	m.provider.FailDeletes(errors.New("provider unavailable"))
	reservation := m.check(t, 3*time.Minute)
	require.Equal(t, model.ReservationTerminating, reservation.Status)
	require.Equal(t, reconcileReasonLaunchFailed, reservation.TerminatingReason)
	require.Equal(t, "provider unavailable", reservation.LastError)
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
	m := initialProviderBoot(t, 3*time.Minute)
	m.provider.SetStatus(m.instance, providers.StatusActive, "")
	reservation := m.check(t, 3*time.Minute)
	require.Equal(t, model.ReservationActive, reservation.Status)
	require.Empty(t, m.provider.Deletes())
	require.False(t, m.tokenRevoked())
	// Provider readiness alone ends this deadline; agent readiness is a
	// separate lifecycle concern, even when no agent has registered yet.
	require.Empty(t, m.repo.machines)
	require.Equal(t, model.ReservationActive, m.check(t, time.Hour).Status)
}

func TestAlreadyActiveReservationDoesNotAcquireAnotherBootDeadline(t *testing.T) {
	m := initialProviderBoot(t, 3*time.Minute)
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
	m := initialProviderBoot(t, 3*time.Minute)
	m.provider.SetStatus(m.instance, providers.StatusUnknown, "maintenance status unavailable")
	require.Equal(t, model.ReservationPending, m.check(t, time.Hour).Status)
	require.Empty(t, m.provider.Deletes())
	require.False(t, m.tokenRevoked())
}

func TestUnavailableProviderDoesNotProveBootTimedOut(t *testing.T) {
	m := initialProviderBoot(t, 3*time.Minute)
	m.provider.getErr = errors.New("provider status unavailable")
	reservation := m.check(t, time.Hour)
	require.Equal(t, model.ReservationPending, reservation.Status)
	require.Contains(t, reservation.LastError, "provider status unavailable")
	require.Empty(t, m.provider.Deletes())
	require.False(t, m.tokenRevoked())
}
