package compute

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAReservationIsOpenUntilItIsGivenBackAndClosedOnceItsMachineIsGone(t *testing.T) {
	for _, tt := range []struct {
		status       ReservationStatus
		open, closed bool
	}{
		{ReservationPending, true, false},
		{ReservationActive, true, false},
		{ReservationTerminating, false, false},
		{ReservationDeleted, false, true},
		{ReservationFailed, false, true},
	} {
		reservation := Reservation{Status: tt.status}
		require.Equal(t, tt.open, reservation.Open(), "%s open", tt.status)
		require.Equal(t, tt.closed, reservation.Closed(), "%s closed", tt.status)
	}
}

func TestAReservationIsActiveWhileItIsOpenAndHasNotRunOut(t *testing.T) {
	now := time.Now()
	for _, status := range []ReservationStatus{ReservationPending, ReservationActive} {
		require.True(t, Reservation{Status: status}.ActiveAt(now), "%s without an end", status)
		require.True(t, Reservation{Status: status, ExpiresAt: now.Add(time.Minute)}.ActiveAt(now), "%s before its end", status)
		require.False(t, Reservation{Status: status, ExpiresAt: now}.ActiveAt(now), "%s at its end", status)
		require.False(t, Reservation{Status: status, ExpiresAt: now.Add(-time.Minute)}.ActiveAt(now), "%s past its end", status)
	}
	for _, status := range []ReservationStatus{ReservationTerminating, ReservationDeleted, ReservationFailed} {
		require.False(t, Reservation{Status: status}.ActiveAt(now), "%s", status)
		require.False(t, Reservation{Status: status, ExpiresAt: now.Add(time.Minute)}.ActiveAt(now), "%s", status)
	}
}
