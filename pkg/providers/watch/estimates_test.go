package watch

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestEstimatesSayWhatToExpectOfAMachine(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	spot := Outcome{At: now, Provider: "verda", Region: "FIN-01", Offer: "1L40S.20V", Spot: true}
	elsewhere, onDemand := spot, spot
	elsewhere.Region, onDemand.Spot = "FIN-02", false
	estimates := Estimates{}
	expect := func(o Outcome, at time.Time, boot time.Duration, odds, taken float64) {
		t.Helper()
		gotBoot, gotOdds, gotTaken := estimates.Of(o, at)
		require.Equal(t, boot, gotBoot)
		require.InDelta(t, odds, gotOdds, 1e-9)
		require.InDelta(t, taken, gotTaken, 1e-9)
	}

	// Before anything is known, a machine comes up, and a spot machine is
	// lost once in twenty hours.
	expect(spot, now, 0, 1, 1.0/20)
	expect(onDemand, now, 0, 1, 0)

	// One came up in two minutes and was taken back after five hours. One
	// was given four minutes, and never came up.
	estimates.Learn(spot.CameUp(2 * time.Minute))
	estimates.Learn(spot.Ended(ResultInterrupted, 5*time.Hour))
	estimates.Learn(spot.Ended(ResultNeverUp, 4*time.Minute))
	estimates.Learn(spot.Ended(ResultReleased, 5*time.Hour))
	expect(spot, now, 150*time.Second, 2.0/3, 2.0/30)

	// Its provider's other spot offers take as long and are lost as often,
	// and have odds of their own. What it sells on demand is another matter.
	expect(elsewhere, now, 150*time.Second, 1, 2.0/30)
	expect(onDemand, now, 0, 1, 0)

	// A week on, all of it counts half. An outcome that is written late
	// does not count the time twice.
	later := spot
	later.At = now.Add(7 * 24 * time.Hour)
	expect(spot, later.At, 150*time.Second, 1.5/2, 1.5/25)
	estimates.Learn(later.CameUp(time.Minute))
	estimates.Learn(spot.Ended(ResultReleased, 0))
	expect(spot, later.At, 90*time.Second, 2.5/3, 1.5/25)
}
