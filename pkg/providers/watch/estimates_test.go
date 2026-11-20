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
	// It was lost once in ten hours of its own, and for two hours more as
	// often as all its provider sells as spot: twice in thirty.
	expect(spot, now, 150*time.Second, 2.0/3, (1+2*2.0/30)/12)

	// Its provider's other spot offers take as long and, until they have
	// had machines of their own, are lost as often. Their odds are their
	// own. What it sells on demand is another matter.
	expect(elsewhere, now, 150*time.Second, 1, 2.0/30)
	expect(onDemand, now, 0, 1, 0)

	// A week on, all of it counts half. An outcome that is written late
	// does not count the time twice.
	later := spot
	later.At = now.Add(7 * 24 * time.Hour)
	expect(spot, later.At, 150*time.Second, 1.5/2, (0.5+2*1.5/25)/7)
	estimates.Learn(later.CameUp(time.Minute))
	estimates.Learn(spot.Ended(ResultReleased, 0))
	expect(spot, later.At, 90*time.Second, 2.5/3, (0.5+2*1.5/25)/7)
}

// A provider keeps its machines in one region and takes them back within
// minutes in another. Each is expected to go on as it has.
func TestEstimatesTellTheOffersOfAProviderApart(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	kept := Outcome{At: now, Provider: "nebius", Region: "eu-north1", Offer: "1gpu-h200", Spot: true}
	taken, never, untried := kept, kept, kept
	taken.Region, never.Region, untried.Region = "eu-west1", "us-central1", "me-west1"
	estimates := Estimates{}
	lostEvery := func(o Outcome) float64 {
		_, _, rate := estimates.Of(o, now)
		return 1 / rate
	}

	// One has run for two days. Four came up elsewhere and were taken back
	// within ten minutes, and four more before they came up at all.
	estimates.Learn(kept.CameUp(2 * time.Minute))
	estimates.Learn(kept.Ended(ResultReleased, 48*time.Hour))
	for range 4 {
		estimates.Learn(taken.CameUp(2 * time.Minute))
		estimates.Learn(taken.Ended(ResultInterrupted, 10*time.Minute))
		estimates.Learn(never.Ended(ResultInterrupted, 6*time.Minute))
	}

	require.Greater(t, lostEvery(kept), 100.0, "hours")
	require.Less(t, lostEvery(taken), 0.75)
	require.Less(t, lostEvery(never), 0.75)
	// One that has had no machines is lost as often as them all.
	require.InDelta(t, (48+4.0/6+0.4+20)/9, lostEvery(untried), 1e-9)

	// The more of its machines an offer loses, the less its provider's
	// other offers say of it.
	for range 12 {
		estimates.Learn(never.Ended(ResultInterrupted, 6*time.Minute))
	}
	require.Less(t, lostEvery(never), 0.25)
	require.Greater(t, lostEvery(kept), 50.0)
}

// A machine that is kept has not ended, and the best do not for days. The
// hours it has run so far count for what it was bought as all the same.
func TestMachinesThatStillRunCountForTheHoursTheyHaveRun(t *testing.T) {
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	kept := Outcome{At: now, Provider: "nebius", Region: "eu-north1", Offer: "1gpu-h200", Spot: true}
	taken := kept
	taken.Region = "eu-west1"
	estimates := Estimates{}
	for range 4 {
		estimates.Learn(taken.CameUp(2 * time.Minute))
		estimates.Learn(taken.Ended(ResultInterrupted, 15*time.Minute))
	}
	estimates.Learn(kept.CameUp(2 * time.Minute))
	rate := func(e Estimates, o Outcome) float64 {
		_, _, rate := e.Of(o, now)
		return rate
	}

	// By what has ended, the region that keeps its machine is one nobody
	// has had a machine in for any time: lost as often as the other.
	all := 5.0 / 21
	require.InDelta(t, all, rate(estimates, kept), 1e-9)

	// Its machine has run for a day, which says otherwise.
	running := kept
	running.Took = 24 * time.Hour
	having := estimates.Having(running)
	require.InDelta(t, (2*5.0/45)/26, rate(having, kept), 1e-9)
	require.Less(t, rate(having, kept)*100, rate(having, taken))
	require.InDelta(t, (4+2*5.0/45)/3, rate(having, taken), 1e-9)

	// Nothing is learned by it: the machine has yet to end.
	require.InDelta(t, all, rate(estimates, kept), 1e-9)
	require.Equal(t, estimates, estimates.Having())
	require.Equal(t, Estimates{"nebius//true": {Hours: 1, At: now}, "nebius//true/eu-north1//1gpu-h200": {Hours: 1, At: now}},
		Estimates(nil).Having(Outcome{At: now, Provider: "nebius", Region: "eu-north1", Offer: "1gpu-h200", Spot: true, Took: time.Hour}))
}
