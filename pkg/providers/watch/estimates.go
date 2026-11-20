package watch

import (
	"cmp"
	"fmt"
	"maps"
	"math"
	"time"
)

const (
	// estimatesHalve is how long an outcome counts in full: one this old
	// counts half, one twice as old a quarter.
	estimatesHalve = 7 * 24 * time.Hour
	// priorHours is how many hours of machines kept without loss an unknown
	// provider is taken to have behind it. A spot machine is taken to have
	// been lost once in them.
	priorHours = 20
	// offerHours is how many hours of machines an offer is taken to have
	// behind it that were lost as often as all its provider sells that way.
	// They are few. A provider keeps its machines for days in one region
	// and takes them back within minutes in another, so what became of an
	// offer's own machines soon says more than what became of the others.
	offerHours = 2
)

// Estimate is what the outcomes say to expect of machines bought one way.
type Estimate struct {
	// Boot is how long a machine takes to come up.
	Boot time.Duration `json:"boot,omitempty"`
	// Up counts the machines that came up, and Lost those that never did.
	Up   float64 `json:"up,omitempty"`
	Lost float64 `json:"lost,omitempty"`
	// Hours is how long machines were had, and Taken how many of them their
	// provider took back, failed or lost.
	Hours float64 `json:"hours,omitempty"`
	Taken float64 `json:"taken,omitempty"`
	// At is the time the counts are as of. They fade from then on.
	At time.Time `json:"at"`
}

// Estimates are what has been learned from outcomes, for each offer and for
// everything each provider sells as spot or on demand.
type Estimates map[string]Estimate

// estimateKeys names an outcome's offer, and all its provider sells that way.
func estimateKeys(o Outcome) [2]string {
	all := fmt.Sprintf("%s/%s/%t", o.Provider, o.Cloud, o.Spot)
	return [2]string{fmt.Sprintf("%s/%s/%s/%s", all, o.Region, o.Zone, o.Offer), all}
}

// at is the estimate with its counts faded to now.
func (e Estimate) at(now time.Time) Estimate {
	if age := now.Sub(e.At); age > 0 {
		keep := math.Pow(0.5, float64(age)/float64(estimatesHalve))
		e.Up, e.Lost, e.Hours, e.Taken = e.Up*keep, e.Lost*keep, e.Hours*keep, e.Taken*keep
		e.At = now
	}
	return e
}

// Learn takes one outcome into the estimates.
func (e Estimates) Learn(o Outcome) {
	for _, key := range estimateKeys(o) {
		estimate := e[key].at(o.At)
		switch {
		case o.Event == OutcomeActive:
			estimate.Up++
			// The mean of the first four machines, and after that each
			// one moves it a quarter of the way to its own time.
			estimate.Boot += time.Duration(float64(o.Took-estimate.Boot) / min(estimate.Up, 4))
		case o.Event != OutcomeEnd:
		case o.Result == ResultNeverUp:
			estimate.Lost++
			// Perhaps it was not given long enough.
			if estimate.Boot > 0 {
				estimate.Boot += (o.Took - estimate.Boot) / 4
			}
		default:
			estimate.Hours += o.Took.Hours()
			if o.Result != ResultReleased && o.Result != ResultExpired {
				estimate.Taken++
			}
		}
		e[key] = estimate
	}
}

// Having is the estimates with the hours that machines which have not ended
// have been had so far: each of running says what its machine was bought as,
// and in Took how long it has been had. A machine that is kept says so when
// it ends, and the best are kept for days. Until then the offer that keeps
// them would be taken for one that nobody has bought.
func (e Estimates) Having(running ...Outcome) Estimates {
	if len(running) == 0 {
		return e
	}
	having := make(Estimates, len(e)+2*len(running))
	maps.Copy(having, e)
	for _, o := range running {
		for _, key := range estimateKeys(o) {
			estimate := having[key].at(o.At)
			estimate.Hours += o.Took.Hours()
			having[key] = estimate
		}
	}
	return having
}

// Of is what to expect of a machine bought as o says: how long it takes to
// come up, or zero when that is not known, the odds that it does, and how
// often an hour it is lost.
//
// How often is what became of the offer's own machines, once it has had
// them for a while. Until then it is lost as often as all its provider
// sells that way, for the hours it is short of offerHours.
func (e Estimates) Of(o Outcome, now time.Time) (boot time.Duration, odds, taken float64) {
	keys := estimateKeys(o)
	own, all := e[keys[0]].at(now), e[keys[1]].at(now)
	prior := 0.0
	if o.Spot {
		prior = 1
	}
	taken = (all.Taken + prior) / (all.Hours + priorHours)
	taken = (own.Taken + taken*offerHours) / (own.Hours + offerHours)
	return cmp.Or(own.Boot, all.Boot), (own.Up + 1) / (own.Up + own.Lost + 1), taken
}
