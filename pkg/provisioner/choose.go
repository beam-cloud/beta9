package provisioner

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
)

// Limits cap what all pools together spend on machines. Zero means no cap.
type Limits struct {
	// MaxHourlyCents is the most the machines held may cost per hour.
	MaxHourlyCents int64
	// MaxDailyCents is the most that may be spent in any 24 hours.
	MaxDailyCents int64
}

// dailySpent reports whether what was spent in the last 24 hours, in cents,
// has reached MaxDailyCents.
func (l Limits) dailySpent(spentToday float64) bool {
	return l.MaxDailyCents > 0 && spentToday >= float64(l.MaxDailyCents)
}

// purchase is everything that decides whether a pool may buy a machine, and
// which.
type purchase struct {
	pool   string
	config types.WorkerPoolConfig
	need   scheduler.MachineNeed
	// offers is what the pool's provider has in stock.
	offers []providers.Offer
	// held is every machine there is a record of, for every pool.
	held   []Machine
	limits Limits
	// spentToday is what machines have cost in the last 24 hours, in cents.
	spentToday float64
}

// Why a pool cannot buy a machine. When it is for want of an offer, noOffer
// says what stands in the way.
var (
	errAtMaxNodes = errors.New("the pool holds as many machines as provisioning.maxNodes allows")
	errHourlyCap  = errors.New("one more machine would take the hourly cost of all machines over providers.maxHourlyCents")
	errDailyCap   = errors.New("providers.maxDailyCents has been spent in the last 24 hours")
)

// microsPerCent converts the cents limits are written in to the millionths
// of a dollar offers are priced in.
const microsPerCent = 10_000

// choose picks the machine to buy: the cheapest one in stock that the pool
// may buy and that has room for what is needed. It fails when the pool is at
// maxNodes, when a spend cap would be passed, and when nothing in stock is
// such a machine. It changes nothing.
func choose(p purchase) (providers.Offer, error) {
	machines := 0
	hourlyMicros := int64(0)
	for _, machine := range p.held {
		// A machine counts until its record is gone: until then it may still
		// exist, and cost money.
		hourlyMicros += machine.HourlyMicros
		if machine.Pool == p.pool {
			machines++
		}
	}
	if machines >= p.config.Provisioning.MaxNodes {
		return providers.Offer{}, errAtMaxNodes
	}
	if p.limits.dailySpent(p.spentToday) {
		return providers.Offer{}, errDailyCap
	}

	var fitting []providers.Offer
	for _, offer := range p.offers {
		if sells(offer, p.config) && holds(offer, p.need) {
			fitting = append(fitting, offer)
		}
	}
	if len(fitting) == 0 {
		return providers.Offer{}, noOffer(p)
	}
	offer := cheapest(fitting)

	if p.limits.MaxHourlyCents > 0 && hourlyMicros+offer.HourlyCostMicros > p.limits.MaxHourlyCents*microsPerCent {
		return providers.Offer{}, errHourlyCap
	}
	return offer, nil
}

// cheapest returns the offer that costs least. Of several at one price, it
// is always the same one.
func cheapest(offers []providers.Offer) providers.Offer {
	return slices.MinFunc(offers, func(a, b providers.Offer) int {
		return cmp.Or(
			cmp.Compare(a.HourlyCostMicros, b.HourlyCostMicros),
			cmp.Compare(a.Region, b.Region),
			cmp.Compare(a.ID, b.ID),
			cmp.Compare(a.Cloud, b.Cloud),
		)
	})
}

// obstacle is what stands between a pool and a machine to buy. The detail is
// how it stands this time: the room that was needed, a price, the regions on
// offer. It differs from one request and one listing to the next, and what
// stands in the way does not.
type obstacle struct {
	what   string
	detail string
}

func (o obstacle) Error() string { return o.what + o.detail }

// noOffer says why nothing in stock is a machine to buy. It lets go of what
// is asked for one thing at a time, from the room that is needed back to
// spot or on-demand, until the provider has something: the last thing let go
// of is what stands in the way.
func noOffer(p purchase) error {
	buying := p.config.Provisioning
	// What the pool buys, as one of them and as many.
	one, many := strings.TrimSpace(p.config.GPUType), strings.TrimSpace(p.config.GPUType)
	if one == "" {
		one, many = "CPU machine", "CPU machines"
	}

	asked := p.config
	if len(selling(p.offers, asked)) > 0 {
		return obstacle{
			what:   fmt.Sprintf("no %s that %s has for the pool has room for what is needed", one, buying.Provider),
			detail: fmt.Sprintf(" (cpu %dm, memory %d MiB, gpus %d)", p.need.CPU, p.need.Memory, p.need.GPUCount),
		}
	}
	asked.Provisioning.MaxNodeHourlyCents = 0
	if atAnyPrice := selling(p.offers, asked); len(atAnyPrice) > 0 {
		return obstacle{
			what:   fmt.Sprintf("%s has no %s within provisioning.maxNodeHourlyCents %d", buying.Provider, one, buying.MaxNodeHourlyCents),
			detail: fmt.Sprintf(": the cheapest costs %s cents an hour", cents(cheapest(atAnyPrice).HourlyCostMicros)),
		}
	}
	asked.Provisioning.Regions = nil
	if anywhere := selling(p.offers, asked); len(anywhere) > 0 {
		return obstacle{
			what:   fmt.Sprintf("%s offers no %s in regions %v", buying.Provider, many, buying.Regions),
			detail: "; it offers them in " + strings.Join(regionsOf(anywhere), ", "),
		}
	}
	asked.Provisioning.Spot = !buying.Spot
	switch theOtherWay := selling(p.offers, asked); {
	case len(theOtherWay) > 0 && buying.Spot:
		return fmt.Errorf("%s sells %s only on demand: remove provisioning.spot", buying.Provider, many)
	case len(theOtherWay) > 0:
		return fmt.Errorf("%s sells %s only as spot: set provisioning.spot", buying.Provider, many)
	case len(p.offers) > 0:
		return fmt.Errorf("%s has no %s in stock", buying.Provider, one)
	}
	return fmt.Errorf("%s is not known to have anything in stock", buying.Provider)
}

// selling returns what of the stock a pool that asks for this buys.
func selling(stock []providers.Offer, asked types.WorkerPoolConfig) []providers.Offer {
	var sold []providers.Offer
	for _, offer := range stock {
		if sells(offer, asked) {
			sold = append(sold, offer)
		}
	}
	return sold
}

// regionsOf lists the regions the offers are in, in a fixed order.
func regionsOf(offers []providers.Offer) []string {
	var regions []string
	for _, offer := range offers {
		if !slices.Contains(regions, offer.Region) {
			regions = append(regions, offer.Region)
		}
	}
	slices.Sort(regions)
	return regions
}

// cents writes a price in cents, with as many decimals as it has.
func cents(micros int64) string {
	return strconv.FormatFloat(float64(micros)/microsPerCent, 'f', -1, 64)
}

// sells reports whether the offer is something the pool buys: its GPU type,
// spot or on-demand as configured, in its regions, within its price limit.
func sells(offer providers.Offer, pool types.WorkerPoolConfig) bool {
	buying := pool.Provisioning
	switch {
	case offer.Spot != buying.Spot:
		return false
	case !strings.EqualFold(offer.GPU, strings.TrimSpace(pool.GPUType)):
		return false
	case (offer.GPUCount > 0) != (offer.GPU != ""):
		return false
	case len(buying.Regions) > 0 && !slices.Contains(buying.Regions, offer.Region):
		return false
	case offer.HourlyCostMicros <= 0:
		// Without a price there is no telling what it does to the caps.
		return false
	case buying.MaxNodeHourlyCents > 0 && offer.HourlyCostMicros > buying.MaxNodeHourlyCents*microsPerCent:
		return false
	}
	return true
}

// holds reports whether a machine bought from the offer has room for need.
func holds(offer providers.Offer, need scheduler.MachineNeed) bool {
	return offer.CPUMillicores >= need.CPU &&
		usableMemoryMB(offer.MemoryMB) >= need.Memory &&
		offer.GPUCount >= need.GPUCount
}
