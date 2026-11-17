package compute

import (
	"math"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
)

const microRoundingEpsilon = 1e-6

func (s *Service) billableMicros(providerMicros int64) int64 {
	return BillableMicros(s.appConfig.ManagedCompute, providerMicros)
}

// BillableMicros is what a tenant is billed an hour, under the config, for
// what costs its provider's price.
func BillableMicros(config types.ManagedComputeConfig, providerMicros int64) int64 {
	return billableMicros(providerMicros, config.BillableMarginPctOrDefault())
}

func (s *Service) providerBudgetMicros(billableMaxMicros int64) int64 {
	return providerBudgetMicros(billableMaxMicros, s.appConfig.ManagedCompute.BillableMarginPctOrDefault())
}

func (s *Service) billableOffer(offer providers.Offer) providers.Offer {
	offer.HourlyCostMicros = s.billableMicros(offer.HourlyCostMicros)
	return offer
}

// spotPriceLimitMicros is the most a spot machine bought from this offer may
// come to cost an hour before it is given up. It is nothing for a machine
// that is not spot, whose price does not move.
//
// The limit is what the tenant is billed an hour, not what the offer costs:
// a spot price moves, and a limit at the price the machine was bought for
// loses the machine to the first rise. Up to what the tenant pays, the
// machine is kept, and it never costs more than it earns.
//
// A provider that takes a price limit is given this one with the purchase,
// and takes the machine back itself. One that takes none is asked its price
// while the machine runs: see spotPriceOver.
func (s *Service) spotPriceLimitMicros(offer providers.Offer) int64 {
	if !offer.Spot {
		return 0
	}
	return s.billableMicros(offer.HourlyCostMicros)
}

func billableMicros(providerMicros int64, marginPct float64) int64 {
	if providerMicros <= 0 {
		return 0
	}
	return int64(math.Ceil(float64(providerMicros)*costMultiplier(marginPct) - microRoundingEpsilon))
}

func providerBudgetMicros(billableMaxMicros int64, marginPct float64) int64 {
	if billableMaxMicros <= 0 {
		return 0
	}
	budget := int64(math.Floor(float64(billableMaxMicros) / costMultiplier(marginPct)))
	if budget < 1 {
		return 1
	}
	return budget
}

func costMultiplier(marginPct float64) float64 {
	if marginPct < 0 {
		return 1
	}
	return 1 + marginPct
}

func managedBYOCNodeHourlyCostMicros(cpuMillicores, memoryMB int64, config types.ManagedComputeConfig) int64 {
	pricing := config.BYOC.Pricing
	return ceilDivInt64(cpuMillicores*pricing.CPUHourlyMicrosPerVCPUOrDefault(), 1000) +
		ceilDivInt64(memoryMB*pricing.MemoryHourlyMicrosPerGBOrDefault(), 1024)
}

func ceilDivInt64(numerator, denominator int64) int64 {
	if numerator <= 0 || denominator <= 0 {
		return 0
	}
	return (numerator + denominator - 1) / denominator
}
