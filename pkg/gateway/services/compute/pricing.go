package compute

import (
	"math"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
)

const microRoundingEpsilon = 1e-6

func (s *Service) billableMicros(providerMicros int64) int64 {
	return billableMicros(providerMicros, s.appConfig.ManagedCompute.BillableMarginPctOrDefault())
}

func (s *Service) providerBudgetMicros(billableMaxMicros int64) int64 {
	return providerBudgetMicros(billableMaxMicros, s.appConfig.ManagedCompute.BillableMarginPctOrDefault())
}

func (s *Service) billableOffer(offer model.Offer) model.Offer {
	offer.HourlyCostMicros = s.billableMicros(offer.HourlyCostMicros)
	return offer
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
