package managedendpoint

import (
	"math/big"

	"github.com/beam-cloud/beta9/pkg/types"
)

// Usage is the authoritative billable usage extracted from an upstream
// response. Never estimated: when the engine reports nothing, the request is
// not billed and is flagged as missing usage.
type Usage struct {
	PromptTokens     int64
	CompletionTokens int64
	CachedTokens     int64
	Images           int64
	Requests         int64
	// Found reports whether the response carried a usage object at all.
	Found bool
}

var microUSD = big.NewRat(1_000_000, 1)

// computeCostMicroUSD prices usage with exact rational arithmetic and rounds
// half-up to micro-dollars. Cached prompt tokens are billed at the cached
// rate when one is set and at the prompt rate otherwise; they are a subset of
// PromptTokens.
func computeCostMicroUSD(p types.Pricing, u Usage) (int64, error) {
	if p.IsZero() {
		return 0, nil
	}
	total := new(big.Rat)
	add := func(price string, quantity int64) error {
		if quantity <= 0 || price == "" {
			return nil
		}
		rate, err := types.PricingRat(price)
		if err != nil {
			return err
		}
		total.Add(total, new(big.Rat).Mul(rate, big.NewRat(quantity, 1)))
		return nil
	}

	cached := u.CachedTokens
	if cached > u.PromptTokens {
		cached = u.PromptTokens
	}
	uncached := u.PromptTokens - cached
	if p.CachedPromptTokens == "" {
		uncached = u.PromptTokens
		cached = 0
	}
	if err := add(p.PromptTokens, uncached); err != nil {
		return 0, err
	}
	if err := add(p.CachedPromptTokens, cached); err != nil {
		return 0, err
	}
	if err := add(p.CompletionTokens, u.CompletionTokens); err != nil {
		return 0, err
	}
	if err := add(p.Image, u.Images); err != nil {
		return 0, err
	}
	if err := add(p.Request, u.Requests); err != nil {
		return 0, err
	}

	total.Mul(total, microUSD)
	// Round half-up.
	total.Add(total, big.NewRat(1, 2))
	return new(big.Int).Quo(total.Num(), total.Denom()).Int64(), nil
}

// costUSD renders micro-dollars as the float OpenRouter puts in usage.cost.
func costUSD(microUSD int64) float64 {
	return float64(microUSD) / 1_000_000
}

// pricingString renders a per-unit price for listings ("0" when unset).
func pricingString(value string) string {
	if value == "" {
		return "0"
	}
	return value
}
