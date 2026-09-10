package managedendpoint

import (
	"errors"
	"math/big"
	"slices"

	"github.com/beam-cloud/beta9/pkg/types"
)

// priceUsage snapshots the actual charge, including its breakdown. OpenAI
// prompt_tokens includes cached input; billing must never charge it twice.
// Round the request total once to micro-USD, then distribute the remaining
// micro-dollars to the largest fractional components (stable ties). This keeps
// the existing total price and makes every displayed component reconcile.
func priceUsage(p types.Pricing, u Usage) (types.Usage, error) {
	if u.PromptTokens < 0 || u.CompletionTokens < 0 || u.CachedTokens < 0 || u.CachedTokens > u.PromptTokens || u.Images < 0 || u.Requests < 0 {
		return types.Usage{}, errors.New("invalid token usage")
	}
	result := types.Usage{Requests: u.Requests, PromptTokens: u.PromptTokens,
		CompletionTokens: u.CompletionTokens, CachedTokens: u.CachedTokens, Images: u.Images}
	cachePrice := p.CachedPromptTokens
	if cachePrice == "" {
		cachePrice = p.PromptTokens
	}
	type costLine struct {
		price    string
		quantity int64
		cost     *int64
		fraction *big.Rat
	}
	lines := []costLine{
		{p.PromptTokens, u.PromptTokens - u.CachedTokens, &result.PromptMicroUSD, nil},
		{p.CompletionTokens, u.CompletionTokens, &result.CompletionMicroUSD, nil},
		{cachePrice, u.CachedTokens, &result.CachedMicroUSD, nil},
		{p.Request, u.Requests, &result.RequestMicroUSD, nil},
		{p.Image, u.Images, &result.ImageMicroUSD, nil},
	}
	total := new(big.Rat)
	floored := int64(0)
	for i := range lines {
		line := &lines[i]
		amount := new(big.Rat)
		if line.price != "" && line.quantity > 0 {
			rate, err := types.PricingRat(line.price)
			if err != nil {
				return types.Usage{}, err
			}
			amount.Mul(rate, big.NewRat(line.quantity, 1))
			amount.Mul(amount, big.NewRat(1_000_000, 1))
		}
		total.Add(total, amount)
		whole := new(big.Int).Quo(amount.Num(), amount.Denom())
		if !whole.IsInt64() {
			return types.Usage{}, errors.New("token cost overflows micro-USD")
		}
		*line.cost = whole.Int64()
		line.fraction = new(big.Rat).Sub(amount, new(big.Rat).SetInt(whole))
		// Check the total before accumulating in an int64 below.
	}
	total.Add(total, big.NewRat(1, 2))
	rounded := new(big.Int).Quo(total.Num(), total.Denom())
	if !rounded.IsInt64() {
		return types.Usage{}, errors.New("token cost overflows micro-USD")
	}
	result.MicroUSD = rounded.Int64()
	for _, line := range lines {
		floored += *line.cost
	}
	slices.SortStableFunc(lines, func(a, b costLine) int { return b.fraction.Cmp(a.fraction) })
	for i := int64(0); i < result.MicroUSD-floored; i++ {
		*lines[i].cost++
	}
	return result, nil
}
