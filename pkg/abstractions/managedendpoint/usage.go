package managedendpoint

import (
	"cmp"
	"encoding/json"
	"errors"
	"math/big"
	"slices"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// Usage is the billable usage the engine reported. It is never estimated: a
// response without usage is not billed.
type Usage struct {
	PromptTokens     int64
	CompletionTokens int64
	CachedTokens     int64 // part of PromptTokens, billed at the cache rate
	Images           int64
	Requests         int64
	Found            bool // the response carried a usage object
}

// valid rejects negative counters, counters beyond exact int64 arithmetic,
// and more cached tokens than prompt tokens.
func (u Usage) valid() bool {
	for _, n := range []int64{u.PromptTokens, u.CompletionTokens, u.CachedTokens, u.Images, u.Requests} {
		if n < 0 || n > types.MaxUsageCounter {
			return false
		}
	}
	return u.CachedTokens <= u.PromptTokens
}

// tokenUsage reads the OpenAI usage object from a response body.
func tokenUsage(body []byte) Usage {
	var env struct {
		Usage *struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
			Details          *struct {
				CachedTokens int64 `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(body, &env); err != nil || env.Usage == nil {
		return Usage{}
	}
	u := Usage{PromptTokens: env.Usage.PromptTokens, CompletionTokens: env.Usage.CompletionTokens, Requests: 1, Found: true}
	if env.Usage.Details != nil {
		u.CachedTokens = env.Usage.Details.CachedTokens
	}
	if !u.valid() {
		return Usage{}
	}
	return u
}

// imageUsage counts generated images plus any token usage the engine reports.
func imageUsage(body []byte) Usage {
	var payload struct {
		Data []json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return Usage{}
	}
	u := tokenUsage(body)
	u.Images, u.Requests = int64(len(payload.Data)), 1
	u.Found = u.Found || u.Images > 0
	return u
}

func billable(endpoint *types.ManagedEndpoint) bool {
	return !endpoint.Spec.Pricing.IsZero()
}

// unbilled is a successful billable response the engine reported no usage for.
func unbilled(endpoint *types.ManagedEndpoint, status int, usage Usage) bool {
	return status < 300 && billable(endpoint) && !usage.Found
}

// costUSD renders micro-dollars as the float OpenRouter puts in usage.cost.
func costUSD(microUSD int64) float64 { return float64(microUSD) / 1_000_000 }

func (r *router) cost(endpoint *types.ManagedEndpoint, usage Usage) int64 {
	if !billable(endpoint) || !usage.Found {
		return 0
	}
	priced, err := priceUsage(endpoint.Spec.Pricing, usage)
	if err != nil {
		log.Warn().Err(err).Str("endpoint_id", endpoint.Spec.ID).Msg("managed endpoints: pricing error")
	}
	return priced.MicroUSD
}

// pricingEntry is the OpenRouter-style price listing of /v1/models.
func pricingEntry(p types.Pricing) map[string]any {
	entry := map[string]any{
		"prompt":     cmp.Or(p.PromptTokens, "0"),
		"completion": cmp.Or(p.CompletionTokens, "0"),
		"request":    cmp.Or(p.Request, "0"),
		"image":      cmp.Or(p.Image, "0"),
	}
	if p.CachedPromptTokens != "" {
		entry["input_cache_read"] = p.CachedPromptTokens
	}
	return entry
}

// costLine is one priced component of a request.
type costLine struct {
	price    string
	quantity int64
	cost     *int64
	fraction *big.Rat
}

// priceUsage snapshots the actual charge with its breakdown. Cached input is
// billed once, at the cache rate. The request total is rounded once to
// micro-USD; the remaining micro-dollars go to the components with the
// largest fractions, so every displayed component reconciles with the total.
func priceUsage(p types.Pricing, u Usage) (types.Usage, error) {
	if !u.valid() {
		return types.Usage{}, errors.New("invalid token usage")
	}
	result := types.Usage{Requests: u.Requests, PromptTokens: u.PromptTokens,
		CompletionTokens: u.CompletionTokens, CachedTokens: u.CachedTokens, Images: u.Images}
	lines := []costLine{
		{p.PromptTokens, u.PromptTokens - u.CachedTokens, &result.PromptMicroUSD, nil},
		{p.CompletionTokens, u.CompletionTokens, &result.CompletionMicroUSD, nil},
		{cmp.Or(p.CachedPromptTokens, p.PromptTokens), u.CachedTokens, &result.CachedMicroUSD, nil},
		{p.Request, u.Requests, &result.RequestMicroUSD, nil},
		{p.Image, u.Images, &result.ImageMicroUSD, nil},
	}
	total := new(big.Rat)
	for i := range lines {
		amount, err := lines[i].amount()
		if err != nil {
			return types.Usage{}, err
		}
		total.Add(total, amount)
	}
	rounded, ok := roundMicroUSD(total)
	if !ok {
		return types.Usage{}, errors.New("token cost overflows micro-USD")
	}
	if rounded > types.MaxUsageCounter {
		return types.Usage{}, errors.New("token cost exceeds exact counter limit")
	}
	result.MicroUSD = rounded
	distributeRemainder(lines, rounded)
	return result, nil
}

// amount prices the line in micro-USD, storing the whole part on the line
// and keeping the fraction for distributeRemainder.
func (l *costLine) amount() (*big.Rat, error) {
	amount := new(big.Rat)
	if l.price != "" && l.quantity > 0 {
		rate, err := types.PricingRat(l.price)
		if err != nil {
			return nil, err
		}
		amount.Mul(rate, big.NewRat(l.quantity, 1))
		amount.Mul(amount, big.NewRat(1_000_000, 1))
	}
	whole := new(big.Int).Quo(amount.Num(), amount.Denom())
	if !whole.IsInt64() {
		return nil, errors.New("token cost overflows micro-USD")
	}
	*l.cost = whole.Int64()
	l.fraction = new(big.Rat).Sub(amount, new(big.Rat).SetInt(whole))
	return amount, nil
}

func roundMicroUSD(total *big.Rat) (int64, bool) {
	half := new(big.Rat).Add(total, big.NewRat(1, 2))
	rounded := new(big.Int).Quo(half.Num(), half.Denom())
	if !rounded.IsInt64() {
		return 0, false
	}
	return rounded.Int64(), true
}

// distributeRemainder hands the micro-dollars lost to flooring to the lines
// with the largest fractions, stable on ties.
func distributeRemainder(lines []costLine, total int64) {
	var floored int64
	for _, line := range lines {
		floored += *line.cost
	}
	slices.SortStableFunc(lines, func(a, b costLine) int { return b.fraction.Cmp(a.fraction) })
	for i := int64(0); i < total-floored; i++ {
		*lines[i].cost++
	}
}
