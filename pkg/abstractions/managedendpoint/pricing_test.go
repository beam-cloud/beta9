package managedendpoint

import (
	"math"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestPriceUsageSeparatesCachedInputAtRequestTime(t *testing.T) {
	p := types.Pricing{PromptTokens: "0.0000001", CompletionTokens: "0.0000003", CachedPromptTokens: "0.000000025"}
	u := Usage{Requests: 1, PromptTokens: 1000, CachedTokens: 800, CompletionTokens: 100}
	priced, err := priceUsage(p, u)
	require.NoError(t, err)
	require.EqualValues(t, 20, priced.PromptMicroUSD)
	require.EqualValues(t, 20, priced.CachedMicroUSD)
	require.EqualValues(t, 30, priced.CompletionMicroUSD)
	require.EqualValues(t, 70, priced.MicroUSD)
	p.CachedPromptTokens = ""
	priced, err = priceUsage(p, u)
	require.NoError(t, err)
	require.EqualValues(t, 800, priced.CachedTokens)
	require.EqualValues(t, 80, priced.CachedMicroUSD, "without a discount cache still remains separately visible")
	require.EqualValues(t, 130, priced.MicroUSD)
}

func TestPriceUsageRoundingReconcilesEveryComponent(t *testing.T) {
	p := types.Pricing{PromptTokens: "0.0000001", CompletionTokens: "0.0000003", CachedPromptTokens: "0.000000025", Request: "0.0000001", Image: "0.0000009"}
	for prompt := int64(0); prompt < 50; prompt++ {
		for cached := int64(0); cached <= prompt; cached++ {
			priced, err := priceUsage(p, Usage{Requests: 1, PromptTokens: prompt, CachedTokens: cached, CompletionTokens: 7, Images: 1})
			require.NoError(t, err)
			require.Equal(t, priced.MicroUSD, priced.PromptMicroUSD+priced.CachedMicroUSD+priced.CompletionMicroUSD+priced.RequestMicroUSD+priced.ImageMicroUSD)
			// Rates in units of 1/40 micro-USD make the independent rounded
			// total an exact integer calculation, including half-way cases.
			units := (prompt-cached)*4 + cached + 7*12 + 4 + 36
			require.Equal(t, (units+20)/40, priced.MicroUSD)
		}
	}
}

func TestPriceUsageRejectsInvalidCountersAndOverflow(t *testing.T) {
	for _, usage := range []Usage{{PromptTokens: -1}, {CompletionTokens: -1}, {PromptTokens: 1, CachedTokens: 2}, {CachedTokens: -1}} {
		_, err := priceUsage(types.Pricing{}, usage)
		require.Error(t, err)
	}
	_, err := priceUsage(types.Pricing{PromptTokens: "1"}, Usage{PromptTokens: math.MaxInt64})
	require.Error(t, err)
	for _, body := range []string{
		`{"usage":{"prompt_tokens":-1}}`,
		`{"usage":{"prompt_tokens":1,"prompt_tokens_details":{"cached_tokens":2}}}`,
		`{"usage":{"completion_tokens":-3}}`,
	} {
		require.False(t, tokenUsage([]byte(body)).Found)
	}
}
