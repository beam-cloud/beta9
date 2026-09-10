package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExplicitZeroPricingIsFree(t *testing.T) {
	for _, pricing := range []Pricing{{}, {PromptTokens: "0", CompletionTokens: "0.000"}, {Image: "0", Request: "0.0"}} {
		require.NoError(t, pricing.Validate())
		require.True(t, pricing.IsZero())
	}
	for _, pricing := range []Pricing{{PromptTokens: "0.000001"}, {CachedPromptTokens: "0.1"}, {Image: "1"}, {Request: "0.01"}} {
		require.False(t, pricing.IsZero())
	}
}

func TestManagedEndpointBranchDefaults(t *testing.T) {
	for _, tc := range []struct{ input, expected string }{{"", "main"}, {"staging", "staging"}, {" main ", "main"}} {
		config := ManagedEndpointsConfig{Repo: ManagedEndpointsRepoConfig{Branch: tc.input}}
		config.ApplyDefaults()
		require.Equal(t, tc.expected, config.Repo.Branch)
	}
}
