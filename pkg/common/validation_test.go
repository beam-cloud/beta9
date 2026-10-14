package common

import (
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAllowList(t *testing.T) {
	for _, tc := range []struct {
		entry string
		valid bool
	}{
		{"8.8.8.8", true}, {"2001:4860:4860::8888", true}, {"10.0.0.1/8", true},
		{"API.OpenAI.com", true}, {"*.openai.com", true}, {"a-b.example.com", true},
		{"", false}, {"localhost", false}, {"https://example.com", false}, {"example.com:443", false},
		{"*example.com", false}, {"foo.*.com", false}, {"-foo.com", false}, {"foo..com", false},
		{"999.999.999.999", true}, {strings.Repeat("a", 64) + ".com", false},
	} {
		t.Run(tc.entry, func(t *testing.T) { require.Equal(t, tc.valid, ValidateAllowList([]string{tc.entry}) == nil) })
	}
	require.NoError(t, ValidateAllowList(slices.Repeat([]string{"api.openai.com"}, 64)))
	require.Error(t, ValidateAllowList(slices.Repeat([]string{"api.openai.com"}, 65)))
	cidrs, hosts := SplitAllowList([]string{"8.8.8.8", "10.1.1.1/8", "API.OpenAI.com"})
	require.Equal(t, []string{"8.8.8.8/32", "10.0.0.0/8"}, cidrs)
	require.Equal(t, []string{"api.openai.com"}, hosts)
}

func TestMatchDomain(t *testing.T) {
	for _, tc := range []struct {
		host, pattern string
		match         bool
	}{
		{"API.OpenAI.com.", "api.openai.com", true}, {"api.openai.com", "*.openai.com", true},
		{"nested.api.openai.com", "*.openai.com", true}, {"openai.com", "*.openai.com", false},
		{"evilopenai.com", "*.openai.com", false}, {"api.openai.com.evil.com", "*.openai.com", false},
	} {
		require.Equal(t, tc.match, MatchDomain(tc.host, tc.pattern), tc.host)
	}
}
