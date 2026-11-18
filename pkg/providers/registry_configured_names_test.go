package providers

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConfiguredNamesIsNilSafeSortedAndCallerOwned(t *testing.T) {
	var missing *Registry
	require.Nil(t, missing.ConfiguredNames())
	require.Empty(t, RegistryOf().ConfiguredNames())
	registry := RegistryOf(NewFake("z", testOwner), NewFake("a", testOwner), NewFake("m", testOwner))
	names := registry.ConfiguredNames()
	require.Equal(t, []string{"a", "m", "z"}, names)
	names[0] = "changed"
	require.Equal(t, []string{"a", "m", "z"}, registry.ConfiguredNames())
}

func TestConfiguredNamesIncludesDisabledWithoutEnablingDiscovery(t *testing.T) {
	registry := NewRegistry(types.ProviderConfig{
		Disabled:  []string{"VAST"},
		Vast:      types.VastProviderConfig{ApiKey: "synthetic-key"},
		Shadeform: types.ShadeformProviderConfig{ApiKey: "synthetic-key"},
		Hetzner:   types.HetznerProviderConfig{ApiToken: "synthetic-key"},
	}, testOwner)
	require.Equal(t, []string{"hetzner", "shadeform", "vast"}, registry.ConfiguredNames())
	require.Equal(t, []string{"hetzner", "shadeform"}, registry.Names())
	require.False(t, registry.IsEnabled("vast"))
	_, configured := registry.Get("vast")
	require.True(t, configured)
}
