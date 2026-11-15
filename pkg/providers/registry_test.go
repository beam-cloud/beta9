package providers

import (
	"bufio"
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestRegistryHoldsTheProvidersThatHaveCredentials(t *testing.T) {
	require.Empty(t, NewRegistry(types.ProviderConfig{}, testOwner).Names())

	registry := NewRegistry(types.ProviderConfig{
		Shadeform: types.ShadeformProviderConfig{ApiKey: "key"},
		Hetzner:   types.HetznerProviderConfig{ApiToken: "token"},
		Verda:     types.VerdaProviderConfig{ClientID: "id", ClientSecret: "secret"},
		Nebius:    types.NebiusProviderConfig{ServiceAccountID: "account", PublicKeyID: "key", PrivateKey: testServiceKeyPEM(t)},
		// AWS may have its credentials from where the gateway runs. It
		// is in use once it has a region to buy in.
		AWS:  types.AWSProviderConfig{Regions: []types.AWSProviderRegionConfig{{Name: "us-east-1"}}},
		GCP:  types.GCPProviderConfig{ServiceAccountKey: gcpTestKeyJSON(t, "https://oauth2.googleapis.com/token")},
		Vast: types.VastProviderConfig{ApiKey: "key"},
	}, testOwner)
	require.Equal(t, []string{"aws", "gcp", "hetzner", "nebius", "shadeform", "vast", "verda"}, registry.Names())

	shadeform, ok := registry.Get("shadeform")
	require.True(t, ok)
	require.Equal(t, "shadeform", shadeform.Name())

	_, ok = registry.Get("lambda")
	require.False(t, ok)
}

func TestRegistryThatWasNeverBuiltHasNoProviders(t *testing.T) {
	var registry *Registry
	_, ok := registry.Get("shadeform")
	require.False(t, ok)
	require.Empty(t, registry.Names())
}

func TestRegistryOfHoldsWhatItIsGiven(t *testing.T) {
	registry := RegistryOf(NewFake("b", testOwner), NewFake("a", testOwner))
	require.Equal(t, []string{"a", "b"}, registry.Names())
	provider, ok := registry.Get("a")
	require.True(t, ok)
	require.Equal(t, "a", provider.Name())
}

// loggedError is a line of the log at the error level.
type loggedError struct {
	Provider string `json:"provider"`
	Error    string `json:"error"`
	Message  string `json:"message"`
}

// loggedErrors collects what is logged while the test runs, and returns the
// errors among it.
func loggedErrors(t *testing.T) func() []loggedError {
	t.Helper()
	var written bytes.Buffer
	previous := log.Logger
	log.Logger = zerolog.New(&written)
	t.Cleanup(func() { log.Logger = previous })
	return func() []loggedError {
		errs := []loggedError{}
		for lines := bufio.NewScanner(bytes.NewReader(written.Bytes())); lines.Scan(); {
			var line struct {
				Level string `json:"level"`
				loggedError
			}
			require.NoError(t, json.Unmarshal(lines.Bytes(), &line), lines.Text())
			if line.Level == "error" {
				errs = append(errs, line.loggedError)
			}
		}
		return errs
	}
}

// The owner goes on every machine as the value of a tag. One that some
// provider could not store is turned down once, where the clients are made,
// and none is made: a gateway starts all the same, and buys nothing.
func TestRegistryHasNoProvidersForAnOwnerThatCannotBeStored(t *testing.T) {
	config := types.ProviderConfig{
		Shadeform: types.ShadeformProviderConfig{ApiKey: "key"},
		Hetzner:   types.HetznerProviderConfig{ApiToken: "token"},
	}
	for name, owner := range map[string]string{
		"no owner":       "",
		"in capitals":    "Beam",
		"too long":       strings.Repeat("a", 64),
		"ends on a dash": "beam-",
	} {
		t.Run(name, func(t *testing.T) {
			errs := loggedErrors(t)
			require.Empty(t, NewRegistry(config, owner).Names())
			require.Len(t, errs(), 1)
			require.Contains(t, errs()[0].Error, "provider owner")
		})
	}

	errs := loggedErrors(t)
	require.Equal(t, []string{"hetzner", "shadeform"}, NewRegistry(config, strings.Repeat("a", 63)).Names())
	require.Empty(t, errs())
}

// A provider that has half of what it signs in with, or a key that cannot
// be read, could not make one call. It is left out, and the log says once
// which provider and what is wrong with it. The others are used.
func TestRegistryLeavesOutAProviderWhoseCredentialsAreIncompleteOrCannotBeRead(t *testing.T) {
	gcpKey := gcpTestKeyJSON(t, "https://oauth2.googleapis.com/token")
	nebius := types.NebiusProviderConfig{ServiceAccountID: nebiusTestAccount, PublicKeyID: nebiusTestKeyID, PrivateKey: testServiceKeyPEM(t)}
	without := func(change func(*types.NebiusProviderConfig)) types.NebiusProviderConfig {
		changed := nebius
		change(&changed)
		return changed
	}
	region := []types.AWSProviderRegionConfig{{Name: "us-east-1"}}

	for name, tc := range map[string]struct {
		config   types.ProviderConfig
		provider string
		want     string
	}{
		"verda without its secret": {
			config:   types.ProviderConfig{Verda: types.VerdaProviderConfig{ClientID: verdaTestClientID}},
			provider: "verda", want: "providers.verda.clientSecret is not set",
		},
		"verda without its id": {
			config:   types.ProviderConfig{Verda: types.VerdaProviderConfig{ClientSecret: verdaTestSecret}},
			provider: "verda", want: "providers.verda.clientId is not set",
		},
		"nebius without its account": {
			config:   types.ProviderConfig{Nebius: without(func(c *types.NebiusProviderConfig) { c.ServiceAccountID = "" })},
			provider: "nebius", want: "providers.nebius.serviceAccountId is not set",
		},
		"nebius without the ID of its key": {
			config:   types.ProviderConfig{Nebius: without(func(c *types.NebiusProviderConfig) { c.PublicKeyID = "" })},
			provider: "nebius", want: "providers.nebius.publicKeyId is not set",
		},
		"nebius without its key": {
			config:   types.ProviderConfig{Nebius: without(func(c *types.NebiusProviderConfig) { c.PrivateKey = "" })},
			provider: "nebius", want: "providers.nebius.privateKey is not set",
		},
		"nebius with a key that is not PEM": {
			config:   types.ProviderConfig{Nebius: without(func(c *types.NebiusProviderConfig) { c.PrivateKey = "c2VjcmV0" })},
			provider: "nebius", want: "providers.nebius.privateKey: the private key is not in PEM form",
		},
		"nebius projects and nothing to sign in with": {
			config:   types.ProviderConfig{Nebius: types.NebiusProviderConfig{Projects: []types.NebiusProjectConfig{{ID: nebiusNorth}}}},
			provider: "nebius", want: "providers.nebius.serviceAccountId is not set",
		},
		"aws keys and no region": {
			config:   types.ProviderConfig{AWS: types.AWSProviderConfig{AccessKeyID: "AKIABEAMTEST", SecretAccessKey: "secret"}},
			provider: "aws", want: "providers.aws.regions names no region",
		},
		"aws with half a key": {
			config:   types.ProviderConfig{AWS: types.AWSProviderConfig{AccessKeyID: "AKIABEAMTEST", Regions: region}},
			provider: "aws", want: "providers.aws.secretAccessKey is not set",
		},
		"gcp regions and no key": {
			config:   types.ProviderConfig{GCP: types.GCPProviderConfig{Regions: []types.GCPProviderRegionConfig{{Name: "us-central1"}}}},
			provider: "gcp", want: "providers.gcp.serviceAccountKey is not set",
		},
		"gcp project and no key": {
			config:   types.ProviderConfig{GCP: types.GCPProviderConfig{Project: gcpTestProject}},
			provider: "gcp", want: "providers.gcp.serviceAccountKey is not set",
		},
		"gcp key that is not JSON": {
			config:   types.ProviderConfig{GCP: types.GCPProviderConfig{ServiceAccountKey: "-----BEGIN PRIVATE KEY-----"}},
			provider: "gcp", want: "providers.gcp.serviceAccountKey is not a service account's JSON key",
		},
		"gcp key file without a key": {
			config:   types.ProviderConfig{GCP: types.GCPProviderConfig{ServiceAccountKey: `{"client_email": "` + gcpTestEmail + `", "project_id": "beam-test"}`}},
			provider: "gcp", want: "providers.gcp.serviceAccountKey: the private key is not in PEM form",
		},
		"gcp key that names no project": {
			config:   types.ProviderConfig{GCP: types.GCPProviderConfig{ServiceAccountKey: strings.Replace(gcpKey, `"project_id":"beam-test"`, `"project_id":""`, 1)}},
			provider: "gcp", want: "providers.gcp.project is not set, and the service account's key names no project",
		},
	} {
		t.Run(name, func(t *testing.T) {
			errs := loggedErrors(t)
			tc.config.Shadeform = types.ShadeformProviderConfig{ApiKey: "key"}
			require.Equal(t, []string{"shadeform"}, NewRegistry(tc.config, testOwner).Names())
			require.Len(t, errs(), 1)
			require.Equal(t, tc.provider, errs()[0].Provider)
			require.Contains(t, errs()[0].Error, tc.want)
		})
	}

	// A provider that nothing was set for is not in use, which is no
	// mistake. The default config has Hetzner's settings and no token.
	errs := loggedErrors(t)
	unused := types.ProviderConfig{
		Hetzner: types.HetznerProviderConfig{BaseURL: "https://api.hetzner.cloud/v1", Image: "ubuntu-24.04", DefaultRegions: []string{"ash", "hil"}},
		Vast:    types.VastProviderConfig{BaseURL: "https://console.vast.ai", DiskGB: 300},
		AWS:     types.AWSProviderConfig{ImageName: "an image", DiskGB: 300, KeyName: "ops"},
		GCP:     types.GCPProviderConfig{Image: "an image", DiskGB: 300},
		Nebius:  types.NebiusProviderConfig{ImageFamily: "a family", DiskGB: 300},
		Verda:   types.VerdaProviderConfig{Images: []string{"an image"}, DiskGB: 300},
	}
	require.Empty(t, NewRegistry(unused, testOwner).Names())
	require.Empty(t, errs())

	// What is complete is used, and nothing is said of it.
	complete := types.ProviderConfig{
		Verda:  types.VerdaProviderConfig{ClientID: verdaTestClientID, ClientSecret: verdaTestSecret},
		Nebius: nebius,
		AWS:    types.AWSProviderConfig{AccessKeyID: "AKIABEAMTEST", SecretAccessKey: "secret", Regions: region},
		GCP:    types.GCPProviderConfig{ServiceAccountKey: gcpKey},
	}
	require.Equal(t, []string{"aws", "gcp", "nebius", "verda"}, NewRegistry(complete, testOwner).Names())
	require.Empty(t, errs())
}
