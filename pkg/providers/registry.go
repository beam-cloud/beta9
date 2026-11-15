package providers

import (
	"maps"
	"slices"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

// Registry holds the providers a control plane can buy from. It is built
// once, at startup.
type Registry struct {
	byName map[string]Provider
}

// NewRegistry builds a client for every provider the config has credentials
// for. Owner identifies this control plane on the instances they buy.
//
// It does not fail, so that a gateway starts whatever its config says of
// providers. What is wrong is logged as an error, once, here: an owner that
// some provider could not store, for which no client is made, and a provider
// whose credentials are incomplete or cannot be read, which is left out.
func NewRegistry(config types.ProviderConfig, owner string) *Registry {
	registry := RegistryOf()
	if err := checkOwner(owner); err != nil {
		log.Error().Err(err).Msg("no provider is used: this control plane's name cannot be put on the machines it buys")
		return registry
	}
	if config.Shadeform.ApiKey != "" {
		registry.add(NewShadeform(config.Shadeform, owner))
	}
	if config.Hetzner.ApiToken != "" {
		registry.add(NewHetzner(config.Hetzner, owner))
	}
	if config.Vast.ApiKey != "" {
		registry.add(NewVast(config.Vast, owner))
	}

	// The others sign in with more than one thing, or buy in places that
	// are named. One that has some of that is meant to be used, and its
	// client says what it lacks.
	if verda := config.Verda; verda.ClientID != "" || verda.ClientSecret != "" {
		client, err := NewVerda(verda, owner)
		registry.use(verdaName, client, err)
	}
	if nebius := config.Nebius; nebius.ServiceAccountID != "" || nebius.PublicKeyID != "" || nebius.PrivateKey != "" || len(nebius.Projects) > 0 {
		client, err := NewNebius(nebius, owner)
		registry.use(nebiusName, client, err)
	}
	// AWS may have its credentials from where the gateway runs, so a
	// region to buy in is enough.
	if aws := config.AWS; len(aws.Regions) > 0 || aws.AccessKeyID != "" || aws.SecretAccessKey != "" {
		client, err := NewAWS(aws, owner)
		registry.use(awsName, client, err)
	}
	if gcp := config.GCP; gcp.ServiceAccountKey != "" || gcp.Project != "" || len(gcp.Regions) > 0 {
		client, err := NewGCP(gcp, owner)
		registry.use(gcpName, client, err)
	}
	return registry
}

// RegistryOf holds exactly the given providers.
func RegistryOf(providers ...Provider) *Registry {
	registry := &Registry{byName: map[string]Provider{}}
	for _, provider := range providers {
		registry.add(provider)
	}
	return registry
}

func (r *Registry) add(provider Provider) {
	r.byName[provider.Name()] = provider
}

// use adds the client of a provider that could be given one. For one that
// could not, it logs why, and the provider is left out.
func (r *Registry) use(name string, client Provider, err error) {
	if err != nil {
		log.Error().Err(err).Str("provider", name).Msg("provider is left out: its credentials are incomplete or cannot be read")
		return
	}
	r.add(client)
}

// Get returns the provider configured under name.
func (r *Registry) Get(name string) (Provider, bool) {
	if r == nil {
		return nil, false
	}
	provider, ok := r.byName[name]
	return provider, ok
}

// Names lists the configured providers in a fixed order.
func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	return slices.Sorted(maps.Keys(r.byName))
}
