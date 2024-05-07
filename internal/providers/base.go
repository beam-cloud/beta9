package providers

import (
	"context"

	"github.com/beam-cloud/beta9/internal/network"
	"github.com/beam-cloud/beta9/internal/repository"
	"github.com/beam-cloud/beta9/internal/types"
)

type ExternalProvider struct {
	Ctx          context.Context
	Name         string
	ClusterName  string
	AppConfig    types.AppConfig
	ProviderRepo repository.ProviderRepository
	Tailscale    *network.Tailscale
	WorkerRepo   repository.WorkerRepository
}

type ExternalProviderConfig struct {
	Name         string
	ClusterName  string
	AppConfig    types.AppConfig
	TailScale    *network.Tailscale
	ProviderRepo repository.ProviderRepository
	WorkerRepo   repository.WorkerRepository
}

func NewExternalProvider(ctx context.Context, cfg *ExternalProviderConfig) *ExternalProvider {
	return &ExternalProvider{
		Ctx:          ctx,
		Name:         cfg.Name,
		ClusterName:  cfg.ClusterName,
		AppConfig:    cfg.AppConfig,
		Tailscale:    cfg.TailScale,
		ProviderRepo: cfg.ProviderRepo,
		WorkerRepo:   cfg.WorkerRepo,
	}
}

func (p *ExternalProvider) GetName() string {
	return p.Name
}
