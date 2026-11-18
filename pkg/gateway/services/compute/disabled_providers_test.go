package compute

import (
	"context"
	"testing"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
)

func TestDisabledProviderCannotSellStaleSampledOffers(t *testing.T) {
	service := launchService(&fakeComputeRepo{})
	service.providers = providers.NewRegistry(types.ProviderConfig{
		Disabled:  []string{"aws"},
		AWS:       types.AWSProviderConfig{Regions: []types.AWSProviderRegionConfig{{Name: "us-east-1"}}},
		Shadeform: types.ShadeformProviderConfig{ApiKey: "synthetic-key"},
	}, testOwner)
	service.appConfig.ManagedCompute.Reservations.Providers = []string{"aws", "shadeform"}
	oldAWS, available := a10gOffer, a10gOffer
	oldAWS.Provider, oldAWS.ID = "aws", "stale-aws"
	available.Provider, available.ID = "shadeform", "available-shadeform"
	service.sampledOffers = heardOffers{"aws": {oldAWS}, "shadeform": {available}}
	result, err := service.ListPoolOffers(context.Background(), &pb.ListPoolOffersRequest{
		Pool: &pb.PoolConfig{Providers: []string{"aws", "shadeform"}, Gpu: []string{"A10G"}},
	})
	require.NoError(t, err)
	require.True(t, result.Ok, result.ErrMsg)
	require.Len(t, result.Offers, 1)
	require.Equal(t, "shadeform", result.Offers[0].Provider)
	require.Equal(t, "available-shadeform", result.Offers[0].Id)
}
