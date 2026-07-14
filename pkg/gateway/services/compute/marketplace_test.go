package compute

import (
	"context"
	"reflect"
	"testing"

	model "github.com/beam-cloud/beta9/pkg/compute"
	pb "github.com/beam-cloud/beta9/proto"
)

type marketplacePoolSwitchRepo struct {
	*fakeComputeRepo
	recreated   *model.MarketplaceListingState
	lockedPools []string
}

func (r *marketplacePoolSwitchRepo) WithPoolStateLock(ctx context.Context, workspaceID, poolName string, fn func(context.Context) error) error {
	r.lockedPools = append(r.lockedPools, poolName)
	if len(r.lockedPools) == 1 {
		r.listings[workspaceID] = []*model.MarketplaceListingState{r.recreated}
	}
	return fn(ctx)
}

func TestUpdateMarketplaceListingReacquiresRecreatedPoolLock(t *testing.T) {
	ctx := testAuthContext("seller-1", "owner-token")
	listing := &model.MarketplaceListingState{
		ID:                "listing-1",
		SellerWorkspaceID: "seller-1",
		DisplayName:       "gpu listing",
		GPU:               "A100-40",
		GPUCount:          1,
		Status:            model.MarketplaceListingStatusActive,
		PoolName:          "pool-old",
	}
	recreated := *listing
	recreated.PoolName = "pool-new"
	repo := &marketplacePoolSwitchRepo{
		fakeComputeRepo: &fakeComputeRepo{listings: map[string][]*model.MarketplaceListingState{
			"seller-1": {listing},
		}},
		recreated: &recreated,
	}

	response, err := (&Service{computeRepo: repo}).UpdateMarketplaceListing(ctx, &pb.UpdateMarketplaceListingRequest{
		ListingId: listing.ID,
		Region:    "us-west",
	})
	if err != nil || !response.GetOk() {
		t.Fatalf("UpdateMarketplaceListing() = %+v, %v", response, err)
	}
	if want := []string{"pool-old", "pool-new"}; !reflect.DeepEqual(repo.lockedPools, want) {
		t.Fatalf("locked pools = %v, want %v", repo.lockedPools, want)
	}
	if response.Listing.PoolName != "pool-new" || response.Listing.Region != "us-west" {
		t.Fatalf("updated listing = %+v, want recreated pool updated", response.Listing)
	}
}
