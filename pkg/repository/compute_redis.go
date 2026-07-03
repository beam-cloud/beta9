package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/compute"
	redis "github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type ComputeRedisRepository struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewComputeRedisRepository(rdb *common.RedisClient) ComputeRepository {
	return &ComputeRedisRepository{rdb: rdb, lock: common.NewRedisLock(rdb)}
}

// LockPoolState serializes pool state writes across the reconciler, launch,
// and release paths.
func (r *ComputeRedisRepository) LockPoolState(ctx context.Context, workspaceID, name string) error {
	return r.lock.Acquire(ctx, common.RedisKeys.ComputePoolStateLock(workspaceID, name), common.RedisLockOptions{
		TtlS:          300,
		Retries:       100,
		RetryInterval: 100 * time.Millisecond,
	})
}

func (r *ComputeRedisRepository) UnlockPoolState(ctx context.Context, workspaceID, name string) error {
	return r.lock.Release(common.RedisKeys.ComputePoolStateLock(workspaceID, name))
}

func (r *ComputeRedisRepository) SavePoolState(ctx context.Context, workspaceID string, state *compute.PoolState) error {
	state.WorkspaceID = workspaceID
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputePoolState(workspaceID, state.Name), data, 0).Err(); err != nil {
		return err
	}
	if err := r.rdb.SAdd(ctx, common.RedisKeys.ComputePoolIndex(workspaceID), state.Name).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.ComputePoolWorkspaceIndex(), workspaceID).Err()
}

func (r *ComputeRedisRepository) GetPoolState(ctx context.Context, workspaceID, name string) (*compute.PoolState, error) {
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputePoolState(workspaceID, name)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state compute.PoolState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	state.WorkspaceID = workspaceID
	return &state, nil
}

func (r *ComputeRedisRepository) ListPoolStates(ctx context.Context, workspaceID string, limit int) ([]*compute.PoolState, error) {
	names, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputePoolIndex(workspaceID)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	if limit > 0 && len(names) > limit {
		names = names[:limit]
	}

	keys := make([]string, 0, len(names))
	for _, name := range names {
		keys = append(keys, common.RedisKeys.ComputePoolState(workspaceID, name))
	}
	return r.poolStates(ctx, keys)
}

func (r *ComputeRedisRepository) ListAllPoolStates(ctx context.Context, limit int) ([]*compute.PoolState, error) {
	workspaceIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputePoolWorkspaceIndex()).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(workspaceIDs)

	states := []*compute.PoolState{}
	for _, workspaceID := range workspaceIDs {
		remaining := 0
		if limit > 0 {
			remaining = limit - len(states)
			if remaining <= 0 {
				break
			}
		}
		pools, err := r.ListPoolStates(ctx, workspaceID, remaining)
		if err != nil {
			return nil, err
		}
		states = append(states, pools...)
	}
	return states, nil
}

func (r *ComputeRedisRepository) DeletePoolState(ctx context.Context, workspaceID, name string) error {
	if err := r.rdb.Del(ctx, common.RedisKeys.ComputePoolState(workspaceID, name)).Err(); err != nil {
		return err
	}
	if err := r.rdb.SRem(ctx, common.RedisKeys.ComputePoolIndex(workspaceID), name).Err(); err != nil {
		return err
	}
	remaining, err := r.rdb.SCard(ctx, common.RedisKeys.ComputePoolIndex(workspaceID)).Result()
	if err != nil {
		return err
	}
	if remaining == 0 {
		return r.rdb.SRem(ctx, common.RedisKeys.ComputePoolWorkspaceIndex(), workspaceID).Err()
	}
	return nil
}

func (r *ComputeRedisRepository) SaveJoinTokenState(ctx context.Context, state *compute.JoinTokenState, ttl time.Duration) error {
	if ttl < 0 {
		ttl = time.Second
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, common.RedisKeys.ComputeJoinToken(state.TokenHash), data, ttl).Err()
}

func (r *ComputeRedisRepository) GetJoinTokenState(ctx context.Context, tokenHash string) (*compute.JoinTokenState, error) {
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputeJoinToken(tokenHash)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state compute.JoinTokenState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *ComputeRedisRepository) SaveAgentTokenState(ctx context.Context, state *compute.AgentTokenState, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputeAgentToken(state.TokenHash), data, ttl).Err(); err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputeAgentMachine(state.WorkspaceID, state.PoolName, state.MachineID), data, 0).Err(); err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputeAgentMachinePool(state.WorkspaceID, state.MachineID), state.PoolName, 0).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.ComputeAgentMachineIndex(state.WorkspaceID, state.PoolName), state.MachineID).Err()
}

func (r *ComputeRedisRepository) GetAgentTokenState(ctx context.Context, tokenHash string) (*compute.AgentTokenState, error) {
	if tokenHash == "" {
		return nil, nil
	}
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputeAgentToken(tokenHash)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state compute.AgentTokenState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *ComputeRedisRepository) GetAgentMachineState(ctx context.Context, workspaceID, poolName, machineID string) (*compute.AgentTokenState, error) {
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputeAgentMachine(workspaceID, poolName, machineID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}

	var state compute.AgentTokenState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *ComputeRedisRepository) GetAgentMachineStateForWorkspace(ctx context.Context, workspaceID, machineID string) (*compute.AgentTokenState, error) {
	poolName, err := r.rdb.Get(ctx, common.RedisKeys.ComputeAgentMachinePool(workspaceID, machineID)).Result()
	if err == nil && poolName != "" {
		return r.GetAgentMachineState(ctx, workspaceID, poolName, machineID)
	}
	if err != nil && err != redis.Nil {
		return nil, err
	}
	return r.scanAgentMachineStateForWorkspace(ctx, workspaceID, machineID)
}

func (r *ComputeRedisRepository) ListAgentTokenStates(ctx context.Context, workspaceID, poolName string) ([]*compute.AgentTokenState, error) {
	machineIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeAgentMachineIndex(workspaceID, poolName)).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(machineIDs))
	for _, machineID := range machineIDs {
		keys = append(keys, common.RedisKeys.ComputeAgentMachine(workspaceID, poolName, machineID))
	}
	states, err := r.machines(ctx, keys)
	if err != nil {
		return nil, err
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].MachineID < states[j].MachineID
	})
	return states, nil
}

func (r *ComputeRedisRepository) DeleteAgentMachineState(ctx context.Context, workspaceID, poolName, machineID string) error {
	state, err := r.GetAgentMachineState(ctx, workspaceID, poolName, machineID)
	if err != nil {
		return err
	}

	workerIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeAgentSlotIndex(workspaceID, poolName, machineID)).Result()
	if err != nil {
		return err
	}

	keys := make([]string, 0, len(workerIDs)+4)
	for _, workerID := range workerIDs {
		keys = append(keys, common.RedisKeys.ComputeAgentSlot(workspaceID, poolName, machineID, workerID))
	}
	keys = append(keys,
		common.RedisKeys.ComputeAgentSlotIndex(workspaceID, poolName, machineID),
		common.RedisKeys.ComputeAgentMachine(workspaceID, poolName, machineID),
		common.RedisKeys.ComputeAgentMachinePool(workspaceID, machineID),
	)
	if state != nil && state.TokenHash != "" {
		keys = append(keys, common.RedisKeys.ComputeAgentToken(state.TokenHash))
	}

	if len(keys) > 0 {
		if err := r.rdb.Del(ctx, keys...).Err(); err != nil {
			return err
		}
	}
	return r.rdb.SRem(ctx, common.RedisKeys.ComputeAgentMachineIndex(workspaceID, poolName), machineID).Err()
}

// PruneAgentMachineIndex removes index entries whose machine state key no
// longer exists.
func (r *ComputeRedisRepository) PruneAgentMachineIndex(ctx context.Context, workspaceID, poolName string) error {
	machineIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeAgentMachineIndex(workspaceID, poolName)).Result()
	if err != nil {
		return err
	}
	for _, machineID := range machineIDs {
		exists, err := r.rdb.Exists(ctx, common.RedisKeys.ComputeAgentMachine(workspaceID, poolName, machineID)).Result()
		if err != nil {
			return err
		}
		if exists == 0 {
			if err := r.rdb.SRem(ctx, common.RedisKeys.ComputeAgentMachineIndex(workspaceID, poolName), machineID).Err(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *ComputeRedisRepository) scanAgentMachineStateForWorkspace(ctx context.Context, workspaceID, machineID string) (*compute.AgentTokenState, error) {
	pools, err := r.ListPoolStates(ctx, workspaceID, 0)
	if err != nil {
		return nil, err
	}
	for _, pool := range pools {
		if pool == nil || pool.Name == "" {
			continue
		}
		machine, err := r.GetAgentMachineState(ctx, workspaceID, pool.Name, machineID)
		if err != nil {
			return nil, err
		}
		if machine != nil {
			return machine, nil
		}
	}
	return nil, nil
}

func (r *ComputeRedisRepository) SaveAgentWorkerSlotState(ctx context.Context, state *compute.AgentWorkerSlotState) error {
	if state == nil || state.WorkerID == "" {
		return fmt.Errorf("worker slot id is required")
	}
	now := time.Now()
	if state.CreatedAt.IsZero() {
		state.CreatedAt = now
	}
	state.UpdatedAt = now
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputeAgentSlot(state.WorkspaceID, state.PoolName, state.MachineID, state.WorkerID), data, 0).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.ComputeAgentSlotIndex(state.WorkspaceID, state.PoolName, state.MachineID), state.WorkerID).Err()
}

func (r *ComputeRedisRepository) ListAgentWorkerSlotStates(ctx context.Context, workspaceID, poolName, machineID string) ([]*compute.AgentWorkerSlotState, error) {
	workerIDs, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeAgentSlotIndex(workspaceID, poolName, machineID)).Result()
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(workerIDs))
	for _, workerID := range workerIDs {
		keys = append(keys, common.RedisKeys.ComputeAgentSlot(workspaceID, poolName, machineID, workerID))
	}
	states, err := r.slots(ctx, keys)
	if err != nil {
		return nil, err
	}
	sort.Slice(states, func(i, j int) bool {
		return states[i].WorkerID < states[j].WorkerID
	})
	return states, nil
}

func (r *ComputeRedisRepository) DeleteAgentWorkerSlotState(ctx context.Context, workspaceID, poolName, machineID, workerID string) error {
	if err := r.rdb.Del(ctx, common.RedisKeys.ComputeAgentSlot(workspaceID, poolName, machineID, workerID)).Err(); err != nil {
		return err
	}
	return r.rdb.SRem(ctx, common.RedisKeys.ComputeAgentSlotIndex(workspaceID, poolName, machineID), workerID).Err()
}

func (r *ComputeRedisRepository) SaveMarketplaceListing(ctx context.Context, state *compute.MarketplaceListingState) error {
	if state == nil || state.SellerWorkspaceID == "" || state.ID == "" {
		return fmt.Errorf("marketplace listing workspace and id are required")
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputeMarketplaceListing(state.SellerWorkspaceID, state.ID), data, 0).Err(); err != nil {
		return err
	}
	if err := r.rdb.SAdd(ctx, common.RedisKeys.ComputeMarketplaceIndex(state.SellerWorkspaceID), state.ID).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.ComputeMarketplaceGlobalIndex(), marketplaceListingGlobalMember(state.SellerWorkspaceID, state.ID)).Err()
}

func (r *ComputeRedisRepository) GetMarketplaceListing(ctx context.Context, sellerWorkspaceID, listingID string) (*compute.MarketplaceListingState, error) {
	if sellerWorkspaceID == "" || listingID == "" {
		return nil, nil
	}
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputeMarketplaceListing(sellerWorkspaceID, listingID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	var state compute.MarketplaceListingState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	if state.SellerWorkspaceID == "" {
		state.SellerWorkspaceID = sellerWorkspaceID
	}
	return &state, nil
}

func (r *ComputeRedisRepository) ListMarketplaceListings(ctx context.Context, sellerWorkspaceID string, limit int) ([]*compute.MarketplaceListingState, error) {
	ids, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeMarketplaceIndex(sellerWorkspaceID)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(ids)
	if limit > 0 && len(ids) > limit {
		ids = ids[:limit]
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, common.RedisKeys.ComputeMarketplaceListing(sellerWorkspaceID, id))
	}
	return r.marketplaceListings(ctx, keys)
}

func (r *ComputeRedisRepository) ListAllMarketplaceListings(ctx context.Context, limit int) ([]*compute.MarketplaceListingState, error) {
	members, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeMarketplaceGlobalIndex()).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(members)
	if limit > 0 && len(members) > limit {
		members = members[:limit]
	}
	keys := make([]string, 0, len(members))
	for _, member := range members {
		workspaceID, listingID, ok := splitMarketplaceListingGlobalMember(member)
		if !ok {
			continue
		}
		keys = append(keys, common.RedisKeys.ComputeMarketplaceListing(workspaceID, listingID))
	}
	return r.marketplaceListings(ctx, keys)
}

// GetMarketplaceListingByID resolves a listing without knowing its seller —
// used by the public direct-link lookup. The global index maps listing ids to
// their workspace, so only the one matching listing body is fetched.
func (r *ComputeRedisRepository) GetMarketplaceListingByID(ctx context.Context, listingID string) (*compute.MarketplaceListingState, error) {
	if listingID == "" {
		return nil, nil
	}
	members, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeMarketplaceGlobalIndex()).Result()
	if err != nil {
		return nil, err
	}
	for _, member := range members {
		workspaceID, memberListingID, ok := splitMarketplaceListingGlobalMember(member)
		if !ok || memberListingID != listingID {
			continue
		}
		return r.GetMarketplaceListing(ctx, workspaceID, listingID)
	}
	return nil, nil
}

func (r *ComputeRedisRepository) DeleteMarketplaceListing(ctx context.Context, sellerWorkspaceID, listingID string) error {
	if sellerWorkspaceID == "" || listingID == "" {
		return nil
	}
	if err := r.rdb.Del(ctx, common.RedisKeys.ComputeMarketplaceListing(sellerWorkspaceID, listingID)).Err(); err != nil {
		return err
	}
	if err := r.rdb.SRem(ctx, common.RedisKeys.ComputeMarketplaceIndex(sellerWorkspaceID), listingID).Err(); err != nil {
		return err
	}
	return r.rdb.SRem(ctx, common.RedisKeys.ComputeMarketplaceGlobalIndex(), marketplaceListingGlobalMember(sellerWorkspaceID, listingID)).Err()
}

// LockMachineRentals serializes rental capacity checks and writes for one
// machine, so concurrent reserves can't both claim the same free GPUs.
func (r *ComputeRedisRepository) LockMachineRentals(ctx context.Context, machineID string) error {
	return r.lock.Acquire(ctx, common.RedisKeys.ComputeMarketplaceRentalMachineLock(machineID), common.RedisLockOptions{
		TtlS:          30,
		Retries:       50,
		RetryInterval: 100 * time.Millisecond,
	})
}

func (r *ComputeRedisRepository) UnlockMachineRentals(machineID string) error {
	return r.lock.Release(common.RedisKeys.ComputeMarketplaceRentalMachineLock(machineID))
}

func (r *ComputeRedisRepository) SaveMarketplaceRental(ctx context.Context, state *compute.MarketplaceRentalState) error {
	if state == nil || state.BuyerWorkspaceID == "" || state.ID == "" || state.MachineID == "" {
		return fmt.Errorf("rental buyer workspace, id, and machine are required")
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, common.RedisKeys.ComputeMarketplaceRental(state.BuyerWorkspaceID, state.ID), data, 0).Err(); err != nil {
		return err
	}
	if err := r.rdb.SAdd(ctx, common.RedisKeys.ComputeMarketplaceRentalIndex(state.BuyerWorkspaceID), state.ID).Err(); err != nil {
		return err
	}
	member := marketplaceListingGlobalMember(state.BuyerWorkspaceID, state.ID)
	if err := r.rdb.SAdd(ctx, common.RedisKeys.ComputeMarketplaceRentalMachineIndex(state.MachineID), member).Err(); err != nil {
		return err
	}
	return r.rdb.SAdd(ctx, common.RedisKeys.ComputeMarketplaceRentalGlobalIndex(), member).Err()
}

func (r *ComputeRedisRepository) GetMarketplaceRental(ctx context.Context, buyerWorkspaceID, rentalID string) (*compute.MarketplaceRentalState, error) {
	if buyerWorkspaceID == "" || rentalID == "" {
		return nil, nil
	}
	data, err := r.rdb.Get(ctx, common.RedisKeys.ComputeMarketplaceRental(buyerWorkspaceID, rentalID)).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	var state compute.MarketplaceRentalState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *ComputeRedisRepository) ListMarketplaceRentals(ctx context.Context, buyerWorkspaceID string) ([]*compute.MarketplaceRentalState, error) {
	ids, err := r.rdb.SMembers(ctx, common.RedisKeys.ComputeMarketplaceRentalIndex(buyerWorkspaceID)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(ids)
	rentals := make([]*compute.MarketplaceRentalState, 0, len(ids))
	for _, id := range ids {
		rental, err := r.GetMarketplaceRental(ctx, buyerWorkspaceID, id)
		if err != nil {
			return nil, err
		}
		if rental != nil {
			rentals = append(rentals, rental)
		}
	}
	return rentals, nil
}

// ListMarketplaceRentalsForMachine returns every buyer's active rental on one
// machine — the scheduler subtracts these from serverless-visible capacity.
func (r *ComputeRedisRepository) ListMarketplaceRentalsForMachine(ctx context.Context, machineID string) ([]*compute.MarketplaceRentalState, error) {
	return r.rentalsFromMembers(ctx, common.RedisKeys.ComputeMarketplaceRentalMachineIndex(machineID))
}

// ListAllMarketplaceRentals returns every active rental across buyers — used
// by the gateway's rental billing ticker.
func (r *ComputeRedisRepository) ListAllMarketplaceRentals(ctx context.Context) ([]*compute.MarketplaceRentalState, error) {
	return r.rentalsFromMembers(ctx, common.RedisKeys.ComputeMarketplaceRentalGlobalIndex())
}

func (r *ComputeRedisRepository) rentalsFromMembers(ctx context.Context, indexKey string) ([]*compute.MarketplaceRentalState, error) {
	members, err := r.rdb.SMembers(ctx, indexKey).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(members)
	rentals := make([]*compute.MarketplaceRentalState, 0, len(members))
	for _, member := range members {
		buyerWorkspaceID, rentalID, ok := splitMarketplaceListingGlobalMember(member)
		if !ok {
			continue
		}
		rental, err := r.GetMarketplaceRental(ctx, buyerWorkspaceID, rentalID)
		if err != nil {
			return nil, err
		}
		if rental != nil {
			rentals = append(rentals, rental)
		}
	}
	return rentals, nil
}

func (r *ComputeRedisRepository) DeleteMarketplaceRental(ctx context.Context, state *compute.MarketplaceRentalState) error {
	if state == nil || state.BuyerWorkspaceID == "" || state.ID == "" {
		return nil
	}
	if err := r.rdb.Del(ctx, common.RedisKeys.ComputeMarketplaceRental(state.BuyerWorkspaceID, state.ID)).Err(); err != nil {
		return err
	}
	if err := r.rdb.SRem(ctx, common.RedisKeys.ComputeMarketplaceRentalIndex(state.BuyerWorkspaceID), state.ID).Err(); err != nil {
		return err
	}
	member := marketplaceListingGlobalMember(state.BuyerWorkspaceID, state.ID)
	if err := r.rdb.SRem(ctx, common.RedisKeys.ComputeMarketplaceRentalMachineIndex(state.MachineID), member).Err(); err != nil {
		return err
	}
	return r.rdb.SRem(ctx, common.RedisKeys.ComputeMarketplaceRentalGlobalIndex(), member).Err()
}

func (r *ComputeRedisRepository) poolStates(ctx context.Context, keys []string) ([]*compute.PoolState, error) {
	if len(keys) == 0 {
		return []*compute.PoolState{}, nil
	}

	values, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	states := make([]*compute.PoolState, 0, len(values))
	for i, value := range values {
		if value == nil {
			continue
		}
		data, ok := stateBytes(value)
		if !ok {
			return nil, fmt.Errorf("unexpected redis value type %T for %s", value, keys[i])
		}

		var state compute.PoolState
		if err := json.Unmarshal(data, &state); err != nil {
			return nil, err
		}
		if state.WorkspaceID == "" {
			state.WorkspaceID = workspaceIDFromComputePoolKey(keys[i])
		}
		states = append(states, &state)
	}
	return states, nil
}

func (r *ComputeRedisRepository) marketplaceListings(ctx context.Context, keys []string) ([]*compute.MarketplaceListingState, error) {
	if len(keys) == 0 {
		return []*compute.MarketplaceListingState{}, nil
	}
	values, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	states := make([]*compute.MarketplaceListingState, 0, len(values))
	for i, value := range values {
		data, ok := stateBytes(value)
		if !ok {
			continue
		}
		var state compute.MarketplaceListingState
		if err := json.Unmarshal(data, &state); err != nil {
			log.Warn().Err(err).Str("key", keys[i]).Msg("skipping corrupt marketplace listing state")
			continue
		}
		// Older records may predate the seller_workspace_id field; the key
		// embeds it (compute:marketplace:{workspace}:listing:id), mirroring the
		// backfill GetMarketplaceListing does.
		if state.SellerWorkspaceID == "" {
			state.SellerWorkspaceID = workspaceIDFromComputePoolKey(keys[i])
		}
		states = append(states, &state)
	}
	return states, nil
}

func marketplaceListingGlobalMember(workspaceID, listingID string) string {
	return workspaceID + "\x00" + listingID
}

func splitMarketplaceListingGlobalMember(member string) (string, string, bool) {
	workspaceID, listingID, ok := strings.Cut(member, "\x00")
	if !ok || workspaceID == "" || listingID == "" {
		return "", "", false
	}
	return workspaceID, listingID, true
}

func workspaceIDFromComputePoolKey(key string) string {
	start := strings.Index(key, "{")
	end := strings.Index(key, "}")
	if start < 0 || end <= start {
		return ""
	}
	return key[start+1 : end]
}

func (r *ComputeRedisRepository) machines(ctx context.Context, keys []string) ([]*compute.AgentTokenState, error) {
	if len(keys) == 0 {
		return []*compute.AgentTokenState{}, nil
	}

	values, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	states := make([]*compute.AgentTokenState, 0, len(values))
	for i, value := range values {
		data, ok := stateBytes(value)
		if !ok {
			continue
		}

		var state compute.AgentTokenState
		if err := json.Unmarshal(data, &state); err != nil {
			log.Warn().Err(err).Str("key", keys[i]).Msg("skipping corrupt agent machine state")
			continue
		}
		states = append(states, &state)
	}
	return states, nil
}

func (r *ComputeRedisRepository) slots(ctx context.Context, keys []string) ([]*compute.AgentWorkerSlotState, error) {
	if len(keys) == 0 {
		return []*compute.AgentWorkerSlotState{}, nil
	}

	values, err := r.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	states := make([]*compute.AgentWorkerSlotState, 0, len(values))
	for i, value := range values {
		data, ok := stateBytes(value)
		if !ok {
			continue
		}

		var state compute.AgentWorkerSlotState
		if err := json.Unmarshal(data, &state); err != nil {
			log.Warn().Err(err).Str("key", keys[i]).Msg("skipping corrupt agent worker slot state")
			continue
		}
		states = append(states, &state)
	}
	return states, nil
}

func stateBytes(value any) ([]byte, bool) {
	switch v := value.(type) {
	case string:
		return []byte(v), true
	case []byte:
		return v, true
	default:
		return nil, false
	}
}
