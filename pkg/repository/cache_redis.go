package repository

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/redis/go-redis/v9"
)

const cacheCoordinatorKeyPrefix = "cache:coordinator"
const cacheRequiredContentKeyPrefix = "cache:required_content"
const requiredContentStubMemberSeparator = "\x1f"

type CacheRedisRepository struct {
	rdb *common.RedisClient
}

type cacheCoordinatorHostRecord struct {
	LogicalHostID    string  `json:"logical_host_id"`
	RegistrationID   string  `json:"registration_id"`
	PoolName         string  `json:"pool_name"`
	Locality         string  `json:"locality"`
	NodeID           string  `json:"node_id"`
	CachePathID      string  `json:"cache_path_id"`
	Addr             string  `json:"addr"`
	PrivateAddr      string  `json:"private_addr"`
	CapacityUsagePct float64 `json:"capacity_usage_pct"`
}

type requiredContentStatusRecord struct {
	Status       cache.RequiredContentReconciliationStatus `json:"status"`
	LastStatusAt time.Time                                 `json:"last_status_at"`
	LastError    string                                    `json:"last_error,omitempty"`
}

func NewCacheRedisRepository(rdb *common.RedisClient) *CacheRedisRepository {
	return &CacheRedisRepository{rdb: rdb}
}

func (r *CacheRedisRepository) SetCacheRegistration(ctx context.Context, host cache.CoordinatorHost, ttl time.Duration) error {
	payload, err := json.Marshal(cacheCoordinatorHostRecord(host))
	if err != nil {
		return err
	}
	logicalPayload, err := json.Marshal(cacheCoordinatorHostRecord(host.LogicalOnly()))
	if err != nil {
		return err
	}

	if err := r.rdb.SAdd(ctx, cacheCoordinatorIndexKey(host.PoolName, host.Locality), host.LogicalHostID).Err(); err != nil {
		return err
	}
	if err := r.rdb.Set(ctx, cacheCoordinatorLogicalHostKey(host.LogicalHostID), logicalPayload, cacheCoordinatorLogicalHostTTL(ttl)).Err(); err != nil {
		return err
	}
	if err := r.rdb.SAdd(ctx, cacheCoordinatorRegistrationSetKey(host.LogicalHostID), host.RegistrationID).Err(); err != nil {
		return err
	}
	return r.rdb.Set(ctx, cacheCoordinatorRegistrationKey(host.LogicalHostID, host.RegistrationID), payload, ttl).Err()
}

func (r *CacheRedisRepository) GetActiveCacheRegistration(ctx context.Context, logicalHostID string) (string, bool, error) {
	registrationID, err := r.rdb.Get(ctx, cacheCoordinatorActiveRegistrationKey(logicalHostID)).Result()
	if err == redis.Nil {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return registrationID, registrationID != "", nil
}

func (r *CacheRedisRepository) SetActiveCacheRegistration(ctx context.Context, logicalHostID, registrationID string, ttl time.Duration) error {
	return r.rdb.Set(ctx, cacheCoordinatorActiveRegistrationKey(logicalHostID), registrationID, ttl).Err()
}

func (r *CacheRedisRepository) ListCacheLogicalHosts(ctx context.Context, poolName, locality string) ([]string, error) {
	if poolName == "" {
		return r.listCacheLogicalHostsForLocality(ctx, locality)
	}
	return r.rdb.SMembers(ctx, cacheCoordinatorIndexKey(poolName, locality)).Result()
}

func (r *CacheRedisRepository) listCacheLogicalHostsForLocality(ctx context.Context, locality string) ([]string, error) {
	pattern := cacheCoordinatorIndexKey("*", locality)
	keys, err := r.rdb.Scan(ctx, pattern)
	if err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}
	for _, key := range keys {
		ids, err := r.rdb.SMembers(ctx, key).Result()
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			if id != "" {
				seen[id] = struct{}{}
			}
		}
	}

	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids, nil
}

func (r *CacheRedisRepository) ListCacheRegistrations(ctx context.Context, logicalHostID string) ([]string, error) {
	return r.rdb.SMembers(ctx, cacheCoordinatorRegistrationSetKey(logicalHostID)).Result()
}

func (r *CacheRedisRepository) GetCacheRegistration(ctx context.Context, logicalHostID, registrationID string) (cache.CoordinatorHost, bool, error) {
	payload, err := r.rdb.Get(ctx, cacheCoordinatorRegistrationKey(logicalHostID, registrationID)).Bytes()
	if err == redis.Nil {
		return cache.CoordinatorHost{}, false, nil
	}
	if err != nil {
		return cache.CoordinatorHost{}, false, err
	}

	record := cacheCoordinatorHostRecord{}
	if err := json.Unmarshal(payload, &record); err != nil {
		return cache.CoordinatorHost{}, false, err
	}

	return cache.CoordinatorHost(record), true, nil
}

func (r *CacheRedisRepository) GetCacheLogicalHost(ctx context.Context, logicalHostID string) (cache.CoordinatorHost, bool, error) {
	payload, err := r.rdb.Get(ctx, cacheCoordinatorLogicalHostKey(logicalHostID)).Bytes()
	if err == redis.Nil {
		return cache.CoordinatorHost{}, false, nil
	}
	if err != nil {
		return cache.CoordinatorHost{}, false, err
	}

	record := cacheCoordinatorHostRecord{}
	if err := json.Unmarshal(payload, &record); err != nil {
		return cache.CoordinatorHost{}, false, err
	}

	return cache.CoordinatorHost(record).LogicalOnly(), true, nil
}

func (r *CacheRedisRepository) RemoveCacheRegistration(ctx context.Context, logicalHostID, registrationID string) error {
	if err := r.rdb.Del(ctx, cacheCoordinatorRegistrationKey(logicalHostID, registrationID)).Err(); err != nil {
		return err
	}
	if err := r.rdb.SRem(ctx, cacheCoordinatorRegistrationSetKey(logicalHostID), registrationID).Err(); err != nil {
		return err
	}

	activeRegistrationID, found, err := r.GetActiveCacheRegistration(ctx, logicalHostID)
	if err != nil || !found || activeRegistrationID != registrationID {
		return err
	}
	return r.rdb.Del(ctx, cacheCoordinatorActiveRegistrationKey(logicalHostID)).Err()
}

func (r *CacheRedisRepository) CountCacheRegistrations(ctx context.Context, logicalHostID string) (int64, error) {
	return r.rdb.SCard(ctx, cacheCoordinatorRegistrationSetKey(logicalHostID)).Result()
}

func (r *CacheRedisRepository) RemoveCacheLogicalHost(ctx context.Context, poolName, locality, logicalHostID string) error {
	if err := r.rdb.SRem(ctx, cacheCoordinatorIndexKey(poolName, locality), logicalHostID).Err(); err != nil {
		return err
	}
	return r.rdb.Del(ctx, cacheCoordinatorActiveRegistrationKey(logicalHostID), cacheCoordinatorRegistrationSetKey(logicalHostID), cacheCoordinatorLogicalHostKey(logicalHostID)).Err()
}

func cacheCoordinatorIndexKey(poolName, locality string) string {
	return fmt.Sprintf("%s:host_index:%s:%s", cacheCoordinatorKeyPrefix, poolName, locality)
}

func cacheCoordinatorRegistrationSetKey(logicalHostID string) string {
	return fmt.Sprintf("%s:host:%s:registrations", cacheCoordinatorKeyPrefix, logicalHostID)
}

func cacheCoordinatorLogicalHostKey(logicalHostID string) string {
	return fmt.Sprintf("%s:host:%s:logical", cacheCoordinatorKeyPrefix, logicalHostID)
}

func cacheCoordinatorRegistrationKey(logicalHostID, registrationID string) string {
	return fmt.Sprintf("%s:host:%s:registration:%s", cacheCoordinatorKeyPrefix, logicalHostID, registrationID)
}

func cacheCoordinatorActiveRegistrationKey(logicalHostID string) string {
	return fmt.Sprintf("%s:host:%s:active_registration", cacheCoordinatorKeyPrefix, logicalHostID)
}

func cacheCoordinatorLogicalHostTTL(registrationTTL time.Duration) time.Duration {
	ttl := registrationTTL * 4
	if ttl < 2*time.Minute {
		return 2 * time.Minute
	}
	return ttl
}

func (r *CacheRedisRepository) MarkStubLocalityAccessed(ctx context.Context, locality, workspaceID, stubID string, ttl time.Duration) error {
	if locality == "" || workspaceID == "" || stubID == "" {
		return fmt.Errorf("locality, workspace id, and stub id are required")
	}
	ttl = requiredContentTTL(ttl)
	now := time.Now().UTC()
	pipe := r.rdb.Pipeline()
	pipeRequiredContentStubAccess(ctx, pipe, locality, workspaceID, stubID, now, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func pipeRequiredContentStubAccess(ctx context.Context, pipe redis.Pipeliner, locality, workspaceID, stubID string, seen time.Time, ttl time.Duration) {
	recentKey := requiredContentRecentStubsKey(locality)
	pipe.ZAdd(ctx, recentKey, redis.Z{
		Score:  float64(seen.UTC().UnixMilli()),
		Member: requiredContentStubMember(workspaceID, stubID),
	})
	pipe.Expire(ctx, recentKey, ttl)
}

func (r *CacheRedisRepository) ListRecentStubLocalities(ctx context.Context, locality string, since time.Time, limit int) ([]cache.RequiredContentStubLocality, error) {
	if locality == "" {
		return nil, fmt.Errorf("locality is required")
	}
	if limit <= 0 {
		limit = cache.DefaultRequiredContentBatchSize
	}
	key := requiredContentRecentStubsKey(locality)
	minScore := "-inf"
	if !since.IsZero() {
		minScore = fmt.Sprintf("%d", since.UTC().UnixMilli())
		if err := r.rdb.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", since.UTC().UnixMilli()-1)).Err(); err != nil {
			return nil, err
		}
	}
	values, err := r.rdb.ZRevRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min:    minScore,
		Max:    "+inf",
		Offset: 0,
		Count:  int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}

	stubs := make([]cache.RequiredContentStubLocality, 0, len(values))
	for _, value := range values {
		member, ok := value.Member.(string)
		if !ok {
			continue
		}
		workspaceID, stubID, ok := parseRequiredContentStubMember(member)
		if !ok {
			continue
		}
		score := int64(value.Score)
		if score <= 0 || value.Score > float64(math.MaxInt64) {
			score = 0
		}
		lastSeen := time.Time{}
		if score > 0 {
			lastSeen = time.UnixMilli(score).UTC()
		}
		stubs = append(stubs, cache.RequiredContentStubLocality{
			Locality:    locality,
			WorkspaceID: workspaceID,
			StubID:      stubID,
			LastSeen:    lastSeen,
		})
	}
	return stubs, nil
}

func (r *CacheRedisRepository) SetRequiredContentReconciliationStatus(ctx context.Context, locality, workspaceID, stubID, hash, routingKey string, status cache.RequiredContentReconciliationStatus, errorMsg string, ttl time.Duration) error {
	if routingKey == "" {
		routingKey = hash
	}
	if locality == "" || workspaceID == "" || stubID == "" || hash == "" || status == "" {
		return fmt.Errorf("locality, workspace id, stub id, hash, and status are required")
	}
	ttl = requiredContentTTL(ttl)
	statusRecord := requiredContentStatusRecord{
		Status:       status,
		LastStatusAt: time.Now().UTC(),
		LastError:    errorMsg,
	}
	statusPayload, err := json.Marshal(statusRecord)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, requiredContentStatusKey(locality, workspaceID, stubID, hash, routingKey), statusPayload, ttl).Err()
}

func (r *CacheRedisRepository) GetRequiredContentReconciliationStatus(ctx context.Context, locality, workspaceID, stubID, hash, routingKey string) (cache.RequiredContentReconciliationStatus, time.Time, string, bool, error) {
	if routingKey == "" {
		routingKey = hash
	}
	if locality == "" || workspaceID == "" || stubID == "" || hash == "" {
		return "", time.Time{}, "", false, fmt.Errorf("locality, workspace id, stub id, and hash are required")
	}
	status, found, err := r.getRequiredContentStatus(ctx, locality, workspaceID, stubID, hash, routingKey)
	if err != nil || !found {
		return "", time.Time{}, "", found, err
	}
	return status.Status, status.LastStatusAt, status.LastError, true, nil
}

func (r *CacheRedisRepository) AcquireRequiredContentReconciliationLock(ctx context.Context, locality, logicalHostID, hash string, ttl time.Duration) (cache.RequiredContentReconciliationLock, bool, error) {
	if locality == "" || logicalHostID == "" || hash == "" {
		return nil, false, fmt.Errorf("locality, logical host id, and hash are required")
	}
	ttl = requiredContentLockTTL(ttl)
	token := requiredContentLockToken()
	key := requiredContentLockKey(locality, logicalHostID, hash)
	acquired, err := r.rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil || !acquired {
		return nil, acquired, err
	}
	return &requiredContentRedisLock{rdb: r.rdb, key: key, token: token}, true, nil
}

func (r *CacheRedisRepository) getRequiredContentStatus(ctx context.Context, locality, workspaceID, stubID, hash, routingKey string) (requiredContentStatusRecord, bool, error) {
	payload, err := r.rdb.Get(ctx, requiredContentStatusKey(locality, workspaceID, stubID, hash, routingKey)).Bytes()
	if err == redis.Nil {
		return requiredContentStatusRecord{}, false, nil
	}
	if err != nil {
		return requiredContentStatusRecord{}, false, err
	}
	status := requiredContentStatusRecord{}
	if err := json.Unmarshal(payload, &status); err != nil {
		return requiredContentStatusRecord{}, false, err
	}
	return status, true, nil
}

type requiredContentRedisLock struct {
	rdb   *common.RedisClient
	key   string
	token string
}

func (l *requiredContentRedisLock) Release(ctx context.Context) error {
	if l == nil || l.rdb == nil || l.key == "" || l.token == "" {
		return nil
	}
	const releaseScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0`
	return l.rdb.Eval(ctx, releaseScript, []string{l.key}, l.token).Err()
}

func (l *requiredContentRedisLock) Refresh(ctx context.Context, ttl time.Duration) error {
	if l == nil || l.rdb == nil || l.key == "" || l.token == "" {
		return nil
	}
	ttl = requiredContentLockTTL(ttl)
	const refreshScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0`
	return l.rdb.Eval(ctx, refreshScript, []string{l.key}, l.token, ttl.Milliseconds()).Err()
}

func requiredContentTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return cache.DefaultRequiredContentStubTTL
	}
	return ttl
}

func requiredContentLockTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return time.Minute
	}
	return ttl
}

func requiredContentRecentStubsKey(locality string) string {
	return fmt.Sprintf(
		"%s:locality:%s:stubs",
		cacheRequiredContentKeyPrefix,
		requiredContentKeyPart(locality),
	)
}

func requiredContentStatusKey(locality, workspaceID, stubID, hash, routingKey string) string {
	return fmt.Sprintf(
		"%s:status:locality:%s:workspace:%s:stub:%s:content:%s",
		cacheRequiredContentKeyPrefix,
		requiredContentKeyPart(locality),
		requiredContentKeyPart(workspaceID),
		requiredContentKeyPart(stubID),
		requiredContentItemID(hash, routingKey),
	)
}

func requiredContentLockKey(locality, logicalHostID, hash string) string {
	return fmt.Sprintf(
		"%s:lock:%s:%s:%s",
		cacheRequiredContentKeyPrefix,
		requiredContentKeyPart(locality),
		requiredContentKeyPart(logicalHostID),
		requiredContentKeyPart(hash),
	)
}

func requiredContentStubMember(workspaceID, stubID string) string {
	return workspaceID + requiredContentStubMemberSeparator + stubID
}

func parseRequiredContentStubMember(member string) (string, string, bool) {
	parts := strings.SplitN(member, requiredContentStubMemberSeparator, 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}

func requiredContentItemID(hash, routingKey string) string {
	if routingKey == "" {
		routingKey = hash
	}
	sum := sha256.Sum256([]byte(routingKey))
	return requiredContentKeyPart(hash) + ":" + hex.EncodeToString(sum[:8])
}

func requiredContentKeyPart(value string) string {
	if value == "" {
		return "_"
	}
	return strings.ReplaceAll(value, ":", "_")
}

func requiredContentLockToken() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("%d", time.Now().UnixNano())
}
