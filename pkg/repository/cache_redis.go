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
	key := requiredContentRecentStubsKey(locality)
	member := requiredContentStubMember(workspaceID, stubID)
	pipe := r.rdb.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: float64(now.UnixMilli()), Member: member})
	pipe.Expire(ctx, key, ttl)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *CacheRedisRepository) UpsertRequiredContent(ctx context.Context, item cache.RequiredContentItem, ttl time.Duration) error {
	item = item.Normalized()
	if item.Locality == "" || item.WorkspaceID == "" || item.StubID == "" || item.Hash == "" {
		return fmt.Errorf("locality, workspace id, stub id, and hash are required")
	}
	ttl = requiredContentTTL(ttl)
	now := time.Now().UTC()
	itemKey := requiredContentItemKey(item.Locality, item.WorkspaceID, item.StubID, item.Hash, item.RoutingKey)

	existing, found, err := r.getRequiredContentItem(ctx, itemKey)
	if err != nil {
		return err
	}
	item = mergeRequiredContentItem(item, existing, found, now)

	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}

	stubItemsKey := requiredContentStubItemsKey(item.Locality, item.WorkspaceID, item.StubID)
	recentKey := requiredContentRecentStubsKey(item.Locality)
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, itemKey, payload, ttl)
	pipe.SAdd(ctx, stubItemsKey, itemKey)
	pipe.Expire(ctx, stubItemsKey, ttl)
	pipe.ZAdd(ctx, recentKey, redis.Z{Score: float64(now.UnixMilli()), Member: requiredContentStubMember(item.WorkspaceID, item.StubID)})
	pipe.Expire(ctx, recentKey, ttl)
	_, err = pipe.Exec(ctx)
	return err
}

func mergeRequiredContentItem(item, existing cache.RequiredContentItem, found bool, now time.Time) cache.RequiredContentItem {
	if !found {
		item.AccessCount = 1
		if item.FirstSeen.IsZero() {
			item.FirstSeen = now
		}
		item.LastSeen = now
		if item.LastStatusAt.IsZero() {
			item.LastStatusAt = now
		}
		return item
	}

	if !existing.FirstSeen.IsZero() {
		item.FirstSeen = existing.FirstSeen
	}
	item.AccessCount = existing.AccessCount + 1
	if item.SizeBytes <= 0 {
		item.SizeBytes = existing.SizeBytes
	}
	if item.Source.Type == cache.RequiredContentSourceUnknown && existing.Source.Type != "" {
		item.Source = existing.Source
	}
	if existing.Status != "" && item.Status == cache.RequiredContentStatusPending {
		item.Status = existing.Status
		item.LastStatusAt = existing.LastStatusAt
		item.LastError = existing.LastError
	}
	if item.FirstSeen.IsZero() {
		item.FirstSeen = now
	}
	item.LastSeen = now
	if item.LastStatusAt.IsZero() {
		item.LastStatusAt = now
	}
	return item
}

func (r *CacheRedisRepository) UpsertRequiredContentBatch(ctx context.Context, items []cache.RequiredContentItem, ttl time.Duration) error {
	for _, item := range items {
		if err := r.UpsertRequiredContent(ctx, item, ttl); err != nil {
			return err
		}
	}
	return nil
}

func (r *CacheRedisRepository) ClaimRequiredContentCatalogItems(ctx context.Context, workspaceID, stubID string, fingerprints map[string]string, ttl time.Duration) (map[string]bool, error) {
	if workspaceID == "" || stubID == "" {
		return nil, fmt.Errorf("workspace id and stub id are required")
	}
	if len(fingerprints) == 0 {
		return map[string]bool{}, nil
	}
	ttl = requiredContentTTL(ttl)
	const claimScript = `
local got = redis.call("GET", KEYS[1])
if got == ARGV[1] then
  return 0
end
redis.call("SET", KEYS[1], ARGV[1], "PX", ARGV[2])
return 1`
	pipe := r.rdb.Pipeline()
	claims := make(map[string]*redis.Cmd, len(fingerprints))
	for itemID, fingerprint := range fingerprints {
		if itemID == "" || fingerprint == "" {
			continue
		}
		key := requiredContentCatalogItemKey(workspaceID, stubID, itemID)
		claims[itemID] = pipe.Eval(ctx, claimScript, []string{key}, fingerprint, ttl.Milliseconds())
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	claimed := make(map[string]bool, len(claims))
	for itemID, claim := range claims {
		result, err := claim.Int()
		if err != nil {
			return nil, err
		}
		if result == 1 {
			claimed[itemID] = true
		}
	}
	return claimed, nil
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

func (r *CacheRedisRepository) ListRequiredContentForStub(ctx context.Context, locality, workspaceID, stubID string, limit int) ([]cache.RequiredContentItem, error) {
	if locality == "" || workspaceID == "" || stubID == "" {
		return nil, fmt.Errorf("locality, workspace id, and stub id are required")
	}
	if limit <= 0 {
		limit = cache.DefaultRequiredContentBatchSize
	}
	itemKeys, err := r.rdb.SMembers(ctx, requiredContentStubItemsKey(locality, workspaceID, stubID)).Result()
	if err != nil {
		return nil, err
	}
	sort.Strings(itemKeys)
	if len(itemKeys) > limit {
		itemKeys = itemKeys[:limit]
	}

	items := make([]cache.RequiredContentItem, 0, len(itemKeys))
	for _, itemKey := range itemKeys {
		item, found, err := r.getRequiredContentItem(ctx, itemKey)
		if err != nil {
			return nil, err
		}
		if found {
			item = r.applyRequiredContentStatus(ctx, item)
			items = append(items, item.Normalized())
		}
	}
	return items, nil
}

func (r *CacheRedisRepository) SetRequiredContentReconciliationStatus(ctx context.Context, locality, workspaceID, stubID, hash, routingKey string, status cache.RequiredContentReconciliationStatus, errorMsg string, ttl time.Duration) error {
	if routingKey == "" {
		routingKey = hash
	}
	if locality == "" || workspaceID == "" || stubID == "" || hash == "" || status == "" {
		return fmt.Errorf("locality, workspace id, stub id, hash, and status are required")
	}
	ttl = requiredContentTTL(ttl)
	key := requiredContentItemKey(locality, workspaceID, stubID, hash, routingKey)
	item, found, err := r.getRequiredContentItem(ctx, key)
	if err != nil {
		return err
	}
	statusRecord := requiredContentStatusRecord{
		Status:       status,
		LastStatusAt: time.Now().UTC(),
		LastError:    errorMsg,
	}
	if !found {
		return r.setRequiredContentStatus(ctx, locality, workspaceID, stubID, hash, routingKey, statusRecord, ttl)
	}
	item.Status = status
	item.LastStatusAt = statusRecord.LastStatusAt
	item.LastError = errorMsg
	payload, err := json.Marshal(item)
	if err != nil {
		return err
	}
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, key, payload, ttl)
	statusKey := requiredContentStatusKey(locality, workspaceID, stubID, hash, routingKey)
	statusPayload, err := json.Marshal(statusRecord)
	if err != nil {
		return err
	}
	pipe.Set(ctx, statusKey, statusPayload, ttl)
	_, err = pipe.Exec(ctx)
	return err
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

func (r *CacheRedisRepository) getRequiredContentItem(ctx context.Context, key string) (cache.RequiredContentItem, bool, error) {
	payload, err := r.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return cache.RequiredContentItem{}, false, nil
	}
	if err != nil {
		return cache.RequiredContentItem{}, false, err
	}
	item := cache.RequiredContentItem{}
	if err := json.Unmarshal(payload, &item); err != nil {
		return cache.RequiredContentItem{}, false, err
	}
	return item, true, nil
}

func (r *CacheRedisRepository) applyRequiredContentStatus(ctx context.Context, item cache.RequiredContentItem) cache.RequiredContentItem {
	status, found, err := r.getRequiredContentStatus(ctx, item.Locality, item.WorkspaceID, item.StubID, item.Hash, item.RoutingKey)
	if err != nil || !found {
		return item
	}
	item.Status = status.Status
	item.LastStatusAt = status.LastStatusAt
	item.LastError = status.LastError
	return item
}

func (r *CacheRedisRepository) setRequiredContentStatus(ctx context.Context, locality, workspaceID, stubID, hash, routingKey string, status requiredContentStatusRecord, ttl time.Duration) error {
	payload, err := json.Marshal(status)
	if err != nil {
		return err
	}
	return r.rdb.Set(ctx, requiredContentStatusKey(locality, workspaceID, stubID, hash, routingKey), payload, ttl).Err()
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

func requiredContentStubItemsKey(locality, workspaceID, stubID string) string {
	return fmt.Sprintf(
		"%s:locality:%s:workspace:%s:stub:%s:items",
		cacheRequiredContentKeyPrefix,
		requiredContentKeyPart(locality),
		requiredContentKeyPart(workspaceID),
		requiredContentKeyPart(stubID),
	)
}

func requiredContentItemKey(locality, workspaceID, stubID, hash, routingKey string) string {
	return fmt.Sprintf(
		"%s:locality:%s:workspace:%s:stub:%s:item:%s",
		cacheRequiredContentKeyPrefix,
		requiredContentKeyPart(locality),
		requiredContentKeyPart(workspaceID),
		requiredContentKeyPart(stubID),
		requiredContentItemID(hash, routingKey),
	)
}

func requiredContentCatalogItemKey(workspaceID, stubID, itemID string) string {
	sum := sha256.Sum256([]byte(itemID))
	return fmt.Sprintf(
		"%s:catalog:workspace:%s:stub:%s:item:%s",
		cacheRequiredContentKeyPrefix,
		requiredContentKeyPart(workspaceID),
		requiredContentKeyPart(stubID),
		hex.EncodeToString(sum[:]),
	)
}

func requiredContentStatusKey(locality, workspaceID, stubID, hash, routingKey string) string {
	return fmt.Sprintf(
		"%s:status:locality:%s:workspace:%s:stub:%s:item:%s",
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
