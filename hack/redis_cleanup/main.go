// redis_cleanup removes expired task state and old CacheFS leaves through
// explicit indexes. It is a dry run unless --apply is supplied.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

const cleanupBatchSize = 250

var removeTask = redis.NewScript(`
if redis.call("EXISTS", KEYS[2]) == 1 then
	return -1
end
if redis.call("EXISTS", KEYS[1]) == 1 and redis.call("PTTL", KEYS[1]) ~= -1 then
	return -1
end
redis.call("UNLINK", KEYS[1], KEYS[2])
redis.call("SREM", KEYS[3], KEYS[1])
redis.call("SREM", KEYS[4], ARGV[1])
redis.call("SREM", KEYS[5], ARGV[1])
return 1
`)

var removeCacheFSLeaf = redis.NewScript(`
local score = redis.call("ZSCORE", KEYS[1], ARGV[1])
if not score or tonumber(score) > tonumber(ARGV[2]) then
	return -1
end
if not redis.call("HGET", KEYS[2], ARGV[1]) then
	redis.call("ZREM", KEYS[1], ARGV[1])
	return 2
end
if redis.call("SCARD", KEYS[3]) ~= 0 then
	return -1
end
if redis.call("SISMEMBER", KEYS[4], ARGV[1]) == 1 then
	return -1
end
redis.call("HDEL", KEYS[2], ARGV[1])
redis.call("UNLINK", KEYS[3])
redis.call("SREM", KEYS[4], ARGV[1])
redis.call("ZREM", KEYS[1], ARGV[1])
return 1
`)

type cleanupReport struct {
	Apply                  bool  `json:"apply"`
	TaskIndexed            int64 `json:"task_indexed"`
	TaskExpired            int64 `json:"task_expired"`
	TaskMissing            int64 `json:"task_missing"`
	TaskProtected          int64 `json:"task_protected"`
	TaskRemoved            int64 `json:"task_removed"`
	TaskBytes              int64 `json:"task_bytes"`
	CacheFSIndexed         int64 `json:"cachefs_indexed"`
	CacheFSMissing         int64 `json:"cachefs_missing"`
	CacheFSOrphanLeaves    int64 `json:"cachefs_orphan_leaves"`
	CacheFSProtected       int64 `json:"cachefs_protected"`
	CacheFSRemoved         int64 `json:"cachefs_removed"`
	CacheFSIndexesRemoved  int64 `json:"cachefs_indexes_removed"`
	CacheFSBytes           int64 `json:"cachefs_bytes"`
	CacheFSOlderThanSecond int64 `json:"cachefs_older_than_seconds"`
}

func main() {
	apply := flag.Bool("apply", false, "apply guarded cleanup")
	cacheAge := flag.Duration("cachefs-older-than", 30*24*time.Hour, "minimum age for compact CacheFS leaves")
	pause := flag.Duration("pause", 10*time.Millisecond, "pause between batches")
	flag.Parse()
	if *cacheAge <= 0 || *pause < 0 {
		fmt.Fprintln(os.Stderr, "invalid cache age or pause")
		os.Exit(2)
	}

	manager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		cleanupExit("load config", err)
	}
	rdb, err := common.NewRedisClient(manager.GetConfig().Database.Redis, common.WithClientName("beta9-redis-cleanup"))
	if err != nil {
		cleanupExit("connect redis", err)
	}
	defer rdb.Close()

	ctx := context.Background()
	result := &cleanupReport{
		Apply:                  *apply,
		CacheFSOlderThanSecond: int64(cacheAge.Seconds()),
	}
	if *apply {
		if err := removeTask.Load(ctx, rdb).Err(); err != nil {
			cleanupExit("load task cleanup script", err)
		}
		if err := removeCacheFSLeaf.Load(ctx, rdb).Err(); err != nil {
			cleanupExit("load cachefs cleanup script", err)
		}
	}
	if err := cleanupTasks(ctx, rdb, *apply, *pause, result); err != nil {
		cleanupExit("cleanup tasks", err)
	}
	if err := cleanupCacheFS(ctx, rdb, *apply, *cacheAge, *pause, result); err != nil {
		cleanupExit("cleanup cachefs", err)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		cleanupExit("encode report", err)
	}
}

func cleanupTasks(ctx context.Context, rdb *common.RedisClient, apply bool, pause time.Duration, result *cleanupReport) error {
	keys, err := rdb.SMembers(ctx, common.RedisKeys.TaskIndex()).Result()
	if err != nil {
		return err
	}
	result.TaskIndexed = int64(len(keys))
	for start := 0; start < len(keys); start += cleanupBatchSize {
		end := min(start+cleanupBatchSize, len(keys))
		batch := keys[start:end]
		values := make(map[string]*redis.StringCmd, len(batch))
		claims := make(map[string]*redis.IntCmd, len(batch))
		memory := make(map[string]*redis.IntCmd, len(batch))
		pipe := rdb.Pipeline()
		for _, key := range batch {
			values[key] = pipe.Get(ctx, key)
			claims[key] = pipe.Exists(ctx, key+":claim")
			memory[key] = pipe.MemoryUsage(ctx, key)
		}
		_, _ = pipe.Exec(ctx)

		type candidate struct {
			key, workspace, stub, task string
		}
		candidates := make([]candidate, 0, len(batch))
		for _, key := range batch {
			if claims[key].Val() != 0 {
				result.TaskProtected++
				continue
			}
			value, valueErr := values[key].Bytes()
			missing := valueErr == redis.Nil
			expired := false
			if valueErr == nil {
				var state struct {
					Policy struct {
						Expires time.Time `json:"expires"`
					} `json:"policy"`
				}
				expired = json.Unmarshal(value, &state) == nil &&
					!state.Policy.Expires.IsZero() &&
					time.Now().After(state.Policy.Expires)
			}
			if !missing && !expired {
				result.TaskProtected++
				continue
			}
			parts := strings.Split(key, ":")
			if len(parts) != 4 {
				result.TaskProtected++
				continue
			}
			if missing {
				result.TaskMissing++
			} else {
				result.TaskExpired++
				result.TaskBytes += memory[key].Val()
			}
			candidates = append(candidates, candidate{key, parts[1], parts[2], parts[3]})
		}
		if apply && len(candidates) > 0 {
			pipe = rdb.Pipeline()
			commands := make([]*redis.Cmd, 0, len(candidates))
			for _, item := range candidates {
				commands = append(commands, removeTask.EvalSha(ctx, pipe, []string{
					item.key,
					item.key + ":claim",
					common.RedisKeys.TaskIndex(),
					common.RedisKeys.TaskIndexByStub(item.workspace, item.stub),
					common.RedisKeys.TaskClaimIndex(item.workspace, item.stub),
				}, item.task))
			}
			_, _ = pipe.Exec(ctx)
			for _, command := range commands {
				if command.Val() == int64(1) {
					result.TaskRemoved++
				} else {
					result.TaskProtected++
				}
			}
		}
		time.Sleep(pause)
	}
	return nil
}

func cleanupCacheFS(ctx context.Context, rdb *common.RedisClient, apply bool, age, pause time.Duration, result *cleanupReport) error {
	cutoff := time.Now().Add(-age).Unix()
	for shard := range cache.FSMetadataShardCount {
		index := cache.MetadataKeys.MetadataFsNodeIndexShard(shard)
		data := cache.MetadataKeys.MetadataFsNodeDataShard(shard)
		var cursor uint64
		for {
			values, next, err := rdb.ZScan(ctx, index, cursor, "*", cleanupBatchSize).Result()
			if err != nil {
				return err
			}
			ids := make([]string, 0, len(values)/2)
			for i := 0; i+1 < len(values); i += 2 {
				score, err := strconv.ParseFloat(values[i+1], 64)
				if err == nil && int64(score) <= cutoff {
					ids = append(ids, values[i])
				}
			}
			result.CacheFSIndexed += int64(len(values) / 2)
			if err := cleanupCacheFSBatch(ctx, rdb, ids, data, index, cutoff, apply, result); err != nil {
				return err
			}
			time.Sleep(pause)
			cursor = next
			if cursor == 0 {
				break
			}
		}
	}
	return nil
}

func cleanupCacheFSBatch(ctx context.Context, rdb *common.RedisClient, ids []string, dataKey, index string, cutoff int64, apply bool, result *cleanupReport) error {
	metadata := make(map[string]*redis.StringCmd, len(ids))
	children := make(map[string]*redis.IntCmd, len(ids))
	pipe := rdb.Pipeline()
	for _, id := range ids {
		metadata[id] = pipe.HGet(ctx, dataKey, id)
		children[id] = pipe.SCard(ctx, cache.MetadataKeys.MetadataFsNodeChildren(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}

	type candidate struct {
		id, parent string
		missing    bool
	}
	candidates := make([]candidate, 0, len(ids))
	for _, id := range ids {
		data, err := metadata[id].Bytes()
		if err == redis.Nil {
			result.CacheFSMissing++
			candidates = append(candidates, candidate{id: id, missing: true})
			continue
		}
		if err != nil || children[id].Val() != 0 {
			result.CacheFSProtected++
			continue
		}
		node, err := cache.UnmarshalFSMetadata(data)
		if err != nil || node.PID == "" {
			result.CacheFSProtected++
			continue
		}
		candidates = append(candidates, candidate{id: id, parent: node.PID})
	}
	if len(candidates) == 0 {
		return nil
	}

	references := make(map[string]*redis.BoolCmd, len(candidates))
	pipe = rdb.Pipeline()
	for _, item := range candidates {
		if item.missing {
			continue
		}
		references[item.id] = pipe.SIsMember(ctx, cache.MetadataKeys.MetadataFsNodeChildren(item.parent), item.id)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}

	orphans := candidates[:0]
	for _, item := range candidates {
		if item.missing {
			orphans = append(orphans, item)
			continue
		}
		if references[item.id].Val() {
			result.CacheFSProtected++
			continue
		}
		result.CacheFSOrphanLeaves++
		data, _ := metadata[item.id].Bytes()
		result.CacheFSBytes += int64(len(item.id) + len(data))
		orphans = append(orphans, item)
	}
	if !apply || len(orphans) == 0 {
		return nil
	}

	pipe = rdb.Pipeline()
	commands := make([]*redis.Cmd, 0, len(orphans))
	for _, item := range orphans {
		commands = append(commands, removeCacheFSLeaf.EvalSha(ctx, pipe, []string{
			index,
			dataKey,
			cache.MetadataKeys.MetadataFsNodeChildren(item.id),
			cache.MetadataKeys.MetadataFsNodeChildren(item.parent),
		}, item.id, cutoff))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}
	for _, command := range commands {
		switch command.Val() {
		case int64(1):
			result.CacheFSRemoved++
		case int64(2):
			result.CacheFSIndexesRemoved++
		default:
			result.CacheFSProtected++
		}
	}
	return nil
}

func cleanupExit(action string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", action, err)
	os.Exit(1)
}
