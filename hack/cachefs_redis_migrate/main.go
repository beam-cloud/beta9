// cachefs_redis_migrate converts CacheFS metadata hashes to the compact v2
// encoding. It is a dry run unless --apply is supplied.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/cache"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/redis/go-redis/v9"
)

const (
	defaultBatchSize = 250
	scanCount        = 1000
)

var replaceMetadata = redis.NewScript(`
if redis.call("TYPE", KEYS[1]).ok ~= "hash" then
	return 0
end
if (redis.call("HGET", KEYS[1], "gen") or "0") ~= ARGV[1] then
	return -1
end
redis.call("HSET", KEYS[2], ARGV[4], ARGV[2])
redis.call("ZADD", KEYS[3], ARGV[3], ARGV[4])
redis.call("DEL", KEYS[1])
return 1
`)

type report struct {
	Apply          bool  `json:"apply"`
	Scanned        int64 `json:"scanned"`
	Legacy         int64 `json:"legacy_hashes"`
	Compact        int64 `json:"compact_values"`
	Children       int64 `json:"children_sets"`
	Invalid        int64 `json:"invalid"`
	Converted      int64 `json:"converted"`
	Changed        int64 `json:"changed_during_migration"`
	LegacyBytes    int64 `json:"legacy_bytes"`
	CompactBytes   int64 `json:"compact_payload_bytes"`
	EstimatedSaved int64 `json:"estimated_bytes_saved"`
}

type metadataRecord struct {
	key     string
	id      string
	gen     string
	encoded []byte
	score   int64
}

func main() {
	apply := flag.Bool("apply", false, "enable v2 and replace legacy hashes")
	batchSize := flag.Int("batch-size", defaultBatchSize, "keys processed per pipeline")
	pause := flag.Duration("pause", 10*time.Millisecond, "pause between batches")
	flag.Parse()

	if *batchSize < 1 || *batchSize > 5000 || *pause < 0 {
		fmt.Fprintln(os.Stderr, "invalid batch size or pause")
		os.Exit(2)
	}

	manager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		exit("load config", err)
	}
	rdb, err := common.NewRedisClient(manager.GetConfig().Database.Redis, common.WithClientName("cachefs-migrate"))
	if err != nil {
		exit("connect redis", err)
	}
	defer rdb.Close()

	ctx := context.Background()
	compact, err := compactNodeCount(ctx, rdb)
	if err != nil {
		exit("count compact metadata", err)
	}
	result := &report{Apply: *apply, Compact: compact}
	if *apply {
		if err := replaceMetadata.Load(ctx, rdb).Err(); err != nil {
			exit("load migration script", err)
		}
		if err := rdb.Set(ctx, cache.MetadataKeys.MetadataFsFormat(), cache.FSMetadataFormatCompact, 0).Err(); err != nil {
			exit("enable compact format", err)
		}
	}

	var cursor uint64
	for {
		keys, next, err := rdb.UniversalClient.Scan(ctx, cursor, cache.MetadataKeys.MetadataFsNode("*"), scanCount).Result()
		if err != nil {
			exit("scan cachefs metadata", err)
		}
		for start := 0; start < len(keys); start += *batchSize {
			end := min(start+*batchSize, len(keys))
			if err := migrateBatch(ctx, rdb, keys[start:end], *apply, result); err != nil {
				exit("migrate batch", err)
			}
			time.Sleep(*pause)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
	result.EstimatedSaved = result.LegacyBytes - result.CompactBytes
	writeReport(result)
}

func writeReport(result *report) {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		exit("encode report", err)
	}
}

func migrateBatch(ctx context.Context, rdb *common.RedisClient, keys []string, apply bool, result *report) error {
	typesByKey := make(map[string]*redis.StatusCmd, len(keys))
	pipe := rdb.Pipeline()
	for _, key := range keys {
		typesByKey[key] = pipe.Type(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}

	hashes := make([]string, 0, len(keys))
	for _, key := range keys {
		result.Scanned++
		switch typesByKey[key].Val() {
		case "hash":
			hashes = append(hashes, key)
		case "set":
			if strings.HasSuffix(key, ":children") {
				result.Children++
			}
		}
	}

	metadataByKey := make(map[string]*redis.MapStringStringCmd, len(hashes))
	memoryByKey := make(map[string]*redis.IntCmd, len(hashes))
	pipe = rdb.Pipeline()
	for _, key := range hashes {
		metadataByKey[key] = pipe.HGetAll(ctx, key)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}
	pipe = rdb.Pipeline()
	for _, key := range hashes {
		memoryByKey[key] = pipe.MemoryUsage(ctx, key)
	}
	// MEMORY USAGE is only an estimate; ACLs or older Redis versions must not
	// prevent conversion.
	_, _ = pipe.Exec(ctx)

	records := make([]metadataRecord, 0, len(hashes))
	for _, key := range hashes {
		metadata := &cache.FSMetadata{}
		if err := cache.ToStruct(metadataByKey[key].Val(), metadata); err != nil {
			result.Invalid++
			continue
		}
		encoded, err := cache.MarshalFSMetadata(metadata)
		if err != nil {
			result.Invalid++
			continue
		}
		memory := memoryByKey[key].Val()
		score := int64(max(metadata.Mtime, metadata.Ctime))
		if score <= 0 || score > time.Now().Unix() {
			score = time.Now().Unix()
		}
		result.Legacy++
		result.LegacyBytes += memory
		result.CompactBytes += int64(len(encoded))
		records = append(records, metadataRecord{
			key:     key,
			id:      strings.TrimPrefix(key, cache.MetadataKeys.MetadataFsNode("")),
			gen:     fmt.Sprint(metadata.Gen),
			encoded: encoded,
			score:   score,
		})
	}

	if !apply || len(records) == 0 {
		return nil
	}

	commands := make([]*redis.Cmd, 0, len(records))
	pipe = rdb.Pipeline()
	for _, record := range records {
		commands = append(commands, replaceMetadata.EvalSha(
			ctx,
			pipe,
			[]string{
				record.key,
				cache.MetadataKeys.MetadataFsNodeData(record.id),
				cache.MetadataKeys.MetadataFsNodeIndex(record.id),
			},
			record.gen,
			record.encoded,
			record.score,
			record.id,
		))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return err
	}
	for _, command := range commands {
		switch command.Val() {
		case int64(1):
			result.Converted++
		case int64(-1):
			result.Changed++
		}
	}
	return nil
}

func compactNodeCount(ctx context.Context, rdb *common.RedisClient) (int64, error) {
	commands := make([]*redis.IntCmd, cache.FSMetadataShardCount)
	pipe := rdb.Pipeline()
	for shard := range cache.FSMetadataShardCount {
		commands[shard] = pipe.HLen(ctx, cache.MetadataKeys.MetadataFsNodeDataShard(shard))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return 0, err
	}
	var count int64
	for _, command := range commands {
		count += command.Val()
	}
	return count, nil
}

func exit(action string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", action, err)
	os.Exit(1)
}
