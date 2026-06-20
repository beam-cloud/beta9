package repository

import (
	"context"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	redis "github.com/redis/go-redis/v9"
)

var acquireEndpointRequestTokenScript = redis.NewScript(`
local max_tokens = tonumber(ARGV[1])
local ttl_seconds = tonumber(ARGV[2])
local current = redis.call("GET", KEYS[1])
if current == false then
  current = max_tokens
else
  current = tonumber(current)
end
if current <= 0 then
  redis.call("SET", KEYS[1], current, "EX", ttl_seconds)
  return -1
end
local next = current - 1
redis.call("SET", KEYS[1], next, "EX", ttl_seconds)
return next
`)

var releaseEndpointRequestTokenScript = redis.NewScript(`
local max_tokens = tonumber(ARGV[1])
local ttl_seconds = tonumber(ARGV[2])
local use_release_key = ARGV[3] == "1"

if use_release_key then
  local created = redis.call("SET", KEYS[2], 1, "NX", "EX", ttl_seconds)
  if created == false then
    return -1
  end
end

local current = redis.call("GET", KEYS[1])
if current == false then
  current = max_tokens
else
  current = tonumber(current)
end
if current >= max_tokens then
  redis.call("SET", KEYS[1], max_tokens, "EX", ttl_seconds)
  return max_tokens
end
local next = current + 1
redis.call("SET", KEYS[1], next, "EX", ttl_seconds)
return next
`)

func (c *ContainerRedisRepository) GetEndpointRequestTokens(ctx context.Context, workspaceName, stubId, containerId string, maxTokens int, ttl time.Duration) (int, error) {
	maxTokens = normalizeEndpointRequestMaxTokens(maxTokens)

	key, ok := endpointRequestTokensKey(workspaceName, stubId, containerId)
	if !ok {
		return maxTokens, nil
	}

	val, err := c.rdb.Get(ctx, key).Int()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	if err == redis.Nil {
		created, err := c.rdb.SetNX(ctx, key, maxTokens, ttl).Result()
		if err != nil {
			return 0, err
		}
		if created {
			return maxTokens, nil
		}

		val, err = c.rdb.Get(ctx, key).Int()
		if err != nil {
			return 0, err
		}
	}

	if val > maxTokens {
		val = maxTokens
		if err := c.rdb.SetEx(ctx, key, maxTokens, ttl).Err(); err != nil {
			return 0, err
		}
	}
	if val < 0 {
		return 0, nil
	}
	return val, nil
}

func (c *ContainerRedisRepository) AcquireEndpointRequestToken(ctx context.Context, workspaceName, stubId, containerId string, maxTokens int, ttl time.Duration) (bool, error) {
	key, ok := endpointRequestTokensKey(workspaceName, stubId, containerId)
	if !ok {
		return true, nil
	}

	result, err := acquireEndpointRequestTokenScript.Run(
		ctx,
		c.rdb,
		[]string{key},
		normalizeEndpointRequestMaxTokens(maxTokens),
		endpointRequestTokenTTLSeconds(ttl),
	).Int64()
	if err != nil {
		return false, err
	}

	// A negative result means the shared bucket had no available request
	// tokens. The script checks capacity before decrementing, so no
	// compensating increment is needed here.
	return result >= 0, nil
}

func (c *ContainerRedisRepository) ReleaseEndpointRequestToken(ctx context.Context, workspaceName, stubId, containerId, taskId string, maxTokens int, ttl time.Duration) error {
	key, ok := endpointRequestTokensKey(workspaceName, stubId, containerId)
	if !ok {
		return nil
	}

	keys := []string{key}
	useReleaseKey := 0
	if taskId != "" {
		keys = append(keys, common.RedisKeys.EndpointRequestRelease(workspaceName, stubId, taskId, containerId))
		useReleaseKey = 1
	}

	if err := releaseEndpointRequestTokenScript.Run(
		ctx,
		c.rdb,
		keys,
		normalizeEndpointRequestMaxTokens(maxTokens),
		endpointRequestTokenTTLSeconds(ttl),
		useReleaseKey,
	).Err(); err != nil {
		return err
	}

	if taskId != "" {
		return c.rdb.Del(ctx, common.RedisKeys.EndpointRequestHeartbeat(workspaceName, stubId, taskId, containerId)).Err()
	}
	return nil
}

func (c *ContainerRedisRepository) RefreshEndpointRequestTokenTTL(ctx context.Context, workspaceName, stubId, containerId string, ttl time.Duration) error {
	key, ok := endpointRequestTokensKey(workspaceName, stubId, containerId)
	if !ok {
		return nil
	}
	return c.rdb.Expire(ctx, key, ttl).Err()
}

func (c *ContainerRedisRepository) SetEndpointRequestHeartbeat(ctx context.Context, workspaceName, stubId, taskId, containerId string, ttl time.Duration) error {
	key, ok := endpointRequestHeartbeatKey(workspaceName, stubId, taskId, containerId)
	if !ok {
		return nil
	}
	return c.rdb.Set(ctx, key, 1, ttl).Err()
}

func (c *ContainerRedisRepository) EndpointRequestHeartbeatExists(ctx context.Context, workspaceName, stubId, taskId, containerId string) (bool, error) {
	key, ok := endpointRequestHeartbeatKey(workspaceName, stubId, taskId, containerId)
	if !ok {
		return false, nil
	}

	exists, err := c.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

func endpointRequestTokensKey(workspaceName, stubId, containerId string) (string, bool) {
	if workspaceName == "" || stubId == "" || containerId == "" {
		return "", false
	}
	return common.RedisKeys.EndpointRequestTokens(workspaceName, stubId, containerId), true
}

func endpointRequestHeartbeatKey(workspaceName, stubId, taskId, containerId string) (string, bool) {
	if workspaceName == "" || stubId == "" || taskId == "" || containerId == "" {
		return "", false
	}
	return common.RedisKeys.EndpointRequestHeartbeat(workspaceName, stubId, taskId, containerId), true
}

func normalizeEndpointRequestMaxTokens(maxTokens int) int {
	if maxTokens > 0 {
		return maxTokens
	}
	return 1
}

func endpointRequestTokenTTLSeconds(ttl time.Duration) int {
	if ttl <= 0 {
		return 1
	}
	seconds := int(ttl / time.Second)
	if seconds > 0 {
		return seconds
	}
	return 1
}
