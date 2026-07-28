package common

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

const (
	keyspacePrefix string = "__keyspace@0__:"
)

type KeyEventManager struct {
	rdb *RedisClient
}

type KeyEvent struct {
	Key       string
	Operation string
}

const (
	KeyOperationHSet    string = "hset"
	KeyOperationSet     string = "set"
	KeyOperationDel     string = "del"
	KeyOperationExpire  string = "expire"
	KeyOperationExpired string = "expired"
)

func NewKeyEventManager(rdb *RedisClient) *KeyEventManager {
	return &KeyEventManager{rdb: rdb}
}

// ListenForPatternEvents watches future keyspace events without replaying
// existing keys. Use it when only expiry/delete events are meaningful.
func (kem *KeyEventManager) ListenForPatternEvents(ctx context.Context, patternPrefix string, keyEventChan chan KeyEvent) error {
	return kem.listenForSubscriptionPattern(ctx, patternPrefix, keyspacePrefix+patternPrefix+"*", keyEventChan, nil)
}

// ListenForContainerPattern replays active container state from its explicit
// index instead of scanning the entire Redis keyspace.
func (kem *KeyEventManager) ListenForContainerPattern(ctx context.Context, containerPrefix string, keyEventChan chan KeyEvent) error {
	patternPrefix := RedisKeys.SchedulerContainerState(containerPrefix)
	return kem.listenForSubscriptionPattern(ctx, patternPrefix, keyspacePrefix+patternPrefix+"*", keyEventChan, func() ([]string, error) {
		now := fmt.Sprint(time.Now().Unix())
		pipe := kem.rdb.TxPipeline()
		active := pipe.ZRangeByScore(ctx, RedisKeys.SchedulerContainerStateIndex(), &redis.ZRangeBy{
			Min: now,
			Max: "+inf",
		})
		pipe.ZRemRangeByScore(ctx, RedisKeys.SchedulerContainerStateIndex(), "-inf", now)
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, err
		}
		keys := active.Val()
		existing := make([]string, 0, len(keys))
		for _, key := range keys {
			if strings.HasPrefix(key, patternPrefix) {
				existing = append(existing, strings.TrimPrefix(key, patternPrefix))
			}
		}
		return existing, nil
	})
}

func (kem *KeyEventManager) ListenForPublishedKey(ctx context.Context, key string, keyEventChan chan KeyEvent) error {
	return kem.listenForSubscriptionPattern(ctx, key, key, keyEventChan, func() ([]string, error) {
		exists, err := kem.rdb.Exists(ctx, key).Result()
		if err != nil || exists == 0 {
			return nil, err
		}
		return []string{""}, nil
	})
}

// ListenForKey watches one exact key without scanning the database. Subscribing
// before checking existence ensures a write cannot be missed between the two.
func (kem *KeyEventManager) ListenForKey(ctx context.Context, key string, keyEventChan chan KeyEvent) error {
	return kem.listenForSubscriptionPattern(ctx, key, keyspacePrefix+key, keyEventChan, func() ([]string, error) {
		exists, err := kem.rdb.Exists(ctx, key).Result()
		if err != nil || exists == 0 {
			return nil, err
		}
		return []string{""}, nil
	})
}

func (kem *KeyEventManager) listenForSubscriptionPattern(
	ctx context.Context,
	patternPrefix string,
	pattern string,
	keyEventChan chan KeyEvent,
	existingKeys func() ([]string, error),
) error {
	messages, errs, close := kem.rdb.PSubscribe(ctx, pattern)

	if existingKeys != nil {
		keys, err := existingKeys()
		if err != nil {
			close()
			return err
		}
		for _, key := range keys {
			select {
			case keyEventChan <- KeyEvent{
				Key:       key,
				Operation: KeyOperationSet,
			}:
			case <-ctx.Done():
				close()
				return ctx.Err()
			}
		}
	}

	go func() {
		defer close()

		for {
			select {
			case m, ok := <-messages:
				if !ok || m == nil {
					return
				}
				select {
				case keyEventChan <- kem.messageToKeyEvent(patternPrefix, m.Channel, string(m.Payload)):
				case <-ctx.Done():
					return
				}

			case <-ctx.Done():
				return

			case err, ok := <-errs:
				if ok && err != nil {
					log.Error().Err(err).Msg("error with key manager subscription")
				}
				return
			}
		}
	}()

	return nil
}

func (kem *KeyEventManager) messageToKeyEvent(patternPrefix, channel, payload string) KeyEvent {
	if strings.HasPrefix(channel, keyspacePrefix) {
		return KeyEvent{
			Key:       strings.TrimPrefix(channel, fmt.Sprintf("%s%s", keyspacePrefix, patternPrefix)),
			Operation: payload,
		}
	}

	operation := payload
	if operation == "" {
		operation = KeyOperationSet
	}
	return KeyEvent{
		Key:       strings.TrimPrefix(channel, patternPrefix),
		Operation: operation,
	}
}
