package common

import (
	"context"
	"fmt"
	"strings"

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

func NewKeyEventManager(rdb *RedisClient) (*KeyEventManager, error) {
	return &KeyEventManager{rdb: rdb}, nil
}

func (kem *KeyEventManager) TrimKeyspacePrefix(key string) string {
	return strings.TrimPrefix(key, keyspacePrefix)
}

func (kem *KeyEventManager) fetchExistingKeys(patternPrefix string) ([]string, error) {
	pattern := fmt.Sprintf("%s*", patternPrefix)

	keys, err := kem.rdb.Scan(context.Background(), pattern)
	if err != nil {
		return nil, err
	}

	trimmedKeys := make([]string, len(keys))
	for i, key := range keys {
		trimmedKeys[i] = strings.TrimPrefix(key, patternPrefix)
	}

	return trimmedKeys, nil
}

func (kem *KeyEventManager) ListenForPattern(ctx context.Context, patternPrefix string, keyEventChan chan KeyEvent) error {
	keyspacePattern := fmt.Sprintf("%s%s*", keyspacePrefix, patternPrefix)
	return kem.listenForSubscriptionPattern(ctx, patternPrefix, keyspacePattern, keyEventChan, func() ([]string, error) {
		return kem.fetchExistingKeys(patternPrefix)
	})
}

func (kem *KeyEventManager) ListenForPublishedPattern(ctx context.Context, patternPrefix string, keyEventChan chan KeyEvent) error {
	pattern := fmt.Sprintf("%s*", patternPrefix)
	return kem.listenForSubscriptionPattern(ctx, patternPrefix, pattern, keyEventChan, func() ([]string, error) {
		return kem.fetchExistingKeys(patternPrefix)
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

	keys, err := existingKeys()
	if err != nil {
		close()
		return err
	}

	for _, key := range keys {
		keyEventChan <- KeyEvent{
			Key:       key,
			Operation: KeyOperationSet,
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
