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
	explicitPattern := fmt.Sprintf("%s*", patternPrefix)
	messages, errs, close := kem.rdb.PSubscribe(ctx, keyspacePattern, explicitPattern)

	existingKeys, err := kem.fetchExistingKeys(patternPrefix)
	if err != nil {
		return err
	}

	for _, key := range existingKeys {
		keyEventChan <- KeyEvent{
			Key:       key,
			Operation: KeyOperationSet,
		}
	}

	go func() {
		defer close()

	retry:
		for {
			select {
			case m := <-messages:
				keyEventChan <- kem.messageToKeyEvent(patternPrefix, m.Channel, string(m.Payload))

			case <-ctx.Done():
				return

			case err := <-errs:
				log.Error().Err(err).Msg("error with key manager subscription")
				break retry
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
