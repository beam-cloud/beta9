package bot

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/redis/go-redis/v9"
)

type botStateManager struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func newBotStateManager(rdb *common.RedisClient) *botStateManager {
	lock := common.NewRedisLock(rdb)
	return &botStateManager{
		rdb:  rdb,
		lock: lock,
	}
}

func (m *botStateManager) loadSession(workspaceName, stubId, sessionId string) (*BotSession, error) {
	err := m.lock.Acquire(context.TODO(), Keys.botSessionStateLock(workspaceName, stubId, sessionId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return nil, err
	}
	defer m.lock.Release(Keys.botSessionStateLock(workspaceName, stubId, sessionId))

	stateKey := Keys.botSessionState(workspaceName, stubId, sessionId)
	jsonData, err := m.rdb.Get(context.TODO(), stateKey).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrBotSessionNotFound
		}
		return nil, fmt.Errorf("failed to retrieve session state: %v", err)
	}

	session := &BotSession{}
	err = json.Unmarshal([]byte(jsonData), session)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize session state: %v", err)
	}

	return session, nil
}

func (m *botStateManager) updateSession(workspaceName, stubId, sessionId string, state *BotSession) error {
	err := m.lock.Acquire(context.TODO(), Keys.botSessionStateLock(workspaceName, stubId, sessionId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer m.lock.Release(Keys.botSessionStateLock(workspaceName, stubId, sessionId))

	jsonData, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to serialize session state: %v", err)
	}

	stateKey := Keys.botSessionState(workspaceName, stubId, sessionId)

	err = m.rdb.Set(context.TODO(), stateKey, jsonData, 0).Err()
	if err != nil {
		return fmt.Errorf("failed to store session state: %v", err)
	}

	return nil
}

func (m *botStateManager) deleteSession(workspaceName, stubId, sessionId string) error {
	stateKey := Keys.botSessionState(workspaceName, stubId, sessionId)
	return m.rdb.Del(context.TODO(), stateKey).Err()
}

// func (m *botStateManager) addMarkerToLocation() error {
// 	return nil
// }
