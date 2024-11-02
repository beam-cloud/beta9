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
	err := m.lock.Acquire(context.TODO(), Keys.botLock(workspaceName, stubId, sessionId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return nil, err
	}
	defer m.lock.Release(Keys.botLock(workspaceName, stubId, sessionId))

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
	err := m.lock.Acquire(context.TODO(), Keys.botLock(workspaceName, stubId, sessionId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer m.lock.Release(Keys.botLock(workspaceName, stubId, sessionId))

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

func (m *botStateManager) pushMarker(workspaceName, stubId, sessionId, locationName string, markerData Marker) error {
	markerKey := Keys.botMarkers(workspaceName, stubId, sessionId, locationName)
	jsonData, err := json.Marshal(markerData)
	if err != nil {
		return err
	}

	return m.rdb.RPush(context.TODO(), markerKey, jsonData).Err()
}

func (m *botStateManager) popMarker(workspaceName, stubId, sessionId, locationName string) (*Marker, error) {
	bytes, err := m.rdb.LPop(context.TODO(), Keys.botMarkers(workspaceName, stubId, sessionId, locationName)).Bytes()
	if err != nil {
		return nil, err
	}

	marker := &Marker{}
	err = json.Unmarshal(bytes, marker)
	if err != nil {
		return nil, err
	}

	return marker, nil
}

func (m *botStateManager) countMarkers(workspaceName, stubId, sessionId, locationName string) (int64, error) {
	return m.rdb.LLen(context.TODO(), Keys.botMarkers(workspaceName, stubId, sessionId, locationName)).Result()
}

func (m *botStateManager) pushTask(workspaceName, stubId, sessionId, locationName string, markerData Marker) error {
	return nil
}

func (m *botStateManager) popTask(workspaceName, stubId, sessionId, locationName string, markerData Marker) error {
	return nil
}

func (m *botStateManager) deleteSession(workspaceName, stubId, sessionId string) error {
	stateKey := Keys.botSessionState(workspaceName, stubId, sessionId)
	return m.rdb.Del(context.TODO(), stateKey).Err()
}

func (m *botStateManager) acquireLock(workspaceName, stubId, sessionId string) error {
	return m.lock.Acquire(context.TODO(), Keys.botLock(workspaceName, stubId, sessionId), common.RedisLockOptions{TtlS: 10, Retries: 0})
}

func (m *botStateManager) releaseLock(workspaceName, stubId, sessionId string) error {
	return m.lock.Release(Keys.botLock(workspaceName, stubId, sessionId))
}
