package bot

import (
	"context"
	"encoding/json"
	"time"

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
		return nil, err
	}

	session := &BotSession{}
	err = json.Unmarshal([]byte(jsonData), session)
	if err != nil {
		return nil, err
	}

	return session, nil
}

func (m *botStateManager) updateSession(workspaceName, stubId, sessionId string, state *BotSession) error {
	ctx := context.TODO()
	err := m.lock.Acquire(ctx, Keys.botLock(workspaceName, stubId, sessionId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer m.lock.Release(Keys.botLock(workspaceName, stubId, sessionId))
	stateKey := Keys.botSessionState(workspaceName, stubId, sessionId)

	exists, err := m.rdb.Exists(ctx, stateKey).Result()
	if err != nil {
		return err
	}

	// State not found, add to index
	if exists == 0 {
		indexKey := Keys.botSessionIndex(workspaceName, stubId)
		err = m.rdb.SAdd(ctx, indexKey, sessionId).Err()
		if err != nil {
			return err
		}
	}

	state.LastUpdatedAt = time.Now().Unix()

	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}

	err = m.rdb.Set(ctx, stateKey, jsonData, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func (m *botStateManager) sessionKeepAlive(workspaceName, stubId, sessionId string) error {
	ctx := context.TODO()
	err := m.lock.Acquire(ctx, Keys.botLock(workspaceName, stubId, sessionId), common.RedisLockOptions{TtlS: 10, Retries: 0})
	if err != nil {
		return err
	}
	defer m.lock.Release(Keys.botLock(workspaceName, stubId, sessionId))

	stateKey := Keys.botSessionState(workspaceName, stubId, sessionId)

	data, err := m.rdb.Get(ctx, stateKey).Result()
	if err != nil {
		return err
	}

	state := &BotSession{}
	err = json.Unmarshal([]byte(data), state)
	if err != nil {
		return err
	}
	state.LastUpdatedAt = time.Now().Unix()

	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}

	err = m.rdb.Set(ctx, stateKey, jsonData, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

func (m *botStateManager) deleteSession(workspaceName, stubId, sessionId string) error {
	stateKey := Keys.botSessionState(workspaceName, stubId, sessionId)
	indexKey := Keys.botSessionIndex(workspaceName, stubId)

	err := m.rdb.Del(context.TODO(), stateKey).Err()
	if err != nil {
		return err
	}

	return m.rdb.SRem(context.TODO(), indexKey, sessionId).Err()
}

func (m *botStateManager) getActiveSessions(workspaceName, stubId string) ([]*BotSession, error) {
	sessions := []*BotSession{}

	sessionIds, err := m.rdb.SMembers(context.TODO(), Keys.botSessionIndex(workspaceName, stubId)).Result()
	if err != nil {
		return nil, err
	}

	for _, sessionId := range sessionIds {
		state, err := m.loadSession(workspaceName, stubId, sessionId)
		if err != nil {
			return nil, err
		}

		sessions = append(sessions, state)
	}

	return sessions, nil
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

func (m *botStateManager) pushTask(workspaceName, stubId, sessionId, transitionName string, markers []Marker) error {
	taskKey := Keys.botTransitionTasks(workspaceName, stubId, sessionId, transitionName)
	jsonData, err := json.Marshal(markers)
	if err != nil {
		return err
	}

	return m.rdb.RPush(context.TODO(), taskKey, jsonData).Err()
}

func (m *botStateManager) popTask(workspaceName, stubId, sessionId, transitionName string) ([]Marker, error) {
	bytes, err := m.rdb.LPop(context.TODO(), Keys.botTransitionTasks(workspaceName, stubId, sessionId, transitionName)).Bytes()
	if err != nil {
		return nil, err
	}

	markers := []Marker{}
	err = json.Unmarshal(bytes, &markers)
	if err != nil {
		return nil, err
	}

	return markers, nil
}

func (m *botStateManager) pushInputMessage(workspaceName, stubId, sessionId, msg string) error {
	messageKey := Keys.botInputBuffer(workspaceName, stubId, sessionId)
	return m.rdb.RPush(context.TODO(), messageKey, msg).Err()
}

func (m *botStateManager) popInputMessage(workspaceName, stubId, sessionId string) (string, error) {
	return m.rdb.LPop(context.TODO(), Keys.botInputBuffer(workspaceName, stubId, sessionId)).Result()
}

func (m *botStateManager) pushEvent(workspaceName, stubId, sessionId string, event *BotEvent) error {
	jsonData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	messageKey := Keys.botEventBuffer(workspaceName, stubId, sessionId)
	return m.rdb.RPush(context.TODO(), messageKey, jsonData).Err()
}

func (m *botStateManager) popEvent(workspaceName, stubId, sessionId string) (*BotEvent, error) {
	val, err := m.rdb.LPop(context.TODO(), Keys.botEventBuffer(workspaceName, stubId, sessionId)).Result()
	if err != nil {
		return nil, err
	}

	event := &BotEvent{}
	err = json.Unmarshal([]byte(val), event)
	if err != nil {
		return nil, err
	}

	return event, nil
}

func (m *botStateManager) acquireLock(workspaceName, stubId, sessionId string) error {
	return m.lock.Acquire(context.TODO(), Keys.botLock(workspaceName, stubId, sessionId), common.RedisLockOptions{TtlS: 10, Retries: 0})
}

func (m *botStateManager) releaseLock(workspaceName, stubId, sessionId string) error {
	return m.lock.Release(Keys.botLock(workspaceName, stubId, sessionId))
}
