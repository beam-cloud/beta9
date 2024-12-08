package bot

import (
	"context"
	"encoding/json"
	"fmt"
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
		state.CreatedAt = time.Now().Unix()

		indexKey := Keys.botSessionIndex(workspaceName, stubId)
		err = m.rdb.SAdd(ctx, indexKey, sessionId).Err()
		if err != nil {
			return err
		}
	}

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

func (m *botStateManager) sessionKeepAlive(workspaceName, stubId, sessionId string, ttlS uint) error {
	return m.rdb.Set(context.TODO(), Keys.botSessionKeepAlive(workspaceName, stubId, sessionId), 1, time.Duration(ttlS)*time.Second).Err()
}

func (m *botStateManager) deleteSession(workspaceName, stubId, sessionId string) error {
	ctx := context.TODO()
	stateKey := Keys.botSessionState(workspaceName, stubId, sessionId)
	indexKey := Keys.botSessionIndex(workspaceName, stubId)
	taskIndexKey := Keys.botTaskIndex(workspaceName, stubId, sessionId)
	historyKey := Keys.botEventHistory(workspaceName, stubId, sessionId)

	err := m.rdb.Del(ctx, stateKey).Err()
	if err != nil {
		return err
	}

	taskKeys, err := m.rdb.SMembers(ctx, taskIndexKey).Result()
	if err != nil {
		return err
	}

	for _, taskKey := range taskKeys {
		err = m.rdb.Del(ctx, taskKey).Err()
		if err != nil {
			return err
		}
	}

	err = m.rdb.Del(ctx, taskIndexKey).Err()
	if err != nil {
		return err
	}

	err = m.rdb.Del(ctx, historyKey).Err()
	if err != nil {
		return err
	}

	// TODO: delete all existing markers
	return m.rdb.SRem(ctx, indexKey, sessionId).Err()
}

func (m *botStateManager) listSessions(workspaceName, stubId string) ([]*BotSession, error) {
	ctx := context.TODO()
	sessions := []*BotSession{}

	sessionIds, err := m.rdb.SMembers(ctx, Keys.botSessionIndex(workspaceName, stubId)).Result()
	if err != nil {
		return nil, err
	}

	for _, sessionId := range sessionIds {
		session, err := m.loadSession(workspaceName, stubId, sessionId)
		if err != nil {
			return nil, err
		}

		sessions = append(sessions, session)
	}

	return sessions, nil
}

func (m *botStateManager) getSession(workspaceName, stubId, sessionId string) (*BotSession, error) {
	var session *BotSession

	session, err := m.loadSession(workspaceName, stubId, sessionId)
	if err != nil {
		return nil, err
	}

	historyKey := Keys.botEventHistory(workspaceName, stubId, sessionId)
	history, err := m.rdb.LRange(context.TODO(), historyKey, 0, -1)
	if err != nil {
		return nil, err
	}

	events := []*BotEvent{}
	for _, jsonData := range history {
		event := &BotEvent{}
		err = json.Unmarshal([]byte(jsonData), event)
		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	session.EventHistory = events
	return session, nil
}

func (m *botStateManager) getActiveSessions(workspaceName, stubId string) ([]*BotSession, error) {
	ctx := context.TODO()
	sessions := []*BotSession{}

	sessionIds, err := m.rdb.SMembers(ctx, Keys.botSessionIndex(workspaceName, stubId)).Result()
	if err != nil {
		return nil, err
	}

	for _, sessionId := range sessionIds {
		if m.rdb.Exists(ctx, Keys.botSessionKeepAlive(workspaceName, stubId, sessionId)).Val() == 0 {
			continue
		}

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

func (m *botStateManager) pushTask(workspaceName, stubId, sessionId, transitionName, taskId string, markers []Marker) error {
	indexKey := Keys.botTaskIndex(workspaceName, stubId, sessionId)
	taskKey := Keys.botTransitionTask(workspaceName, stubId, sessionId, transitionName, taskId)

	jsonData, err := json.Marshal(markers)
	if err != nil {
		return err
	}

	err = m.rdb.Set(context.TODO(), taskKey, jsonData, 0).Err()
	if err != nil {
		return err
	}

	return m.rdb.SAdd(context.TODO(), indexKey, taskKey).Err()
}

func (m *botStateManager) popTask(workspaceName, stubId, sessionId, transitionName, taskId string) ([]Marker, error) {
	bytes, err := m.rdb.Get(context.TODO(), Keys.botTransitionTask(workspaceName, stubId, sessionId, transitionName, taskId)).Bytes()
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

func (m *botStateManager) pushUserEvent(workspaceName, stubId, sessionId string, event *BotEvent) error {
	jsonData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	historyKey := Keys.botEventHistory(workspaceName, stubId, sessionId)
	err = m.rdb.RPush(context.TODO(), historyKey, jsonData).Err()
	if err != nil {
		return err
	}

	messageKey := Keys.botInputBuffer(workspaceName, stubId, sessionId)
	return m.rdb.RPush(context.TODO(), messageKey, jsonData).Err()
}

func (m *botStateManager) popUserEvent(workspaceName, stubId, sessionId string) (*BotEvent, error) {
	val, err := m.rdb.LPop(context.TODO(), Keys.botInputBuffer(workspaceName, stubId, sessionId)).Result()
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

func (m *botStateManager) pushEvent(workspaceName, stubId, sessionId string, event *BotEvent) error {
	jsonData, err := json.Marshal(event)
	if err != nil {
		return err
	}

	messageKey := Keys.botEventBuffer(workspaceName, stubId, sessionId)
	historyKey := Keys.botEventHistory(workspaceName, stubId, sessionId)

	if event.Type != BotEventTypeNetworkState {
		err = m.rdb.RPush(context.TODO(), historyKey, jsonData).Err()
		if err != nil {
			return err
		}
	}

	return m.rdb.RPush(context.TODO(), messageKey, jsonData).Err()
}

const eventPairTtlS = 120 * time.Second

func (m *botStateManager) pushEventPair(workspaceName, stubId, sessionId, pairId string, request *BotEvent, response *BotEvent) error {
	eventPairKey := Keys.botEventPair(workspaceName, stubId, sessionId, pairId)
	eventPair := &BotEventPair{
		Request:  request,
		Response: response,
	}

	jsonData, err := json.Marshal(eventPair)
	if err != nil {
		return err
	}

	return m.rdb.Set(context.TODO(), eventPairKey, jsonData, eventPairTtlS).Err()
}

func (m *botStateManager) waitForEventPair(ctx context.Context, workspaceName, stubId, sessionId, pairId string) (*BotEventPair, error) {
	for {
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("timeout reached while waiting for event pair")
		case <-time.After(100 * time.Millisecond):
			jsonData, err := m.rdb.Get(context.TODO(), Keys.botEventPair(workspaceName, stubId, sessionId, pairId)).Result()
			if err == nil {
				eventPair := &BotEventPair{}
				err = json.Unmarshal([]byte(jsonData), eventPair)
				if err != nil {
					return nil, err
				}

				return eventPair, nil
			}
		}
	}
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
