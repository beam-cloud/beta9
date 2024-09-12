package bot

import (
	"github.com/beam-cloud/beta9/pkg/common"
)

type botStateManager struct {
	rdb *common.RedisClient
}

func newBotStateManager(rdb *common.RedisClient) *botStateManager {
	return &botStateManager{
		rdb: rdb,
	}
}

func (m *botStateManager) updateSessionMemory(sessionId string) error {
	return nil
}

func (m *botStateManager) getSessionMemory(sessionId string) error {
	return nil
}

func (m *botStateManager) deleteSessionMemory(sessionId string) error {
	return nil
}

func (m *botStateManager) addMarkerToLocation() error {
	return nil
}
