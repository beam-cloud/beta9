package bot

import (
	"log"

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

func (m *botStateManager) addMarkerToLocation() error {
	log.Println("addMarkerToLocation")
	return nil
}
