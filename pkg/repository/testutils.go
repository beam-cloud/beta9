package repository

import (
	"os"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

func NewRedisClientForTest() (*common.RedisClient, error) {
	s, err := miniredis.Run()
	if err != nil {
		return nil, err
	}

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		return nil, err
	}

	return rdb, nil
}

func NewWorkerRedisRepositoryForTest(rdb *common.RedisClient) WorkerRepository {
	lock := common.NewRedisLock(rdb)
	config := types.WorkerConfig{
		CleanupPendingWorkerAgeLimit: time.Duration(time.Minute * 10),
	}
	return &WorkerRedisRepository{rdb: rdb, lock: lock, config: config}
}

func NewWorkerPoolRedisRepositoryForTest(rdb *common.RedisClient) WorkerPoolRepository {
	lock := common.NewRedisLock(rdb)
	return &WorkerPoolRedisRepository{rdb: rdb, lock: lock}
}

func NewContainerRedisRepositoryForTest(rdb *common.RedisClient) ContainerRepository {
	lock := common.NewRedisLock(rdb)
	return &ContainerRedisRepository{rdb: rdb, lock: lock}
}

func NewWorkspaceRedisRepositoryForTest(rdb *common.RedisClient) WorkspaceRepository {
	return &WorkspaceRedisRepository{rdb: rdb}
}

func NewProviderRedisRepositoryForTest(rdb *common.RedisClient) ProviderRepository {
	lock := common.NewRedisLock(rdb)
	lockOptions := common.RedisLockOptions{TtlS: 10, Retries: 0}
	return &ProviderRedisRepository{rdb: rdb, lock: lock, lockOptions: lockOptions}
}

func NewBackendPostgresRepositoryForTest() (BackendRepository, sqlmock.Sqlmock) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		log.Error().Err(err).Msg("error creating mock db")
		os.Exit(1)
	}
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	return &PostgresBackendRepository{
		client: sqlxDB,
		config: types.PostgresConfig{
			EncryptionKey: "sk_pKz38fK8v7lz01AneJI8MJnR70akmP2CtDNf1IufKcY=",
		},
	}, mock
}
