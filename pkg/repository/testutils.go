package repository

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
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

// RedisCommandCounts counts what a test Redis is asked to do between Start and
// Stop: every command the server runs, the ones a script issues among them (as
// Redis's commandstats counts them), and the round trips the client makes. A
// pipeline or a script is one round trip.
type RedisCommandCounts struct {
	mu         sync.Mutex
	counting   bool
	commands   map[string]int
	roundTrips int
}

// NewRedisClientForTestWithCounts is NewRedisClientForTest with a counter of
// the commands and round trips the client's Redis sees.
func NewRedisClientForTestWithCounts() (*common.RedisClient, *RedisCommandCounts, error) {
	s, err := miniredis.Run()
	if err != nil {
		return nil, nil, err
	}

	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{s.Addr()}, Mode: types.RedisModeSingle})
	if err != nil {
		return nil, nil, err
	}

	counts := &RedisCommandCounts{}
	s.Server().SetPreHook(func(_ *server.Peer, cmd string, _ ...string) bool {
		switch cmd {
		case "HELLO", "CLIENT", "AUTH", "SELECT":
			// What a new connection says first is not the caller's work.
			return false
		}
		counts.mu.Lock()
		if counts.counting {
			counts.commands[cmd]++
		}
		counts.mu.Unlock()
		return false
	})
	rdb.AddHook(counts)
	return rdb, counts, nil
}

// Start begins a new count.
func (c *RedisCommandCounts) Start() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counting, c.commands, c.roundTrips = true, map[string]int{}, 0
}

// Stop ends the count and returns the commands the server ran, by name, and
// the round trips the client made.
func (c *RedisCommandCounts) Stop() (map[string]int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counting = false
	return c.commands, c.roundTrips
}

func (c *RedisCommandCounts) roundTrip() {
	c.mu.Lock()
	if c.counting {
		c.roundTrips++
	}
	c.mu.Unlock()
}

func (c *RedisCommandCounts) DialHook(next redis.DialHook) redis.DialHook { return next }

func (c *RedisCommandCounts) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		c.roundTrip()
		return next(ctx, cmd)
	}
}

func (c *RedisCommandCounts) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		c.roundTrip()
		return next(ctx, cmds)
	}
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
