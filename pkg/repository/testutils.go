package repository

import (
	"log"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
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
		AddWorkerTimeout: time.Duration(time.Minute * 10),
	}
	return &WorkerRedisRepository{rdb: rdb, lock: lock, config: config}
}

func NewContainerRedisRepositoryForTest(rdb *common.RedisClient) ContainerRepository {
	lock := common.NewRedisLock(rdb)
	return &ContainerRedisRepository{rdb: rdb, lock: lock}
}

func NewBackendPostgresRepositoryForTest() (BackendRepository, sqlmock.Sqlmock) {
	mockDB, mock, err := sqlmock.New()
	if err != nil {
		log.Fatalf("error creating mock db: %v", err)
	}
	sqlxDB := sqlx.NewDb(mockDB, "sqlmock")

	return &PostgresBackendRepository{
		client: sqlxDB,
	}, mock
}

type MockHttpDetails struct {
	BackendRepo  BackendRepository
	Mock         sqlmock.Sqlmock
	TokenForTest types.Token
}

func (m *MockHttpDetails) AddExpectedDBTokenSelect(
	mock sqlmock.Sqlmock,
	workspace types.Workspace,
	token types.Token,
) {
	mock.ExpectQuery("SELECT (.+) FROM token").
		WillReturnRows(
			sqlmock.NewRows(
				[]string{"id", "external_id", "key", "active", "reusable", "workspace_id", "workspace.external_id", "token_type", "created_at", "updated_at"},
			).AddRow(
				token.Id,
				token.ExternalId,
				token.Key,
				token.Active,
				token.Reusable,
				token.WorkspaceId,
				workspace.ExternalId,
				token.TokenType,
				token.CreatedAt,
				token.UpdatedAt,
			),
		)
}

func MockBackendWithValidToken() MockHttpDetails {
	backendRepo, mock := NewBackendPostgresRepositoryForTest()
	workspaceForTest := types.Workspace{
		Id:         1,
		ExternalId: "test",
		Name:       "test",
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
	}

	tokenForTest := types.Token{
		Id:          1,
		ExternalId:  "test",
		Key:         "test",
		Active:      true,
		Reusable:    true,
		WorkspaceId: &workspaceForTest.Id,
		Workspace:   &workspaceForTest,
		TokenType:   types.TokenTypeWorkspace,
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
	}

	return MockHttpDetails{
		BackendRepo:  backendRepo,
		Mock:         mock,
		TokenForTest: tokenForTest,
	}
}
