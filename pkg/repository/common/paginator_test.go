package common

import (
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Masterminds/squirrel"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
)

func TestStructToMap(t *testing.T) {
	type args struct {
		obj interface{}
	}

	MockCreatedAtDatetime, err := time.Parse("2006-01-02 15:04:05.999999 -0700 MST", "2021-01-01 00:00:00.000000 +0000 UTC")
	if err != nil {
		t.Errorf("Failed to parse datetime: %v", err)
	}

	tests := []struct {
		name string
		args args
		want map[string]interface{}
	}{
		{
			name: "test-task-struct-to-map",
			args: args{
				obj: types.Task{
					Id:          1,
					ExternalId:  "test",
					WorkspaceId: 1,
					Status:      "test",
					ContainerId: "1",
					CreatedAt:   types.Time{Time: MockCreatedAtDatetime},
				},
			},
			want: map[string]interface{}{
				"id":           uint(1),
				"external_id":  "test",
				"workspace_id": uint(1),
				"status":       types.TaskStatus("test"),
				"container_id": "1",
				"created_at":   types.Time{Time: MockCreatedAtDatetime},
			},
		},
		{
			name: "test-deployment-struct-to-map",
			args: args{
				obj: types.Deployment{
					Id:          1,
					ExternalId:  "test",
					WorkspaceId: 1,
					CreatedAt:   types.Time{Time: MockCreatedAtDatetime},
				},
			},
			want: map[string]interface{}{
				"id":           uint(1),
				"external_id":  "test",
				"workspace_id": uint(1),
				"created_at":   types.Time{Time: MockCreatedAtDatetime},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StructToMap(tt.args.obj)

			for k, v := range tt.want {
				if v != got[k] {
					t.Errorf("Expected %v, got %v", v, got[k])
				}
			}
		})
	}
}

func TestEncodeDecodeCursor(t *testing.T) {
	mockDatetimeCursor := DatetimeCursor{
		Value: time.Now().Format(CursorTimestampFormat),
		Id:    1,
	}
	cursorEncoded := EncodeCursor(mockDatetimeCursor)

	decodedCursor, err := DecodeCursor(cursorEncoded)
	if err != nil {
		t.Errorf("Failed to decode cursor: %v", err)
	}

	assert.Equal(t, mockDatetimeCursor, *decodedCursor)
}

func TestPaginateTasks(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	sqlxDB := sqlx.NewDb(db, "sqlmock")

	// Paginate page with no cursor and should not return a next cursor because the total rows == page size
	tasksMockRows := sqlmock.NewRows(
		[]string{"id", "external_id", "status", "container_id", "started_at", "ended_at", "workspace_id", "stub_id", "created_at", "updated_at"},
	).
		AddRow(1, "test", "test", "1", time.Now(), time.Now(), 1, 1, time.Now(), time.Now()).
		AddRow(2, "test", "test", "1", time.Now(), time.Now(), 1, 1, time.Now(), time.Now()).
		AddRow(3, "test", "test", "1", time.Now(), time.Now(), 1, 1, time.Now(), time.Now())

	mock.ExpectQuery("SELECT \\* FROM tasks t ORDER BY \\w+ (asc|desc) LIMIT \\d+").WillReturnRows(tasksMockRows)

	cursorPaginator, err := Paginate(SquirrelCursorPaginator[types.Task]{
		Client:          sqlxDB,
		SelectBuilder:   squirrel.Select("*").From("tasks t"),
		SortOrder:       "asc",
		SortColumn:      "created_at",
		SortQueryPrefix: "t",
		PageSize:        3,
	}, "")
	if err != nil {
		t.Errorf("Failed to paginate tasks: %v", err)
	}

	assert.Equal(t, 3, len(cursorPaginator.Data))
	assert.Equal(t, "", cursorPaginator.Next)

	// Paginate page with no cursor and should return a next cursor because the total rows > page size
	tasksMockRows = sqlmock.NewRows(
		[]string{"id", "external_id", "status", "container_id", "started_at", "ended_at", "workspace_id", "stub_id", "created_at", "updated_at"},
	).
		AddRow(1, "test", "test", "1", time.Now(), time.Now(), 1, 1, time.Now(), time.Now()).
		AddRow(2, "test", "test", "1", time.Now(), time.Now(), 1, 1, time.Now(), time.Now()).
		AddRow(3, "test", "test", "1", time.Now(), time.Now(), 1, 1, time.Now(), time.Now())

	mock.ExpectQuery("SELECT \\* FROM tasks t ORDER BY \\w+ (asc|desc) LIMIT \\d+").WillReturnRows(tasksMockRows)

	cursorPaginator, err = Paginate(SquirrelCursorPaginator[types.Task]{
		Client:          sqlxDB,
		SelectBuilder:   squirrel.Select("*").From("tasks t"),
		SortOrder:       "asc",
		SortColumn:      "created_at",
		SortQueryPrefix: "t",
		PageSize:        2,
	}, "")

	if err != nil {
		t.Errorf("Failed to paginate tasks: %v", err)
	}

	assert.Equal(t, 2, len(cursorPaginator.Data))
	assert.NotEqual(t, "", cursorPaginator.Next)

	// Encode the cursor for row id 2 (assuming that previous paginate returned next at id 2)
	// Paginate page with cursor and should not return a next cursor because the total rows == page size and there are no more rows after the cursor
	mockDatetimeCursor := DatetimeCursor{
		Value: time.Now().Format(CursorTimestampFormat),
		Id:    2,
	}
	cursorEncoded := EncodeCursor(mockDatetimeCursor)

	tasksMockRows = sqlmock.NewRows(
		[]string{"id", "external_id", "status", "container_id", "started_at", "ended_at", "workspace_id", "stub_id", "created_at", "updated_at"},
	).
		AddRow(2, "test", "test", "1", time.Now(), time.Now(), 1, 1, time.Now(), time.Now()).
		AddRow(3, "test", "test", "1", time.Now(), time.Now(), 1, 1, time.Now(), time.Now())

	mock.ExpectQuery("SELECT \\* FROM tasks t WHERE (.*) ORDER BY \\w+ (asc|desc) LIMIT \\d+").WillReturnRows(tasksMockRows)

	cursorPaginator, err = Paginate(SquirrelCursorPaginator[types.Task]{
		Client:          sqlxDB,
		SelectBuilder:   squirrel.Select("*").From("tasks t"),
		SortOrder:       "asc",
		SortColumn:      "created_at",
		SortQueryPrefix: "t",
		PageSize:        2,
	}, cursorEncoded)

	if err != nil {
		t.Errorf("Failed to paginate tasks: %v", err)
	}

	assert.Equal(t, 2, len(cursorPaginator.Data))
	assert.Equal(t, "", cursorPaginator.Next)

	defer db.Close()
}
