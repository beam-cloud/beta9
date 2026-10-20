package repository

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestTaskAggregationRejectsInvalidIntervalBeforeQuery(t *testing.T) {
	for _, interval := range []string{"hour', t.created_at)) UNION SELECT 1 --", "hour'); SELECT pg_sleep(1); --", "invalid", " hour", "hour\x00"} {
		_, err := (&PostgresBackendRepository{}).AggregateTasksByTimeWindow(context.Background(), types.TaskFilter{Interval: interval, WorkspaceID: 1})
		require.ErrorIs(t, err, ErrInvalidTaskInterval)
	}
}

func TestTaskAggregationBindsIntervalAndWorkspace(t *testing.T) {
	for _, interval := range []string{"", "HOUR", "microseconds", "milliseconds", "second", "minute", "day", "week", "month", "quarter", "year", "decade", "century", "millennium"} {
		t.Run(interval, func(t *testing.T) {
			repo, mock := NewBackendPostgresRepositoryForTest()
			t.Cleanup(func() { repo.(*PostgresBackendRepository).client.Close() })
			unit := strings.ToLower(interval)
			if unit == "" {
				unit = "hour"
			}
			mock.ExpectQuery(`(?s)SELECT DATE_TRUNC\(\$1, t.created_at\).*FROM task t WHERE t.workspace_id = \$2 GROUP BY time ORDER BY time DESC`).
				WithArgs(unit, uint(7)).WillReturnRows(sqlmock.NewRows([]string{"time", "count", "status_counts"}))
			_, err := repo.AggregateTasksByTimeWindow(context.Background(), types.TaskFilter{Interval: interval, WorkspaceID: 7})
			require.NoError(t, err)
			require.NoError(t, mock.ExpectationsWereMet())
		})
	}
}
