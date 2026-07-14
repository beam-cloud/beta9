package backend_postgres_migrations

import (
	"context"
	"database/sql"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestTaskFailureReasonMigrationTakesLockWithoutWaiting(t *testing.T) {
	tests := []struct {
		name  string
		run   func(context.Context, *sql.Tx) error
		alter string
	}{
		{
			name:  "up",
			run:   upAddTaskFailureReason,
			alter: `ALTER TABLE task ADD COLUMN IF NOT EXISTS failure_reason TEXT NOT NULL DEFAULT '';`,
		},
		{
			name:  "down",
			run:   downAddTaskFailureReason,
			alter: `ALTER TABLE task DROP COLUMN IF EXISTS failure_reason;`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			mock.ExpectBegin()
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			mock.ExpectExec(regexp.QuoteMeta(`LOCK TABLE task IN ACCESS EXCLUSIVE MODE NOWAIT;`)).
				WillReturnResult(sqlmock.NewResult(0, 0))
			mock.ExpectExec(regexp.QuoteMeta(test.alter)).
				WillReturnResult(sqlmock.NewResult(0, 0))
			if err := test.run(context.Background(), tx); err != nil {
				t.Fatal(err)
			}
			mock.ExpectCommit()
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
