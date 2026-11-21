package main

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func TestDeleteBatchesKeepsTokensThatChangedBeforeTheDelete(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := sqlx.NewDb(db, "postgres").Connx(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	unbound := purges(time.Unix(0, 0))[1]
	selectBatch := regexp.QuoteMeta(`SELECT id FROM token WHERE id > $1 AND ` + unbound.cond + ` ORDER BY id LIMIT 2`)
	mock.ExpectQuery(selectBatch).
		WithArgs(int64(0), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(4).AddRow(9))
	// Token 9 was bound to a worker after the select, so only token 4 matches.
	mock.ExpectExec(regexp.QuoteMeta(`DELETE FROM token WHERE id = ANY($1) AND `+unbound.cond)).
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery(selectBatch).
		WithArgs(int64(9), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	deleted, batches, err := deleteBatches(context.Background(), conn, unbound, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != 1 || batches != 1 {
		t.Fatalf("deleted %d in %d batches, want 1 in 1", deleted, batches)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
