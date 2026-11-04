package repository

import (
	"errors"
	"github.com/lib/pq"
)

// Server-reported data/constraint errors and transaction rollbacks have no
// committed effects. Never replay an ambiguous transport or commit failure.
func retryVMBatchIndividually(err error) bool {
	var pgErr *pq.Error
	return errors.As(err, &pgErr) && (pgErr.Code.Class() == "22" || pgErr.Code.Class() == "23" || pgErr.Code.Class() == "40")
}
