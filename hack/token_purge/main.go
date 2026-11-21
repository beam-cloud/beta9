// token_purge deletes tokens that can no longer authenticate: rows of token
// types the gateway no longer issues, and worker tokens that were never bound
// to a worker. It deletes in batches and is a dry run unless --apply is
// supplied.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

var issuedTokenTypes = []string{
	types.TokenTypeClusterAdmin,
	types.TokenTypeWorkspacePrimary,
	types.TokenTypeWorkspace,
	types.TokenTypePlatformDeployer,
	types.TokenTypeWorker,
	types.TokenTypeWorkerPrivate,
	types.TokenTypeWorkspaceRestricted,
}

var workerTokenTypes = []string{types.TokenTypeWorker, types.TokenTypeWorkerPrivate}

// A purge selects tokens with a condition whose placeholders start at $2; $1
// is the batch cursor or the batch's IDs.
type purge struct {
	Name string `json:"name"`
	cond string
	args []any
}

type purgeResult struct {
	Name    string `json:"name"`
	Matched int64  `json:"matched"`
	Deleted int64  `json:"deleted"`
	Batches int    `json:"batches"`
	Elapsed string `json:"elapsed"`
}

type report struct {
	Apply         bool          `json:"apply"`
	UnboundBefore time.Time     `json:"unbound_before"`
	Purges        []purgeResult `json:"purges"`
}

func purges(unboundBefore time.Time) []purge {
	return []purge{
		{
			Name: "retired token types",
			cond: `NOT (token_type = ANY($2::token_type[]))`,
			args: []any{pq.Array(issuedTokenTypes)},
		},
		{
			Name: "unbound worker tokens",
			cond: `token_type = ANY($2::token_type[]) AND worker_id = '' AND created_at < $3`,
			args: []any{pq.Array(workerTokenTypes), unboundBefore},
		},
	}
}

func main() {
	apply := flag.Bool("apply", false, "delete the tokens instead of counting them")
	unboundAge := flag.Duration("unbound-older-than", 24*time.Hour, "minimum age of unbound worker tokens to delete")
	batchSize := flag.Int("batch", 5000, "tokens per delete")
	pause := flag.Duration("pause", 100*time.Millisecond, "pause between batches")
	flag.Parse()
	if *unboundAge <= 0 || *batchSize <= 0 || *pause < 0 {
		fmt.Fprintln(os.Stderr, "invalid age, batch or pause")
		os.Exit(2)
	}

	manager, err := common.NewConfigManager[types.AppConfig]()
	if err != nil {
		exit("load config", err)
	}
	db, err := sqlx.Connect("postgres", repository.GenerateDSN(manager.GetConfig().Database.Postgres))
	if err != nil {
		exit("connect postgres", err)
	}
	defer db.Close()

	ctx := context.Background()
	conn, err := db.Connx(ctx)
	if err != nil {
		exit("open connection", err)
	}
	defer conn.Close()
	for _, statement := range []string{`SET statement_timeout = '60s'`, `SET lock_timeout = '5s'`} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			exit("configure session", err)
		}
	}
	if err := checkReferences(ctx, conn); err != nil {
		exit("check references", err)
	}

	result := report{Apply: *apply, UnboundBefore: time.Now().Add(-*unboundAge).UTC().Truncate(time.Second)}
	for _, p := range purges(result.UnboundBefore) {
		started := time.Now()
		r := purgeResult{Name: p.Name}
		if *apply {
			r.Deleted, r.Batches, err = deleteBatches(ctx, conn, p, *batchSize, *pause)
		} else {
			err = conn.GetContext(ctx, &r.Matched, `SELECT count(*) FROM token WHERE id > $1 AND `+p.cond, append([]any{0}, p.args...)...)
		}
		if err != nil {
			exit(p.Name, err)
		}
		r.Elapsed = time.Since(started).Round(time.Millisecond).String()
		result.Purges = append(result.Purges, r)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		exit("encode report", err)
	}
}

// checkReferences refuses to run unless every foreign key to token clears
// itself on delete through an index. Other actions delete or block on the
// referencing rows, and an unindexed key scans its table for every token.
func checkReferences(ctx context.Context, conn *sqlx.Conn) error {
	var unsafe []string
	err := conn.SelectContext(ctx, &unsafe, `
	SELECT c.conname
	FROM pg_constraint c
	WHERE c.contype = 'f' AND c.confrelid = 'token'::regclass
	AND NOT (c.confdeltype = 'n' AND EXISTS (
		SELECT 1 FROM pg_index i WHERE i.indrelid = c.conrelid AND i.indkey[0] = c.conkey[1]
	))`)
	if err != nil {
		return err
	}
	if len(unsafe) > 0 {
		return fmt.Errorf("foreign keys %v must be ON DELETE SET NULL and indexed", unsafe)
	}
	return nil
}

// deleteBatches walks the primary key so each batch starts after the last
// one, and repeats the condition when deleting so a token that changed in
// between is kept.
func deleteBatches(ctx context.Context, conn *sqlx.Conn, p purge, batchSize int, pause time.Duration) (int64, int, error) {
	var deleted int64
	var cursor int64
	batches := 0
	for {
		var ids []int64
		query := fmt.Sprintf(`SELECT id FROM token WHERE id > $1 AND %s ORDER BY id LIMIT %d`, p.cond, batchSize)
		if err := conn.SelectContext(ctx, &ids, query, append([]any{cursor}, p.args...)...); err != nil {
			return deleted, batches, err
		}
		if len(ids) == 0 {
			return deleted, batches, nil
		}
		cursor = ids[len(ids)-1]

		res, err := conn.ExecContext(ctx, `DELETE FROM token WHERE id = ANY($1) AND `+p.cond, append([]any{pq.Array(ids)}, p.args...)...)
		if err != nil {
			return deleted, batches, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return deleted, batches, err
		}
		deleted += n
		batches++
		if batches%100 == 0 {
			fmt.Fprintf(os.Stderr, "%s: deleted %d in %d batches\n", p.Name, deleted, batches)
		}
		time.Sleep(pause)
	}
}

func exit(step string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", step, err)
	os.Exit(1)
}
