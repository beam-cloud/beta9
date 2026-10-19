package repository

import (
	"context"
	"database/sql/driver"
	"errors"
	"hash/crc32"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

// Each session can hold many independent advisory locks. Keep their short
// acquire/release queries separate from the lifecycle work: a hundred ongoing
// snapshots must not consume a hundred connections or block unrelated starts.
// The local set prevents PostgreSQL's same-session, reentrant lock acquisition.
type vmLockSession struct {
	gate     chan struct{}
	client   *sqlx.DB
	conn     *sqlx.Conn
	held     map[string]chan struct{}
	acquires commitBatch[struct{}, *vmLockAcquire, vmLockResult]
}

type vmLockAcquire struct{ id string }

type vmLockResult struct {
	conn *sqlx.Conn
	busy *vmOperationBusy
}

// Concurrent lifecycles acquire their independent locks in one round trip per
// session. The local set still rejects duplicate keys before PostgreSQL's
// reentrant advisory locks can grant the same lock to two callers.
func (s *vmLockSession) acquire(ctx context.Context, requests []*vmLockAcquire) (map[*vmLockAcquire]vmLockResult, error) {
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-s.gate }()
	ids := make([]string, 0, len(requests))
	acquired := make(map[string]bool, len(requests))
	for _, request := range requests {
		if _, seen := acquired[request.id]; s.held[request.id] == nil && !seen {
			ids = append(ids, request.id)
			acquired[request.id] = false
		}
	}
	if len(ids) > 0 {
		if s.conn == nil {
			conn, err := s.client.Connx(ctx)
			if err != nil {
				return nil, err
			}
			s.conn = conn
		}
		// The batch context outlives individual callers. Canceling one HTTP
		// request must not close a session holding another VM's locks.
		queryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		var rows []struct {
			ID       string `db:"id"`
			Acquired bool   `db:"acquired"`
		}
		err := s.conn.SelectContext(queryCtx, &rows, `SELECT id, pg_try_advisory_lock(hashtextextended(id, 947)) AS acquired FROM unnest($1::text[]) AS locks(id)`, pq.Array(ids))
		for _, row := range rows {
			acquired[row.ID] = row.Acquired
		}

		if err != nil {
			s.discard()
			return nil, err
		}
	}
	results := make(map[*vmLockAcquire]vmLockResult, len(requests))
	for _, request := range requests {
		if released := s.held[request.id]; released != nil {
			results[request] = vmLockResult{busy: &vmOperationBusy{released: released}}
		} else if acquired[request.id] {
			s.held[request.id] = make(chan struct{})
			results[request] = vmLockResult{conn: s.conn}
		} else {
			results[request] = vmLockResult{busy: &vmOperationBusy{}}
		}
	}
	if len(s.held) == 0 && s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	return results, nil
}

func (s *vmLockSession) discard() {
	if s.conn != nil {
		_ = s.conn.Raw(func(any) error { return driver.ErrBadConn })
		_ = s.conn.Close()
		s.conn = nil
	}
}

// release runs with the session gate held. A callback from a lost session
// cannot unlock a lock belonging to its replacement connection.
func (s *vmLockSession) release(id string, conn *sqlx.Conn) {
	if s.conn == conn {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := conn.ExecContext(cleanup, `SELECT pg_advisory_unlock(hashtextextended($1, 947))`, id)
		cancel()
		if err != nil {
			// Never return a possibly locked connection to the pool.
			s.discard()
		}
	}
	close(s.held[id])
	delete(s.held, id)
	if len(s.held) == 0 && s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

type vmOperationBusy struct {
	released <-chan struct{}
}

func (*vmOperationBusy) Error() string { return "VM operation already in progress" }

// LockVM queues user operations behind an existing lifecycle operation without
// holding a connection or blocking other VMs on the shared lock session.
func (r *PostgresBackendRepository) LockVM(ctx context.Context, id string) (func(), error) {
	for {
		unlock, err := r.TryLockVM(ctx, id)
		var busy *vmOperationBusy
		if !errors.As(err, &busy) {
			return unlock, err
		}
		if busy.released != nil {
			select {
			case <-busy.released:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			continue
		}
		// Another gateway owns the PostgreSQL lock. A blocking SQL lock on
		// this multiplexed session would also prevent its owners unlocking.
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

// TryLockVM lets background reconciliation skip busy VMs. Session advisory
// locks fence lifecycle operations across gateway replicas. Sessions close
// when their last lock is released; PostgreSQL also releases all
// their locks when a gateway dies. Persisted intent remains recoverable.
func (r *PostgresBackendRepository) TryLockVM(ctx context.Context, id string) (func(), error) {
	r.vmLocksOnce.Do(func() {
		count := 4
		if max := r.client.Stats().MaxOpenConnections; max > 0 {
			count = min(count, max/2)
		}
		r.vmLocks = make([]vmLockSession, count)
		for i := range r.vmLocks {
			r.vmLocks[i] = vmLockSession{client: r.client, gate: make(chan struct{}, 1), held: make(map[string]chan struct{})}
		}
	})
	if len(r.vmLocks) == 0 {
		return nil, errors.New("VM lifecycle requires at least two database connections")
	}
	s := &r.vmLocks[crc32.ChecksumIEEE([]byte(id))%uint32(len(r.vmLocks))]
	result, err := s.acquires.commit(ctx, struct{}{}, &vmLockAcquire{id: id}, s.acquire)
	if err != nil {
		return nil, err
	}
	if result.busy != nil {
		return nil, result.busy
	}
	conn := result.conn
	var once sync.Once
	release := func() {
		once.Do(func() {
			s.gate <- struct{}{}
			defer func() { <-s.gate }()
			s.release(id, conn)
		})
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
