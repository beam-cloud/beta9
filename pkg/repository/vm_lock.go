package repository

import (
	"context"
	"database/sql/driver"
	"fmt"
	"hash/crc32"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

// Each session can hold many independent advisory locks. Keep their short
// acquire/release queries separate from the lifecycle work: a hundred ongoing
// snapshots must not consume a hundred connections or block unrelated starts.
// The local set prevents PostgreSQL's same-session, reentrant lock acquisition.
type vmLockSession struct {
	gate chan struct{}
	conn *sqlx.Conn
	held map[string]bool
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
	delete(s.held, id)
	if len(s.held) == 0 && s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
}

// A session advisory lock fences lifecycle operations across gateway replicas.
// The hash and namespace are shared with older gateways during rolling deploys.
// Sessions close when their last lock is released; PostgreSQL also releases all
// their locks when a gateway dies. Persisted intent remains recoverable.
func (r *PostgresBackendRepository) LockVM(ctx context.Context, id string) (func(), error) {
	r.vmLocksOnce.Do(func() {
		count := 4
		if max := r.client.Stats().MaxOpenConnections; max > 0 {
			count = min(count, max/2)
		}
		r.vmLocks = make([]vmLockSession, count)
		for i := range r.vmLocks {
			r.vmLocks[i] = vmLockSession{gate: make(chan struct{}, 1), held: make(map[string]bool)}
		}
	})
	if len(r.vmLocks) == 0 {
		return nil, fmt.Errorf("VM lifecycle requires at least two database connections")
	}
	s := &r.vmLocks[crc32.ChecksumIEEE([]byte(id))%uint32(len(r.vmLocks))]
	select {
	case s.gate <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	defer func() { <-s.gate }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.held[id] {
		return nil, fmt.Errorf("VM operation already in progress")
	}
	if s.conn == nil {
		conn, err := r.client.Connx(ctx)
		if err != nil {
			return nil, err
		}
		s.conn = conn
	}
	// Canceling one HTTP request must not tear down a session holding locks
	// for other VMs. Finish this short query, then release a canceled caller's
	// lock before returning. A failed database session is always discarded.
	queryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	var acquired bool
	err := s.conn.GetContext(queryCtx, &acquired, `SELECT pg_try_advisory_lock(hashtextextended($1, 947))`, id)
	if err != nil {
		s.discard()
		return nil, err
	}
	if !acquired {
		if len(s.held) == 0 {
			s.conn.Close()
			s.conn = nil
		}
		return nil, fmt.Errorf("VM operation already in progress")
	}
	s.held[id] = true
	conn := s.conn
	if err := ctx.Err(); err != nil {
		s.release(id, conn)
		return nil, err
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			s.gate <- struct{}{}
			defer func() { <-s.gate }()
			s.release(id, conn)
		})
	}, nil
}
