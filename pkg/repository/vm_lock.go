package repository

import (
	"context"
	"database/sql/driver"
	"errors"
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
	held map[string]chan struct{}
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
			r.vmLocks[i] = vmLockSession{gate: make(chan struct{}, 1), held: make(map[string]chan struct{})}
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
	if released := s.held[id]; released != nil {
		return nil, &vmOperationBusy{released: released}
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
		return nil, &vmOperationBusy{}
	}
	s.held[id] = make(chan struct{})
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
