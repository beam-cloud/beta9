package provisioner

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/redislock"
	"github.com/redis/go-redis/v9"
)

// errNoRecord: the machine's record was removed while it was being changed.
var errNoRecord = errors.New("the machine has no record")

// Store keeps the records, and what their machines have cost, in Redis,
// where every gateway replica sees them. Records are written one at a time
// under one lock.
type Store struct {
	rdb  *common.RedisClient
	lock *common.RedisLock
}

func NewStore(rdb *common.RedisClient) *Store {
	return &Store{rdb: rdb, lock: common.NewRedisLock(rdb)}
}

// Writes are a few Redis commands, so a writer that finds the lock taken
// waits for it rather than giving up.
var writeLock = common.RedisLockOptions{TtlS: 10, Retries: 100, RetryInterval: 50 * time.Millisecond}

// List returns every record, ordered by machine ID.
func (s *Store) List(ctx context.Context) ([]Machine, error) {
	stored, err := s.rdb.HGetAll(ctx, common.RedisKeys.ComputeProvisionedMachines()).Result()
	if err != nil {
		return nil, fmt.Errorf("list bought machines: %w", err)
	}
	machines := make([]Machine, 0, len(stored))
	for id, data := range stored {
		var machine Machine
		if err := json.Unmarshal([]byte(data), &machine); err != nil {
			return nil, fmt.Errorf("read the record of machine %s: %w", id, err)
		}
		machines = append(machines, machine)
	}
	slices.SortFunc(machines, func(a, b Machine) int { return strings.Compare(a.ID, b.ID) })
	return machines, nil
}

// Reserve adds the record newRecord returns. newRecord sees every record
// there is and nothing can be added while it looks, so a limit it checks
// holds across replicas. When newRecord fails, nothing is added.
func (s *Store) Reserve(ctx context.Context, newRecord func(held []Machine) (Machine, error)) (Machine, error) {
	var reserved Machine
	err := s.lock.WithLease(ctx, common.RedisKeys.ComputeProvisionedLock(), writeLock, func(ctx context.Context) error {
		held, err := s.List(ctx)
		if err != nil {
			return err
		}
		if reserved, err = newRecord(held); err != nil {
			return err
		}
		return s.save(ctx, reserved, 0)
	})
	return reserved, err
}

// Update changes one record and returns it as it is now. It fails with
// errNoRecord when the record is gone.
//
// A change that moves BilledUntil on counts what the machine cost in between
// as spent. The record and the spend are written in one transaction: a write
// that fails has counted nothing, and one that succeeds has counted it once.
func (s *Store) Update(ctx context.Context, id string, change func(*Machine)) (Machine, error) {
	var updated Machine
	err := s.lock.WithLease(ctx, common.RedisKeys.ComputeProvisionedLock(), writeLock, func(ctx context.Context) error {
		stored, err := s.get(ctx, id)
		if err != nil {
			return err
		}
		updated = stored
		change(&updated)
		return s.save(ctx, updated, updated.cost(stored.billedFrom(), updated.BilledUntil))
	})
	return updated, err
}

// Remove deletes a record and, in the same transaction, counts what its
// machine cost since it was last billed, up to until, as spent. Removing a
// record that is gone is not an error, and counts nothing.
func (s *Store) Remove(ctx context.Context, id string, until time.Time) error {
	return s.lock.WithLease(ctx, common.RedisKeys.ComputeProvisionedLock(), writeLock, func(ctx context.Context) error {
		machine, err := s.get(ctx, id)
		if errors.Is(err, errNoRecord) {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HDel(ctx, common.RedisKeys.ComputeProvisionedMachines(), id)
			addSpend(ctx, pipe, until, machine.cost(machine.billedFrom(), until))
			return nil
		})
		return err
	})
}

func (s *Store) get(ctx context.Context, id string) (Machine, error) {
	data, err := s.rdb.HGet(ctx, common.RedisKeys.ComputeProvisionedMachines(), id).Result()
	if errors.Is(err, redis.Nil) {
		return Machine{}, errNoRecord
	}
	if err != nil {
		return Machine{}, err
	}
	var machine Machine
	if err := json.Unmarshal([]byte(data), &machine); err != nil {
		return Machine{}, fmt.Errorf("read the record of machine %s: %w", id, err)
	}
	return machine, nil
}

// save writes a record and, in the same transaction, counts cents as spent
// at the time the record is billed to.
func (s *Store) save(ctx context.Context, machine Machine, cents float64) error {
	data, err := json.Marshal(machine)
	if err != nil {
		return err
	}
	_, err = s.rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.HSet(ctx, common.RedisKeys.ComputeProvisionedMachines(), machine.ID, data)
		addSpend(ctx, pipe, machine.BilledUntil, cents)
		return nil
	})
	return err
}

const (
	// spendHour names the hour whose spend a moment's cost is added to.
	spendHour = "2006010215"
	// spendHours is how many hours of spend make a day's: the hour of now
	// and the 23 before it.
	spendHours = 24
	// spendKept is how long an hour's spend is kept: an hour longer than it
	// is counted, so that it is still there while it is.
	spendKept = 25 * time.Hour
)

func spendKey(at time.Time) string {
	return common.RedisKeys.ComputeProvisionedSpend(at.UTC().Format(spendHour))
}

// addSpend has the transaction count cents as spent in the hour of at.
func addSpend(ctx context.Context, tx redis.Pipeliner, at time.Time, cents float64) {
	if cents <= 0 {
		return
	}
	tx.IncrByFloat(ctx, spendKey(at), cents)
	tx.Expire(ctx, spendKey(at), spendKept)
}

// Spent returns what the machines have cost in the day up to now, in cents.
func (s *Store) Spent(ctx context.Context, now time.Time) (float64, error) {
	hours := make([]string, spendHours)
	for i := range hours {
		hours[i] = spendKey(now.Add(-time.Duration(i) * time.Hour))
	}
	spent, err := s.rdb.MGet(ctx, hours...).Result()
	if err != nil {
		return 0, fmt.Errorf("read what was spent: %w", err)
	}
	total := 0.0
	for i, value := range spent {
		text, ok := value.(string)
		if !ok {
			continue // nothing was spent in that hour
		}
		cents, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return 0, fmt.Errorf("read what was spent (%s): %w", hours[i], err)
		}
		total += cents
	}
	return total, nil
}

// MarkSoldOut records, for every replica, that an offer is not to be bought
// until the given time.
func (s *Store) MarkSoldOut(ctx context.Context, offer offerKey, until time.Time) error {
	field, err := json.Marshal(offer)
	if err != nil {
		return err
	}
	return s.rdb.HSet(ctx, common.RedisKeys.ComputeProvisionedSoldOut(), string(field), until.UnixNano()).Err()
}

// SoldOut returns the offers that are not to be bought now, and until when.
// It removes the marks that have lapsed.
func (s *Store) SoldOut(ctx context.Context, now time.Time) (map[offerKey]time.Time, error) {
	stored, err := s.rdb.HGetAll(ctx, common.RedisKeys.ComputeProvisionedSoldOut()).Result()
	if err != nil {
		return nil, fmt.Errorf("read what is sold out: %w", err)
	}
	marks := make(map[offerKey]time.Time, len(stored))
	var lapsed []string
	for field, value := range stored {
		var offer offerKey
		nanos, err := strconv.ParseInt(value, 10, 64)
		if err == nil {
			err = json.Unmarshal([]byte(field), &offer)
		}
		if until := time.Unix(0, nanos); err == nil && now.Before(until) {
			marks[offer] = until
			continue
		}
		lapsed = append(lapsed, field)
	}
	if len(lapsed) > 0 {
		// Whoever reads next removes what this could not.
		_ = s.rdb.HDel(ctx, common.RedisKeys.ComputeProvisionedSoldOut(), lapsed...).Err()
	}
	return marks, nil
}

// reconcileLock is held by the replica that runs a pass, which renews it for
// as long as the pass runs. Its TTL is how long the other replicas wait
// before one of them runs a pass when that replica is gone.
var reconcileLock = common.RedisLockOptions{TtlS: 60}

// Reconcile runs pass unless another replica is running one, or one started
// less than every ago. Either way another replica has it, which is no error.
// Passes delete machines and add up what was spent, so two must never run at
// once, and there is no use in every replica asking the providers the same
// questions.
func (s *Store) Reconcile(ctx context.Context, now time.Time, every time.Duration, pass func(context.Context)) error {
	err := s.lock.WithLease(ctx, common.RedisKeys.ComputeProvisionedReconcileLock(), reconcileLock, func(ctx context.Context) error {
		last, err := s.rdb.Get(ctx, common.RedisKeys.ComputeProvisionedReconcile()).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		if nanos, err := strconv.ParseInt(last, 10, 64); err == nil && now.Sub(time.Unix(0, nanos)) < every {
			return nil
		}
		if err := s.rdb.Set(ctx, common.RedisKeys.ComputeProvisionedReconcile(), now.UnixNano(), 0).Err(); err != nil {
			return err
		}
		pass(ctx)
		return nil
	})
	if errors.Is(err, redislock.ErrNotObtained) {
		return nil
	}
	return err
}
