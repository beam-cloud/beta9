package cache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	defaultCoordinatorHostRegistrationTTL = 30 * time.Second
)

var (
	ErrCoordinatorUnavailable  = errors.New("cache coordinator unavailable")
	ErrInvalidHostRegistration = errors.New("invalid cache host registration")
)

type CoordinatorHost struct {
	// LogicalHostID is the stable cache routing identity. Multiple worker
	// process registrations can advertise addresses for the same logical host.
	LogicalHostID string
	// RegistrationID identifies one live worker/cache-server process lease.
	RegistrationID   string
	PoolName         string
	Locality         string
	NodeID           string
	CachePathID      string
	Addr             string
	PrivateAddr      string
	CapacityUsagePct float64
}

type CoordinatorRepository interface {
	SetCacheRegistration(ctx context.Context, host CoordinatorHost, ttl time.Duration) error
	GetActiveCacheRegistration(ctx context.Context, logicalHostID string) (registrationID string, found bool, err error)
	SetActiveCacheRegistration(ctx context.Context, logicalHostID, registrationID string, ttl time.Duration) error
	ListCacheLogicalHosts(ctx context.Context, poolName, locality string) ([]string, error)
	ListCacheRegistrations(ctx context.Context, logicalHostID string) ([]string, error)
	GetCacheRegistration(ctx context.Context, logicalHostID, registrationID string) (CoordinatorHost, bool, error)
	RemoveCacheRegistration(ctx context.Context, logicalHostID, registrationID string) error
	CountCacheRegistrations(ctx context.Context, logicalHostID string) (int64, error)
	RemoveCacheLogicalHost(ctx context.Context, poolName, locality, logicalHostID string) error
}

type Coordinator struct {
	repository CoordinatorRepository
}

func NewCoordinator(repository CoordinatorRepository) *Coordinator {
	return &Coordinator{repository: repository}
}

func (c *Coordinator) RegisterHost(ctx context.Context, host CoordinatorHost, ttl time.Duration) error {
	if c == nil || c.repository == nil {
		return ErrCoordinatorUnavailable
	}
	if host.LogicalHostID == "" || host.RegistrationID == "" || host.PoolName == "" || host.Locality == "" {
		return fmt.Errorf("%w: logical host, registration, pool, and locality are required", ErrInvalidHostRegistration)
	}
	if host.Addr == "" && host.PrivateAddr == "" {
		return fmt.Errorf("%w: host address is required", ErrInvalidHostRegistration)
	}
	if ttl <= 0 {
		ttl = defaultCoordinatorHostRegistrationTTL
	}

	if err := c.repository.SetCacheRegistration(ctx, host, ttl); err != nil {
		return err
	}

	activeRegistrationID, found, err := c.repository.GetActiveCacheRegistration(ctx, host.LogicalHostID)
	if err != nil {
		return err
	}
	if !found || activeRegistrationID == host.RegistrationID {
		return c.repository.SetActiveCacheRegistration(ctx, host.LogicalHostID, host.RegistrationID, ttl)
	}

	if _, activeFound, err := c.repository.GetCacheRegistration(ctx, host.LogicalHostID, activeRegistrationID); err != nil {
		return err
	} else if !activeFound {
		return c.repository.SetActiveCacheRegistration(ctx, host.LogicalHostID, host.RegistrationID, ttl)
	}

	return nil
}

func (c *Coordinator) UnregisterHost(ctx context.Context, poolName, locality, logicalHostID, registrationID string) error {
	if c == nil || c.repository == nil {
		return ErrCoordinatorUnavailable
	}
	if logicalHostID == "" || registrationID == "" {
		return fmt.Errorf("%w: logical host and registration are required", ErrInvalidHostRegistration)
	}

	if err := c.repository.RemoveCacheRegistration(ctx, logicalHostID, registrationID); err != nil {
		return err
	}
	if poolName != "" && locality != "" {
		return c.pruneLogicalHost(ctx, poolName, locality, logicalHostID)
	}
	return nil
}

func (c *Coordinator) ListHosts(ctx context.Context, poolName, locality string) ([]CoordinatorHost, error) {
	if c == nil || c.repository == nil {
		return nil, ErrCoordinatorUnavailable
	}
	if poolName == "" || locality == "" {
		return nil, fmt.Errorf("%w: pool and locality are required", ErrInvalidHostRegistration)
	}

	logicalHostIDs, err := c.repository.ListCacheLogicalHosts(ctx, poolName, locality)
	if err != nil {
		return nil, err
	}

	hosts := make([]CoordinatorHost, 0, len(logicalHostIDs))
	for _, logicalHostID := range logicalHostIDs {
		host, ok, err := c.activeHost(ctx, poolName, locality, logicalHostID)
		if err != nil {
			return nil, err
		}
		if ok {
			hosts = append(hosts, host)
		}
	}
	return hosts, nil
}

func (c *Coordinator) activeHost(ctx context.Context, poolName, locality, logicalHostID string) (CoordinatorHost, bool, error) {
	activeRegistrationID, found, err := c.repository.GetActiveCacheRegistration(ctx, logicalHostID)
	if err != nil {
		return CoordinatorHost{}, false, err
	}
	if found && activeRegistrationID != "" {
		if host, ok, err := c.getRegistration(ctx, logicalHostID, activeRegistrationID); err != nil || ok {
			return host, ok, err
		}
	}

	registrationIDs, err := c.repository.ListCacheRegistrations(ctx, logicalHostID)
	if err != nil {
		return CoordinatorHost{}, false, err
	}

	for _, registrationID := range registrationIDs {
		host, ok, err := c.getRegistration(ctx, logicalHostID, registrationID)
		if err != nil {
			return CoordinatorHost{}, false, err
		}
		if !ok {
			_ = c.repository.RemoveCacheRegistration(ctx, logicalHostID, registrationID)
			continue
		}

		_ = c.repository.SetActiveCacheRegistration(ctx, logicalHostID, registrationID, defaultCoordinatorHostRegistrationTTL)
		return host, true, nil
	}

	return CoordinatorHost{}, false, c.pruneLogicalHost(ctx, poolName, locality, logicalHostID)
}

func (c *Coordinator) getRegistration(ctx context.Context, logicalHostID, registrationID string) (CoordinatorHost, bool, error) {
	return c.repository.GetCacheRegistration(ctx, logicalHostID, registrationID)
}

func (c *Coordinator) pruneLogicalHost(ctx context.Context, poolName, locality, logicalHostID string) error {
	count, err := c.repository.CountCacheRegistrations(ctx, logicalHostID)
	if err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	return c.repository.RemoveCacheLogicalHost(ctx, poolName, locality, logicalHostID)
}
