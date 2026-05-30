package cache

import (
	"context"
	"errors"
	"fmt"
	"sort"
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

func (h CoordinatorHost) LogicalOnly() CoordinatorHost {
	h.RegistrationID = ""
	h.Addr = ""
	h.PrivateAddr = ""
	h.CapacityUsagePct = 0
	return h
}

type CoordinatorRepository interface {
	SetCacheRegistration(ctx context.Context, host CoordinatorHost, ttl time.Duration) error
	GetActiveCacheRegistration(ctx context.Context, logicalHostID string) (registrationID string, found bool, err error)
	SetActiveCacheRegistration(ctx context.Context, logicalHostID, registrationID string, ttl time.Duration) error
	ListCacheLogicalHosts(ctx context.Context, poolName, locality string) ([]string, error)
	ListCacheRegistrations(ctx context.Context, logicalHostID string) ([]string, error)
	GetCacheRegistration(ctx context.Context, logicalHostID, registrationID string) (CoordinatorHost, bool, error)
	GetCacheLogicalHost(ctx context.Context, logicalHostID string) (CoordinatorHost, bool, error)
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

	// A logical host represents one node-local cache path, not one worker
	// process. Any live registration for that logical host can serve the same
	// disk cache, so prefer the freshest registration. This avoids a worker
	// cycle leaving clients routed to a dead pod until the previous active lease
	// expires.
	return c.repository.SetActiveCacheRegistration(ctx, host.LogicalHostID, host.RegistrationID, ttl)
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
	if locality == "" {
		return nil, fmt.Errorf("%w: locality is required", ErrInvalidHostRegistration)
	}

	logicalHostIDs, err := c.repository.ListCacheLogicalHosts(ctx, poolName, locality)
	if err != nil {
		return nil, err
	}

	hosts := make([]CoordinatorHost, 0, len(logicalHostIDs))
	for _, logicalHostID := range logicalHostIDs {
		logicalHosts, err := c.logicalHosts(ctx, poolName, locality, logicalHostID)
		if err != nil {
			return nil, err
		}
		hosts = append(hosts, logicalHosts...)
	}
	return hosts, nil
}

func (c *Coordinator) logicalHosts(ctx context.Context, poolName, locality, logicalHostID string) ([]CoordinatorHost, error) {
	activeRegistrationID, found, err := c.repository.GetActiveCacheRegistration(ctx, logicalHostID)
	if err != nil {
		return nil, err
	}

	registrationIDs, err := c.repository.ListCacheRegistrations(ctx, logicalHostID)
	if err != nil {
		return nil, err
	}

	registrationIDs = orderedCacheRegistrations(registrationIDs, activeRegistrationID)
	hosts := make([]CoordinatorHost, 0, len(registrationIDs))
	for _, registrationID := range registrationIDs {
		host, ok, err := c.getRegistration(ctx, logicalHostID, registrationID)
		if err != nil {
			return nil, err
		}
		if !ok {
			_ = c.repository.RemoveCacheRegistration(ctx, logicalHostID, registrationID)
			continue
		}
		if host.Locality != locality {
			continue
		}
		if poolName != "" && host.PoolName != poolName {
			continue
		}

		hosts = append(hosts, host)
	}

	if len(hosts) == 0 {
		logicalHost, ok, err := c.repository.GetCacheLogicalHost(ctx, logicalHostID)
		if err != nil {
			return nil, err
		}
		if ok && logicalHost.Locality == locality && (poolName == "" || logicalHost.PoolName == poolName) {
			return []CoordinatorHost{logicalHost.LogicalOnly()}, nil
		}
		return nil, c.pruneLogicalHost(ctx, poolName, locality, logicalHostID)
	}

	if !found || activeRegistrationID == "" || hosts[0].RegistrationID != activeRegistrationID {
		_ = c.repository.SetActiveCacheRegistration(ctx, logicalHostID, hosts[0].RegistrationID, defaultCoordinatorHostRegistrationTTL)
	}

	return hosts, nil
}

func (c *Coordinator) getRegistration(ctx context.Context, logicalHostID, registrationID string) (CoordinatorHost, bool, error) {
	return c.repository.GetCacheRegistration(ctx, logicalHostID, registrationID)
}

func orderedCacheRegistrations(registrationIDs []string, activeRegistrationID string) []string {
	ordered := make([]string, 0, len(registrationIDs))
	seen := make(map[string]struct{}, len(registrationIDs))

	if activeRegistrationID != "" {
		for _, registrationID := range registrationIDs {
			if registrationID == activeRegistrationID {
				ordered = append(ordered, registrationID)
				seen[registrationID] = struct{}{}
				break
			}
		}
	}

	sort.Strings(registrationIDs)
	for _, registrationID := range registrationIDs {
		if _, ok := seen[registrationID]; ok {
			continue
		}
		ordered = append(ordered, registrationID)
	}
	return ordered
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
