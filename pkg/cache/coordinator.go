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
	DefaultCacheServerRoleWorker          = "worker"
	DefaultCacheServerRoleAgent           = "agent"
	DefaultWorkerCacheServerPriority      = 10
	DefaultAgentCacheServerPriority       = 100
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
	Role             string
	Priority         int
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
	host = normalizeCoordinatorHost(host)

	if err := c.repository.SetCacheRegistration(ctx, host, ttl); err != nil {
		return err
	}

	return c.refreshActiveRegistration(ctx, host, ttl)
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

	selected := selectActiveCoordinatorHost(hosts, activeRegistrationID)
	if selected.RegistrationID != "" && (!found || activeRegistrationID == "" || selected.RegistrationID != activeRegistrationID) {
		_ = c.repository.SetActiveCacheRegistration(ctx, logicalHostID, selected.RegistrationID, defaultCoordinatorHostRegistrationTTL)
	}

	hosts = orderCoordinatorHosts(hosts, selected.RegistrationID)
	return hosts, nil
}

func (c *Coordinator) getRegistration(ctx context.Context, logicalHostID, registrationID string) (CoordinatorHost, bool, error) {
	return c.repository.GetCacheRegistration(ctx, logicalHostID, registrationID)
}

func (c *Coordinator) refreshActiveRegistration(ctx context.Context, registering CoordinatorHost, ttl time.Duration) error {
	activeRegistrationID, found, err := c.repository.GetActiveCacheRegistration(ctx, registering.LogicalHostID)
	if err != nil {
		return err
	}

	if found && activeRegistrationID != "" {
		active, ok, err := c.getRegistration(ctx, registering.LogicalHostID, activeRegistrationID)
		if err != nil {
			return err
		}
		if ok {
			if registering.RegistrationID == activeRegistrationID || coordinatorHostPreempts(registering, active) {
				return c.repository.SetActiveCacheRegistration(ctx, registering.LogicalHostID, registering.RegistrationID, ttl)
			}
			return nil
		}
	}

	hosts, err := c.liveRegistrations(ctx, registering.PoolName, registering.Locality, registering.LogicalHostID)
	if err != nil {
		return err
	}
	selected := selectActiveCoordinatorHost(hosts, "")
	if selected.RegistrationID == "" {
		selected = registering
	}
	return c.repository.SetActiveCacheRegistration(ctx, registering.LogicalHostID, selected.RegistrationID, ttl)
}

func (c *Coordinator) liveRegistrations(ctx context.Context, poolName, locality, logicalHostID string) ([]CoordinatorHost, error) {
	registrationIDs, err := c.repository.ListCacheRegistrations(ctx, logicalHostID)
	if err != nil {
		return nil, err
	}

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
		hosts = append(hosts, normalizeCoordinatorHost(host))
	}
	return hosts, nil
}

func normalizeCoordinatorHost(host CoordinatorHost) CoordinatorHost {
	if host.Role == "" {
		host.Role = DefaultCacheServerRoleWorker
	}
	if host.Priority == 0 {
		if host.Role == DefaultCacheServerRoleAgent {
			host.Priority = DefaultAgentCacheServerPriority
		} else {
			host.Priority = DefaultWorkerCacheServerPriority
		}
	}
	return host
}

func selectActiveCoordinatorHost(hosts []CoordinatorHost, activeRegistrationID string) CoordinatorHost {
	var active CoordinatorHost
	var best CoordinatorHost
	for _, host := range hosts {
		host = normalizeCoordinatorHost(host)
		if host.RegistrationID == activeRegistrationID {
			active = host
		}
		if best.RegistrationID == "" || coordinatorHostSortsBefore(host, best) {
			best = host
		}
	}
	if active.RegistrationID == "" {
		return best
	}
	if best.RegistrationID != "" && coordinatorHostPreempts(best, active) {
		return best
	}
	return active
}

func coordinatorHostPreempts(candidate, current CoordinatorHost) bool {
	candidate = normalizeCoordinatorHost(candidate)
	current = normalizeCoordinatorHost(current)
	return candidate.Priority > current.Priority
}

func coordinatorHostSortsBefore(candidate, current CoordinatorHost) bool {
	candidate = normalizeCoordinatorHost(candidate)
	current = normalizeCoordinatorHost(current)
	if candidate.Priority != current.Priority {
		return candidate.Priority > current.Priority
	}
	return candidate.RegistrationID < current.RegistrationID
}

func orderCoordinatorHosts(hosts []CoordinatorHost, activeRegistrationID string) []CoordinatorHost {
	ordered := make([]CoordinatorHost, 0, len(hosts))
	rest := make([]CoordinatorHost, 0, len(hosts))
	for _, host := range hosts {
		host = normalizeCoordinatorHost(host)
		if host.RegistrationID == activeRegistrationID {
			ordered = append(ordered, host)
			continue
		}
		rest = append(rest, host)
	}
	sort.SliceStable(rest, func(i, j int) bool {
		if rest[i].Priority != rest[j].Priority {
			return rest[i].Priority > rest[j].Priority
		}
		return rest[i].RegistrationID < rest[j].RegistrationID
	})
	ordered = append(ordered, rest...)
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

	logicalHost, ok, err := c.repository.GetCacheLogicalHost(ctx, logicalHostID)
	if err != nil {
		return err
	}
	if ok && logicalHost.Locality == locality && (poolName == "" || logicalHost.PoolName == poolName) {
		return nil
	}

	return c.repository.RemoveCacheLogicalHost(ctx, poolName, locality, logicalHostID)
}
