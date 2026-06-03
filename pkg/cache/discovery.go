package cache

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"time"

	proto "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type DiscoveryClient struct {
	cfg           GlobalConfig
	hostMap       *HostMap
	hostDirectory HostDirectory
	locality      string
}

func NewDiscoveryClient(cfg GlobalConfig, hostMap *HostMap, hostDirectory HostDirectory, locality string) *DiscoveryClient {
	return &DiscoveryClient{
		cfg:           cfg,
		hostMap:       hostMap,
		hostDirectory: hostDirectory,
		locality:      locality,
	}
}

func (d *DiscoveryClient) updateHostMap(newHosts []*Host) {
	for _, h := range newHosts {
		d.hostMap.Set(h)
	}
}

// Used by cache servers to discover their closest peers
func (d *DiscoveryClient) Start(ctx context.Context) error {
	if jitter := d.discoveryJitter(); jitter > 0 {
		timer := time.NewTimer(jitter)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return nil
		}
	}

	hosts, err := d.discoverHosts(ctx)
	if err == nil {
		d.updateHostMap(hosts)
	}

	interval := time.Duration(d.cfg.DiscoveryIntervalS) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			hosts, err := d.discoverHosts(ctx)
			if err != nil {
				continue
			}

			d.updateHostMap(hosts)
		case <-ctx.Done():
			return nil
		}
	}
}

func (d *DiscoveryClient) discoveryJitter() time.Duration {
	if d.cfg.DiscoveryJitterS <= 0 {
		return 0
	}

	maxJitter := time.Duration(d.cfg.DiscoveryJitterS) * time.Second
	return time.Duration(time.Now().UnixNano() % int64(maxJitter))
}

func (d *DiscoveryClient) discoverHosts(ctx context.Context) ([]*Host, error) {
	hosts, err := d.hostDirectory.GetAvailableHosts(ctx, d.locality)
	if err != nil {
		return nil, err
	}

	selectedHosts := []*Host{}
	hostGroups := cacheHostCandidateGroups(hosts)
	var wg sync.WaitGroup
	mu := sync.Mutex{}
	maxConcurrency := d.cfg.MaxDiscoveryConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = 8
	}
	sem := make(chan struct{}, maxConcurrency)

	for _, group := range hostGroups {
		if group.hasEndpoint(d.hostMap.Get(group.hostID)) {
			continue
		}

		wg.Add(1)
		go func(group cacheHostCandidateGroup) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}

			host, ok := group.firstReachable(ctx, d.GetHostState)
			if !ok {
				if logicalHost := group.logicalHost(); logicalHost != nil {
					mu.Lock()
					selectedHosts = append(selectedHosts, logicalHost)
					mu.Unlock()
				}
				return
			}

			d.hostMap.Set(host)
			mu.Lock()
			selectedHosts = append(selectedHosts, host)
			mu.Unlock()

			Logger.Debugf("Added host with private address to map: %s", host.PrivateAddr)
		}(group)
	}

	wg.Wait()
	return selectedHosts, nil
}

type cacheHostCandidateGroup struct {
	hostID     string
	candidates []*Host
}

type cacheHostVerifier func(context.Context, *Host) (*Host, error)

func cacheHostCandidateGroups(hosts []*Host) []cacheHostCandidateGroup {
	groupsByID := make(map[string][]*Host, len(hosts))
	orderedIDs := make([]string, 0, len(hosts))

	for _, host := range hosts {
		if host == nil || host.HostId == "" {
			continue
		}
		if _, exists := groupsByID[host.HostId]; !exists {
			orderedIDs = append(orderedIDs, host.HostId)
		}
		groupsByID[host.HostId] = append(groupsByID[host.HostId], host)
	}

	groups := make([]cacheHostCandidateGroup, 0, len(orderedIDs))
	for _, hostID := range orderedIDs {
		groups = append(groups, cacheHostCandidateGroup{
			hostID:     hostID,
			candidates: groupsByID[hostID],
		})
	}
	return groups
}

// hasEndpoint reports whether the client is already using one of the
// registered worker endpoints for this logical cache host. Logical host IDs are
// node/cache-path scoped, so switching between healthy endpoints on the same
// node only churns connections and can cancel in-flight cache streams.
func (g cacheHostCandidateGroup) hasEndpoint(host *Host) bool {
	if host == nil || !host.HasEndpoint() {
		return false
	}
	for _, candidate := range g.candidates {
		if !candidate.HasEndpoint() {
			continue
		}
		if sameCacheHostEndpoint(candidate, host) {
			return true
		}
	}
	return false
}

func (g cacheHostCandidateGroup) logicalHost() *Host {
	for _, candidate := range g.candidates {
		if candidate == nil || candidate.HostId == "" {
			continue
		}
		return candidate.LogicalOnly()
	}
	return nil
}

func (g cacheHostCandidateGroup) firstReachable(ctx context.Context, verify cacheHostVerifier) (*Host, bool) {
	for _, candidate := range g.candidates {
		if candidate == nil || candidate.HostId == "" {
			continue
		}
		if !candidate.HasEndpoint() {
			continue
		}

		host, err := verify(ctx, candidate)
		if err != nil || host == nil || host.HostId == "" {
			continue
		}
		return host, true
	}
	return nil, false
}

func sameCacheHostEndpoint(a *Host, b *Host) bool {
	if a == nil || b == nil || !a.HasEndpoint() || !b.HasEndpoint() {
		return false
	}
	return a.HostId == b.HostId && a.Addr == b.Addr && a.PrivateAddr == b.PrivateAddr
}

// GetHostState attempts to connect to the gRPC service and verifies its availability
func (d *DiscoveryClient) GetHostState(ctx context.Context, host *Host) (*Host, error) {
	addr := host.Addr

	if host.PrivateAddr != "" {
		addr = host.PrivateAddr
	}

	transportCredentials := grpc.WithTransportCredentials(insecure.NewCredentials())
	if isTLSEnabled(addr) {
		h2creds := credentials.NewTLS(&tls.Config{NextProtos: []string{"h2"}})
		transportCredentials = grpc.WithTransportCredentials(h2creds)
	}

	var dialOpts = []grpc.DialOption{
		transportCredentials,
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(d.cfg.GRPCMessageSizeBytes),
			grpc.MaxCallSendMsgSize(d.cfg.GRPCMessageSizeBytes),
		),
	}

	timeout := time.Duration(d.cfg.GRPCDialTimeoutS) * time.Second
	if timeout <= 0 {
		timeout = time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	c := proto.NewCacheClient(conn)

	resp, err := c.GetState(dialCtx, &proto.CacheGetStateRequest{})
	if err != nil {
		return nil, err
	}

	host.RTT = 0
	host.CapacityUsagePct = float64(resp.GetCapacityUsagePct())

	if resp.GetVersion() != Version {
		return nil, fmt.Errorf("version mismatch: %s != %s", resp.GetVersion(), Version)
	}

	return host, nil
}
