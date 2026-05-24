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
	mu            sync.Mutex
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

	var wg sync.WaitGroup
	filteredHosts := []*Host{}
	mu := sync.Mutex{}
	maxConcurrency := d.cfg.MaxDiscoveryConcurrency
	if maxConcurrency <= 0 {
		maxConcurrency = 8
	}
	sem := make(chan struct{}, maxConcurrency)

	for _, host := range hosts {
		// Don't try to get the state on peers we're already aware of
		if existing := d.hostMap.Get(host.HostId); existing != nil && existing.Addr == host.Addr && existing.PrivateAddr == host.PrivateAddr {
			continue
		}

		wg.Add(1)
		go func(host *Host) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}

			hostState, err := d.GetHostState(ctx, host)
			if err != nil {
				return
			}

			mu.Lock()
			filteredHosts = append(filteredHosts, hostState)
			mu.Unlock()

			Logger.Debugf("Added host with private address to map: %s", hostState.PrivateAddr)
		}(host)

	}

	wg.Wait()
	return filteredHosts, nil
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
