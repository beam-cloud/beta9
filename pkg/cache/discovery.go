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
	cfg         GlobalConfig
	hostMap     *HostMap
	coordinator Registry
	locality    string
	mu          sync.Mutex
}

func NewDiscoveryClient(cfg GlobalConfig, hostMap *HostMap, coordinator Registry, locality string) *DiscoveryClient {
	return &DiscoveryClient{
		cfg:         cfg,
		hostMap:     hostMap,
		coordinator: coordinator,
		locality:    locality,
	}
}

func (d *DiscoveryClient) updateHostMap(newHosts []*Host) {
	for _, h := range newHosts {
		d.hostMap.Set(h)
	}
}

// Used by cache servers to discover their closest peers
func (d *DiscoveryClient) Start(ctx context.Context) error {
	hosts, err := d.discoverHosts(ctx)
	if err == nil {
		d.updateHostMap(hosts)
	}

	ticker := time.NewTicker(time.Duration(d.cfg.DiscoveryIntervalS) * time.Second)
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

func (d *DiscoveryClient) discoverHosts(ctx context.Context) ([]*Host, error) {
	hosts, err := d.coordinator.GetAvailableHosts(ctx, d.locality)
	if err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	filteredHosts := []*Host{}
	mu := sync.Mutex{}

	for _, host := range hosts {
		// Don't try to get the state on peers we're already aware of
		if d.hostMap.Get(host.HostId) != nil {
			continue
		}

		wg.Add(1)
		go func(hostId string) {
			defer wg.Done()

			hostState, err := d.GetHostState(ctx, host)
			if err != nil {
				return
			}

			mu.Lock()
			filteredHosts = append(filteredHosts, hostState)
			mu.Unlock()

			Logger.Debugf("Added host with private address to map: %s", hostState.PrivateAddr)
		}(host.HostId)

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

	dialCtx, cancel := context.WithTimeout(ctx, time.Duration(d.cfg.GRPCDialTimeoutS)*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, host.PrivateAddr, dialOpts...)
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
