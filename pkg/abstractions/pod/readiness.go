package pod

import (
	"bytes"
	"encoding/binary"
	"io"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"golang.org/x/time/rate"
)

const (
	postgresProtocolVersion              uint32 = 196608
	postgresReadinessUser                       = "postgres"
	containerReadinessRefresh                   = 10 * time.Second
	containerReadinessRefreshRate               = 32
	containerReadinessRefreshConcurrency        = 4
)

// Shared across buffers: background probes stay bounded as the fleet grows.
var readinessRefreshBudget = rate.NewLimiter(containerReadinessRefreshRate, containerReadinessRefreshRate)
var readinessRefreshSlots = make(chan struct{}, containerReadinessRefreshConcurrency)

type readinessTarget struct {
	containerID string
	port        int32
	address     string
}

type readinessResult struct {
	address string
	until   time.Time
}

// Discovery runs frequently to find new containers and newly listening ports.
// Reuse successful probes so healthy ports do not continuously fill the host's
// conntrack table. Request failures invalidate them immediately.
type readinessCache struct {
	mu             sync.Mutex
	ready          map[string]map[int32]readinessResult
	refreshLimiter *rate.Limiter
}

func (c *readinessCache) check(target readinessTarget, probe func() bool) bool {
	if target.address == "" {
		return false
	}
	c.mu.Lock()
	cached := c.ready[target.containerID][target.port]
	c.mu.Unlock()
	if cached.address == target.address {
		limiter := c.refreshLimiter
		if limiter == nil {
			limiter = readinessRefreshBudget
		}
		if time.Now().Before(cached.until) || !limiter.Allow() {
			return true
		}
		select {
		case readinessRefreshSlots <- struct{}{}:
			defer func() { <-readinessRefreshSlots }()
		default:
			return true
		}
	}
	// First probes and retries after failures never wait for background work.
	if !probe() {
		c.mu.Lock()
		delete(c.ready[target.containerID], target.port)
		c.mu.Unlock()
		return false
	}
	c.mu.Lock()
	if c.ready == nil {
		c.ready = make(map[string]map[int32]readinessResult)
	}
	if c.ready[target.containerID] == nil {
		c.ready[target.containerID] = make(map[int32]readinessResult)
	}
	c.ready[target.containerID][target.port] = readinessResult{target.address, time.Now().Add(containerReadinessRefresh)}
	c.mu.Unlock()
	return true
}

func (c *readinessCache) invalidate(containerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.ready, containerID)
}

func (c *readinessCache) prune(active map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for containerID := range c.ready {
		if _, ok := active[containerID]; !ok {
			delete(c.ready, containerID)
		}
	}
}

func (pb *PodProxyBuffer) checkContainerReady(port int32, address string, timeout time.Duration) bool {
	if pb.stubConfig != nil {
		if database := pb.stubConfig.EffectiveDatabaseConfig(); database != nil && database.IsPostgres() && uint32(port) == database.Port {
			return pb.checkPostgresContainerReady(address, timeout)
		}
	}
	return pb.checkContainerAvailableWithTimeout(address, timeout)
}

func (pb *PodProxyBuffer) checkPostgresContainerReady(address string, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = containerAvailableTimeout
	}
	conn, err := network.ConnectToBackend(pb.baseContext(), address, timeout, pb.tailscale, pb.tsConfig, pb.containerRepo)
	if err != nil {
		return false
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	payload := []byte("user\x00" + postgresReadinessUser + "\x00database\x00" + postgresReadinessUser + "\x00application_name\x00beta9_readiness\x00\x00")
	startup := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(startup[0:4], uint32(len(startup)))
	binary.BigEndian.PutUint32(startup[4:8], postgresProtocolVersion)
	copy(startup[8:], payload)
	if _, err := conn.Write(startup); err != nil {
		return false
	}

	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return false
	}
	size := int(binary.BigEndian.Uint32(header[1:5])) - 4
	if size < 0 || size > 1<<20 {
		return false
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(conn, body); err != nil {
		return false
	}
	return header[0] != 'E' || !bytes.Contains(bytes.ToLower(body), []byte("starting up"))
}
