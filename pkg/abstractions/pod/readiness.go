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

type readinessEntry struct {
	address   string
	refreshAt time.Time
	ready     bool
	probing   chan struct{}
}

// Idle discovery reuses both successful and failed probes. New targets and
// queued requests bypass refresh limits; request failures invalidate only
// their target. All entry state is protected by mu, never held during I/O.
type readinessCache struct {
	mu             sync.Mutex
	entries        map[string]map[int32]*readinessEntry
	refreshLimiter *rate.Limiter
}

func (c *readinessCache) check(target readinessTarget, foreground bool, probe func() bool) bool {
	if target.address == "" {
		return false
	}
	c.mu.Lock()
	cached := c.entries[target.containerID][target.port]
	first := cached == nil || cached.address != target.address
	if first {
		cached = &readinessEntry{address: target.address}
		if c.entries == nil {
			c.entries = make(map[string]map[int32]*readinessEntry)
		}
		if c.entries[target.containerID] == nil {
			c.entries[target.containerID] = make(map[int32]*readinessEntry)
		}
		c.entries[target.containerID][target.port] = cached
	}
	if !cached.ready && cached.probing != nil {
		if !foreground {
			c.mu.Unlock()
			return false
		}
		done := cached.probing
		c.mu.Unlock()
		<-done
		c.mu.Lock()
		ready := c.entries[target.containerID][target.port] == cached && cached.ready
		c.mu.Unlock()
		return ready
	}
	if first || (!cached.ready && foreground) {
		cached.probing = make(chan struct{})
		c.mu.Unlock()
		// First checks and queued requests bypass idle refresh limits. Keeping
		// the entry during I/O makes invalidation authoritative for every probe.
		return c.probe(target, probe, cached)
	}
	limiter := c.refreshLimiter
	if limiter == nil {
		limiter = readinessRefreshBudget
	}
	if cached.probing == nil && time.Now().After(cached.refreshAt) && acquireReadinessRefresh(limiter) {
		cached.probing = make(chan struct{})
		// Discovery waits for first probes, never background refreshes.
		go func() {
			defer func() { <-readinessRefreshSlots }()
			c.probe(target, probe, cached)
		}()
	}
	ready := cached.ready
	c.mu.Unlock()
	return ready
}

func acquireReadinessRefresh(limiter *rate.Limiter) bool {
	select {
	case readinessRefreshSlots <- struct{}{}:
		if limiter.Allow() {
			return true
		}
		<-readinessRefreshSlots
	default:
	}
	return false
}

func (c *readinessCache) probe(target readinessTarget, probe func() bool, entry *readinessEntry) bool {
	ready := probe()
	c.mu.Lock()
	defer c.mu.Unlock()
	close(entry.probing)
	entry.probing = nil
	if c.entries[target.containerID][target.port] != entry {
		return false // Do not resurrect an invalidated or replaced target.
	}
	entry.ready = ready
	entry.refreshAt = time.Now().Add(containerReadinessRefresh)
	return ready
}

func (c *readinessCache) invalidate(containerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, containerID)
}

func (c *readinessCache) invalidateTarget(target readinessTarget) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry := c.entries[target.containerID][target.port]; entry != nil && entry.address == target.address {
		delete(c.entries[target.containerID], target.port)
	}
}

func (c *readinessCache) prunePorts(containerID string, addresses map[int32]string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for port, entry := range c.entries[containerID] {
		if addresses[port] != entry.address {
			delete(c.entries[containerID], port)
		}
	}
}

func (c *readinessCache) prune(active map[string]struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for containerID := range c.entries {
		if _, ok := active[containerID]; !ok {
			delete(c.entries, containerID)
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
