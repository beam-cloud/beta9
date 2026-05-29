package cache

import (
	"sort"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
)

func NewHostMap(cfg GlobalConfig, onHostAdded func(*Host) error) *HostMap {
	return &HostMap{
		hosts:       make(map[string]*Host),
		mu:          sync.RWMutex{},
		onHostAdded: onHostAdded,
		cfg:         cfg,
	}
}

type HostMap struct {
	hosts       map[string]*Host
	mu          sync.RWMutex
	onHostAdded func(*Host) error
	cfg         GlobalConfig
}

func (hm *HostMap) Set(host *Host) {
	if host == nil || host.HostId == "" {
		return
	}

	hm.mu.Lock()
	existing, exists := hm.hosts[host.HostId]
	if exists && existing.Addr == host.Addr && existing.PrivateAddr == host.PrivateAddr && existing.CapacityUsagePct == host.CapacityUsagePct {
		hm.mu.Unlock()
		return
	}

	hm.hosts[host.HostId] = host
	hm.mu.Unlock()

	if hm.onHostAdded == nil {
		return
	}

	if exists {
		Logger.Debugf("Updated host @ %s (PrivateAddr=%s, RTT=%s)", host.HostId, host.PrivateAddr, host.RTT)
	} else {
		Logger.Infof("Added new host @ %s (PrivateAddr=%s, RTT=%s)", host.HostId, host.PrivateAddr, host.RTT)
	}
	if err := hm.onHostAdded(host); err != nil {
		Logger.Warnf("failed to initialize cache host %s: %v", host.HostId, err)
		hm.mu.Lock()
		if exists {
			hm.hosts[host.HostId] = existing
		} else {
			delete(hm.hosts, host.HostId)
		}
		hm.mu.Unlock()
	}
}

func (hm *HostMap) Remove(host *Host) bool {
	if host == nil || host.HostId == "" {
		return false
	}

	hm.mu.Lock()
	defer hm.mu.Unlock()

	existing, exists := hm.hosts[host.HostId]
	if !exists {
		return false
	}
	if !sameCacheHostEndpoint(existing, host) {
		Logger.Debugf("Ignored stale host removal @ %s (stale PrivateAddr=%s active PrivateAddr=%s)", host.HostId, host.PrivateAddr, existing.PrivateAddr)
		return false
	}

	Logger.Infof("Removed host @ %s (PrivateAddr=%s, RTT=%s)", host.HostId, host.PrivateAddr, host.RTT)
	delete(hm.hosts, host.HostId)
	return true
}

func (hm *HostMap) Members() mapset.Set[string] {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	set := mapset.NewSet[string]()
	for host := range hm.hosts {
		set.Add(hm.hosts[host].HostId)
	}

	return set
}

func (hm *HostMap) GetAll() []*Host {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	hosts := make([]*Host, 0, len(hm.hosts))
	for _, host := range hm.hosts {
		hosts = append(hosts, host)
	}
	return hosts
}

func (hm *HostMap) Get(hostId string) *Host {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	host, exists := hm.hosts[hostId]
	if !exists {
		return nil
	}

	return host
}

// Closest finds the nearest host within a given timeout
// If no hosts are found, it will error out
func (hm *HostMap) Closest(timeout time.Duration) (*Host, error) {
	start := time.Now()
	for {
		hm.mu.RLock()
		hosts := make([]*Host, 0, len(hm.hosts))
		for _, host := range hm.hosts {
			hosts = append(hosts, host)
		}
		hm.mu.RUnlock()

		// If there are hosts, find the closest one.
		if len(hosts) > 0 {
			// Sort by rount-trip time, return closest
			sort.Slice(hosts, func(i, j int) bool { return hosts[i].RTT < hosts[j].RTT })
			return hosts[0], nil
		}

		elapsed := time.Since(start)

		// We reached the timeout
		if elapsed > timeout {
			return nil, ErrHostNotFound
		}

		time.Sleep(time.Second)
	}
}

// ClosestWithCapacity finds the nearest host with available storage capacity within a given timeout
// If no hosts are found, it will error out
func (hm *HostMap) ClosestWithCapacity(timeout time.Duration) (*Host, error) {
	start := time.Now()
	for {
		hm.mu.RLock()
		hosts := make([]*Host, 0, len(hm.hosts))
		for _, host := range hm.hosts {
			hosts = append(hosts, host)
		}
		hm.mu.RUnlock()

		// If there are hosts, find the closest one.
		if len(hosts) > 0 {
			// Sort by rount-trip time and capacity usage
			sort.Slice(hosts, func(i, j int) bool {
				if hosts[i].RTT != hosts[j].RTT {
					return hosts[i].RTT < hosts[j].RTT
				}
				return hosts[i].CapacityUsagePct < hosts[j].CapacityUsagePct
			})

			if len(hosts) == 0 {
				return nil, ErrHostNotFound
			}

			return hosts[0], nil
		}

		elapsed := time.Since(start)

		// We reached the timeout
		if elapsed > timeout {
			return nil, ErrHostNotFound
		}

		time.Sleep(time.Second)
	}
}
