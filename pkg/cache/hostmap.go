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
		mu:          sync.Mutex{},
		onHostAdded: onHostAdded,
		cfg:         cfg,
	}
}

type HostMap struct {
	hosts       map[string]*Host
	mu          sync.Mutex
	onHostAdded func(*Host) error
	cfg         GlobalConfig
}

func (hm *HostMap) Set(host *Host) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	_, exists := hm.hosts[host.HostId]
	if exists {
		return
	}

	hm.hosts[host.HostId] = host
	if hm.onHostAdded != nil {
		Logger.Infof("Added new host @ %s (PrivateAddr=%s, RTT=%s)", host.HostId, host.PrivateAddr, host.RTT)
		hm.onHostAdded(host)
	}
}

func (hm *HostMap) Remove(host *Host) {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	_, exists := hm.hosts[host.HostId]
	if !exists {
		return
	}

	Logger.Infof("Removed host @ %s (PrivateAddr=%s, RTT=%s)", host.HostId, host.PrivateAddr, host.RTT)
	delete(hm.hosts, host.HostId)
}

func (hm *HostMap) Members() mapset.Set[string] {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	set := mapset.NewSet[string]()
	for host := range hm.hosts {
		set.Add(hm.hosts[host].HostId)
	}

	return set
}

func (hm *HostMap) GetAll() []*Host {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hosts := make([]*Host, 0, len(hm.hosts))
	for _, host := range hm.hosts {
		hosts = append(hosts, host)
	}
	return hosts
}

func (hm *HostMap) Get(hostId string) *Host {
	hm.mu.Lock()
	defer hm.mu.Unlock()

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
		// If there are hosts, find the closet one
		if len(hm.hosts) > 0 {

			hm.mu.Lock()
			hosts := make([]*Host, 0, len(hm.hosts)) // Convert map into slice of hosts
			for _, host := range hm.hosts {
				hosts = append(hosts, host)
			}
			hm.mu.Unlock()

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
		// If there are hosts, find the closet one
		if len(hm.hosts) > 0 {

			hm.mu.Lock()
			hosts := make([]*Host, 0, len(hm.hosts)) // Convert map into slice of hosts
			for _, host := range hm.hosts {
				hosts = append(hosts, host)
			}
			hm.mu.Unlock()

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
