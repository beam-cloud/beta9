package worker

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/metrics"
	repo "github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/coreos/go-iptables/iptables"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/rs/zerolog/log"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

const (
	containerBridgeLinkName       string = "b9_br0"
	containerVethHostPrefix       string = "b9h"
	containerVethContainerPrefix  string = "b9c"
	legacyContainerVethHostPrefix string = "b9_veth_h_"
	networkInterfaceNameMaxLength        = 15
	containerSubnet               string = "192.168.0.0/20"
	containerGatewayAddress       string = "192.168.0.1"
	containerBridgeAddress        string = "192.168.0.1"
	containerSubnetIPv6           string = "fd00:abcd::/64"
	containerGatewayAddressIPv6   string = "fd00:abcd::1"
	containerBridgeAddressIPv6    string = "fd00:abcd::1"
	containerNetworkSlotPrefix    string = "network-slot"

	containerNetworkCleanupInterval     time.Duration = time.Minute * 1
	defaultContainerNetworkSlotPoolSize               = 16
	containerNetworkSlotPoolEnv         string        = "CONTAINER_NETWORK_SLOT_POOL_SIZE"
	containerNetworkSlotFillInterval    time.Duration = 2 * time.Second
	containerNetworkCleanupRPCTimeout   time.Duration = 30 * time.Second
)

type ContainerNetworkManager struct {
	ctx                 context.Context
	defaultLink         netlink.Link
	ipt                 *iptables.IPTables
	ipt6                *iptables.IPTables
	worker              *types.Worker
	workerId            string
	workerRepoClient    pb.WorkerRepositoryServiceClient
	containerRepoClient pb.ContainerRepositoryServiceClient
	eventRepo           repo.EventRepository
	networkPrefix       string
	bridgeMu            sync.Mutex
	ipMu                sync.Mutex
	iptablesMu          sync.Mutex
	containerLocksMu    sync.Mutex
	containerLocks      sync.Map
	config              types.AppConfig
	containerInstances  *common.SafeMap[*ContainerInstance]
	bridgeConfigured    bool
	bridgeLink          netlink.Link
	allocatedIPsLoaded  bool
	allocatedIPs        map[string]struct{}
	containerIPs        map[string]string
	nextIPv4Offset      uint32
	releasedIPs         []string
	slotPoolSize        int
	slotMu              sync.Mutex
	freeSlots           []*containerNetworkSlot
	containerSlots      map[string]*containerNetworkSlot
	totalSlots          int
	slotFillRunning     bool
	slotPoolClosed      bool
}

type PortBinding struct {
	HostPort      int
	ContainerPort int
}

type containerNetworkLock struct {
	mu   sync.Mutex
	refs int
}

type containerNetworkSlot struct {
	id            string
	reservationID string
	namespace     string
	vethHost      string
	ip            string
	ipv6          string
	netnsPath     string
}

func containerVethNames(containerId string) (string, string) {
	suffixLength := min(
		networkInterfaceNameMaxLength-len(containerVethHostPrefix),
		networkInterfaceNameMaxLength-len(containerVethContainerPrefix),
	)
	suffix := containerIdHashSuffix(containerId, suffixLength)
	return containerVethHostPrefix + suffix, containerVethContainerPrefix + suffix
}

func containerNetworkPrefix(clusterName, baseNetworkPrefix string) string {
	return common.NormalizeWorkerNetworkPrefix(clusterName, baseNetworkPrefix)
}

func containerNetworkSlotReservationID(slotID string) string {
	return fmt.Sprintf("%s:%s", containerNetworkSlotPrefix, slotID)
}

func containerNetworkSlotReservationIDForWorker(workerID, slotID string) string {
	if workerID == "" {
		return containerNetworkSlotReservationID(slotID)
	}
	return fmt.Sprintf("%s:%s:%s", containerNetworkSlotPrefix, workerID, slotID)
}

func containerNetworkSlotReservationParts(reservationID string) (string, string, bool) {
	prefix := containerNetworkSlotPrefix + ":"
	value, ok := strings.CutPrefix(reservationID, prefix)
	if !ok || value == "" {
		return "", "", false
	}

	parts := strings.Split(value, ":")
	slotID := parts[len(parts)-1]
	if slotID == "" {
		return "", "", false
	}
	if len(parts) == 1 {
		return "", slotID, true
	}
	return parts[0], slotID, true
}

func containerNetworkSlotIDFromReservationID(reservationID string) (string, bool) {
	_, slotID, ok := containerNetworkSlotReservationParts(reservationID)
	return slotID, ok
}

func (m *ContainerNetworkManager) containerNetworkSlotReservationID(slotID string) string {
	return containerNetworkSlotReservationIDForWorker(m.workerId, slotID)
}

func (m *ContainerNetworkManager) containerNetworkSlotReservation(slot *containerNetworkSlot) string {
	if slot == nil {
		return ""
	}
	if slot.reservationID != "" {
		return slot.reservationID
	}
	return m.containerNetworkSlotReservationID(slot.id)
}

func containerIPv4AddressCount() int {
	_, ipNet, _ := net.ParseCIDR(containerSubnet)
	ones, bits := ipNet.Mask.Size()
	if bits != 32 || ones < 0 {
		return 0
	}
	return 1 << uint(bits-ones)
}

func containerNetworkSlotPoolSizeForPool(poolConfig types.WorkerPoolConfig, startLimit int) int {
	poolSize := 0
	if containerNetworkPreallocationEnabled(poolConfig) {
		poolSize = poolConfig.NetworkSlotPoolSize
		if poolSize <= 0 {
			poolSize = startLimit
		}
	}
	if poolSize <= 0 && containerNetworkPreallocationEnabled(poolConfig) {
		poolSize = defaultContainerNetworkSlotPoolSize
	}
	if raw := os.Getenv(containerNetworkSlotPoolEnv); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed >= 0 {
			poolSize = parsed
		}
	}
	if poolSize > containerIPv4AddressCount()-2 {
		return containerIPv4AddressCount() - 2
	}
	return poolSize
}

func containerNetworkPreallocationEnabled(poolConfig types.WorkerPoolConfig) bool {
	if poolConfig.NetworkPreallocation == nil {
		return true
	}
	return *poolConfig.NetworkPreallocation
}

func containerIdHashSuffix(containerId string, length int) string {
	sum := sha1.Sum([]byte(containerId))
	encoded := hex.EncodeToString(sum[:])
	if length > len(encoded) {
		length = len(encoded)
	}
	return encoded[:length]
}

func containerIPv4Mask() net.IPMask {
	_, ipNet, err := net.ParseCIDR(containerSubnet)
	if err != nil {
		return net.CIDRMask(24, 32)
	}
	return ipNet.Mask
}

func containerIPv4HostOffset(ip net.IP) (uint32, error) {
	ipv4 := ip.To4()
	if ipv4 == nil {
		return 0, fmt.Errorf("invalid IPv4 address: %s", ip)
	}

	_, ipNet, err := net.ParseCIDR(containerSubnet)
	if err != nil {
		return 0, fmt.Errorf("failed to parse IPv4 subnet: %w", err)
	}
	if !ipNet.Contains(ipv4) {
		return 0, fmt.Errorf("IPv4 address %s is outside container subnet %s", ip, containerSubnet)
	}

	base := ipNet.IP.To4()
	if base == nil {
		return 0, fmt.Errorf("invalid IPv4 subnet base: %s", containerSubnet)
	}

	return binary.BigEndian.Uint32(ipv4) - binary.BigEndian.Uint32(base), nil
}

func containerIPv6Address(ip net.IP, ipv6Net *net.IPNet) (net.IP, error) {
	offset, err := containerIPv4HostOffset(ip)
	if err != nil {
		return nil, err
	}

	ipv6 := append(net.IP(nil), ipv6Net.IP.To16()...)
	if ipv6 == nil {
		return nil, fmt.Errorf("invalid IPv6 subnet: %s", ipv6Net.String())
	}
	binary.BigEndian.PutUint32(ipv6[12:16], offset)
	return ipv6, nil
}

type containerNetworkRuleInfo struct {
	ContainerID string
	Namespace   string
	VethHost    string
	IPv4        string
	IPv6        string
}

func containerNetworkComment(vethHost, containerId, namespace string) string {
	if namespace == "" {
		namespace = containerId
	}
	return fmt.Sprintf("%s:%s:%s", vethHost, containerId, namespace)
}

func containerNetworkRuleInfoFromIptablesRule(rule string) (containerNetworkRuleInfo, bool) {
	idx := strings.LastIndex(rule, containerVethHostPrefix)
	if idx == -1 {
		idx = strings.LastIndex(rule, legacyContainerVethHostPrefix)
	}
	if idx == -1 {
		return containerNetworkRuleInfo{}, false
	}

	comment := rule[idx:]
	comment = strings.Fields(comment)[0]
	comment = strings.Trim(comment, `"`)
	comment = strings.TrimRight(comment, `\`)
	comment = strings.TrimSuffix(comment, "*/")

	parts := strings.Split(comment, ":")
	if len(parts) < 2 {
		return containerNetworkRuleInfo{}, false
	}

	info := containerNetworkRuleInfo{
		VethHost:    parts[0],
		ContainerID: parts[1],
		Namespace:   parts[1],
	}
	if len(parts) >= 3 && parts[2] != "" {
		info.Namespace = parts[2]
	}

	if ip, ok := iptablesRuleDestinationIP(rule); ok {
		if strings.Contains(ip, ":") {
			info.IPv6 = ip
		} else {
			info.IPv4 = ip
		}
	}

	return info, info.ContainerID != ""
}

func containerIdFromIptablesRule(rule string) (string, bool) {
	info, ok := containerNetworkRuleInfoFromIptablesRule(rule)
	return info.ContainerID, ok
}

func iptablesRuleDestinationIP(rule string) (string, bool) {
	fields := strings.Fields(rule)
	for i, field := range fields {
		if field != "--to-destination" || i+1 >= len(fields) {
			continue
		}
		destination := strings.Trim(fields[i+1], `"`)
		if strings.HasPrefix(destination, "[") {
			end := strings.Index(destination, "]")
			if end > 1 {
				return destination[1:end], true
			}
			return "", false
		}
		host, _, err := net.SplitHostPort(destination)
		if err == nil {
			return host, true
		}
		if idx := strings.LastIndex(destination, ":"); idx > 0 {
			return destination[:idx], true
		}
		return destination, destination != ""
	}
	return "", false
}

func NewContainerNetworkManager(ctx context.Context, workerId, poolName string, workerRepoClient pb.WorkerRepositoryServiceClient, containerRepoClient pb.ContainerRepositoryServiceClient, eventRepo repo.EventRepository, config types.AppConfig, containerInstances *common.SafeMap[*ContainerInstance], poolConfig types.WorkerPoolConfig, containerStartLimit int) (*ContainerNetworkManager, error) {
	defaultLink, err := getDefaultInterface()
	if err != nil {
		return nil, err
	}

	ipTablesMode := detectIptablesMode()

	ipv4Path := ""
	ipv6Path := ""
	switch ipTablesMode {
	case "nftables":
		ipv4Path = "/usr/sbin/iptables-nft"
		ipv6Path = "/usr/sbin/ip6tables-nft"
	case "legacy":
		fallthrough
	default:
		ipv4Path = "/usr/sbin/iptables"
		ipv6Path = "/usr/sbin/ip6tables"
	}

	ipt, err := iptables.New(iptables.Path(ipv4Path), iptables.IPFamily(iptables.ProtocolIPv4))
	if err != nil {
		return nil, err
	}

	// Initialize ip6tables for IPv6 support
	var ipt6 *iptables.IPTables

	ipt6Supported := true
	ipt6, err = iptables.New(iptables.Path(ipv6Path), iptables.IPFamily(iptables.ProtocolIPv6))
	if err != nil {
		log.Warn().Err(err).Msg("IPv6 iptables initialization failed, falling back to IPv4 only")
		ipt6Supported = false
	} else {
		// Check if the ip6tables NAT table can be accessed
		_, err := ipt6.List("nat", "POSTROUTING")
		if err != nil {
			log.Warn().Err(err).Msg("IPv6 iptables NAT table not available, falling back to IPv4 only")
			ipt6Supported = false
		}
	}

	baseNetworkPrefix := os.Getenv("NETWORK_PREFIX")
	if baseNetworkPrefix == "" {
		return nil, errors.New("invalid network prefix")
	}
	networkPrefix := containerNetworkPrefix(config.ClusterName, baseNetworkPrefix)

	m := &ContainerNetworkManager{
		ctx:                 ctx,
		ipt:                 ipt,
		ipt6:                ipt6,
		defaultLink:         defaultLink,
		workerId:            workerId,
		workerRepoClient:    workerRepoClient,
		containerRepoClient: containerRepoClient,
		eventRepo:           eventRepo,
		networkPrefix:       networkPrefix,
		config:              config,
		containerInstances:  containerInstances,
		allocatedIPs:        map[string]struct{}{},
		containerIPs:        map[string]string{},
		slotPoolSize:        containerNetworkSlotPoolSizeForPool(poolConfig, containerStartLimit),
		containerSlots:      map[string]*containerNetworkSlot{},
	}

	// Disable IPv6 if ip6tables is not supported
	if !ipt6Supported {
		m.ipt6 = nil
	}

	if _, err := m.getOrSetupBridge(containerBridgeLinkName); err != nil {
		return nil, err
	}

	go m.cleanupOrphanedNamespaces()
	if m.slotPoolSize > 0 {
		if err := m.cleanupStaleNetworkSlots(); err != nil {
			if isRedisLockNotObtained(err) {
				log.Debug().Err(err).Msg("skipped stale preallocated network slot cleanup because another worker holds the cleanup lock")
			} else {
				log.Warn().Err(err).Msg("failed to clean up stale preallocated network slots")
			}
		}
		go m.maintainNetworkSlotPool()
	}

	return m, nil
}

func (m *ContainerNetworkManager) lockContainerNetwork(containerId string) func() {
	m.containerLocksMu.Lock()
	lockValue, exists := m.containerLocks.Load(containerId)
	if !exists {
		lockValue = &containerNetworkLock{}
		m.containerLocks.Store(containerId, lockValue)
	}
	lock := lockValue.(*containerNetworkLock)
	lock.refs++
	m.containerLocksMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()

		m.containerLocksMu.Lock()
		defer m.containerLocksMu.Unlock()

		lock.refs--
		if lock.refs == 0 {
			m.containerLocks.Delete(containerId)
		}
	}
}

func isRedisLockNotObtained(err error) bool {
	return err != nil && strings.Contains(err.Error(), "redislock: not obtained")
}

func isMissingNetworkReservation(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "redis: nil") ||
		strings.Contains(msg, "source container does not own requested ip")
}

func (m *ContainerNetworkManager) maintainNetworkSlotPool() {
	m.fillNetworkSlotPool()

	ticker := time.NewTicker(containerNetworkSlotFillInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.fillNetworkSlotPool()
		}
	}
}

func (m *ContainerNetworkManager) fillNetworkSlotPool() {
	m.slotMu.Lock()
	if m.slotPoolClosed || m.slotFillRunning {
		m.slotMu.Unlock()
		return
	}
	m.slotFillRunning = true
	m.slotMu.Unlock()

	defer func() {
		m.slotMu.Lock()
		m.slotFillRunning = false
		m.slotMu.Unlock()
	}()

	for {
		m.slotMu.Lock()
		if m.slotPoolClosed {
			m.slotMu.Unlock()
			return
		}
		needed := m.slotPoolSize - m.totalSlots
		m.slotMu.Unlock()
		if needed <= 0 {
			return
		}

		slot, err := m.createNetworkSlot()
		if err != nil {
			log.Debug().Err(err).Msg("failed to preallocate container network slot")
			return
		}

		m.slotMu.Lock()
		if m.slotPoolClosed {
			m.slotMu.Unlock()
			if err := m.releaseUnusedNetworkSlot(slot); err != nil {
				log.Debug().Str("network_slot", slot.id).Err(err).Msg("failed to release network slot created during shutdown")
			}
			return
		}
		m.freeSlots = append(m.freeSlots, slot)
		m.totalSlots++
		m.slotMu.Unlock()
	}
}

func (m *ContainerNetworkManager) withNetworkSlotPoolLock(fn func() error) error {
	lockResponse, err := handleGRPCResponse(m.workerRepoClient.SetNetworkLock(m.ctx, &pb.SetNetworkLockRequest{
		NetworkPrefix: m.networkPrefix + ":slot_pool",
		Ttl:           30,
		Retries:       3,
	}))
	if err != nil {
		return err
	}
	defer m.workerRepoClient.RemoveNetworkLock(m.ctx, &pb.RemoveNetworkLockRequest{
		NetworkPrefix: m.networkPrefix + ":slot_pool",
		Token:         lockResponse.Token,
	})

	return fn()
}

func (m *ContainerNetworkManager) cleanupStaleNetworkSlots() error {
	return m.withNetworkSlotPoolLock(func() error {
		response, err := handleGRPCResponse(m.workerRepoClient.GetContainerIpAssignments(m.ctx, &pb.GetContainerIpAssignmentsRequest{
			NetworkPrefix: m.networkPrefix,
		}))
		if err != nil {
			return err
		}

		removed := 0
		workerExists := map[string]bool{m.workerId: true}
		for _, assignment := range response.Assignments {
			slotWorkerID, slotID, ok := containerNetworkSlotReservationParts(assignment.ContainerId)
			if !ok {
				continue
			}
			shouldCleanup, err := shouldCleanupNetworkSlotReservation(
				m.workerId,
				slotWorkerID,
				m.networkSlotResourcesExist(slotID),
				func(workerID string) (bool, error) {
					return m.workerExistsCached(workerID, workerExists)
				},
			)
			if err != nil {
				log.Debug().Str("network_slot", slotID).Str("reservation_id", assignment.ContainerId).Err(err).Msg("skipping stale network slot cleanup because worker liveness is unknown")
				continue
			}
			if !shouldCleanup {
				continue
			}
			m.deleteNetworkSlotResources(slotID)

			if err := m.removeContainerIPFromRepository(assignment.ContainerId); err != nil {
				log.Debug().Str("network_slot", slotID).Str("reservation_id", assignment.ContainerId).Err(err).Msg("failed to remove stale network slot reservation")
				continue
			}
			removed++
		}

		if removed > 0 {
			m.ipMu.Lock()
			m.allocatedIPsLoaded = false
			m.ipMu.Unlock()
			log.Info().Int("removed", removed).Str("network_prefix", m.networkPrefix).Msg("removed stale preallocated network slot reservations")
		}

		return nil
	})
}

func shouldCleanupNetworkSlotReservation(currentWorkerID, slotWorkerID string, resourcesExist bool, workerExists func(string) (bool, error)) (bool, error) {
	if slotWorkerID == "" || slotWorkerID == currentWorkerID {
		return !resourcesExist, nil
	}

	alive, err := workerExists(slotWorkerID)
	if err != nil {
		return false, err
	}
	return !alive, nil
}

func (m *ContainerNetworkManager) workerExistsCached(workerID string, cache map[string]bool) (bool, error) {
	alive, cached := cache[workerID]
	if cached {
		return alive, nil
	}

	alive, err := m.workerExists(workerID)
	if err != nil {
		return false, err
	}
	cache[workerID] = alive
	return alive, nil
}

func (m *ContainerNetworkManager) workerExists(workerID string) (bool, error) {
	_, err := handleGRPCResponse(m.workerRepoClient.GetWorkerById(m.ctx, &pb.GetWorkerByIdRequest{
		WorkerId: workerID,
	}))
	if err == nil {
		return true, nil
	}

	notFoundErr := &types.ErrWorkerNotFound{}
	if notFoundErr.From(err) {
		return false, nil
	}
	return false, err
}

func (m *ContainerNetworkManager) networkSlotResourcesExist(slotID string) bool {
	if _, err := os.Stat(filepath.Join("/var/run/netns", slotID)); err != nil {
		return false
	}

	vethHost, _ := containerVethNames(slotID)
	if _, err := netlink.LinkByName(vethHost); err != nil {
		return false
	}

	return true
}

func (m *ContainerNetworkManager) deleteNetworkSlotResources(slotID string) {
	vethHost, _ := containerVethNames(slotID)
	if hostVeth, err := netlink.LinkByName(vethHost); err == nil {
		if err := netlink.LinkDel(hostVeth); err != nil {
			log.Debug().Str("network_slot", slotID).Err(err).Msg("failed to delete stale network slot veth")
		}
	}
	if err := netns.DeleteNamed(slotID); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Debug().Str("network_slot", slotID).Err(err).Msg("failed to delete stale network slot namespace")
	}
}

func (m *ContainerNetworkManager) Close() error {
	slots := m.drainFreeNetworkSlots()
	var errs error
	for _, slot := range slots {
		if err := m.releaseUnusedNetworkSlot(slot); err != nil {
			errs = errors.Join(errs, err)
		}
	}
	return errs
}

func (m *ContainerNetworkManager) drainFreeNetworkSlots() []*containerNetworkSlot {
	m.slotMu.Lock()
	defer m.slotMu.Unlock()

	m.slotPoolClosed = true
	slots := m.freeSlots
	m.freeSlots = nil
	if m.totalSlots >= len(slots) {
		m.totalSlots -= len(slots)
	} else {
		m.totalSlots = 0
	}
	return slots
}

func (m *ContainerNetworkManager) releaseUnusedNetworkSlot(slot *containerNetworkSlot) error {
	if slot == nil {
		return nil
	}

	m.deleteNetworkSlotResources(slot.id)
	if err := m.removeContainerIPFromRepositoryWithContext(context.Background(), m.containerNetworkSlotReservation(slot)); err != nil {
		return fmt.Errorf("failed to release preallocated network slot %s: %w", slot.id, err)
	}
	m.forgetContainerIP(m.containerNetworkSlotReservation(slot), slot.ip)
	return nil
}

func (m *ContainerNetworkManager) setupPreallocatedNetworkSlot(containerId string, spec *specs.Spec, request *types.ContainerRequest) (bool, error) {
	slot := m.acquireNetworkSlot()
	if slot == nil {
		return false, nil
	}

	phaseStart := time.Now()
	err := m.assignPreallocatedNetworkSlot(containerId, slot)
	metrics.RecordWorkerStartupPhase("network_set_container_ip", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil), "mode": "preallocated"})
	m.recordNetworkLifecycle(request, types.ContainerLifecycleNetworkSetContainerIP, phaseStart, err == nil, map[string]string{"mode": "preallocated"})
	if err != nil {
		m.discardNetworkSlot(containerId, slot)
		return true, err
	}

	spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{
		Type: specs.NetworkNamespace,
		Path: slot.netnsPath,
	})

	m.slotMu.Lock()
	m.containerSlots[containerId] = slot
	m.slotMu.Unlock()

	if containerInstance, exists := m.containerInstances.Get(containerId); exists {
		containerInstance.ContainerIp = slot.ip
		m.containerInstances.Set(containerId, containerInstance)
	}

	log.Debug().Str("container_id", containerId).Str("ip_address", slot.ip).Str("network_slot", slot.id).Msg("container preallocated network slot assigned")
	if err := m.setupNetworkRestrictions(containerId, request); err != nil {
		m.rollbackPreallocatedNetworkSlotAssignment(containerId, slot)
		return true, err
	}

	go m.fillNetworkSlotPool()
	return true, nil
}

func (m *ContainerNetworkManager) assignPreallocatedNetworkSlot(containerId string, slot *containerNetworkSlot) error {
	reservationID := m.containerNetworkSlotReservation(slot)
	_, err := handleGRPCResponse(m.workerRepoClient.MoveContainerIp(m.ctx, &pb.MoveContainerIpRequest{
		NetworkPrefix:   m.networkPrefix,
		FromContainerId: reservationID,
		ToContainerId:   containerId,
		IpAddress:       slot.ip,
	}))
	if err != nil {
		return err
	}

	m.ipMu.Lock()
	m.forgetContainerIPLocked(reservationID, slot.ip)
	m.rememberContainerIPLocked(containerId, slot.ip)
	m.ipMu.Unlock()
	return nil
}

func (m *ContainerNetworkManager) acquireNetworkSlot() *containerNetworkSlot {
	m.slotMu.Lock()
	defer m.slotMu.Unlock()

	for len(m.freeSlots) > 0 {
		last := len(m.freeSlots) - 1
		slot := m.freeSlots[last]
		m.freeSlots = m.freeSlots[:last]
		if slot != nil {
			return slot
		}
	}

	return nil
}

func (m *ContainerNetworkManager) returnNetworkSlot(containerId string, slot *containerNetworkSlot) {
	if slot == nil {
		return
	}

	m.slotMu.Lock()
	if containerId != "" {
		delete(m.containerSlots, containerId)
	}
	m.freeSlots = append(m.freeSlots, slot)
	m.slotMu.Unlock()
}

func (m *ContainerNetworkManager) rollbackPreallocatedNetworkSlotAssignment(containerId string, slot *containerNetworkSlot) {
	if err := m.removePreallocatedNetworkSlotRules(slot); err != nil {
		log.Warn().Str("container_id", containerId).Str("network_slot", slot.id).Err(err).Msg("failed to remove preallocated network slot rules after setup error")
		m.discardNetworkSlot(containerId, slot)
		return
	}

	if err := m.releasePreallocatedNetworkSlot(containerId, slot); err != nil {
		log.Warn().Str("container_id", containerId).Str("network_slot", slot.id).Err(err).Msg("failed to release preallocated network slot after setup error")
		m.discardNetworkSlot(containerId, slot)
		return
	}

	m.clearContainerInstanceIP(containerId)
	m.returnNetworkSlot(containerId, slot)
}

func (m *ContainerNetworkManager) discardNetworkSlot(containerId string, slot *containerNetworkSlot) {
	if slot == nil {
		return
	}

	m.slotMu.Lock()
	if containerId != "" {
		delete(m.containerSlots, containerId)
	}
	if m.totalSlots > 0 {
		m.totalSlots--
	}
	m.slotMu.Unlock()

	m.clearContainerInstanceIP(containerId)

	if hostVeth, err := netlink.LinkByName(slot.vethHost); err == nil {
		if err := netlink.LinkDel(hostVeth); err != nil {
			log.Debug().Str("network_slot", slot.id).Err(err).Msg("failed to delete discarded network slot veth")
		}
	}
	if err := netns.DeleteNamed(slot.namespace); err != nil {
		log.Debug().Str("network_slot", slot.id).Err(err).Msg("failed to delete discarded network slot namespace")
	}

	if containerId != "" && m.workerRepoClient != nil {
		if err := m.removeContainerIPFromRepository(containerId); err != nil {
			log.Debug().Str("container_id", containerId).Str("network_slot", slot.id).Err(err).Msg("failed to remove container ip while discarding network slot")
		}
	}
	if m.workerRepoClient != nil {
		if err := m.removeContainerIPFromRepository(m.containerNetworkSlotReservation(slot)); err != nil {
			log.Debug().Str("network_slot", slot.id).Err(err).Msg("failed to remove slot ip while discarding network slot")
		}
	}

	m.forgetContainerIP(containerId, slot.ip)
	m.forgetContainerIP(m.containerNetworkSlotReservation(slot), slot.ip)
}

func (m *ContainerNetworkManager) clearContainerInstanceIP(containerId string) {
	if containerId == "" || m.containerInstances == nil {
		return
	}

	if containerInstance, exists := m.containerInstances.Get(containerId); exists {
		containerInstance.ContainerIp = ""
		m.containerInstances.Set(containerId, containerInstance)
	}
}

func (m *ContainerNetworkManager) containerNetworkSlot(containerId string) *containerNetworkSlot {
	m.slotMu.Lock()
	defer m.slotMu.Unlock()
	return m.containerSlots[containerId]
}

func (m *ContainerNetworkManager) createNetworkSlot() (*containerNetworkSlot, error) {
	var slot *containerNetworkSlot
	err := m.withNetworkSlotPoolLock(func() error {
		created, err := m.createNetworkSlotLocked()
		if err != nil {
			return err
		}
		slot = created
		return nil
	})
	return slot, err
}

func (m *ContainerNetworkManager) createNetworkSlotLocked() (*containerNetworkSlot, error) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	slotID := randomNetworkSlotID()
	reservationID := m.containerNetworkSlotReservationID(slotID)
	namespace := slotID
	vethHost, vethContainer := containerVethNames(slotID)
	slotReady := false
	defer func() {
		if slotReady {
			return
		}
		if hostVeth, linkErr := netlink.LinkByName(vethHost); linkErr == nil {
			_ = netlink.LinkDel(hostVeth)
		}
		_ = netns.DeleteNamed(namespace)
	}()

	hostNS, err := netns.Get()
	if err != nil {
		return nil, err
	}
	defer hostNS.Close()

	ipAddr, err := m.reserveNetworkSlotIP(reservationID)
	if err != nil {
		return nil, err
	}
	releaseIP := func() {
		if slotReady {
			return
		}
		if err := m.removeContainerIPFromRepository(reservationID); err != nil {
			log.Debug().Str("network_slot", slotID).Err(err).Msg("failed to release preallocated network slot reservation")
		}
		m.forgetContainerIP(reservationID, ipAddr.IP.String())
	}
	defer releaseIP()

	if err = m.createVethPair(vethHost, vethContainer); err != nil {
		return nil, err
	}

	hostVeth, err := netlink.LinkByName(vethHost)
	if err != nil {
		return nil, err
	}
	bridge, err := m.getOrSetupBridge(containerBridgeLinkName)
	if err != nil {
		return nil, err
	}
	if err = netlink.LinkSetMaster(hostVeth, bridge); err != nil {
		return nil, err
	}
	if err = netlink.LinkSetUp(hostVeth); err != nil {
		return nil, err
	}

	newNs, err := netns.NewNamed(namespace)
	if err != nil {
		return nil, err
	}
	defer newNs.Close()

	if err = netns.Set(hostNS); err != nil {
		return nil, err
	}

	containerVeth, err := netlink.LinkByName(vethContainer)
	if err != nil {
		return nil, err
	}
	if err = netlink.LinkSetNsFd(containerVeth, int(newNs)); err != nil {
		return nil, err
	}

	err = m.configureContainerLink(&containerNetworkConfigOpts{
		containerId:   slotID,
		containerVeth: containerVeth,
		hostNS:        hostNS,
		containerNS:   newNs,
	}, ipAddr)
	if err != nil {
		return nil, err
	}

	ipv6 := ""
	if m.ipt6 != nil {
		_, ipv6Net, _ := net.ParseCIDR(containerSubnetIPv6)
		ipv6Address, ipv6Err := containerIPv6Address(ipAddr.IP, ipv6Net)
		if ipv6Err != nil {
			return nil, ipv6Err
		}
		ipv6 = ipv6Address.String()
	}

	slotReady = true
	return &containerNetworkSlot{
		id:            slotID,
		reservationID: reservationID,
		namespace:     namespace,
		vethHost:      vethHost,
		ip:            ipAddr.IP.String(),
		ipv6:          ipv6,
		netnsPath:     filepath.Join("/var/run/netns", namespace),
	}, nil
}

func (m *ContainerNetworkManager) reserveNetworkSlotIP(reservationID string) (*netlink.Addr, error) {
	m.ipMu.Lock()
	defer m.ipMu.Unlock()

	if err := m.reloadAllocatedIPsLocked(); err != nil {
		return nil, err
	}

	var lastErr error
	for attempts := 0; attempts < containerIPv4AddressCount(); attempts++ {
		ipAddr := m.nextAvailableContainerIPLocked()
		if ipAddr == nil {
			if lastErr != nil {
				return nil, fmt.Errorf("unable to assign IP address to preallocated network slot: no available addresses after reservation conflicts: %w", lastErr)
			}
			return nil, errors.New("unable to assign IP address to preallocated network slot: no available addresses")
		}

		_, err := handleGRPCResponse(m.workerRepoClient.SetContainerIp(m.ctx, &pb.SetContainerIpRequest{
			NetworkPrefix: m.networkPrefix,
			ContainerId:   reservationID,
			IpAddress:     ipAddr.IP.String(),
		}))
		if err != nil {
			lastErr = err
			m.allocatedIPs[ipAddr.IP.String()] = struct{}{}
			continue
		}

		m.rememberContainerIPLocked(reservationID, ipAddr.IP.String())
		return ipAddr, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("unable to reserve unique IP address for preallocated network slot: %w", lastErr)
	}
	return nil, errors.New("unable to reserve unique IP address for preallocated network slot")
}

func (m *ContainerNetworkManager) recordNetworkLifecycle(request *types.ContainerRequest, lifecycleID types.ContainerLifecycleID, startedAt time.Time, success bool, attrs map[string]string) {
	if m.eventRepo == nil || request == nil || request.ContainerId == "" || startedAt.IsZero() {
		return
	}
	if attrs == nil {
		attrs = map[string]string{}
	}
	def := types.ContainerLifecycleDefinitionFor(lifecycleID)
	endTime := time.Now()
	m.eventRepo.PushContainerLifecycleEvent(types.EventContainerLifecycleSchema{
		ID:          lifecycleID,
		Domain:      def.Domain,
		ParentID:    def.ParentID,
		StartTime:   startedAt.UTC(),
		EndTime:     endTime.UTC(),
		DurationMs:  endTime.Sub(startedAt).Milliseconds(),
		ContainerID: request.ContainerId,
		StubID:      request.StubId,
		StubType:    string(request.Stub.Type.Kind()),
		TaskID:      taskIDFromContainerRequestEnv(request.Env),
		WorkspaceID: request.WorkspaceId,
		WorkerID:    m.workerId,
		Success:     &success,
		Source:      types.EventSourceWorkerNetwork.String(),
		Attrs:       attrs,
	})
}

func (m *ContainerNetworkManager) getContainerNetworkInfo(containerId string) (*containerNetworkInfo, error) {
	if slot := m.containerNetworkSlot(containerId); slot != nil {
		return containerNetworkInfoFromSlot(containerId, slot, m.ipt6 != nil)
	}
	if instance, exists := m.containerInstances.Get(containerId); exists && instance.ContainerIp != "" {
		return containerNetworkInfoFromIP(containerId, instance.ContainerIp, m.ipt6 != nil)
	}
	info, err := getContainerNetworkInfo(m.ctx, m.workerRepoClient, m.networkPrefix, containerId, m.ipt6 != nil)
	if err == nil {
		return info, nil
	}
	if fallback, fallbackErr := m.getContainerNetworkInfoFromIptables(containerId); fallbackErr == nil {
		log.Debug().Str("container_id", containerId).Err(err).Msg("recovered container network info from iptables")
		return fallback, nil
	}
	return nil, err
}

func taskIDFromContainerRequestEnv(env []string) string {
	for _, entry := range env {
		if value, ok := strings.CutPrefix(entry, "TASK_ID="); ok {
			return value
		}
	}
	return ""
}

// detectIptablesMode detects which iptables version is use on the host based on where the KUBE-FORWARD chain has been setup
func detectIptablesMode() string {
	iptNft, err := iptables.New(iptables.IPFamily(iptables.ProtocolIPv4), iptables.Path("/usr/sbin/iptables-nft"))
	if err == nil {
		if exists, _ := iptNft.ChainExists("filter", "KUBE-FORWARD"); exists {
			return "nftables"
		}
	}

	iptLegacy, err := iptables.New(iptables.IPFamily(iptables.ProtocolIPv4), iptables.Path("/usr/sbin/iptables-legacy"))
	if err == nil {
		if exists, _ := iptLegacy.ChainExists("filter", "KUBE-FORWARD"); exists {
			return "legacy"
		}
	}

	// Default to legacy if no KUBE-FORWARD chain found
	return "legacy"
}

func (m *ContainerNetworkManager) Setup(containerId string, spec *specs.Spec, request *types.ContainerRequest) error {
	if spec == nil || spec.Linux == nil {
		return errors.New("container network setup requires a Linux runtime spec")
	}

	unlockContainer := m.lockContainerNetwork(containerId)
	defer unlockContainer()

	usedSlot, err := m.setupPreallocatedNetworkSlot(containerId, spec, request)
	if usedSlot || err != nil {
		return err
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	namespace := containerId
	vethHost, vethContainer := containerVethNames(containerId)

	// Store default network namespace for later
	hostNS, err := netns.Get()
	if err != nil {
		return err
	}
	defer hostNS.Close()

	// Create a veth pair in the host namespace
	phaseStart := time.Now()
	if err = m.createVethPair(vethHost, vethContainer); err != nil {
		metrics.RecordWorkerStartupPhase("network_create_veth", time.Since(phaseStart), request, map[string]string{"success": "false"})
		m.recordNetworkLifecycle(request, types.ContainerLifecycleNetworkCreateVeth, phaseStart, false, nil)
		return err
	}
	metrics.RecordWorkerStartupPhase("network_create_veth", time.Since(phaseStart), request, map[string]string{"success": "true"})
	m.recordNetworkLifecycle(request, types.ContainerLifecycleNetworkCreateVeth, phaseStart, true, nil)

	// Set up the bridge in the host namespace and add the host side of the veth pair to it
	hostVeth, err := netlink.LinkByName(vethHost)
	if err != nil {
		return err
	}
	phaseStart = time.Now()
	bridge, err := m.getOrSetupBridge(containerBridgeLinkName)
	metrics.RecordWorkerStartupPhase("network_setup_bridge", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	m.recordNetworkLifecycle(request, types.ContainerLifecycleNetworkSetupBridge, phaseStart, err == nil, nil)
	if err != nil {
		return err
	}

	// Associate new veth on the host side with the bridge device
	if err := netlink.LinkSetMaster(hostVeth, bridge); err != nil {
		return err
	}

	if err := netlink.LinkSetUp(hostVeth); err != nil {
		return err
	}

	// Create a new namespace for the container
	phaseStart = time.Now()
	newNs, err := netns.NewNamed(namespace)
	metrics.RecordWorkerStartupPhase("network_create_namespace", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	m.recordNetworkLifecycle(request, types.ContainerLifecycleNetworkCreateNamespace, phaseStart, err == nil, nil)
	if err != nil {
		return err
	}
	defer newNs.Close()

	// By default, creating a new namespace automatically sets the current namespace
	// to that newly created namespace. So we have to move back to host namespace.
	err = netns.Set(hostNS)
	if err != nil {
		return err
	}

	// Move the container side of the veth pair into the new namespace
	containerVeth, err := netlink.LinkByName(vethContainer)
	if err != nil {
		return err
	}
	err = netlink.LinkSetNsFd(containerVeth, int(newNs))
	if err != nil {
		return err
	}

	// Update the runc spec to use the new network namespace
	spec.Linux.Namespaces = append(spec.Linux.Namespaces, specs.LinuxNamespace{
		Type: specs.NetworkNamespace,
		Path: filepath.Join("/var/run/netns", namespace),
	})

	// Configure the network inside the container's namespace
	phaseStart = time.Now()
	err = m.configureContainerNetwork(&containerNetworkConfigOpts{
		containerId:   containerId,
		containerVeth: containerVeth,
		hostNS:        hostNS,
		containerNS:   newNs,
		request:       request,
	})
	metrics.RecordWorkerStartupPhase("network_configure_namespace", time.Since(phaseStart), request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	m.recordNetworkLifecycle(request, types.ContainerLifecycleNetworkConfigureNamespace, phaseStart, err == nil, nil)

	if err != nil {
		return err
	}

	if err := m.setupNetworkRestrictions(containerId, request); err != nil {
		return err
	}

	return nil
}

func (m *ContainerNetworkManager) setupNetworkRestrictions(containerId string, request *types.ContainerRequest) error {
	mode, apply := m.networkRestriction(containerId, request)
	if apply == nil {
		return nil
	}

	phaseStart := time.Now()
	err := apply()
	success := err == nil
	metrics.RecordWorkerStartupPhase("network_restrictions", time.Since(phaseStart), request, map[string]string{
		"mode":    mode,
		"success": fmt.Sprintf("%t", success),
	})
	m.recordNetworkLifecycle(request, types.ContainerLifecycleNetworkRestrictions, phaseStart, success, map[string]string{"mode": mode})
	return err
}

func (m *ContainerNetworkManager) networkRestriction(containerId string, request *types.ContainerRequest) (string, func() error) {
	if request == nil {
		return "", nil
	}
	if len(request.AllowList) > 0 {
		return "allowlist", func() error {
			return m.setupAllowList(containerId, request, request.AllowList)
		}
	}
	if request.BlockNetwork {
		return "block", func() error {
			return m.setupBlockNetwork(containerId, request)
		}
	}
	return "", nil
}

func (m *ContainerNetworkManager) createVethPair(hostVethName, containerVethName string) error {
	link := &netlink.Veth{
		LinkAttrs: netlink.LinkAttrs{Name: hostVethName,
			MTU:          m.defaultLink.Attrs().MTU,
			HardwareAddr: generateUniqueMAC(),
		},
		PeerName:         containerVethName,
		PeerHardwareAddr: generateUniqueMAC(),
	}

	return netlink.LinkAdd(link)
}

func (m *ContainerNetworkManager) getOrSetupBridge(bridgeName string) (netlink.Link, error) {
	m.bridgeMu.Lock()
	defer m.bridgeMu.Unlock()

	if m.bridgeConfigured {
		if m.bridgeLink != nil {
			return m.bridgeLink, nil
		}

		bridge, err := netlink.LinkByName(bridgeName)
		if err == nil {
			m.bridgeLink = bridge
			return m.bridgeLink, nil
		}

		m.bridgeConfigured = false
		m.bridgeLink = nil
	}

	bridge, err := m.setupBridge(bridgeName)
	if err != nil {
		return nil, err
	}

	m.bridgeConfigured = true
	m.bridgeLink = bridge
	return m.bridgeLink, nil
}

func (m *ContainerNetworkManager) setupBridge(bridgeName string) (netlink.Link, error) {
	lockResponse, err := handleGRPCResponse(m.workerRepoClient.SetNetworkLock(m.ctx, &pb.SetNetworkLockRequest{
		NetworkPrefix: m.networkPrefix,
		Ttl:           10,
		Retries:       10,
	}))
	if err != nil {
		return nil, err
	}
	defer m.workerRepoClient.RemoveNetworkLock(m.ctx, &pb.RemoveNetworkLockRequest{
		NetworkPrefix: m.networkPrefix,
		Token:         lockResponse.Token,
	})

	bridge, err := netlink.LinkByName(bridgeName)
	if err == nil {
		if err := m.ensureBridgeConfigured(bridgeName, bridge); err != nil {
			return nil, err
		}
		return bridge, nil
	}

	bridge = &netlink.Bridge{
		LinkAttrs: netlink.LinkAttrs{
			Name:         bridgeName,
			MTU:          m.defaultLink.Attrs().MTU,
			HardwareAddr: generateUniqueMAC(),
		},
	}

	if err := netlink.LinkAdd(bridge); err != nil && err != unix.EEXIST {
		return nil, err
	}

	bridge, err = netlink.LinkByName(bridgeName)
	if err != nil {
		return nil, err
	}

	return bridge, m.ensureBridgeConfigured(bridgeName, bridge)
}

func (m *ContainerNetworkManager) ensureBridgeConfigured(bridgeName string, bridge netlink.Link) error {
	if err := netlink.LinkSetUp(bridge); err != nil {
		return err
	}
	bridgeIPv4 := &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   net.ParseIP(containerBridgeAddress),
			Mask: containerIPv4Mask(),
		},
	}
	if err := netlink.AddrReplace(bridge, bridgeIPv4); err != nil {
		return err
	}

	if m.ipt6 != nil {
		_, ipv6Net, _ := net.ParseCIDR(containerSubnetIPv6)
		bridgeIPv6 := &netlink.Addr{
			IPNet: &net.IPNet{
				IP:   net.ParseIP(containerBridgeAddressIPv6),
				Mask: ipv6Net.Mask,
			},
		}
		if err := netlink.AddrReplace(bridge, bridgeIPv6); err != nil {
			return err
		}
	}

	// Allow containers to communicate with each other and the internet
	// (NAT outgoing traffic from the containers)
	m.iptablesMu.Lock()
	defer m.iptablesMu.Unlock()

	// IPv4
	if err := m.ipt.AppendUnique("nat", "POSTROUTING", "-s", containerSubnet, "-o", m.defaultLink.Attrs().Name, "-j", "MASQUERADE"); err != nil {
		return err
	}

	// IPv6
	if m.ipt6 != nil {
		if err := m.ipt6.AppendUnique("nat", "POSTROUTING", "-s", containerSubnetIPv6, "-o", m.defaultLink.Attrs().Name, "-j", "MASQUERADE"); err != nil {
			return err
		}
	}

	// Allow forwarding of traffic from the bridge to the external network and back
	if err := m.ipt.InsertUnique("filter", "FORWARD", 1, "-i", bridgeName, "-o", m.defaultLink.Attrs().Name, "-j", "ACCEPT"); err != nil {
		return err
	}

	if err := m.ipt.InsertUnique("filter", "FORWARD", 1, "-i", m.defaultLink.Attrs().Name, "-o", bridgeName, "-j", "ACCEPT"); err != nil {
		return err
	}

	return nil
}

type containerNetworkConfigOpts struct {
	containerId   string
	containerVeth netlink.Link
	hostNS        netns.NsHandle
	containerNS   netns.NsHandle
	request       *types.ContainerRequest
}

func (m *ContainerNetworkManager) configureContainerNetwork(opts *containerNetworkConfigOpts) error {
	ipAddr, err := m.reserveContainerIP(opts)
	if err != nil {
		return err
	}

	phaseStart := time.Now()
	err = m.configureContainerLink(opts, ipAddr)
	metrics.RecordWorkerStartupPhase("network_ip_assign", time.Since(phaseStart), opts.request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	m.recordNetworkLifecycle(opts.request, types.ContainerLifecycleNetworkIPAssign, phaseStart, err == nil, nil)
	if err != nil {
		if releaseErr := m.releaseReservedContainerIP(opts.containerId); releaseErr != nil {
			log.Warn().Str("container_id", opts.containerId).Err(releaseErr).Msg("failed to release reserved container ip after network setup error")
		}
		return err
	}

	return nil
}

func (m *ContainerNetworkManager) reserveContainerIP(opts *containerNetworkConfigOpts) (*netlink.Addr, error) {
	phaseStart := time.Now()
	m.ipMu.Lock()
	metrics.RecordWorkerStartupPhase("network_ip_lock", time.Since(phaseStart), opts.request, map[string]string{"success": "true", "mode": "local"})
	m.recordNetworkLifecycle(opts.request, types.ContainerLifecycleNetworkIPLock, phaseStart, true, map[string]string{"mode": "local"})
	defer m.ipMu.Unlock()

	phaseStart = time.Now()
	err := m.reloadAllocatedIPsLocked()
	metrics.RecordWorkerStartupPhase("network_ip_scan", time.Since(phaseStart), opts.request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
	m.recordNetworkLifecycle(opts.request, types.ContainerLifecycleNetworkIPScan, phaseStart, err == nil, map[string]string{
		"source": "redis",
	})
	if err != nil {
		return nil, err
	}

	var ipAddr *netlink.Addr
	reserved := false
	var lastErr error
	for attempts := 0; attempts < containerIPv4AddressCount(); attempts++ {
		ipAddr = m.nextAvailableContainerIPLocked()
		if ipAddr == nil {
			if lastErr != nil {
				return nil, fmt.Errorf("unable to assign IP address to container: no available addresses after reservation conflicts: %w", lastErr)
			}
			return nil, errors.New("unable to assign IP address to container: no available addresses")
		}

		phaseStart = time.Now()
		_, err := handleGRPCResponse(m.workerRepoClient.SetContainerIp(m.ctx, &pb.SetContainerIpRequest{
			NetworkPrefix: m.networkPrefix,
			ContainerId:   opts.containerId,
			IpAddress:     ipAddr.IP.String(),
		}))
		metrics.RecordWorkerStartupPhase("network_set_container_ip", time.Since(phaseStart), opts.request, map[string]string{"success": fmt.Sprintf("%t", err == nil)})
		m.recordNetworkLifecycle(opts.request, types.ContainerLifecycleNetworkSetContainerIP, phaseStart, err == nil, nil)
		if err != nil {
			lastErr = err
			m.allocatedIPs[ipAddr.IP.String()] = struct{}{}
			continue
		}

		m.rememberContainerIPLocked(opts.containerId, ipAddr.IP.String())
		reserved = true
		break
	}
	if !reserved {
		if lastErr != nil {
			return nil, fmt.Errorf("unable to reserve unique IP address for container: %w", lastErr)
		}
		return nil, errors.New("unable to reserve unique IP address for container")
	}

	log.Debug().Str("container_id", opts.containerId).Str("ip_address", ipAddr.IP.String()).Msg("container ip address set")

	containerInstance, exists := m.containerInstances.Get(opts.containerId)
	if exists {
		containerInstance.ContainerIp = ipAddr.IP.String()
		m.containerInstances.Set(opts.containerId, containerInstance)
	}

	return ipAddr, nil
}

func (m *ContainerNetworkManager) ensureAllocatedIPsLoadedLocked() error {
	if m.allocatedIPsLoaded {
		return nil
	}

	return m.reloadAllocatedIPsLocked()
}

func (m *ContainerNetworkManager) reloadAllocatedIPsLocked() error {
	getContainerIpsResponse, err := handleGRPCResponse(m.workerRepoClient.GetContainerIps(m.ctx, &pb.GetContainerIpsRequest{
		NetworkPrefix: m.networkPrefix,
	}))
	if err != nil {
		return err
	}

	m.allocatedIPs = map[string]struct{}{}
	for _, ip := range getContainerIpsResponse.Ips {
		m.allocatedIPs[ip] = struct{}{}
	}
	for _, ip := range m.containerIPs {
		if ip != "" {
			m.allocatedIPs[ip] = struct{}{}
		}
	}
	m.allocatedIPsLoaded = true
	return nil
}

func (m *ContainerNetworkManager) nextAvailableContainerIPLocked() *netlink.Addr {
	_, ipNet, _ := net.ParseCIDR(containerSubnet)
	ones, bits := ipNet.Mask.Size()
	if bits != 32 || ones < 0 {
		return nil
	}

	for len(m.releasedIPs) > 0 {
		last := len(m.releasedIPs) - 1
		ipStr := m.releasedIPs[last]
		m.releasedIPs = m.releasedIPs[:last]

		if _, allocated := m.allocatedIPs[ipStr]; allocated {
			continue
		}
		ip := net.ParseIP(ipStr)
		if ip == nil || ip.To4() == nil || !ipNet.Contains(ip) {
			continue
		}
		ip = ip.To4()

		return &netlink.Addr{
			IPNet: &net.IPNet{
				IP:   append(net.IP(nil), ip...),
				Mask: ipNet.Mask,
			},
		}
	}

	addressCount := uint32(1) << uint32(bits-ones)
	if m.nextIPv4Offset < 2 || m.nextIPv4Offset >= addressCount {
		m.nextIPv4Offset = 2
	}

	baseIP := ipNet.IP.Mask(ipNet.Mask)
	for attempts := uint32(0); attempts < addressCount; attempts++ {
		offset := m.nextIPv4Offset
		ip := nextIP(baseIP, uint(offset))
		ipStr := ip.String()
		m.nextIPv4Offset++
		if m.nextIPv4Offset >= addressCount {
			m.nextIPv4Offset = 2
		}

		if ipStr == containerBridgeAddress || ipStr == ipNet.IP.String() || !ipNet.Contains(ip) {
			continue
		}
		if _, allocated := m.allocatedIPs[ipStr]; allocated {
			continue
		}

		ipCopy := append(net.IP(nil), ip...)
		return &netlink.Addr{
			IPNet: &net.IPNet{
				IP:   ipCopy,
				Mask: ipNet.Mask,
			},
		}
	}

	return nil
}

func (m *ContainerNetworkManager) rememberContainerIPLocked(containerId string, ip string) {
	if ip == "" {
		return
	}
	m.allocatedIPs[ip] = struct{}{}
	m.containerIPs[containerId] = ip
}

func (m *ContainerNetworkManager) forgetContainerIPLocked(containerId string, ip string) {
	if ip == "" {
		ip = m.containerIPs[containerId]
	}
	if ip != "" {
		delete(m.allocatedIPs, ip)
		m.releasedIPs = append(m.releasedIPs, ip)
	}
	delete(m.containerIPs, containerId)
}

func (m *ContainerNetworkManager) forgetContainerIP(containerId string, ip string) {
	m.ipMu.Lock()
	defer m.ipMu.Unlock()
	m.forgetContainerIPLocked(containerId, ip)
}

func (m *ContainerNetworkManager) releaseReservedContainerIP(containerId string) error {
	if containerId == "" {
		return nil
	}

	m.ipMu.Lock()
	defer m.ipMu.Unlock()

	containerIP := m.containerIPs[containerId]
	if containerInstance, exists := m.containerInstances.Get(containerId); exists {
		if containerInstance.ContainerIp != "" {
			containerIP = containerInstance.ContainerIp
		}
	}

	err := m.removeContainerIPFromRepository(containerId)
	if err == nil {
		m.forgetContainerIPLocked(containerId, containerIP)
		if containerInstance, exists := m.containerInstances.Get(containerId); exists {
			containerInstance.ContainerIp = ""
			m.containerInstances.Set(containerId, containerInstance)
		}
	}
	return err
}

func (m *ContainerNetworkManager) removeContainerIPFromRepository(containerId string) error {
	return m.removeContainerIPFromRepositoryWithContext(m.ctx, containerId)
}

func (m *ContainerNetworkManager) removeContainerIPFromRepositoryWithContext(ctx context.Context, containerId string) error {
	_, err := handleGRPCResponse(m.workerRepoClient.RemoveContainerIp(ctx, &pb.RemoveContainerIpRequest{
		NetworkPrefix: m.networkPrefix,
		ContainerId:   containerId,
	}))
	return err
}

func (m *ContainerNetworkManager) configureContainerLink(opts *containerNetworkConfigOpts, ipAddr *netlink.Addr) error {
	if err := netns.Set(opts.containerNS); err != nil {
		return err
	}

	err := m.configureContainerLinkInNamespace(opts.containerVeth, ipAddr)
	if nsErr := netns.Set(opts.hostNS); nsErr != nil {
		if err != nil {
			return fmt.Errorf("%w; also failed to switch back to host namespace: %v", err, nsErr)
		}
		return fmt.Errorf("failed to switch back to host namespace: %w", nsErr)
	}
	return err
}

func (m *ContainerNetworkManager) configureContainerLinkInNamespace(containerVeth netlink.Link, ipAddr *netlink.Addr) error {
	lo, err := netlink.LinkByName("lo")
	if err != nil {
		return err
	}

	if err := netlink.LinkSetUp(lo); err != nil {
		return err
	}

	// Force-enable IPv6 in the namespace (ensures AF_INET6 is usable for CRIU restore binds)
	sysctlDisableIPv6 := "/proc/sys/net/ipv6/conf/all/disable_ipv6"
	if err := os.WriteFile(sysctlDisableIPv6, []byte("0\n"), 0644); err != nil {
		return err
	}

	// As backup, allow binds to non-local addresses
	sysctlNonlocalBind := "/proc/sys/net/ipv6/ip_nonlocal_bind"
	if err := os.WriteFile(sysctlNonlocalBind, []byte("1\n"), 0644); err != nil {
		return err
	}

	// Always add IPv6 loopback address to enable IPv6 wildcard binds
	ipv6Lo := &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   net.ParseIP("::1"),
			Mask: net.CIDRMask(128, 128),
		},
	}
	if err := netlink.AddrAdd(lo, ipv6Lo); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}

	ipv4Lo := &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   net.ParseIP("127.0.0.1"),
			Mask: net.CIDRMask(8, 32),
		},
	}
	if err := netlink.AddrAdd(lo, ipv4Lo); err != nil && !errors.Is(err, unix.EEXIST) {
		return err
	}

	if err := netlink.LinkSetUp(containerVeth); err != nil {
		return err
	}

	if err := netlink.AddrAdd(containerVeth, ipAddr); err != nil {
		return err
	}

	defaultRoute := &netlink.Route{
		LinkIndex: containerVeth.Attrs().Index,
		Gw:        net.ParseIP(containerGatewayAddress),
	}
	if err := netlink.RouteAdd(defaultRoute); err != nil {
		return err
	}

	if m.ipt6 == nil {
		return nil
	}

	_, ipv6Net, _ := net.ParseCIDR(containerSubnetIPv6)
	ipv6Address, err := containerIPv6Address(ipAddr.IP, ipv6Net)
	if err != nil {
		return err
	}
	ipv6Addr := &netlink.Addr{
		IPNet: &net.IPNet{
			IP:   ipv6Address,
			Mask: ipv6Net.Mask,
		},
	}
	if err := netlink.AddrAdd(containerVeth, ipv6Addr); err != nil {
		return err
	}

	defaultIPv6Route := &netlink.Route{
		LinkIndex: containerVeth.Attrs().Index,
		Gw:        net.ParseIP(containerGatewayAddressIPv6),
	}
	return netlink.RouteAdd(defaultIPv6Route)
}

func (m *ContainerNetworkManager) setupBlockNetwork(containerId string, request *types.ContainerRequest) error {
	info, err := m.getContainerNetworkInfo(containerId)
	if err != nil {
		return err
	}

	m.iptablesMu.Lock()
	defer m.iptablesMu.Unlock()

	// Block IPv4 outbound traffic (but allow reply packets for exposed ports)
	err = m.ipt.InsertUnique("filter", "FORWARD", 1, "-s", info.ContainerIp, "-o", m.defaultLink.Attrs().Name, "-m", "conntrack", "!", "--ctstate", "ESTABLISHED,RELATED", "-j", "DROP", "-m", "comment", "--comment", info.Comment)
	if err != nil {
		return err
	}

	// Block IPv6 outbound traffic if enabled (but allow reply packets for exposed ports)
	if m.ipt6 != nil {
		err = m.ipt6.InsertUnique("filter", "FORWARD", 1, "-s", info.ContainerIpv6, "-o", m.defaultLink.Attrs().Name, "-m", "conntrack", "!", "--ctstate", "ESTABLISHED,RELATED", "-j", "DROP", "-m", "comment", "--comment", info.Comment)
		if err != nil {
			return err
		}
	}

	log.Info().Str("container_id", containerId).Str("ip_address", info.ContainerIp).Msg("outbound network access blocked for container")
	return nil
}

func (m *ContainerNetworkManager) setupAllowList(containerId string, request *types.ContainerRequest, allowList []string) error {
	// First block all network traffic
	err := m.setupBlockNetwork(containerId, request)
	if err != nil {
		return err
	}

	info, err := m.getContainerNetworkInfo(containerId)
	if err != nil {
		return err
	}

	// Validate and normalize allowlist entries, then add iptables rules
	for _, entry := range allowList {
		normalizedCIDR, isIPv6, err := validateCIDR(entry)
		if err != nil {
			return fmt.Errorf("invalid allowlist entry %q: %w", entry, err)
		}

		if isIPv6 {
			if m.ipt6 != nil {
				m.iptablesMu.Lock()
				err = m.ipt6.InsertUnique("filter", "FORWARD", 1, "-s", info.ContainerIpv6, "-d", normalizedCIDR, "-o", m.defaultLink.Attrs().Name, "-j", "ACCEPT", "-m", "comment", "--comment", info.Comment)
				m.iptablesMu.Unlock()
				if err != nil {
					return fmt.Errorf("failed to add IPv6 allowlist rule for %s: %w", normalizedCIDR, err)
				}
				log.Info().Str("container_id", containerId).Str("container_ipv6", info.ContainerIpv6).Str("allowed_destination", normalizedCIDR).Msg("outbound IPv6 network access allowed")
			}
		} else {
			m.iptablesMu.Lock()
			err = m.ipt.InsertUnique("filter", "FORWARD", 1, "-s", info.ContainerIp, "-d", normalizedCIDR, "-o", m.defaultLink.Attrs().Name, "-j", "ACCEPT", "-m", "comment", "--comment", info.Comment)
			m.iptablesMu.Unlock()
			if err != nil {
				return fmt.Errorf("failed to add IPv4 allowlist rule for %s: %w", normalizedCIDR, err)
			}
			log.Info().Str("container_id", containerId).Str("container_ip", info.ContainerIp).Str("allowed_destination", normalizedCIDR).Msg("outbound IPv4 network access allowed")
		}
	}
	return nil
}

func (m *ContainerNetworkManager) cleanupOrphanedNamespaces() {
	ticker := time.NewTicker(containerNetworkCleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			containerIds, err := m.listContainerIdsFromIptables()
			if err != nil {
				log.Error().Err(err).Msg("error listing container ids")
				continue
			}

			for _, containerId := range containerIds {
				func() {
					// Only allow one worker on this machine/worker handle the cleanup
					// We have a secondary lock for the IP assignment, but we need this lock for the "container" level consistency
					lockResponse, err := handleGRPCResponse(m.workerRepoClient.SetNetworkLock(m.ctx, &pb.SetNetworkLockRequest{
						NetworkPrefix: m.networkPrefix + "-" + containerId,
						Ttl:           10,
						Retries:       0,
					}))
					if err != nil {
						return
					}
					defer m.workerRepoClient.RemoveNetworkLock(m.ctx, &pb.RemoveNetworkLockRequest{
						NetworkPrefix: m.networkPrefix + "-" + containerId,
						Token:         lockResponse.Token,
					})

					// Check if the container still exists
					notFoundErr := &types.ErrContainerStateNotFound{}
					_, err = handleGRPCResponse(m.containerRepoClient.GetContainerState(context.Background(), &pb.GetContainerStateRequest{ContainerId: containerId}))
					if err != nil && notFoundErr.From(err) {
						// Container state not found, so tear down the namespace and associated resources
						log.Info().Str("container_id", containerId).Msg("orphaned namespace detected, cleaning up")

						if err := m.TearDown(containerId); err != nil {
							log.Error().Str("container_id", containerId).Err(err).Msg("error tearing down orphaned namespace")
						}
					}

				}()

			}
		}
	}
}

// Taken from: https://gist.github.com/udhos/b468fbfd376aa0b655b6b0c539a88c03
func nextIP(ip net.IP, inc uint) net.IP {
	i := ip.To4()
	v := uint(i[0])<<24 + uint(i[1])<<16 + uint(i[2])<<8 + uint(i[3])
	v += inc
	v3 := byte(v & 0xFF)
	v2 := byte((v >> 8) & 0xFF)
	v1 := byte((v >> 16) & 0xFF)
	v0 := byte((v >> 24) & 0xFF)
	return net.IPv4(v0, v1, v2, v3)
}

func (m *ContainerNetworkManager) TearDown(containerId string) error {
	unlockContainer := m.lockContainerNetwork(containerId)
	defer unlockContainer()

	if slot := m.containerNetworkSlot(containerId); slot != nil {
		return m.tearDownPreallocatedNetworkSlot(containerId, slot)
	}

	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), containerNetworkCleanupRPCTimeout)
	defer cleanupCancel()

	lockResponse, err := handleGRPCResponse(m.workerRepoClient.SetNetworkLock(cleanupCtx, &pb.SetNetworkLockRequest{
		NetworkPrefix: m.networkPrefix,
		Ttl:           10,
		Retries:       10,
	}))
	if err != nil {
		return err
	}
	defer func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), containerNetworkCleanupRPCTimeout)
		defer unlockCancel()
		m.workerRepoClient.RemoveNetworkLock(unlockCtx, &pb.RemoveNetworkLockRequest{
			NetworkPrefix: m.networkPrefix,
			Token:         lockResponse.Token,
		})
	}()

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	info, err := m.getContainerNetworkInfo(containerId)
	if err != nil {
		return err
	}

	hostVeth, err := netlink.LinkByName(info.VethHost)
	if err == nil {
		// Remove the veth from the bridge
		if err := netlink.LinkSetNoMaster(hostVeth); err != nil {
			return err
		}

		// Immediately delete the veth without setting it down first
		if err := netlink.LinkDel(hostVeth); err != nil {
			return err
		}
	}

	// Remove iptables and ip6tables rules
	m.iptablesMu.Lock()
	if err := m.removeIPTablesRules(info.ContainerIp, m.ipt); err != nil {
		m.iptablesMu.Unlock()
		return err
	}

	if m.ipt6 != nil && info.ContainerIpv6 != "" {
		if err := m.removeIPTablesRules(info.ContainerIpv6, m.ipt6); err != nil {
			m.iptablesMu.Unlock()
			return err
		}
	}
	m.iptablesMu.Unlock()

	// Delete container namespace don't bother handling
	// the error because the namespace is likely to be gone at this point
	if info.Namespace != "" {
		netns.DeleteNamed(info.Namespace)
	}

	_, err = handleGRPCResponse(m.workerRepoClient.RemoveContainerIp(cleanupCtx, &pb.RemoveContainerIpRequest{
		NetworkPrefix: m.networkPrefix,
		ContainerId:   containerId,
	}))
	if err != nil {
		return err
	}
	m.forgetContainerIP(containerId, info.ContainerIp)

	// Flush ARP cache on the bridge device
	cmd := exec.Command("ip", "neigh", "flush", "dev", containerBridgeLinkName)
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Debug().Err(err).Str("output", string(output)).Msg("failed to flush ARP entries")
	}

	return nil
}

func (m *ContainerNetworkManager) tearDownPreallocatedNetworkSlot(containerId string, slot *containerNetworkSlot) error {
	if err := m.removePreallocatedNetworkSlotRules(slot); err != nil {
		return err
	}

	if err := m.releasePreallocatedNetworkSlot(containerId, slot); err != nil {
		m.discardNetworkSlot(containerId, slot)
		if isMissingNetworkReservation(err) {
			log.Debug().Str("container_id", containerId).Str("network_slot", slot.id).Err(err).Msg("discarded preallocated network slot with missing reservation")
			return nil
		}
		return err
	}

	m.clearContainerInstanceIP(containerId)

	m.returnNetworkSlot(containerId, slot)
	return nil
}

func (m *ContainerNetworkManager) removePreallocatedNetworkSlotRules(slot *containerNetworkSlot) error {
	info, err := containerNetworkInfoFromIP(slot.id, slot.ip, m.ipt6 != nil)
	if err != nil {
		return err
	}

	m.iptablesMu.Lock()
	defer m.iptablesMu.Unlock()

	if err := m.removeIPTablesRules(info.ContainerIp, m.ipt); err != nil {
		return err
	}
	if m.ipt6 != nil && info.ContainerIpv6 != "" {
		if err := m.removeIPTablesRules(info.ContainerIpv6, m.ipt6); err != nil {
			return err
		}
	}

	return nil
}

func (m *ContainerNetworkManager) releasePreallocatedNetworkSlot(containerId string, slot *containerNetworkSlot) error {
	reservationID := m.containerNetworkSlotReservation(slot)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), containerNetworkCleanupRPCTimeout)
	defer cleanupCancel()

	_, err := handleGRPCResponse(m.workerRepoClient.MoveContainerIp(cleanupCtx, &pb.MoveContainerIpRequest{
		NetworkPrefix:   m.networkPrefix,
		FromContainerId: containerId,
		ToContainerId:   reservationID,
		IpAddress:       slot.ip,
	}))
	if err != nil {
		return err
	}

	m.ipMu.Lock()
	m.forgetContainerIPLocked(containerId, slot.ip)
	m.rememberContainerIPLocked(reservationID, slot.ip)
	m.ipMu.Unlock()
	return nil
}

func (m *ContainerNetworkManager) removeIPTablesRules(ip string, ipt *iptables.IPTables) error {
	tables := []string{"nat", "filter"}
	for _, table := range tables {
		chains := []string{"PREROUTING", "FORWARD"}

		for _, chain := range chains {
			// List rules in the chain
			rules, err := ipt.List(table, chain)
			if err != nil {
				continue
			}

			for _, rule := range rules {
				if strings.Contains(rule, ip) {
					parts := strings.Fields(rule)

					// Remove any double quotes
					for i, part := range parts {
						parts[i] = strings.ReplaceAll(part, `"`, "")
					}

					if err := ipt.Delete(table, chain, parts[2:]...); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}

func (m *ContainerNetworkManager) ExposePort(containerId string, hostPort, containerPort int) error {
	return m.ExposePorts(containerId, []PortBinding{{HostPort: hostPort, ContainerPort: containerPort}})
}

func (m *ContainerNetworkManager) ExposePorts(containerId string, bindings []PortBinding) error {
	if len(bindings) == 0 {
		return nil
	}

	unlockContainer := m.lockContainerNetwork(containerId)
	defer unlockContainer()

	info, err := m.getContainerNetworkInfo(containerId)
	if err != nil {
		return err
	}

	m.iptablesMu.Lock()
	defer m.iptablesMu.Unlock()

	for _, binding := range bindings {
		if err := m.exposePortLocked(info, binding.HostPort, binding.ContainerPort); err != nil {
			return err
		}
	}

	return nil
}

func (m *ContainerNetworkManager) exposePortLocked(info *containerNetworkInfo, hostPort, containerPort int) error {
	// Insert NAT PREROUTING rule at the top of the chain
	// IPv4
	err := m.ipt.InsertUnique("nat", "PREROUTING", 1, "-p", "tcp", "--dport", fmt.Sprintf("%d", hostPort), "-j", "DNAT", "--to-destination", fmt.Sprintf("%s:%d", info.ContainerIp, containerPort), "-m", "comment", "--comment", info.Comment)
	if err != nil {
		return err
	}

	// IPv6
	if m.ipt6 != nil && info.ContainerIpv6 != "" {
		err = m.ipt6.InsertUnique("nat", "PREROUTING", 1, "-p", "tcp", "--dport", fmt.Sprintf("%d", hostPort), "-j", "DNAT", "--to-destination", fmt.Sprintf("[%s]:%d", info.ContainerIpv6, containerPort), "-m", "comment", "--comment", info.Comment)
		if err != nil {
			return err
		}
	}

	// Add FORWARD rule for the DNAT'd traffic
	// IPv4
	err = m.ipt.AppendUnique("filter", "FORWARD", "-p", "tcp", "-d", info.ContainerIp, "--dport", fmt.Sprintf("%d", containerPort), "-j", "ACCEPT", "-m", "comment", "--comment", info.Comment)
	if err != nil {
		return err
	}

	// IPv6
	if m.ipt6 != nil && info.ContainerIpv6 != "" {
		err = m.ipt6.AppendUnique("filter", "FORWARD", "-p", "tcp", "-d", info.ContainerIpv6, "--dport", fmt.Sprintf("%d", containerPort), "-j", "ACCEPT", "-m", "comment", "--comment", info.Comment)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *ContainerNetworkManager) UpdateNetworkPermissions(containerId string, request *types.ContainerRequest) error {
	unlockContainer := m.lockContainerNetwork(containerId)
	defer unlockContainer()

	info, err := m.getContainerNetworkInfo(containerId)
	if err != nil {
		return err
	}

	// Remove existing restriction rules (search by comment tag)
	m.iptablesMu.Lock()
	if err := m.removeNetworkRestrictionRules(info.ContainerIp, m.ipt); err != nil {
		m.iptablesMu.Unlock()
		return err
	}

	if m.ipt6 != nil && info.ContainerIpv6 != "" {
		if err := m.removeNetworkRestrictionRules(info.ContainerIpv6, m.ipt6); err != nil {
			m.iptablesMu.Unlock()
			return err
		}
	}
	m.iptablesMu.Unlock()

	// Apply new rules
	if len(request.AllowList) > 0 {
		if err := m.setupAllowList(containerId, request, request.AllowList); err != nil {
			return err
		}
	} else if request.BlockNetwork {
		if err := m.setupBlockNetwork(containerId, request); err != nil {
			return err
		}
	}

	return nil
}

func (m *ContainerNetworkManager) removeNetworkRestrictionRules(ip string, ipt *iptables.IPTables) error {
	// Similar to removeIPTablesRules but only remove DROP/ACCEPT rules
	// Preserve DNAT rules (exposed ports) and other infrastructure rules
	tables := []string{"filter"}
	for _, table := range tables {
		chains := []string{"PREROUTING", "FORWARD"}

		for _, chain := range chains {
			// List rules in the chain
			rules, err := ipt.List(table, chain)
			if err != nil {
				continue
			}

			for _, rule := range rules {
				if strings.Contains(rule, ip) {
					if strings.Contains(rule, "DROP") || strings.Contains(rule, "ACCEPT") {
						parts := strings.Fields(rule)

						// Remove any double quotes
						for i, part := range parts {
							parts[i] = strings.ReplaceAll(part, `"`, "")
						}

						if err := ipt.Delete(table, chain, parts[2:]...); err != nil {
							return err
						}
					}
				}
			}
		}
	}
	return nil
}

// getRandomFreePort chooses a random free port
func getRandomFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

// GetPodAddr gets the IP from the POD_IP env var.
// Returns an error if it fails to retrieve an IP.
func GetPodAddr() (string, error) {
	addr, exists := os.LookupEnv("POD_HOSTNAME")
	if exists {
		return addr, nil
	}

	return getIPFromEnv("POD_IP")
}

// getDefaultInterface returns the link that goes to the internet.
func getDefaultInterface() (netlink.Link, error) {
	file, err := os.Open("/proc/net/route")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	linkName := ""
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if fields[1] == "00000000" { // Destination of default route
			linkName = fields[0]
		}
	}

	if linkName == "" {
		return nil, fmt.Errorf("default route not found")
	}

	link, err := netlink.LinkByName(linkName)
	if err != nil {
		return nil, err
	}

	return link, nil
}

// getIPFromEnv gets the IP address from an environment variable.
func getIPFromEnv(varName string) (string, error) {
	addr := os.Getenv(varName)
	if addr == "" {
		return "", fmt.Errorf("no ip found in environment variable")
	}

	ip := net.ParseIP(addr)
	if ip == nil {
		return "", errors.New("failed to parse ip address")
	}

	// If the parsed IP is an IPv6 address, encapsulate in brackets
	if ip.To4() == nil {
		return fmt.Sprintf("[%s]", ip.String()), nil
	}

	return ip.String(), nil
}

func (m *ContainerNetworkManager) listContainerIdsFromIptables() ([]string, error) {
	containerIdsSet := make(map[string]struct{})

	rules, err := m.ipt.List("nat", "PREROUTING")
	if err != nil {
		return nil, err
	}

	for _, rule := range rules {
		if containerId, ok := containerIdFromIptablesRule(rule); ok {
			containerIdsSet[containerId] = struct{}{}
		}
	}

	if m.ipt6 != nil {
		rules6, err := m.ipt6.List("nat", "PREROUTING")
		if err != nil {
			return nil, err
		}

		for _, rule := range rules6 {
			if containerId, ok := containerIdFromIptablesRule(rule); ok {
				containerIdsSet[containerId] = struct{}{}
			}
		}
	}

	containerIds := make([]string, 0, len(containerIdsSet))
	for id := range containerIdsSet {
		containerIds = append(containerIds, id)
	}

	return containerIds, nil
}

func (m *ContainerNetworkManager) getContainerNetworkInfoFromIptables(containerId string) (*containerNetworkInfo, error) {
	ruleInfo, err := m.findContainerNetworkRuleInfo(containerId)
	if err != nil {
		return nil, err
	}
	if ruleInfo.IPv4 == "" {
		return nil, fmt.Errorf("container %s has no IPv4 iptables destination", containerId)
	}

	info := &containerNetworkInfo{
		ContainerIp:   ruleInfo.IPv4,
		ContainerIpv6: ruleInfo.IPv6,
		Namespace:     ruleInfo.Namespace,
		VethHost:      ruleInfo.VethHost,
		Comment:       containerNetworkComment(ruleInfo.VethHost, ruleInfo.ContainerID, ruleInfo.Namespace),
	}
	if info.Namespace == "" {
		info.Namespace = containerId
	}
	if info.VethHost == "" {
		info.VethHost, _ = containerVethNames(info.Namespace)
	}
	if m.ipt6 != nil && info.ContainerIpv6 == "" {
		ip := net.ParseIP(info.ContainerIp)
		if ip == nil || ip.To4() == nil {
			return nil, fmt.Errorf("invalid IPv4 address from iptables: %s", info.ContainerIp)
		}
		_, ipv6Net, err := net.ParseCIDR(containerSubnetIPv6)
		if err != nil {
			return nil, fmt.Errorf("failed to parse IPv6 subnet: %w", err)
		}
		ipv6Address, err := containerIPv6Address(ip, ipv6Net)
		if err != nil {
			return nil, err
		}
		info.ContainerIpv6 = ipv6Address.String()
	}

	return info, nil
}

func (m *ContainerNetworkManager) findContainerNetworkRuleInfo(containerId string) (containerNetworkRuleInfo, error) {
	var found containerNetworkRuleInfo

	rules, err := m.ipt.List("nat", "PREROUTING")
	if err != nil {
		return containerNetworkRuleInfo{}, err
	}
	for _, rule := range rules {
		info, ok := containerNetworkRuleInfoFromIptablesRule(rule)
		if !ok || info.ContainerID != containerId {
			continue
		}
		if found.ContainerID == "" {
			found = info
		}
		if info.IPv4 != "" {
			found.IPv4 = info.IPv4
			found.VethHost = info.VethHost
			found.Namespace = info.Namespace
		}
	}

	if m.ipt6 != nil {
		rules6, err := m.ipt6.List("nat", "PREROUTING")
		if err != nil {
			return containerNetworkRuleInfo{}, err
		}
		for _, rule := range rules6 {
			info, ok := containerNetworkRuleInfoFromIptablesRule(rule)
			if !ok || info.ContainerID != containerId {
				continue
			}
			if found.ContainerID == "" {
				found = info
			}
			if info.IPv6 != "" {
				found.IPv6 = info.IPv6
			}
			if found.VethHost == "" {
				found.VethHost = info.VethHost
			}
			if found.Namespace == "" {
				found.Namespace = info.Namespace
			}
		}
	}

	if found.ContainerID == "" {
		return containerNetworkRuleInfo{}, fmt.Errorf("container %s not found in iptables", containerId)
	}
	return found, nil
}

// generateUniqueMAC generates a random MAC address with a specific OUI (Organizationally Unique Identifier).
func generateUniqueMAC() net.HardwareAddr {
	mac := make([]byte, 6)
	_, err := rand.Read(mac)
	if err != nil {
		// Fall back to using a deterministic value in case of failure
		// However, the chance of crypto/rand failing is extremely low
		mac = []byte{0x02, 0x00, 0x00, 0x00, 0x00, 0x00}
	}

	// Set the local bit (second least significant bit of the first byte)
	// and unset the multicast bit (least significant bit of the first byte).
	mac[0] = (mac[0] | 0x02) & 0xfe

	return net.HardwareAddr(mac)
}

func randomNetworkSlotID() string {
	buf := make([]byte, 8)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("slot-%d", time.Now().UnixNano())
	}
	return "slot-" + hex.EncodeToString(buf)
}
