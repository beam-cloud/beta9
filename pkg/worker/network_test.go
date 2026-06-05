package worker

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/grpc"
)

type cleanupContextWorkerRepoClient struct {
	pb.WorkerRepositoryServiceClient
	moveCtxErr error
}

func (c *cleanupContextWorkerRepoClient) MoveContainerIp(ctx context.Context, in *pb.MoveContainerIpRequest, opts ...grpc.CallOption) (*pb.MoveContainerIpResponse, error) {
	c.moveCtxErr = ctx.Err()
	return &pb.MoveContainerIpResponse{Ok: true}, nil
}

func TestGetIPFromEnv(t *testing.T) {
	tests := []struct {
		name      string
		envName   string
		envValue  string
		want      string
		expectErr bool
	}{
		{
			name:      "No IP set",
			envName:   "EMPTY_IP",
			envValue:  "",
			want:      "",
			expectErr: true,
		},
		{
			name:      "Invalid IP",
			envName:   "INVALID_IP",
			envValue:  "invalid",
			want:      "",
			expectErr: true,
		},
		{
			name:      "Valid IPv4",
			envName:   "VALID_IPV4",
			envValue:  "192.168.0.1",
			want:      "192.168.0.1",
			expectErr: false,
		},
		{
			name:      "Valid IPv6",
			envName:   "VALID_IPV6",
			envValue:  "2001:db8::1",
			want:      "[2001:db8::1]",
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Setenv(tt.envName, tt.envValue)
			defer os.Unsetenv(tt.envName)

			got, err := getIPFromEnv(tt.envName)

			if (err != nil) != tt.expectErr {
				t.Errorf("getIPFromEnv() error = %v, expectErr %v", err, tt.expectErr)
				return
			}

			if got != tt.want {
				t.Errorf("getIPFromEnv() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultInterfaceNameFromProcRoute(t *testing.T) {
	route := strings.NewReader(`Iface	Destination	Gateway	Flags	RefCnt	Use	Metric	Mask	MTU	Window	IRTT
eth0	00000000	010012AC	0003	0	0	0	00000000	0	0	0
eth1	0012AC0A	00000000	0001	0	0	0	00FFFFFF	0	0	0
`)

	name, err := defaultInterfaceNameFromProcRoute(route)
	if err != nil {
		t.Fatal(err)
	}
	if name != "eth0" {
		t.Fatalf("expected eth0 default interface, got %q", name)
	}
}

func TestContainerVethNamesAvoidBurstCollisions(t *testing.T) {
	firstHost, firstContainer := containerVethNames("sandbox-a-abcde")
	secondHost, secondContainer := containerVethNames("sandbox-b-abcde")

	if firstHost == secondHost || firstContainer == secondContainer {
		t.Fatalf("veth names should not collide for ids with the same suffix: %s/%s", firstHost, secondHost)
	}
	if len(firstHost) > networkInterfaceNameMaxLength || len(firstContainer) > networkInterfaceNameMaxLength {
		t.Fatalf("veth names exceed Linux interface limit: %s/%s", firstHost, firstContainer)
	}
}

func TestContainerNetworkPrefixIsNodeScoped(t *testing.T) {
	nodePrefix := "k3d-beta9-agent-0"

	first := containerNetworkPrefix("beta9", nodePrefix)
	second := containerNetworkPrefix("beta9", nodePrefix)

	if second != first {
		t.Fatalf("workers on the same node must share network prefix: %q != %q", second, first)
	}
}

func TestContainerNetworkPrefixIsPoolAgnosticForNodeScopedNetworking(t *testing.T) {
	cpuPoolPrefix := common.WorkerNetworkPrefix("beta9", "node-a")
	buildPoolPrefix := common.WorkerNetworkPrefix("beta9", "node-a")

	cpu := containerNetworkPrefix("beta9", cpuPoolPrefix)
	build := containerNetworkPrefix("beta9", buildPoolPrefix)

	if cpu != build {
		t.Fatalf("workers from different pools on the same node must share network prefix: %q != %q", cpu, build)
	}
}

func TestContainerNetworkPrefixIncludesClusterScope(t *testing.T) {
	nodePrefix := "node-a"

	first := containerNetworkPrefix("cluster-a", nodePrefix)
	second := containerNetworkPrefix("cluster-b", nodePrefix)

	if first == second {
		t.Fatalf("workers from different clusters with the same node name must not share network prefix: %q", first)
	}
}

func TestContainerNetworkPrefixSanitizesParts(t *testing.T) {
	got := containerNetworkPrefix("beta9:dev", "node/a")
	want := "cluster:beta9_dev:node:node_a"

	if got != want {
		t.Fatalf("unexpected sanitized network prefix: got %q want %q", got, want)
	}
}

func TestContainerNetworkPrefixPreservesScopedPrefix(t *testing.T) {
	scoped := "cluster:beta9:node:node-a"

	if got := containerNetworkPrefix("other", scoped); got != scoped {
		t.Fatalf("expected scoped network prefix to pass through unchanged: got %q want %q", got, scoped)
	}
}

func TestContainerNetworkPrefixCanonicalizesLegacyPoolScopedPrefix(t *testing.T) {
	legacy := "cluster:beta9:namespace:default:pool:cpu:node:node-a"
	want := "cluster:beta9:node:node-a"

	if got := containerNetworkPrefix("other", legacy); got != want {
		t.Fatalf("expected legacy pool-scoped network prefix to canonicalize: got %q want %q", got, want)
	}
}

func TestContainerNetworkSlotReservationParsing(t *testing.T) {
	legacy := containerNetworkSlotReservationID("slot-a")
	if got, ok := containerNetworkSlotIDFromReservationID(legacy); !ok || got != "slot-a" {
		t.Fatalf("failed to parse legacy slot reservation: got %q ok=%t", got, ok)
	}
	if workerID, slotID, ok := containerNetworkSlotReservationParts(legacy); !ok || workerID != "" || slotID != "slot-a" {
		t.Fatalf("failed to parse legacy reservation parts: worker=%q slot=%q ok=%t", workerID, slotID, ok)
	}

	scoped := containerNetworkSlotReservationIDForWorker("worker-a", "slot-b")
	if got, ok := containerNetworkSlotIDFromReservationID(scoped); !ok || got != "slot-b" {
		t.Fatalf("failed to parse worker-scoped slot reservation: got %q ok=%t", got, ok)
	}
	if workerID, slotID, ok := containerNetworkSlotReservationParts(scoped); !ok || workerID != "worker-a" || slotID != "slot-b" {
		t.Fatalf("failed to parse scoped reservation parts: worker=%q slot=%q ok=%t", workerID, slotID, ok)
	}

	manager := &ContainerNetworkManager{workerId: "worker-a"}
	if got := manager.containerNetworkSlotReservationID("slot-b"); got != scoped {
		t.Fatalf("unexpected worker-scoped reservation id: got %q want %q", got, scoped)
	}
}

func TestReleasePreallocatedNetworkSlotUsesFreshCleanupContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	repoClient := &cleanupContextWorkerRepoClient{}
	manager := &ContainerNetworkManager{
		ctx:              ctx,
		workerId:         "worker-a",
		workerRepoClient: repoClient,
		networkPrefix:    "network-a",
		allocatedIPs:     map[string]struct{}{"192.168.0.2": {}},
		containerIPs:     map[string]string{"container-a": "192.168.0.2"},
	}

	err := manager.releasePreallocatedNetworkSlot("container-a", &containerNetworkSlot{
		id: "slot-a",
		ip: "192.168.0.2",
	})

	if err != nil {
		t.Fatalf("releasePreallocatedNetworkSlot failed: %v", err)
	}
	if repoClient.moveCtxErr != nil {
		t.Fatalf("cleanup RPC used canceled worker context: %v", repoClient.moveCtxErr)
	}
}

func TestIsRedisLockNotObtained(t *testing.T) {
	if !isRedisLockNotObtained(errors.New("redislock: not obtained")) {
		t.Fatal("expected redislock not obtained error to match")
	}
	if isRedisLockNotObtained(errors.New("other error")) {
		t.Fatal("did not expect unrelated error to match")
	}
}

func TestShouldCleanupNetworkSlotReservation(t *testing.T) {
	workerExists := func(workerID string) (bool, error) {
		switch workerID {
		case "live-worker":
			return true, nil
		case "dead-worker":
			return false, nil
		default:
			return false, errors.New("lookup failed")
		}
	}

	tests := []struct {
		name           string
		currentWorker  string
		slotWorker     string
		resourcesExist bool
		want           bool
		wantErr        bool
	}{
		{
			name:           "legacy slot with resources stays",
			currentWorker:  "worker-a",
			resourcesExist: true,
			want:           false,
		},
		{
			name:          "legacy slot without resources is stale",
			currentWorker: "worker-a",
			want:          true,
		},
		{
			name:           "current worker slot with resources stays",
			currentWorker:  "worker-a",
			slotWorker:     "worker-a",
			resourcesExist: true,
			want:           false,
		},
		{
			name:          "current worker slot without resources is stale",
			currentWorker: "worker-a",
			slotWorker:    "worker-a",
			want:          true,
		},
		{
			name:          "live other worker slot stays",
			currentWorker: "worker-a",
			slotWorker:    "live-worker",
			want:          false,
		},
		{
			name:          "dead other worker slot is stale",
			currentWorker: "worker-a",
			slotWorker:    "dead-worker",
			want:          true,
		},
		{
			name:          "unknown worker liveness is not cleaned",
			currentWorker: "worker-a",
			slotWorker:    "unknown-worker",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := shouldCleanupNetworkSlotReservation(tt.currentWorker, tt.slotWorker, tt.resourcesExist, workerExists)
			if (err != nil) != tt.wantErr {
				t.Fatalf("unexpected error state: err=%v wantErr=%t", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("unexpected cleanup decision: got %t want %t", got, tt.want)
			}
		})
	}
}

func TestDrainFreeNetworkSlots(t *testing.T) {
	manager := &ContainerNetworkManager{
		freeSlots: []*containerNetworkSlot{
			{id: "slot-a"},
			{id: "slot-b"},
		},
		totalSlots: 3,
	}

	slots := manager.drainFreeNetworkSlots()
	if len(slots) != 2 {
		t.Fatalf("expected two drained slots, got %d", len(slots))
	}
	if len(manager.freeSlots) != 0 {
		t.Fatalf("expected free slot pool to be empty, got %d", len(manager.freeSlots))
	}
	if !manager.slotPoolClosed {
		t.Fatal("expected slot pool to be marked closed")
	}
	if manager.totalSlots != 1 {
		t.Fatalf("expected only assigned slot to remain counted, got %d", manager.totalSlots)
	}
}

func TestFillNetworkSlotPoolSkipsClosedPool(t *testing.T) {
	manager := &ContainerNetworkManager{
		slotPoolSize:   1,
		slotPoolClosed: true,
	}

	manager.fillNetworkSlotPool()

	if len(manager.freeSlots) != 0 || manager.totalSlots != 0 {
		t.Fatalf("closed slot pool should not be refilled: free=%d total=%d", len(manager.freeSlots), manager.totalSlots)
	}
}

func boolPtr(v bool) *bool {
	return &v
}

func TestContainerNetworkSlotPoolSizeEnabledByDefault(t *testing.T) {
	t.Setenv(containerNetworkSlotPoolEnv, "")

	if got := containerNetworkSlotPoolSizeForPool(types.WorkerPoolConfig{}, 128); got != 128 {
		t.Fatalf("expected slot pool to match start limit by default, got %d", got)
	}
}

func TestContainerNetworkSlotPoolSizeCanBeDisabled(t *testing.T) {
	t.Setenv(containerNetworkSlotPoolEnv, "")

	poolConfig := types.WorkerPoolConfig{NetworkPreallocation: boolPtr(false)}

	if got := containerNetworkSlotPoolSizeForPool(poolConfig, 128); got != 0 {
		t.Fatalf("expected slot pool to be disabled, got %d", got)
	}
}

func TestContainerNetworkSlotPoolSizeUsesStartLimit(t *testing.T) {
	t.Setenv(containerNetworkSlotPoolEnv, "")

	poolConfig := types.WorkerPoolConfig{NetworkPreallocation: boolPtr(true)}

	if got := containerNetworkSlotPoolSizeForPool(poolConfig, 128); got != 128 {
		t.Fatalf("expected slot pool to match start limit, got %d", got)
	}
	if got := containerNetworkSlotPoolSizeForPool(poolConfig, 0); got != defaultContainerNetworkSlotPoolSize {
		t.Fatalf("expected default slot pool, got %d", got)
	}
}

func TestContainerNetworkSlotPoolSizeUsesPoolConfig(t *testing.T) {
	t.Setenv(containerNetworkSlotPoolEnv, "")

	poolConfig := types.WorkerPoolConfig{
		NetworkPreallocation: boolPtr(true),
		NetworkSlotPoolSize:  64,
	}

	if got := containerNetworkSlotPoolSizeForPool(poolConfig, 128); got != 64 {
		t.Fatalf("expected configured slot pool, got %d", got)
	}
}

func TestContainerNetworkSlotPoolSizeEnvOverride(t *testing.T) {
	t.Setenv(containerNetworkSlotPoolEnv, "256")

	if got := containerNetworkSlotPoolSizeForPool(types.WorkerPoolConfig{}, 128); got != 256 {
		t.Fatalf("expected env override slot pool, got %d", got)
	}
}

func TestExposePortsWithNoBindingsSkipsNetworkLookup(t *testing.T) {
	manager := &ContainerNetworkManager{}

	if err := manager.ExposePorts("missing-container", nil); err != nil {
		t.Fatalf("ExposePorts with no bindings should not touch network state: %v", err)
	}
}

func TestLockContainerNetworkRemovesReleasedEntry(t *testing.T) {
	manager := &ContainerNetworkManager{}

	unlock := manager.lockContainerNetwork("container-a")
	if _, exists := manager.containerLocks.Load("container-a"); !exists {
		t.Fatal("expected container lock entry while lock is held")
	}

	unlock()
	if _, exists := manager.containerLocks.Load("container-a"); exists {
		t.Fatal("expected container lock entry to be removed after release")
	}
}

func TestLockContainerNetworkSerializesConcurrentUsers(t *testing.T) {
	manager := &ContainerNetworkManager{}
	var active int32
	errs := make(chan string, 32)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			unlock := manager.lockContainerNetwork("container-a")
			defer unlock()

			if current := atomic.AddInt32(&active, 1); current != 1 {
				errs <- "container lock allowed concurrent access"
			}
			time.Sleep(time.Millisecond)
			atomic.AddInt32(&active, -1)
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Fatal(err)
	}
	if _, exists := manager.containerLocks.Load("container-a"); exists {
		t.Fatal("expected container lock entry to be removed after concurrent users finish")
	}
}

func TestDiscardNetworkSlotClearsLocalAssignment(t *testing.T) {
	manager := &ContainerNetworkManager{
		containerInstances: common.NewSafeMap[*ContainerInstance](),
		allocatedIPs:       map[string]struct{}{},
		containerIPs:       map[string]string{},
		containerSlots:     map[string]*containerNetworkSlot{},
		totalSlots:         1,
	}
	slot := &containerNetworkSlot{
		id:        "slot-a",
		namespace: "missing-netns",
		vethHost:  "missing-veth",
		ip:        "192.168.0.44",
	}
	reservationID := containerNetworkSlotReservationID(slot.id)

	manager.containerInstances.Set("container-a", &ContainerInstance{ContainerIp: slot.ip})
	manager.containerSlots["container-a"] = slot
	manager.containerIPs["container-a"] = slot.ip
	manager.containerIPs[reservationID] = slot.ip
	manager.allocatedIPs[slot.ip] = struct{}{}

	manager.discardNetworkSlot("container-a", slot)

	if manager.totalSlots != 0 {
		t.Fatalf("expected slot count to decrement, got %d", manager.totalSlots)
	}
	if _, exists := manager.containerSlots["container-a"]; exists {
		t.Fatal("expected discarded slot to be removed from container slot map")
	}
	if _, exists := manager.containerIPs["container-a"]; exists {
		t.Fatal("expected container ip cache to be cleared")
	}
	if _, exists := manager.containerIPs[reservationID]; exists {
		t.Fatal("expected slot reservation ip cache to be cleared")
	}
	if _, exists := manager.allocatedIPs[slot.ip]; exists {
		t.Fatal("expected allocated ip cache to be cleared")
	}
	instance, exists := manager.containerInstances.Get("container-a")
	if !exists || instance.ContainerIp != "" {
		t.Fatalf("expected container instance ip to be cleared, got %+v", instance)
	}
}

func TestNetworkRestrictionSelection(t *testing.T) {
	manager := &ContainerNetworkManager{}

	mode, apply := manager.networkRestriction("container-a", &types.ContainerRequest{})
	if mode != "" || apply != nil {
		t.Fatalf("expected unrestricted request to skip restrictions, got mode %q", mode)
	}

	mode, apply = manager.networkRestriction("container-a", &types.ContainerRequest{BlockNetwork: true})
	if mode != "block" || apply == nil {
		t.Fatalf("expected block restriction, got mode %q", mode)
	}

	mode, apply = manager.networkRestriction("container-a", &types.ContainerRequest{
		BlockNetwork: true,
		AllowList:    []string{"10.0.0.0/8"},
	})
	if mode != "allowlist" || apply == nil {
		t.Fatalf("expected allowlist restriction to take precedence, got mode %q", mode)
	}
}

func TestContainerSubnetSupportsThousandContainerBurst(t *testing.T) {
	_, ipNet, err := net.ParseCIDR(containerSubnet)
	if err != nil {
		t.Fatalf("invalid container subnet: %v", err)
	}

	usable := 0
	for ip := ipNet.IP.Mask(ipNet.Mask); ipNet.Contains(ip); ip = nextIP(ip, 1) {
		ipStr := ip.String()
		if ipStr == containerBridgeAddress || ipStr == ipNet.IP.String() {
			continue
		}
		usable++
		if usable >= 1000 {
			return
		}
	}

	t.Fatalf("container subnet only has %d usable addresses, need at least 1000", usable)
}

func TestContainerNetworkManagerReusesReleasedLocalIP(t *testing.T) {
	manager := &ContainerNetworkManager{
		allocatedIPsLoaded: true,
		allocatedIPs:       map[string]struct{}{},
		containerIPs:       map[string]string{},
	}

	first := manager.nextAvailableContainerIPLocked()
	if first == nil {
		t.Fatal("expected an available container IP")
	}
	manager.rememberContainerIPLocked("container-1", first.IP.String())

	second := manager.nextAvailableContainerIPLocked()
	if second == nil {
		t.Fatal("expected a second available container IP")
	}
	if first.IP.Equal(second.IP) {
		t.Fatalf("expected second allocation to skip %s", first.IP)
	}

	manager.forgetContainerIPLocked("container-1", "")

	reused := manager.nextAvailableContainerIPLocked()
	if reused == nil {
		t.Fatal("expected released IP to be available")
	}
	if !first.IP.Equal(reused.IP) {
		t.Fatalf("expected released IP %s to be reusable, got %s", first.IP, reused.IP)
	}
}

func TestContainerIPv6AddressUsesFullIPv4HostOffset(t *testing.T) {
	_, ipv6Net, err := net.ParseCIDR(containerSubnetIPv6)
	if err != nil {
		t.Fatalf("invalid IPv6 subnet: %v", err)
	}

	first, err := containerIPv6Address(net.ParseIP("192.168.0.2"), ipv6Net)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	later, err := containerIPv6Address(net.ParseIP("192.168.4.2"), ipv6Net)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if first.Equal(later) {
		t.Fatalf("IPv6 addresses should not collide across IPv4 /20 host offsets: %s", first)
	}
}

func TestContainerIdFromIptablesRuleHandlesIPv6Colons(t *testing.T) {
	rule := `-A PREROUTING -p tcp -m tcp --dport 12345 -j DNAT --to-destination [fd00:abcd::3e8]:8080 -m comment --comment "b9habcdef123456:sandbox-123"`

	containerId, ok := containerIdFromIptablesRule(rule)
	if !ok {
		t.Fatal("expected container id in iptables rule")
	}
	if containerId != "sandbox-123" {
		t.Fatalf("expected sandbox-123, got %s", containerId)
	}
}

func TestIPTablesRuleFieldsPreservesIPv6DNATBrackets(t *testing.T) {
	rule := `-A PREROUTING -p tcp -m tcp --dport 56449 -j DNAT --to-destination [fd00:abcd::11]:2222 -m comment --comment "b9h1a5f2d74dd3b:endpoint-one:slot-one"`

	parts := iptablesRuleFields(rule)
	if !slices.Contains(parts, "[fd00:abcd::11]:2222") {
		t.Fatalf("expected bracketed IPv6 DNAT destination in rule fields: %#v", parts)
	}
	if slices.Contains(parts, "fd00:abcd::11]:2222") {
		t.Fatalf("found malformed IPv6 DNAT destination in rule fields: %#v", parts)
	}
	if !iptablesRuleMatchesIP(rule, "fd00:abcd::11") {
		t.Fatal("expected bracketed IPv6 DNAT destination to match exact IP")
	}
}

func TestContainerNetworkRuleInfoFromIptablesRule(t *testing.T) {
	rule := `-A PREROUTING -p tcp -m tcp --dport 12345 -j DNAT --to-destination 192.168.0.44:8080 -m comment --comment "b9habcdef123456:sandbox-123:network-slot-abc"`

	info, ok := containerNetworkRuleInfoFromIptablesRule(rule)
	if !ok {
		t.Fatal("expected container network info in iptables rule")
	}
	if info.ContainerID != "sandbox-123" {
		t.Fatalf("expected sandbox-123, got %s", info.ContainerID)
	}
	if info.Namespace != "network-slot-abc" {
		t.Fatalf("expected network-slot-abc namespace, got %s", info.Namespace)
	}
	if info.VethHost != "b9habcdef123456" {
		t.Fatalf("expected b9habcdef123456 veth, got %s", info.VethHost)
	}
	if info.IPv4 != "192.168.0.44" {
		t.Fatalf("expected IPv4 destination, got %s", info.IPv4)
	}
}

func TestIPTablesRuleMatchesExactIP(t *testing.T) {
	rule := `-A PREROUTING -p tcp -m tcp --dport 12345 -j DNAT --to-destination 192.168.0.44:8080 -m comment --comment "b9habcdef123456:sandbox-123"`

	if !iptablesRuleMatchesIP(rule, "192.168.0.44") {
		t.Fatal("expected exact destination IP to match")
	}
	if iptablesRuleMatchesIP(rule, "192.168.0.4") {
		t.Fatal("did not expect substring IP to match")
	}
}

func TestIPTablesRuleMatchesCIDRSourceIP(t *testing.T) {
	rule := `-A FORWARD -s 192.168.0.44/32 -o eth0 -m conntrack ! --ctstate ESTABLISHED,RELATED -j DROP -m comment --comment "b9habcdef123456:sandbox-123"`

	if !iptablesRuleMatchesSourceIP(rule, "192.168.0.44") {
		t.Fatal("expected source CIDR to match")
	}
	if iptablesRuleMatchesSourceIP(rule, "192.168.0.4") {
		t.Fatal("did not expect substring source IP to match")
	}
}

func TestNetworkRestrictionMatcherPreservesExposeForwardRule(t *testing.T) {
	exposeRule := `-A FORWARD -p tcp -d 192.168.0.44/32 --dport 8001 -j ACCEPT -m comment --comment "b9habcdef123456:sandbox-123"`
	restrictionRule := `-A FORWARD -s 192.168.0.44/32 -o eth0 -d 10.0.0.0/8 -j ACCEPT -m comment --comment "b9habcdef123456:sandbox-123"`

	if iptablesRuleMatchesSourceIP(exposeRule, "192.168.0.44") {
		t.Fatal("exposed-port rule should not be treated as a network restriction")
	}
	if !iptablesRuleMatchesSourceIP(restrictionRule, "192.168.0.44") {
		t.Fatal("allowlist rule should be treated as a network restriction")
	}
}

func TestContainerNetworkInfoFromSlotUsesSlotResources(t *testing.T) {
	slot := &containerNetworkSlot{
		id:        "network-slot-abc",
		namespace: "network-slot-abc",
		vethHost:  "b9hslotabc",
		ip:        "192.168.0.44",
	}

	info, err := containerNetworkInfoFromSlot("sandbox-123", slot, false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if info.Namespace != slot.namespace {
		t.Fatalf("expected namespace %s, got %s", slot.namespace, info.Namespace)
	}
	if info.VethHost != slot.vethHost {
		t.Fatalf("expected veth %s, got %s", slot.vethHost, info.VethHost)
	}
	if info.Comment != "b9hslotabc:sandbox-123:network-slot-abc" {
		t.Fatalf("unexpected comment: %s", info.Comment)
	}
}

func containerNetworkAddress() string {
	_, ipNet, _ := net.ParseCIDR(containerSubnet)
	return ipNet.IP.String()
}
