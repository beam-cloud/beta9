package worker

import (
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

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

	first := containerNetworkPrefix("beta9", "default", "cpu", nodePrefix)
	second := containerNetworkPrefix("beta9", "default", "cpu", nodePrefix)

	if second != first {
		t.Fatalf("workers on the same node must share network prefix: %q != %q", second, first)
	}
}

func TestContainerNetworkPrefixIncludesPoolScope(t *testing.T) {
	nodePrefix := "node-a"

	cpu := containerNetworkPrefix("beta9", "default", "cpu", nodePrefix)
	gpu := containerNetworkPrefix("beta9", "default", "gpu", nodePrefix)

	if cpu == gpu {
		t.Fatalf("workers from different pools with the same node name must not share network prefix: %q", cpu)
	}
}

func TestContainerNetworkPrefixIncludesClusterScope(t *testing.T) {
	nodePrefix := "node-a"

	first := containerNetworkPrefix("cluster-a", "default", "cpu", nodePrefix)
	second := containerNetworkPrefix("cluster-b", "default", "cpu", nodePrefix)

	if first == second {
		t.Fatalf("workers from different clusters with the same node name must not share network prefix: %q", first)
	}
}

func TestContainerNetworkPrefixSanitizesParts(t *testing.T) {
	got := containerNetworkPrefix("beta9:dev", "beta9/default", "cpu pool", "node/a")
	want := "cluster:beta9_dev:namespace:beta9_default:pool:cpu_pool:node:node_a"

	if got != want {
		t.Fatalf("unexpected sanitized network prefix: got %q want %q", got, want)
	}
}

func TestContainerNetworkPrefixPreservesScopedPrefix(t *testing.T) {
	scoped := "cluster:beta9:namespace:default:pool:cpu:node:node-a"

	if got := containerNetworkPrefix("other", "other", "other", scoped); got != scoped {
		t.Fatalf("expected scoped network prefix to pass through unchanged: got %q want %q", got, scoped)
	}
}

func TestContainerNetworkSlotPoolSizeDisabledByDefault(t *testing.T) {
	t.Setenv(containerNetworkSlotPoolEnv, "")

	if got := containerNetworkSlotPoolSizeForPool(types.WorkerPoolConfig{}, 128); got != 0 {
		t.Fatalf("expected slot pool to be disabled by default, got %d", got)
	}
}

func TestContainerNetworkSlotPoolSizeUsesStartLimit(t *testing.T) {
	t.Setenv(containerNetworkSlotPoolEnv, "")

	poolConfig := types.WorkerPoolConfig{NetworkPreallocation: true}

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
		NetworkPreallocation: true,
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

func containerNetworkAddress() string {
	_, ipNet, _ := net.ParseCIDR(containerSubnet)
	return ipNet.IP.String()
}
