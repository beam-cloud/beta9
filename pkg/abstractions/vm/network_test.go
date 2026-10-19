package vm

import (
	"bufio"
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	vmetrics "github.com/VictoriaMetrics/metrics"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
	proxyproto "github.com/pires/go-proxyproto"
	"github.com/stretchr/testify/require"
)

func publicNetworkConfig() types.VMNetworkConfig {
	return types.VMNetworkConfig{PublicIPv6Prefix: "2001:db8:1234::/80", IngressPort: 1996, TrustedProxyCIDRs: []string{"127.0.0.0/8"}}
}

func TestPublicVMNetworkConfig(t *testing.T) {
	p, err := parsePublicVMNetwork(types.VMNetworkConfig{})
	require.NoError(t, err)
	require.Nil(t, p)
	for _, prefix := range []string{"garbage", "192.0.2.0/24", "fd00::/64", "::/64", "ff00::/8", "::ffff:192.0.2.0/120", "2001:db8::1/64", "2001:db8::/120"} {
		cfg := publicNetworkConfig()
		cfg.PublicIPv6Prefix = prefix
		_, err := parsePublicVMNetwork(cfg)
		require.Error(t, err, prefix)
	}
	for _, cfg := range []types.VMNetworkConfig{
		{PublicIPv6Prefix: "2001:db8::/64", IngressPort: 1996},
		{PublicIPv6Prefix: "2001:db8::/64", TrustedProxyCIDRs: []string{"127.0.0.0/8"}},
		{PublicIPv6Prefix: "2001:db8::/64", IngressPort: 65536, TrustedProxyCIDRs: []string{"127.0.0.0/8"}},
		{PublicIPv6Prefix: "2001:db8::/64", IngressPort: 1996, TrustedProxyCIDRs: []string{"invalid"}},
		{PublicIPv6Prefix: "2001:db8::/64", IngressPort: 1996, TrustedProxyCIDRs: []string{"127.0.0.1/32"}, MaxConnections: -1},
		{PublicIPv6Prefix: "2001:db8::/64", IngressPort: 1996, TrustedProxyCIDRs: []string{"127.0.0.1/32"}, MaxConnectionsPerVM: -1},
		{PublicIPv6Prefix: "2001:db8::/64", IngressPort: 1996, TrustedProxyCIDRs: []string{"127.0.0.1/32"}, MaxConnections: 4, MaxConnectionsPerVM: 5},
		{MaxConnectionsPerVM: 4},
	} {
		_, err := parsePublicVMNetwork(cfg)
		require.Error(t, err)
	}
	p, err = parsePublicVMNetwork(publicNetworkConfig())
	require.NoError(t, err)
	require.Equal(t, 16384, p.maxConnections)
	require.Equal(t, 256, p.maxConnectionsPerVM)
	cfg := publicNetworkConfig()
	cfg.MaxConnections = 32768
	cfg.MaxConnectionsPerVM = 1024
	custom, err := parsePublicVMNetwork(cfg)
	require.NoError(t, err)
	require.Equal(t, 32768, custom.maxConnections)
	require.Equal(t, 1024, custom.maxConnectionsPerVM)
	for _, limit := range []int{1, 4, 256} {
		cfg.MaxConnections, cfg.MaxConnectionsPerVM = limit, 0
		custom, err := parsePublicVMNetwork(cfg)
		require.NoError(t, err)
		require.Equal(t, max(1, limit/4), custom.maxConnectionsPerVM)
	}
	require.True(t, p.trustedProxy(&net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 4567}))
	require.False(t, p.trustedProxy(&net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 4567}))
}

func TestPublicVMAddressSurvivesLifecycleAndIsNotInherited(t *testing.T) {
	s, v, info, _, _ := fixture()
	store := &networkVMStore{vmStore: s.repo.(*vmStore)}
	s.repo = store
	s.publicNetwork, _ = parsePublicVMNetwork(publicNetworkConfig())
	s.publicNetwork.repo = store
	ctx := auth.ContextWithAuthInfo(context.Background(), info)
	require.NoError(t, s.ensurePublicIPv6(ctx, v))
	address := v.PublicIPv6
	require.True(t, s.publicNetwork.prefix.Contains(netip.MustParseAddr(address)))
	require.Equal(t, address, s.response(v).PublicIPv6)
	require.NoError(t, s.stop(ctx, v, false))
	v.DesiredState = types.VMStateRunning
	require.NoError(t, s.start(ctx, info, v))
	require.Equal(t, address, v.PublicIPv6)
	stored, err := s.repo.GetVM(ctx, v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.Equal(t, address, stored.PublicIPv6)

	// Address ownership lives on the VM record, outside the artifact's spec.
	child := &types.VM{ID: "vm-child", WorkspaceID: v.WorkspaceID, Spec: artifact(v, "snapshot", "template", "child", "").Spec, DesiredState: types.VMStateStopped}
	require.NoError(t, store.SaveVM(ctx, child))
	require.NoError(t, s.ensurePublicIPv6(ctx, child))
	require.NotEqual(t, address, child.PublicIPv6)
	child.DesiredState = types.VMStatePaused
	require.NoError(t, s.ensurePublicIPv6(ctx, child))
	require.NotEmpty(t, child.PublicIPv6)
	deleted := &types.VM{ID: "deleted", DesiredState: types.VMStateDeleted}
	require.NoError(t, s.ensurePublicIPv6(ctx, deleted))
	require.Empty(t, deleted.PublicIPv6)
}

func TestPublicVMAddressesUseAllRoutedPoolsAndRemainStable(t *testing.T) {
	s, _, _, _, _ := fixture()
	store := &networkVMStore{vmStore: s.repo.(*vmStore)}
	cfg := publicNetworkConfig()
	cfg.PublicIPv6Prefixes = []string{"2001:db8:5678::/80", cfg.PublicIPv6Prefix}
	var err error
	s.publicNetwork, err = parsePublicVMNetwork(cfg)
	require.NoError(t, err)
	require.Len(t, s.publicNetwork.prefixes, 2)
	s.publicNetwork.repo = store
	seen := make(map[netip.Prefix]bool)
	for i := range 128 {
		v := &types.VM{ID: fmt.Sprintf("vm-pool-%d", i), DesiredState: types.VMStateStopped}
		require.NoError(t, store.SaveVM(context.Background(), v))
		require.NoError(t, s.ensurePublicIPv6(context.Background(), v))
		address := v.PublicIPv6
		for _, prefix := range s.publicNetwork.prefixes {
			if prefix.Contains(netip.MustParseAddr(address)) {
				seen[prefix] = true
			}
		}
		require.NoError(t, s.ensurePublicIPv6(context.Background(), v))
		require.Equal(t, address, v.PublicIPv6)
		header := proxyproto.HeaderProxyFromAddrs(2, &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 1234}, &net.TCPAddr{IP: net.ParseIP(address), Port: 2222})
		require.NoError(t, s.publicNetwork.validateHeader(header))
	}
	require.Len(t, seen, 2)
}

type unavailableVMAddresses struct{ repository.PublicVMRepository }

func (unavailableVMAddresses) AssignVMPublicIPv6(context.Context, string, string) (string, error) {
	return "", errors.New("address allocation unavailable")
}

func TestPublicVMAllocationCannotDelayStopOrIdleExpiry(t *testing.T) {
	for _, idle := range []bool{false, true} {
		s, v, _, _, gateway := fixture()
		s.publicNetwork, _ = parsePublicVMNetwork(publicNetworkConfig())
		s.publicNetwork.repo = unavailableVMAddresses{}
		if idle {
			v.Spec.IdleTimeout = 1
			v.LastActiveAt = time.Now().Add(-time.Hour)
		} else {
			v.DesiredState = types.VMStateStopped
		}
		require.NoError(t, s.reconcileVM(context.Background(), v))
		require.Equal(t, 1, gateway.stops)
		require.Empty(t, v.ContainerID)
		require.Equal(t, types.VMStateStopped, v.Status)
		require.NotEmpty(t, v.RootSnapshotID)
	}
}

type networkVMStore struct {
	*vmStore
	mu         sync.Mutex
	saveError  error
	saveCalls  int
	touchCalls int
}

func (r *networkVMStore) CreateVMWithStub(ctx context.Context, v *types.VM, _ types.Stub) error {
	return r.SaveVM(ctx, v)
}

func TestVMCreationReservesAddressInInitialWrite(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure error
		writes  int
	}{
		{name: "allocated", writes: 1},
		{name: "address collision", failure: &pq.Error{Code: "23505", Constraint: "persistent_vm_public_ipv6"}, writes: 2},
		{name: "name collision", failure: &pq.Error{Code: "23505", Constraint: "persistent_vm_name"}, writes: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			s, v, _, _, _ := fixture()
			delete(s.repo.(*vmStore).rows, v.ID)
			store := &networkVMStore{vmStore: s.repo.(*vmStore), saveError: test.failure}
			s.repo = store
			var err error
			s.publicNetwork, err = parsePublicVMNetwork(publicNetworkConfig())
			require.NoError(t, err)
			s.publicNetwork.repo = unavailableVMAddresses{}
			err = s.createVMRecord(context.Background(), v, types.Stub{})
			require.Equal(t, test.writes, store.saveCalls)
			if test.name == "name collision" {
				require.ErrorIs(t, err, test.failure)
				require.Empty(t, v.PublicIPv6)
				require.NotContains(t, store.rows, v.ID)
				return
			}
			require.NoError(t, err)
			address, err := netip.ParseAddr(v.PublicIPv6)
			require.NoError(t, err)
			require.True(t, s.publicNetwork.prefix.Contains(address))
			require.Equal(t, v.PublicIPv6, store.rows[v.ID].PublicIPv6)
			require.NoError(t, s.ensurePublicIPv6(context.Background(), v), "launch must not allocate again")
		})
	}
}

func (r *networkVMStore) SaveVM(ctx context.Context, v *types.VM) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saveCalls++
	if r.saveError != nil {
		err := r.saveError
		r.saveError = nil
		return err
	}
	for _, other := range r.rows {
		if v.PublicIPv6 != "" && other.ID != v.ID && other.PublicIPv6 == v.PublicIPv6 {
			return &pq.Error{Code: "23505", Constraint: "persistent_vm_public_ipv6"}
		}
	}
	return r.vmStore.SaveVM(ctx, v)
}

func (r *networkVMStore) GetVM(ctx context.Context, ws uint, id string) (*types.VM, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.vmStore.GetVM(ctx, ws, id)
}

func (r *networkVMStore) GetVMByHandle(ctx context.Context, handle string) (*types.VM, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.vmStore.GetVMByHandle(ctx, handle)
}

func (r *networkVMStore) GetVMByPublicIPv6(_ context.Context, address string) (*types.VM, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, v := range r.rows {
		if v.PublicIPv6 == address && v.DesiredState != types.VMStateDeleted {
			copy := *v
			return &copy, nil
		}
	}
	return nil, sql.ErrNoRows
}

func (r *networkVMStore) AssignVMPublicIPv6(ctx context.Context, id, address string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.saveCalls++
	if r.saveError != nil {
		err := r.saveError
		r.saveError = nil
		return "", err
	}
	v, ok := r.rows[id]
	if !ok || v.DesiredState == types.VMStateDeleted {
		return "", sql.ErrNoRows
	}
	if v.PublicIPv6 != "" {
		return v.PublicIPv6, nil
	}
	for _, other := range r.rows {
		if other.PublicIPv6 == address {
			return "", &pq.Error{Code: "23505", Constraint: "persistent_vm_public_ipv6"}
		}
	}
	copy := *v
	copy.PublicIPv6 = address
	return address, r.vmStore.SaveVM(ctx, &copy)
}

func (r *networkVMStore) TouchVM(ctx context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.touchCalls++
	return r.vmStore.TouchVM(ctx, id)
}

func TestPublicVMConnectionsShareActivityUntilLastClose(t *testing.T) {
	s, v, _, _, _, _ := startPublicVMIngress(t, nil)
	store := s.repo.(*networkVMStore)
	const connections = 2048
	releases := make(chan func(), connections)
	errors := make(chan error, connections)
	var pending sync.WaitGroup
	for range connections {
		pending.Add(1)
		go func() {
			defer pending.Done()
			release, err := s.keepPublicVMActive(context.Background(), v)
			if err != nil {
				errors <- err
				return
			}
			releases <- release
		}()
	}
	pending.Wait()
	require.Empty(t, errors)
	store.mu.Lock()
	require.Equal(t, 1, store.touchCalls)
	store.mu.Unlock()
	require.Len(t, s.publicNetwork.activities, 1)
	for range connections - 1 {
		release := <-releases
		release()
		release() // A double close cannot remove another socket's reference.
	}
	require.Len(t, s.publicNetwork.activities, 1)
	(<-releases)()
	require.Empty(t, s.publicNetwork.activities)
	release, err := s.keepPublicVMActive(context.Background(), v)
	require.NoError(t, err)
	release()
	store.mu.Lock()
	require.Equal(t, 2, store.touchCalls)
	store.mu.Unlock()
}

func TestPublicVMAddressRetriesOnlyAddressConflicts(t *testing.T) {
	for _, failure := range []error{&pq.Error{Code: "23505", Constraint: "persistent_vm_public_ipv6"}, &pq.Error{Code: "23505", Constraint: "persistent_vm_name"}, errors.New("database unavailable")} {
		s, v, _, _, _ := fixture()
		s.publicNetwork, _ = parsePublicVMNetwork(publicNetworkConfig())
		store := &networkVMStore{vmStore: s.repo.(*vmStore), saveError: failure}
		s.repo = store
		s.publicNetwork.repo = store
		err := s.ensurePublicIPv6(context.Background(), v)
		if pg, ok := failure.(*pq.Error); ok && pg.Constraint == "persistent_vm_public_ipv6" {
			require.NoError(t, err)
			require.Equal(t, 2, store.saveCalls)
		} else {
			require.ErrorIs(t, err, failure)
			require.Empty(t, v.PublicIPv6)
			require.Equal(t, 1, store.saveCalls)
		}
	}
}

func TestPublicVMAddressReadsDatabaseReservationInsteadOfStaleJSON(t *testing.T) {
	for _, address := range []any{nil, "2001:db8:1234::abcd"} {
		backend, mock := repository.NewBackendPostgresRepositoryForTest()
		data, err := json.Marshal(types.VM{ID: "vm-test", PublicIPv6: "stale-lifecycle-copy"})
		require.NoError(t, err)
		mock.ExpectQuery("SELECT .* FROM persistent_vm").WithArgs("2001:db8:1234::abcd").WillReturnRows(
			sqlmock.NewRows([]string{"workspace_id", "workspace_external_id", "token_id", "data", "last_active_at", "public_ipv6"}).AddRow(7, "workspace", "token", data, time.Now(), address))
		vm, err := backend.(repository.PublicVMRepository).GetVMByPublicIPv6(context.Background(), "2001:db8:1234::abcd")
		require.NoError(t, err)
		if address == nil {
			require.Empty(t, vm.PublicIPv6)
		} else {
			require.Equal(t, address, vm.PublicIPv6)
		}
		require.NoError(t, mock.ExpectationsWereMet())
	}
}

type networkVMContainers struct {
	*vmContainers
	mu           sync.Mutex
	addresses    map[string]map[int32]string
	routes       map[string]*types.BackendRoute
	onAddressMap func()
}

type networkVMBackend struct {
	*vmBackend
	mu sync.Mutex
}

func (b *networkVMBackend) GetTokenByExternalId(ctx context.Context, ws uint, id string) (*types.Token, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.vmBackend.GetTokenByExternalId(ctx, ws, id)
}

func (b *networkVMBackend) setToken(token *types.Token) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.token = token
}

func (r *networkVMContainers) GetContainerAddressMap(cid string) (map[int32]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.onAddressMap != nil {
		r.onAddressMap()
	}
	return r.addresses[cid], nil
}

func (r *networkVMContainers) GetBackendRoute(_ context.Context, id string) (*types.BackendRoute, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.routes[id], nil
}

func startPublicVMIngress(t *testing.T, addresses map[int32]string, configure ...func(*types.VMNetworkConfig)) (*Service, *types.VM, *networkVMStore, *networkVMContainers, string, context.CancelFunc) {
	t.Helper()
	s, v, _, _, _ := fixture()
	v.PublicIPv6 = "2001:db8:1234::abcd"
	v.TrafficAccessToken = "traffic-token"
	v.Spec.Ports = []uint32{2222, 5432, 7681, 8080, 8443, 9000}
	v.Spec.SSH = true
	v.Spec.PrivatePorts = []uint32{6379}
	v.Spec.ProtectedPorts = []uint32{9000}
	store := &networkVMStore{vmStore: s.repo.(*vmStore)}
	require.NoError(t, store.SaveVM(context.Background(), v))
	s.repo = store
	containers := &networkVMContainers{vmContainers: s.containers.(*vmContainers), addresses: map[string]map[int32]string{v.ContainerID: addresses}, routes: map[string]*types.BackendRoute{}}
	s.containers = containers
	s.backend = &networkVMBackend{vmBackend: s.backend.(*vmBackend)}
	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	t.Cleanup(cancel)
	// Reserve an ephemeral test port rather than a fixed gateway port.
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	cfg := publicNetworkConfig()
	cfg.IngressPort = port
	for _, option := range configure {
		option(&cfg)
	}
	dialer := network.NewBackendDialer(nil, types.TailscaleConfig{}, containers, time.Second)
	require.NoError(t, s.startPublicNetwork(cfg, dialer))
	return s, v, store, containers, net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), cancel
}

func publicVMClient(t *testing.T, ingress, address string, port int) *net.TCPConn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", ingress, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
	header := proxyproto.HeaderProxyFromAddrs(2, &net.TCPAddr{IP: net.ParseIP("2001:db8:ffff::1"), Port: 4567}, &net.TCPAddr{IP: net.ParseIP(address), Port: port})
	_, err = header.WriteTo(conn)
	require.NoError(t, err)
	return conn.(*net.TCPConn)
}

func publicVMBackend(t *testing.T, handle func(net.Conn)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() { defer conn.Close(); handle(conn) }()
		}
	}()
	return listener.Addr().String()
}

func TestPublicVMTCPRoutesPublishedPortsAndHalfCloses(t *testing.T) {
	ssh := publicVMBackend(t, func(c net.Conn) { io.WriteString(c, "SSH-2.0-beam-test\r\n"); io.Copy(c, c) })
	binary := publicVMBackend(t, func(c net.Conn) { data, _ := io.ReadAll(c); c.Write(data) })
	s, v, store, _, ingress, _ := startPublicVMIngress(t, map[int32]string{2222: ssh, 5432: binary})
	conn := publicVMClient(t, ingress, v.PublicIPv6, 2222)
	banner, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "SSH-2.0-beam-test\r\n", banner, "SSH servers can speak before the client sends application bytes")
	require.NoError(t, conn.Close())
	conn = publicVMClient(t, ingress, v.PublicIPv6, 5432)
	payload := []byte{0, 255, 1, 2, 0, 254}
	_, err = conn.Write(payload)
	require.NoError(t, err)
	require.NoError(t, conn.CloseWrite())
	data, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Equal(t, payload, data)
	current, err := store.GetVM(context.Background(), v.WorkspaceID, v.ID)
	require.NoError(t, err)
	require.False(t, current.LastActiveAt.IsZero())
	require.NotNil(t, s.publicNetwork)
}

func TestPublicVMTCPReusesBackendRoutes(t *testing.T) {
	preface := make(chan string, 1)
	backend := publicVMBackend(t, func(c net.Conn) {
		r := bufio.NewReader(c)
		line, _ := r.ReadString('\n')
		preface <- line
		io.WriteString(c, "ready")
	})
	_, v, _, containers, ingress, _ := startPublicVMIngress(t, map[int32]string{5432: types.BackendRouteAddress("private-worker-port")})
	containers.mu.Lock()
	containers.routes["private-worker-port"] = &types.BackendRoute{Transport: types.BackendRouteTransportLocalDirect, State: types.BackendRouteStateReady, ProxyTarget: backend}
	containers.mu.Unlock()
	conn := publicVMClient(t, ingress, v.PublicIPv6, 5432)
	data, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Equal(t, "ready", string(data))
	require.Equal(t, network.BackendRoutePreface+"private-worker-port\n", <-preface)
}

func TestPublicVMTCPPreservesApplicationTLS(t *testing.T) {
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "application TLS") }))
	t.Cleanup(backend.Close)
	_, v, _, _, ingress, _ := startPublicVMIngress(t, map[int32]string{8443: backend.Listener.Addr().String()})
	conn := publicVMClient(t, ingress, v.PublicIPv6, 8443)
	// This is httptest's ephemeral certificate, not a production trust policy.
	client := tls.Client(conn, &tls.Config{InsecureSkipVerify: true})
	require.NoError(t, client.Handshake())
	_, err := io.WriteString(client, "GET / HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n")
	require.NoError(t, err)
	response, err := http.ReadResponse(bufio.NewReader(client), nil)
	require.NoError(t, err)
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, "application TLS", string(data))
}

func TestPublicVMPortsFollowCurrentPolicyAndRuntime(t *testing.T) {
	backend := publicVMBackend(t, func(c net.Conn) { io.WriteString(c, "old") })
	_, v, store, containers, ingress, _ := startPublicVMIngress(t, map[int32]string{5432: backend, 6379: backend})
	for _, port := range []int{6379, 12345, int(types.WorkerSandboxProcessManagerPort)} {
		conn := publicVMClient(t, ingress, v.PublicIPv6, port)
		_, err := conn.Read(make([]byte, 1))
		require.Error(t, err)
		var timeout net.Error
		require.False(t, errors.As(err, &timeout) && timeout.Timeout())
	}
	v.Spec.Ports = append(v.Spec.Ports, 7777)
	containers.mu.Lock()
	containers.addresses[v.ContainerID][7777] = backend
	containers.mu.Unlock()
	require.NoError(t, store.SaveVM(context.Background(), v))
	conn := publicVMClient(t, ingress, v.PublicIPv6, 7777)
	data, err := io.ReadAll(conn)
	require.NoError(t, err)
	require.Equal(t, "old", string(data))
	v.Spec.Ports = []uint32{5432}
	require.NoError(t, store.SaveVM(context.Background(), v))
	conn = publicVMClient(t, ingress, v.PublicIPv6, 7777)
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err, "unexpose must take effect without restarting the gateway")
	v.DesiredState = types.VMStatePaused
	v.Spec.AutoResume = true
	require.NoError(t, store.SaveVM(context.Background(), v))
	conn = publicVMClient(t, ingress, v.PublicIPv6, 5432)
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err, "unauthenticated TCP access must not wake paused compute")
	newBackend := publicVMBackend(t, func(c net.Conn) { io.WriteString(c, "new") })
	v.ContainerID = "resumed-runtime"
	v.Generation++
	v.DesiredState = types.VMStateRunning
	containers.mu.Lock()
	containers.addresses[v.ContainerID] = map[int32]string{5432: newBackend}
	containers.mu.Unlock()
	require.NoError(t, store.SaveVM(context.Background(), v))
	conn = publicVMClient(t, ingress, v.PublicIPv6, 5432)
	data, err = io.ReadAll(conn)
	require.NoError(t, err)
	require.Equal(t, "new", string(data))
}

func TestPublicVMAccessFailsClosedAndFencesDialRaces(t *testing.T) {
	backend := publicVMBackend(t, func(c net.Conn) { io.WriteString(c, "must not escape") })
	s, v, store, containers, ingress, cancel := startPublicVMIngress(t, map[int32]string{5432: backend})
	for _, address := range []string{"2001:db8:1234::dead", "2001:db8:5678::1"} {
		conn := publicVMClient(t, ingress, address, 5432)
		_, err := conn.Read(make([]byte, 1))
		require.Error(t, err)
	}
	s.backend.(*networkVMBackend).setToken(&types.Token{Active: false})
	conn := publicVMClient(t, ingress, v.PublicIPv6, 5432)
	_, err := conn.Read(make([]byte, 1))
	require.Error(t, err)
	s.backend.(*networkVMBackend).setToken(&types.Token{Active: true})
	containers.mu.Lock()
	containers.onAddressMap = func() {
		current := *v
		current.Generation++
		require.NoError(t, store.SaveVM(context.Background(), &current))
	}
	containers.mu.Unlock()
	conn = publicVMClient(t, ingress, v.PublicIPv6, 5432)
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err)
	cancel()
	require.Eventually(t, func() bool {
		c, err := net.DialTimeout("tcp", ingress, 50*time.Millisecond)
		if c != nil {
			c.Close()
		}
		return err != nil
	}, time.Second, 10*time.Millisecond)
}

func TestPublicVMHTTPKeepsProtectionAndManagedWebAuth(t *testing.T) {
	s, v, store, _, _, _ := startPublicVMIngress(t, nil)
	request := func(port uint16, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "http://["+v.PublicIPv6+"]/path", nil)
		r = r.WithContext(context.WithValue(r.Context(), publicVMDestinationKey{}, netip.AddrPortFrom(netip.MustParseAddr(v.PublicIPv6), port)))
		r.Header.Set("X-Beam-VM-Token", token)
		w := httptest.NewRecorder()
		s.servePublicVMHTTP(w, r)
		if w.Code == http.StatusOK {
			require.Empty(t, r.Header.Get("X-Beam-VM-Token"), "traffic credentials must not reach the guest")
		}
		return w
	}
	for _, port := range []uint16{7681, 8080, 9000} {
		require.Equal(t, http.StatusForbidden, request(port, "").Code)
		require.Equal(t, http.StatusForbidden, request(port, "wrong").Code)
		require.Equal(t, http.StatusOK, request(port, v.TrafficAccessToken).Code)
	}
	v.Spec.Ports = []uint32{2222}
	require.NoError(t, store.SaveVM(context.Background(), v))
	require.Equal(t, http.StatusNotFound, request(9000, v.TrafficAccessToken).Code)
}

func TestPublicVMHTTPIngressChecksEveryKeepAliveRequest(t *testing.T) {
	_, v, store, _, ingress, _ := startPublicVMIngress(t, nil)
	conn := publicVMClient(t, ingress, v.PublicIPv6, 9000)
	reader := bufio.NewReader(conn)
	request := func(token string) int {
		_, err := io.WriteString(conn, "GET /path HTTP/1.1\r\nHost: ["+v.PublicIPv6+"]:9000\r\nX-Beam-VM-Token: "+token+"\r\n\r\n")
		require.NoError(t, err)
		response, err := http.ReadResponse(reader, nil)
		require.NoError(t, err)
		_, err = io.Copy(io.Discard, response.Body)
		require.NoError(t, err)
		require.NoError(t, response.Body.Close())
		return response.StatusCode
	}
	require.Equal(t, http.StatusForbidden, request("wrong"))
	require.Equal(t, http.StatusOK, request(v.TrafficAccessToken))
	v.Spec.Ports = []uint32{2222}
	require.NoError(t, store.SaveVM(context.Background(), v))
	require.Equal(t, http.StatusNotFound, request(v.TrafficAccessToken))
}

func TestPublicVMConnectionLimitsIsolateVMsAndReleaseClosedSockets(t *testing.T) {
	backend := publicVMBackend(t, func(c net.Conn) {
		io.WriteString(c, "ready\n")
		io.Copy(io.Discard, c)
	})
	s, v, store, _, ingress, cancel := startPublicVMIngress(t, map[int32]string{2222: backend}, func(cfg *types.VMNetworkConfig) {
		cfg.MaxConnections, cfg.MaxConnectionsPerVM = 4, 2
	})
	other := *v
	other.ID, other.Name, other.Handle, other.PublicIPv6 = "other-vm", "other", "other-handle", "2001:db8:1234::abce"
	require.NoError(t, store.SaveVM(context.Background(), &other))
	third := other
	third.ID, third.Name, third.Handle, third.PublicIPv6 = "third-vm", "third", "third-handle", "2001:db8:1234::abcf"
	require.NoError(t, store.SaveVM(context.Background(), &third))
	openSSH := func(address string) *net.TCPConn {
		conn := publicVMClient(t, ingress, address, 2222)
		banner, err := bufio.NewReader(conn).ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "ready\n", banner)
		return conn
	}
	count := func(id string) int {
		s.publicNetwork.connectionMu.Lock()
		defer s.publicNetwork.connectionMu.Unlock()
		return s.publicNetwork.connections[id]
	}
	first := openSSH(v.PublicIPv6)
	// An idle HTTP connection must count alongside SSH and other TCP ports.
	httpConn := publicVMClient(t, ingress, v.PublicIPv6, 9000)
	_, err := fmt.Fprintf(httpConn, "GET /path HTTP/1.1\r\nHost: vm\r\nX-Beam-VM-Token: %s\r\n\r\n", v.TrafficAccessToken)
	require.NoError(t, err)
	response, err := http.ReadResponse(bufio.NewReader(httpConn), nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())
	require.Equal(t, 2, count(v.ID))
	vmRejections := vmetrics.GetOrCreateCounter(`vm_public_connections_rejected_total{reason="vm_limit"}`)
	before := vmRejections.Get()
	rejected := publicVMClient(t, ingress, v.PublicIPv6, 2222)
	_, err = rejected.Read(make([]byte, 1))
	require.Error(t, err)
	require.Equal(t, before+1, vmRejections.Get())
	// The busy VM cannot prevent another VM from using the remaining capacity.
	openSSH(other.PublicIPv6)
	openSSH(other.PublicIPv6)
	gatewayRejections := vmetrics.GetOrCreateCounter(`vm_public_connections_rejected_total{reason="gateway_limit"}`)
	before = gatewayRejections.Get()
	// Do not write a PROXY header: a full gateway rejects immediately on accept.
	full, err := net.Dial("tcp", ingress)
	require.NoError(t, err)
	defer full.Close()
	require.NoError(t, full.SetDeadline(time.Now().Add(time.Second)))
	_, err = full.Read(make([]byte, 1))
	require.Error(t, err)
	require.Equal(t, before+1, gatewayRejections.Get())
	require.NoError(t, httpConn.Close())
	require.Eventually(t, func() bool { return count(v.ID) == 1 }, time.Second, time.Millisecond)
	replacement := openSSH(v.PublicIPv6)
	require.NoError(t, first.Close())
	require.Eventually(t, func() bool { return count(v.ID) == 1 }, time.Second, time.Millisecond)
	openSSH(third.PublicIPv6)
	require.NoError(t, replacement.Close())
	require.Eventually(t, func() bool { return count(v.ID) == 0 }, time.Second, time.Millisecond)
	cancel()
	require.Eventually(t, func() bool {
		s.publicNetwork.connectionMu.Lock()
		defer s.publicNetwork.connectionMu.Unlock()
		return len(s.publicNetwork.connections) == 0
	}, time.Second, time.Millisecond)
}

type networkVMHTTPRuntime struct {
	Runtime
	handler http.Handler
}

func (r *networkVMHTTPRuntime) ForwardVM(c echo.Context, _, _ string) error {
	r.handler.ServeHTTP(c.Response(), c.Request())
	return nil
}

func TestPublicVMManagedWebTokensDoNotReachGuest(t *testing.T) {
	for _, port := range []int{7681, 8080} {
		for _, protected := range []bool{false, true} {
			t.Run(fmt.Sprintf("port=%d/protected=%t", port, protected), func(t *testing.T) {
				headers := make(chan http.Header, 2)
				guest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					headers <- r.Header.Clone()
					if websocket.IsWebSocketUpgrade(r) {
						upgrader := websocket.Upgrader{}
						conn, err := upgrader.Upgrade(w, r, nil)
						if err == nil {
							defer conn.Close()
							conn.WriteMessage(websocket.TextMessage, []byte("guest"))
						}
						return
					}
					io.WriteString(w, "guest")
				}))
				defer guest.Close()
				s, v, store, _, ingress, _ := startPublicVMIngress(t, nil)
				if protected {
					v.Spec.ProtectedPorts = []uint32{uint32(port)}
					require.NoError(t, store.SaveVM(context.Background(), v))
				}
				target, err := url.Parse(guest.URL)
				require.NoError(t, err)
				s.runtime = &networkVMHTTPRuntime{Runtime: s.runtime, handler: httputil.NewSingleHostReverseProxy(target)}
				conn := publicVMClient(t, ingress, v.PublicIPv6, port)
				reader := bufio.NewReader(conn)
				for _, token := range []string{"wrong", v.TrafficAccessToken} {
					_, err := fmt.Fprintf(conn, "GET /path HTTP/1.1\r\nHost: vm\r\nX-Beam-VM-Token: %s\r\n\r\n", token)
					require.NoError(t, err)
					response, err := http.ReadResponse(reader, nil)
					require.NoError(t, err)
					body, err := io.ReadAll(response.Body)
					require.NoError(t, err)
					require.NoError(t, response.Body.Close())
					if token == "wrong" {
						require.Equal(t, http.StatusForbidden, response.StatusCode)
						require.Empty(t, headers, "failed authentication must not reach the guest")
					} else {
						require.Equal(t, http.StatusOK, response.StatusCode)
						require.Equal(t, "guest", string(body))
						require.Empty(t, (<-headers).Values("X-Beam-VM-Token"))
					}
				}
				// Explicit public opt-outs and live protection changes must agree
				// across hostname and IPv6 access, including an existing keep-alive.
				v.Spec.PublicPorts = []uint32{uint32(port)}
				v.Spec.ProtectedPorts = nil
				require.NoError(t, store.SaveVM(context.Background(), v))
				_, err = fmt.Fprint(conn, "GET /path HTTP/1.1\r\nHost: vm\r\nX-Beam-VM-Token: unused\r\n\r\n")
				require.NoError(t, err)
				response, err := http.ReadResponse(reader, nil)
				require.NoError(t, err)
				_, err = io.Copy(io.Discard, response.Body)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				require.Equal(t, http.StatusOK, response.StatusCode)
				require.Empty(t, (<-headers).Values("X-Beam-VM-Token"))
				v.Spec.PublicPorts = nil
				v.Spec.ProtectedPorts = []uint32{uint32(port)}
				require.NoError(t, store.SaveVM(context.Background(), v))
				_, err = fmt.Fprint(conn, "GET /path HTTP/1.1\r\nHost: vm\r\n\r\n")
				require.NoError(t, err)
				response, err = http.ReadResponse(reader, nil)
				require.NoError(t, err)
				_, err = io.Copy(io.Discard, response.Body)
				require.NoError(t, err)
				require.NoError(t, response.Body.Close())
				require.Equal(t, http.StatusForbidden, response.StatusCode)
				require.Empty(t, headers)
				dialer := websocket.Dialer{NetDialContext: func(context.Context, string, string) (net.Conn, error) {
					return publicVMClient(t, ingress, v.PublicIPv6, port), nil
				}}
				ws, _, err := dialer.Dial(fmt.Sprintf("ws://[%s]:%d/ws", v.PublicIPv6, port), http.Header{"X-Beam-VM-Token": {v.TrafficAccessToken}})
				require.NoError(t, err)
				defer ws.Close()
				require.NoError(t, ws.SetReadDeadline(time.Now().Add(3*time.Second)))
				_, data, err := ws.ReadMessage()
				require.NoError(t, err)
				require.Equal(t, "guest", string(data))
				require.Empty(t, (<-headers).Values("X-Beam-VM-Token"), "websocket handshakes must also strip credentials")
			})
		}
	}
}

func TestPublicVMIngressRequiresTrustedProxyV2Metadata(t *testing.T) {
	p, err := parsePublicVMNetwork(publicNetworkConfig())
	require.NoError(t, err)
	for _, mutate := range []func(*proxyproto.Header){
		func(h *proxyproto.Header) { h.Version = 1 },
		func(h *proxyproto.Header) { h.Command = proxyproto.LOCAL },
		func(h *proxyproto.Header) { h.TransportProtocol = proxyproto.UDPv6 },
		func(h *proxyproto.Header) {
			h.DestinationAddr = &net.TCPAddr{IP: net.ParseIP("2001:db8:5678::1"), Port: 5432}
		},
	} {
		h := proxyproto.HeaderProxyFromAddrs(2, &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 1234}, &net.TCPAddr{IP: net.ParseIP("2001:db8:1234::1"), Port: 5432})
		mutate(h)
		require.Error(t, p.validateHeader(h))
	}
	_, _, _, _, ingress, _ := startPublicVMIngress(t, nil)
	conn, err := net.DialTimeout("tcp", ingress, time.Second)
	require.NoError(t, err)
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(time.Second))
	io.Copy(conn, strings.NewReader("GET / HTTP/1.1\r\nHost: spoof\r\n\r\n"))
	_, err = conn.Read(make([]byte, 1))
	require.Error(t, err)
}
