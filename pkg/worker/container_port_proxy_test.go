package worker

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestContainerPortTargetsPreferNativeFamily(t *testing.T) {
	info := &containerNetworkInfo{
		ContainerIp:   "192.168.0.63",
		ContainerIpv6: "fd00:abcd::3f",
	}

	native, fallback := containerPortTargets(info, 8781, addressFamilyIPv6)
	require.Equal(t, "[fd00:abcd::3f]:8781", native)
	require.Equal(t, "192.168.0.63:8781", fallback)

	native, fallback = containerPortTargets(info, 8781, addressFamilyIPv4)
	require.Equal(t, "192.168.0.63:8781", native)
	require.Equal(t, "[fd00:abcd::3f]:8781", fallback)
}

func TestAddressFamilyForHostHandlesBracketedIPv6(t *testing.T) {
	require.Equal(t, addressFamilyIPv6, addressFamilyForHost("[2600:1f18:37a4:c02::7286]"))
	require.Equal(t, addressFamilyIPv4, addressFamilyForHost("10.42.0.34"))
	require.Equal(t, addressFamilyUnknown, addressFamilyForHost("worker.local"))
}

func TestContainerPortProxyListenConfigUsesAdvertisedFamily(t *testing.T) {
	network, address := containerPortProxyListenConfig(8781, addressFamilyIPv6)
	require.Equal(t, "tcp6", network)
	require.Equal(t, "[::]:8781", address)

	network, address = containerPortProxyListenConfig(8781, addressFamilyIPv4)
	require.Equal(t, "tcp4", network)
	require.Equal(t, "0.0.0.0:8781", address)

	network, address = containerPortProxyListenConfig(8781, addressFamilyUnknown)
	require.Equal(t, "tcp", network)
	require.Equal(t, ":8781", address)
}

func TestContainerPortProxyFallsBackToReachableBackendFamily(t *testing.T) {
	backend := startLineServer(t, "tcp4", "127.0.0.1:0", "proxy-ok\n")
	hostPort := freeTCPPortForNetwork(t, "tcp4", "127.0.0.1:0")
	unusedIPv6Port, err := getRandomFreePort()
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	proxy := newContainerPortProxy(ctx, "container-one", PortBinding{
		HostPort:      hostPort,
		ContainerPort: 8781,
	}, addressFamilyIPv4, []string{
		net.JoinHostPort("::1", fmt.Sprintf("%d", unusedIPv6Port)),
		backend.Addr().String(),
	})
	defer proxy.close()
	go proxy.run()

	select {
	case <-proxy.ready:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for port proxy to become ready")
	}

	conn, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", hostPort)), time.Second)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("ping\n"))
	require.NoError(t, err)

	line, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)
	require.Equal(t, "proxy-ok\n", line)
}

func TestContainerPortProxyAcceptsIPv6ForIPv4Backend(t *testing.T) {
	backend := startLineServer(t, "tcp4", "127.0.0.1:0", "ipv6-to-ipv4-ok\n")
	hostPort := freeTCPPortForNetwork(t, "tcp6", "[::1]:0")

	proxy := startTestPortProxy(t, hostPort, addressFamilyIPv6, []string{backend.Addr().String()})
	defer proxy.close()

	line := dialLineServer(t, "tcp6", net.JoinHostPort("::1", fmt.Sprintf("%d", hostPort)))
	require.Equal(t, "ipv6-to-ipv4-ok\n", line)
}

func TestContainerPortProxyAcceptsIPv4ForIPv6Backend(t *testing.T) {
	backend := startLineServer(t, "tcp6", "[::1]:0", "ipv4-to-ipv6-ok\n")
	hostPort := freeTCPPortForNetwork(t, "tcp4", "127.0.0.1:0")

	proxy := startTestPortProxy(t, hostPort, addressFamilyIPv4, []string{backend.Addr().String()})
	defer proxy.close()

	line := dialLineServer(t, "tcp4", net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", hostPort)))
	require.Equal(t, "ipv4-to-ipv6-ok\n", line)
}

func startTestPortProxy(t *testing.T, hostPort int, family addressFamily, targets []string) *containerPortProxy {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	proxy := newContainerPortProxy(ctx, "container-one", PortBinding{
		HostPort:      hostPort,
		ContainerPort: 8781,
	}, family, targets)
	go proxy.run()

	select {
	case <-proxy.ready:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for port proxy to become ready")
	}
	return proxy
}

func startLineServer(t *testing.T, network, address, response string) net.Listener {
	t.Helper()

	listener, err := net.Listen(network, address)
	if err != nil && network == "tcp6" {
		t.Skipf("IPv6 loopback is unavailable: %v", err)
	}
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				if _, err := bufio.NewReader(conn).ReadString('\n'); err != nil {
					return
				}
				_, _ = conn.Write([]byte(response))
			}()
		}
	}()

	return listener
}

func freeTCPPortForNetwork(t *testing.T, network, address string) int {
	t.Helper()

	listener, err := net.Listen(network, address)
	if err != nil && network == "tcp6" {
		t.Skipf("IPv6 loopback is unavailable: %v", err)
	}
	require.NoError(t, err)
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}

func dialLineServer(t *testing.T, network, address string) string {
	t.Helper()

	conn, err := net.DialTimeout(network, address, time.Second)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("ping\n"))
	require.NoError(t, err)

	line, err := bufio.NewReader(conn).ReadString('\n')
	require.NoError(t, err)
	return line
}
