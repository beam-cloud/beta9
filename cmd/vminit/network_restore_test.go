//go:build linux

package main

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/vishvananda/netlink"
)

func procAddress(ip net.IP) string {
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	bytes := append([]byte{}, ip...)
	for i := 0; i < len(bytes); i += 4 {
		binary.NativeEndian.PutUint32(bytes[i:i+4], binary.BigEndian.Uint32(bytes[i:i+4]))
	}
	return strings.ToUpper(hex.EncodeToString(bytes))
}

func tcp(local, remote string) tcpConnection {
	return tcpConnection{local: netip.MustParseAddrPort(local), remote: netip.MustParseAddrPort(remote)}
}

func TestTCPSockets(t *testing.T) {
	listeners, established := map[string][]int{}, map[tcpConnection]bool{}
	input := "sl local_address rem_address st\n"
	for _, ip := range []string{"192.168.0.21", "fd00:abcd::15"} {
		input += fmt.Sprintf("0: %s:1F40 00000000:0000 0A\n", procAddress(net.ParseIP(ip)))
		input += fmt.Sprintf("1: %s:1F41 %s:9C40 01\n", procAddress(net.ParseIP(ip)), procAddress(net.ParseIP(ip)))
		input += fmt.Sprintf("2: %s:1F42 %s:9C41 06\n", procAddress(net.ParseIP(ip)), procAddress(net.ParseIP(ip)))
	}
	// As a socket of the IPv6 family holds a connection over IPv4.
	mapped := func(ip string) string {
		bytes := append([]byte{}, net.ParseIP(ip).To16()...)
		for i := 0; i < len(bytes); i += 4 {
			binary.NativeEndian.PutUint32(bytes[i:i+4], binary.BigEndian.Uint32(bytes[i:i+4]))
		}
		return strings.ToUpper(hex.EncodeToString(bytes))
	}
	input += fmt.Sprintf("3: %s:1BC7 %s:9826 01\n", mapped("192.168.0.3"), mapped("192.168.0.1"))
	if err := readTCPSockets(strings.NewReader(input), listeners, established); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(listeners, map[string][]int{"192.168.0.21": {8000}, "fd00:abcd::15": {8000}}) {
		t.Fatal(listeners)
	}
	want := map[tcpConnection]bool{
		tcp("192.168.0.21:8001", "192.168.0.21:40000"):       true,
		tcp("[fd00:abcd::15]:8001", "[fd00:abcd::15]:40000"): true,
		tcp("192.168.0.3:7111", "192.168.0.1:38950"):         true,
	}
	if !reflect.DeepEqual(established, want) {
		t.Fatal(established)
	}
}

func TestHostConnections(t *testing.T) {
	listeners := map[string][]int{"::": {7111}, "0.0.0.0": {2222}, "192.168.0.3": {8000}}
	established := map[tcpConnection]bool{
		tcp("192.168.0.3:7111", "192.168.0.1:38950"):     true, // from the worker
		tcp("192.168.0.4:8000", "192.168.0.1:41000"):     true, // to an address the guest had before
		tcp("[fd00:abcd::3]:2222", "[fd00:abcd::1]:500"): true,
		tcp("192.168.0.3:51000", "192.168.0.1:53"):       true, // made by the guest, to its gateway
		tcp("192.168.0.3:7111", "192.168.0.9:38950"):     true, // from another guest
		tcp("127.0.0.1:7111", "127.0.0.1:40000"):         true,
	}
	gateways := []netip.Addr{netip.MustParseAddr("192.168.0.1"), netip.MustParseAddr("fd00:abcd::1")}
	want := []tcpConnection{
		tcp("192.168.0.3:7111", "192.168.0.1:38950"),
		tcp("192.168.0.4:8000", "192.168.0.1:41000"),
		tcp("[fd00:abcd::3]:2222", "[fd00:abcd::1]:500"),
	}
	if got := hostConnections(listeners, established, gateways); !reflect.DeepEqual(got, want) {
		t.Fatal(got)
	}
	if got := hostConnections(listeners, established, nil); got != nil {
		t.Fatal(got)
	}
}

func TestResetRules(t *testing.T) {
	stale := []tcpConnection{tcp("192.168.0.3:7111", "192.168.0.1:38950"), tcp("[fd00:abcd::3]:2222", "[fd00:abcd::1]:500")}
	for _, test := range []struct {
		name, installed string
		stale           []tcpConnection
		want            string
	}{
		{"first", "\n:OUTPUT ACCEPT [0:0]\n-A OUTPUT -j GUEST_RULES\n", stale, "*filter\n" +
			":BEAM_VM_RESTORE_RESET - [0:0]\n" +
			"-F BEAM_VM_RESTORE_RESET\n" +
			"-I OUTPUT 1 -j BEAM_VM_RESTORE_RESET\n" +
			"-A BEAM_VM_RESTORE_RESET -s 192.168.0.3 -d 192.168.0.1 -p tcp --sport 7111 --dport 38950 -j REJECT --reject-with tcp-reset\n" +
			"-A BEAM_VM_RESTORE_RESET -s fd00:abcd::3 -d fd00:abcd::1 -p tcp --sport 2222 --dport 500 -j REJECT --reject-with tcp-reset\n" +
			"COMMIT\n"},
		{"once they are reset", "\n:BEAM_VM_RESTORE_RESET - [0:0]\n-A OUTPUT -j BEAM_VM_RESTORE_RESET\n-A OUTPUT -j GUEST_RULES\n", nil, "*filter\n" +
			":BEAM_VM_RESTORE_RESET - [0:0]\n" +
			"-F BEAM_VM_RESTORE_RESET\n" +
			"COMMIT\n"},
	} {
		if got := resetRules(test.installed, test.stale); got != test.want {
			t.Errorf("%s:\n%s\nwant:\n%s", test.name, got, test.want)
		}
	}
}

// Explicitly run inside a fresh network namespace on the staging CPU host.
// Ordinary CI cannot mutate its host network or firewall through this test.
func TestInterfaceBoundTCPAcrossRepeatedNetworkRestore(t *testing.T) {
	if os.Getenv("BEAM_VM_ISOLATED_NETWORK_TEST") != "1" {
		t.Skip("requires unshare --net and explicit isolated-test opt-in")
	}
	if os.Geteuid() != 0 {
		t.Fatal("isolated network test requires root")
	}
	if _, err := exec.LookPath("iptables"); err != nil {
		t.Fatal(err)
	}
	for _, binary := range []string{"iptables", "ip6tables"} {
		for _, args := range [][]string{
			{"-t", "nat", "-N", "GUEST_RULES"},
			{"-t", "nat", "-A", "OUTPUT", "-j", "GUEST_RULES"},
			{"-t", "nat", "-A", "GUEST_RULES", "-p", "tcp", "--dport", "9", "-j", "RETURN"},
		} {
			if output, err := exec.Command(binary, args...).CombinedOutput(); err != nil {
				t.Fatalf("guest firewall setup: %v: %s", err, output)
			}
		}
	}
	lo, err := netlink.LinkByName("lo")
	if err != nil {
		t.Fatal(err)
	}
	if err := netlink.LinkSetUp(lo); err != nil {
		t.Fatal(err)
	}
	mac, _ := net.ParseMAC("02:00:00:00:00:01")
	link := &netlink.Dummy{LinkAttrs: netlink.LinkAttrs{Name: "vmnic", HardwareAddr: mac}}
	if err := netlink.LinkAdd(link); err != nil {
		t.Fatal(err)
	}
	cfg := microvm.Network{MAC: mac.String(), IPv4: "192.168.50.21/24", IPv6: "fd00:abcd::21/64", Gateway4: "192.168.50.1"}
	if err := configureNetworkLink(cfg, link); err != nil {
		t.Fatal(err)
	}
	accepting, err := net.Listen("tcp4", "0.0.0.0:9000")
	if err != nil {
		t.Fatal(err)
	}
	defer accepting.Close()
	// The host connects from the gateway's address, which is here only while
	// it does: nothing answers what its connection is sent afterwards.
	gateway := &netlink.Addr{IPNet: &net.IPNet{IP: net.ParseIP("192.168.50.1"), Mask: net.CIDRMask(32, 32)}}
	fromHost := func(ip string, hostIsThere func(guest, host net.Conn)) net.Conn {
		t.Helper()
		if err := netlink.AddrAdd(lo, gateway); err != nil {
			t.Fatal(err)
		}
		host, err := (&net.Dialer{LocalAddr: &net.TCPAddr{IP: gateway.IP}, Timeout: 3 * time.Second}).Dial("tcp4", net.JoinHostPort(ip, "9000"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { host.Close() })
		guest, err := accepting.Accept()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { guest.Close() })
		if hostIsThere != nil {
			hostIsThere(guest, host)
		}
		if err := netlink.AddrDel(lo, gateway); err != nil {
			t.Fatal(err)
		}
		return guest
	}
	isReset := func(conn net.Conn) {
		t.Helper()
		_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Write([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.Read(make([]byte, 1)); !errors.Is(err, syscall.ECONNRESET) {
			t.Fatalf("connection from before the restore: %v", err)
		}
	}
	bound, err := net.Listen("tcp4", "192.168.50.21:8000")
	if err != nil {
		t.Fatal(err)
	}
	wildcard, err := net.Listen("tcp4", "0.0.0.0:8001")
	if err != nil {
		t.Fatal(err)
	}
	bound6, err := net.Listen("tcp6", "[fd00:abcd::21]:8000")
	if err != nil {
		t.Fatal(err)
	}
	for _, listener := range []net.Listener{bound, wildcard, bound6} {
		defer listener.Close()
		go http.Serve(listener, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "same-process") }))
	}
	control := &control{systemd: true}
	client := &http.Client{Transport: &http.Transport{}, Timeout: 3 * time.Second}
	previous := "192.168.50.21"
	for _, ip := range []string{"192.168.50.34", "192.168.50.35", "192.168.50.21", "192.168.50.21"} {
		// The guest is saved with a connection from the host, and restored
		// at another address or, the last time, at the same one.
		stale := fromHost(previous, nil)
		control.noteHostConnections()
		previous = ip
		cfg.IPv4 = ip + "/24"
		ip6 := "fd00:abcd::" + strings.TrimPrefix(ip, "192.168.50.")
		cfg.IPv6 = ip6 + "/64"
		if err := control.restoreNetworkLink(cfg, link); err != nil {
			t.Fatal(err)
		}
		isReset(stale)
		// Once it is, nothing is refused: not a connection the host makes now.
		if err := control.refreshRestoredSockets(); err != nil {
			t.Fatal(err)
		}
		if rules, err := exec.Command("iptables-save", "-t", "filter").CombinedOutput(); err != nil || strings.Contains(string(rules), "REJECT") {
			t.Fatalf("rules that reset: %v: %s", err, rules)
		}
		fromHost(ip, func(guest, host net.Conn) {
			_ = host.SetDeadline(time.Now().Add(3 * time.Second))
			if _, err := guest.Write([]byte("x")); err != nil {
				t.Fatal(err)
			}
			if _, err := host.Read(make([]byte, 1)); err != nil {
				t.Fatalf("connection from after the restore: %v", err)
			}
			// Closed while the host is there to hear of it.
			guest.Close()
			host.Close()
			time.Sleep(50 * time.Millisecond)
		})
		for _, address := range []string{net.JoinHostPort(ip, "8000"), net.JoinHostPort(ip, "8001"), net.JoinHostPort(ip6, "8000")} {
			resp, err := client.Get("http://" + address)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || string(body) != "same-process" {
				t.Fatalf("preserved listener: %q %v", body, err)
			}
		}
	}
	// A later restart binds the current address and must bypass the retired
	// listener's mapping, without requiring another VM lifecycle operation.
	bound.Close()
	cfg.IPv4 = "192.168.50.36/24"
	if err := control.restoreNetworkLink(cfg, link); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.Listen("tcp4", "192.168.50.36:8000")
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	go http.Serve(replacement, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "restarted-process") }))
	if err := control.refreshRestoredSockets(); err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get("http://192.168.50.36:8000")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil || string(body) != "restarted-process" {
		t.Fatalf("restarted listener: %q %v", body, err)
	}
	for _, binary := range []string{"iptables-save", "ip6tables-save"} {
		rules, err := exec.Command(binary, "-t", "nat").CombinedOutput()
		if err != nil {
			t.Fatalf("read restored firewall: %v: %s", err, rules)
		}
		for _, rule := range []string{
			"-A OUTPUT -j GUEST_RULES",
			"-A GUEST_RULES -p tcp -m tcp --dport 9 -j RETURN",
			"-A PREROUTING -j BEAM_VM_RESTORE_IN",
			"-A OUTPUT -j BEAM_VM_RESTORE_IN",
			"-A POSTROUTING -j BEAM_VM_RESTORE_OUT",
		} {
			if strings.Count(string(rules), rule+"\n") != 1 {
				t.Fatalf("restore must preserve guest rules and avoid duplicate jumps: %s: %s", rule, rules)
			}
		}
	}
	rules, err := exec.Command("iptables-save", "-t", "filter").CombinedOutput()
	if err != nil || strings.Count(string(rules), "-A OUTPUT -j BEAM_VM_RESTORE_RESET\n") != 1 {
		t.Fatalf("one jump to the rules that reset: %v: %s", err, rules)
	}
}
