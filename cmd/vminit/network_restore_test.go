//go:build linux

package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"reflect"
	"strings"
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

func TestTCPListenerAddresses(t *testing.T) {
	listeners := map[string][]int{}
	input := "sl local_address rem_address st\n"
	for _, ip := range []string{"192.168.0.21", "fd00:abcd::15"} {
		input += fmt.Sprintf("0: %s:1F40 00000000:0000 0A\n", procAddress(net.ParseIP(ip)))
		input += fmt.Sprintf("1: %s:1F41 00000000:0000 01\n", procAddress(net.ParseIP(ip)))
	}
	if err := readTCPListeners(strings.NewReader(input), listeners); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(listeners, map[string][]int{"192.168.0.21": {8000}, "fd00:abcd::15": {8000}}) {
		t.Fatal(listeners)
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
	cfg := microvm.Network{MAC: mac.String(), IPv4: "192.168.50.21/24", IPv6: "fd00:abcd::21/64"}
	if err := configureNetworkLink(cfg, link); err != nil {
		t.Fatal(err)
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
	for _, ip := range []string{"192.168.50.34", "192.168.50.35", "192.168.50.21"} {
		cfg.IPv4 = ip + "/24"
		ip6 := "fd00:abcd::" + strings.TrimPrefix(ip, "192.168.50.")
		cfg.IPv6 = ip6 + "/64"
		if err := control.restoreNetworkLink(cfg, link); err != nil {
			t.Fatal(err)
		}
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
}
