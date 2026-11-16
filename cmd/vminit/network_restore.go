//go:build linux

package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/beam-cloud/beta9/pkg/runtime/microvm"
	"github.com/coreos/go-iptables/iptables"
	"github.com/vishvananda/netlink"
	"golang.org/x/sys/unix"
)

type restoredSocketNetwork struct {
	current   map[iptables.Protocol]net.IP
	nic       string
	signature string
}

// tcpConnection is an established connection: its address here, and the
// other end's.
type tcpConnection struct {
	local, remote netip.AddrPort
}

func (conn tcpConnection) String() string {
	return conn.local.String() + "<-" + conn.remote.String()
}

// A restored process can still listen on its old interface address. Preserve
// that address on guest loopback and translate new connections inside the
// guest, where addresses may overlap other VMs without weakening host filters.
// Wildcard listeners continue directly on the current interface.
func (c *control) restoreNetwork(cfg microvm.Network) error {
	if !c.systemd {
		return reconfigureNetwork(cfg)
	}

	link, err := findNIC(cfg.MAC)
	if err != nil {
		return err
	}

	return c.restoreNetworkLink(cfg, link)
}

func (c *control) restoreNetworkLink(cfg microvm.Network, link netlink.Link) error {
	c.networkMu.Lock()
	defer c.networkMu.Unlock()
	previous, err := netlink.AddrList(link, netlink.FAMILY_ALL)
	if err != nil {
		return err
	}

	if c.networkAliases == nil {
		c.networkAliases = map[string]bool{}
	}

	for _, addr := range previous {
		if addr.Scope != int(netlink.SCOPE_LINK) {
			c.networkAliases[addr.IP.String()] = true
		}
	}

	lo, err := netlink.LinkByName("lo")
	if err != nil {
		return err
	}

	current := map[iptables.Protocol]net.IP{}
	for _, cidr := range []string{cfg.IPv4, cfg.IPv6} {
		if cidr == "" {
			continue
		}

		ip, _, err := net.ParseCIDR(cidr)
		if err != nil {
			return err
		}

		family := iptables.ProtocolIPv6
		bits := 128
		if ip.To4() != nil {
			family, bits = iptables.ProtocolIPv4, 32
		}

		current[family] = ip
		if c.networkAliases[ip.String()] {
			// An address may cycle back to this VM after several suspensions.
			_ = netlink.AddrDel(lo, &netlink.Addr{IPNet: &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)}})
			delete(c.networkAliases, ip.String())
		}
	}

	if err := reconfigureNetworkLink(cfg, link); err != nil {
		return err
	}

	for address := range c.networkAliases {
		ip := net.ParseIP(address)
		bits := 128
		if ip.To4() != nil {
			bits = 32
		}

		if err := netlink.AddrReplace(lo, &netlink.Addr{IPNet: &net.IPNet{IP: ip, Mask: net.CIDRMask(bits, bits)}, Flags: unix.IFA_F_NODAD}); err != nil {
			return fmt.Errorf("retain paused socket address: %w", err)
		}
	}

	// What the host had connected went with the network it had for the VM.
	c.staleConnections, c.hostConnections = c.hostConnections, nil
	c.restoredNetwork = &restoredSocketNetwork{current: current, nic: link.Attrs().Name}
	return c.refreshSocketRulesLocked()
}

// Refresh with the existing control heartbeat so restarting an application on
// the current address removes its old translation. No new background service.
func (c *control) refreshRestoredSockets() error {
	c.networkMu.Lock()
	defer c.networkMu.Unlock()
	return c.refreshSocketRulesLocked()
}

func (c *control) refreshSocketRulesLocked() error {
	if c.restoredNetwork == nil {
		return nil
	}
	if len(c.networkAliases) == 0 && len(c.staleConnections) == 0 && c.restoredNetwork.signature != "" {
		return nil
	}

	listeners, established, err := tcpSockets()
	if err != nil {
		return err
	}

	// A connection that has been reset is no longer there to be.
	c.staleConnections = slices.DeleteFunc(c.staleConnections, func(conn tcpConnection) bool { return !established[conn] })

	// Only relevant listener changes require a firewall transaction.
	tracked := map[string][]int{}
	for address := range c.networkAliases {
		tracked[address] = listeners[address]
	}

	for _, ip := range c.restoredNetwork.current {
		tracked[ip.String()] = listeners[ip.String()]
	}

	for _, ports := range tracked {
		sort.Ints(ports)
	}

	stale := make([]string, 0, len(c.staleConnections))
	for _, conn := range c.staleConnections {
		stale = append(stale, conn.String())
	}

	signature, err := json.Marshal([]any{tracked, stale})
	if err != nil {
		return err
	}

	if string(signature) == c.restoredNetwork.signature {
		return nil
	}

	for family, ip := range c.restoredNetwork.current {
		if err := c.restoreSocketRules(family, ip, c.restoredNetwork.nic, listeners); err != nil {
			return fmt.Errorf("restore interface-bound sockets: %w", err)
		}
	}

	// The guest runs as well without: a connection that is not reset is one
	// that its owner waits for, as it did before there was this.
	if err := c.resetStaleConnections(); err != nil {
		logf("connections from before the restore are not reset: %v", err)
	}

	c.restoredNetwork.signature = string(signature)
	return nil
}

// tcpSockets reads the ports that are listened on, by address, and the
// connections that are established.
func tcpSockets() (map[string][]int, map[tcpConnection]bool, error) {
	listeners, established := map[string][]int{}, map[tcpConnection]bool{}
	for _, file := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		f, err := os.Open(file)
		if os.IsNotExist(err) {
			continue
		}

		if err != nil {
			return nil, nil, err
		}

		err = readTCPSockets(f, listeners, established)
		_ = f.Close()
		if err != nil {
			return nil, nil, err
		}
	}

	return listeners, established, nil
}

func readTCPSockets(r io.Reader, listeners map[string][]int, established map[tcpConnection]bool) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 || (fields[3] != "0A" && fields[3] != "01") {
			continue
		}

		local, err := procTCPAddress(fields[1])
		if err != nil {
			return err
		}

		if fields[3] == "0A" {
			ip := net.IP(local.Addr().AsSlice()).String()
			listeners[ip] = append(listeners[ip], int(local.Port()))
			continue
		}

		remote, err := procTCPAddress(fields[2])
		if err != nil {
			return err
		}

		// A socket of either family carries IPv4, which is how it is filtered.
		established[tcpConnection{
			local:  netip.AddrPortFrom(local.Addr().Unmap(), local.Port()),
			remote: netip.AddrPortFrom(remote.Addr().Unmap(), remote.Port()),
		}] = true
	}

	return scanner.Err()
}

// procTCPAddress reads an address as /proc/net/tcp writes it: each 32 bits of
// it in the byte order of the machine, then the port.
func procTCPAddress(field string) (netip.AddrPort, error) {
	address, port, ok := strings.Cut(field, ":")
	if !ok {
		return netip.AddrPort{}, fmt.Errorf("invalid TCP address %q", field)
	}

	bytes, err := hex.DecodeString(address)
	if err != nil || (len(bytes) != 4 && len(bytes) != 16) {
		return netip.AddrPort{}, fmt.Errorf("invalid TCP IP %q", address)
	}

	for i := 0; i < len(bytes); i += 4 {
		binary.BigEndian.PutUint32(bytes[i:i+4], binary.NativeEndian.Uint32(bytes[i:i+4]))
	}

	n, err := strconv.ParseUint(port, 16, 16)
	if err != nil {
		return netip.AddrPort{}, err
	}

	ip, _ := netip.AddrFromSlice(bytes)
	return netip.AddrPortFrom(ip, uint16(n)), nil
}

func (c *control) restoreSocketRules(family iptables.Protocol, current net.IP, nic string, listeners map[string][]int) error {
	var aliases []string
	for address := range c.networkAliases {
		if (net.ParseIP(address).To4() != nil) == (family == iptables.ProtocolIPv4) {
			aliases = append(aliases, address)
		}
	}

	if len(aliases) == 0 {
		return nil
	}

	sort.Strings(aliases)
	ports := map[int]string{}
	for _, address := range aliases {
		for _, port := range listeners[address] {
			if previous := ports[port]; previous != "" && previous != address {
				return fmt.Errorf("multiple paused interfaces listen on TCP port %d", port)
			}
			ports[port] = address
		}
	}
	for _, port := range listeners[current.String()] {
		delete(ports, port)
	}

	binary := "iptables"
	if family == iptables.ProtocolIPv6 {
		binary = "ip6tables"
	}
	existing, err := helperOutput(exec.Command(binary+"-save", "-t", "nat"))
	if err != nil {
		return fmt.Errorf("read restore rules: %w: %s", err, existing)
	}
	installed := "\n" + string(existing)

	const incoming, outgoing = "BEAM_VM_RESTORE_IN", "BEAM_VM_RESTORE_OUT"
	var rules strings.Builder
	fmt.Fprintf(&rules, "*nat\n:%s - [0:0]\n:%s - [0:0]\n-F %s\n-F %s\n", incoming, outgoing, incoming, outgoing)
	// Replace only our chains, in one transaction. Guest and Docker rules
	// stay intact, and listeners never see a partially rebuilt translation.
	for _, rule := range []struct{ chain, target string }{{"PREROUTING", incoming}, {"OUTPUT", incoming}, {"POSTROUTING", outgoing}} {
		if !strings.Contains(installed, "\n-A "+rule.chain+" -j "+rule.target+"\n") {
			fmt.Fprintf(&rules, "-I %s 1 -j %s\n", rule.chain, rule.target)
		}
	}
	for _, address := range aliases {
		fmt.Fprintf(&rules, "-A %s -s %s -o %q -j SNAT --to-source %s\n", outgoing, address, nic, current)
	}
	orderedPorts := make([]int, 0, len(ports))
	for port := range ports {
		orderedPorts = append(orderedPorts, port)
	}
	sort.Ints(orderedPorts)
	for _, port := range orderedPorts {
		fmt.Fprintf(&rules, "-A %s -d %s -p tcp --dport %d -j DNAT --to-destination %s\n", incoming, current, port, net.JoinHostPort(ports[port], strconv.Itoa(port)))
	}
	rules.WriteString("COMMIT\n")
	command := exec.Command(binary+"-restore", "--noflush", "--wait", "5")
	command.Stdin = strings.NewReader(rules.String())
	if output, err := helperOutput(command); err != nil {
		return fmt.Errorf("apply restore rules: %w: %s", err, output)
	}
	return nil
}

// The host reaches the guest from the gateway's address, and what it has
// connected when the guest is saved does not outlive the network it had for
// the VM: a restored guest holds its end of a connection that nothing will
// answer, not even to refuse it. The process manager holds one from the
// worker, and would wait for it when asked to stop.
//
// noteHostConnections records those connections as the host freezes the guest
// to save it. A guest restored from what was saved refuses what they send,
// which resets each: at its next keepalive, or when it is next written to.
func (c *control) noteHostConnections() {
	if !c.systemd {
		return
	}

	var gateways []netip.Addr
	listeners, established, err := tcpSockets()
	if err == nil {
		gateways, err = defaultGateways()
	}

	if err != nil {
		logf("connections from the host are not recorded: %v", err)
	}

	c.networkMu.Lock()
	defer c.networkMu.Unlock()
	c.hostConnections = hostConnections(listeners, established, gateways)
}

// hostConnections picks out the connections that the host made: those from a
// gateway to a port that is listened on here. One that the guest made to its
// gateway may still be answered.
func hostConnections(listeners map[string][]int, established map[tcpConnection]bool, gateways []netip.Addr) []tcpConnection {
	listening := map[uint16]bool{}
	for _, ports := range listeners {
		for _, port := range ports {
			listening[uint16(port)] = true
		}
	}

	var found []tcpConnection
	for conn := range established {
		if listening[conn.local.Port()] && slices.Contains(gateways, conn.remote.Addr()) {
			found = append(found, conn)
		}
	}

	slices.SortFunc(found, func(a, b tcpConnection) int { return strings.Compare(a.String(), b.String()) })
	return found
}

func defaultGateways() ([]netip.Addr, error) {
	routes, err := netlink.RouteList(nil, netlink.FAMILY_ALL)
	if err != nil {
		return nil, err
	}

	var gateways []netip.Addr
	for _, route := range routes {
		if route.Dst != nil {
			if ones, _ := route.Dst.Mask.Size(); ones != 0 {
				continue
			}
		}

		if gateway, ok := netip.AddrFromSlice(route.Gw); ok {
			gateways = append(gateways, gateway.Unmap())
		}
	}

	return gateways, nil
}

const resetChain = "BEAM_VM_RESTORE_RESET"

func (c *control) resetStaleConnections() error {
	if c.resetting == nil {
		c.resetting = map[string]bool{}
	}

	var errs []error
	for _, binary := range []string{"iptables", "ip6tables"} {
		var stale []tcpConnection
		for _, conn := range c.staleConnections {
			if conn.local.Addr().Is4() == (binary == "iptables") {
				stale = append(stale, conn)
			}
		}

		// Nothing of this family to reset, and nothing left that resets.
		if len(stale) == 0 && !c.resetting[binary] {
			continue
		}

		existing, err := helperOutput(exec.Command(binary+"-save", "-t", "filter"))
		if err != nil {
			errs = append(errs, fmt.Errorf("read filter rules: %w: %s", err, existing))
			continue
		}

		command := exec.Command(binary+"-restore", "--noflush", "--wait", "5")
		command.Stdin = strings.NewReader(resetRules("\n"+string(existing), stale))
		if output, err := helperOutput(command); err != nil {
			errs = append(errs, fmt.Errorf("apply filter rules: %w: %s", err, output))
			continue
		}

		c.resetting[binary] = len(stale) > 0
	}

	return errors.Join(errs...)
}

// resetRules refuses, with a reset, what each connection sends. They replace
// those of the chain, which the guest's own rules and Docker's do not see. A
// rule goes once its connection has: the host may connect again from the same
// port, and that connection is not the one that was refused.
func resetRules(installed string, connections []tcpConnection) string {
	var rules strings.Builder
	fmt.Fprintf(&rules, "*filter\n:%s - [0:0]\n-F %s\n", resetChain, resetChain)
	if !strings.Contains(installed, "\n-A OUTPUT -j "+resetChain+"\n") {
		fmt.Fprintf(&rules, "-I OUTPUT 1 -j %s\n", resetChain)
	}

	for _, conn := range connections {
		fmt.Fprintf(&rules, "-A %s -s %s -d %s -p tcp --sport %d --dport %d -j REJECT --reject-with tcp-reset\n",
			resetChain, conn.local.Addr(), conn.remote.Addr(), conn.local.Port(), conn.remote.Port())
	}

	rules.WriteString("COMMIT\n")
	return rules.String()
}
