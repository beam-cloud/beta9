//go:build linux

package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
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
	if c.restoredNetwork == nil || len(c.networkAliases) == 0 {
		return nil
	}
	listeners := map[string][]int{}
	for _, file := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		f, err := os.Open(file)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		err = readTCPListeners(f, listeners)
		_ = f.Close()
		if err != nil {
			return err
		}
	}
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
	signature, err := json.Marshal(tracked)
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
	c.restoredNetwork.signature = string(signature)
	return nil
}

func readTCPListeners(r io.Reader, listeners map[string][]int) error {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 4 || fields[3] != "0A" {
			continue
		}
		address, port, ok := strings.Cut(fields[1], ":")
		if !ok {
			return fmt.Errorf("invalid TCP listener address")
		}
		bytes, err := hex.DecodeString(address)
		if err != nil || (len(bytes) != 4 && len(bytes) != 16) {
			return fmt.Errorf("invalid TCP listener IP %q", address)
		}
		for i := 0; i < len(bytes); i += 4 {
			binary.BigEndian.PutUint32(bytes[i:i+4], binary.NativeEndian.Uint32(bytes[i:i+4]))
		}
		n, err := strconv.ParseUint(port, 16, 16)
		if err != nil {
			return err
		}
		ip := net.IP(bytes).String()
		listeners[ip] = append(listeners[ip], int(n))
	}
	return scanner.Err()
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
	ipt, err := iptables.New(iptables.IPFamily(family), iptables.Timeout(5))
	if err != nil {
		return err
	}
	const incoming, outgoing = "BEAM_VM_RESTORE_IN", "BEAM_VM_RESTORE_OUT"
	for _, chain := range []string{incoming, outgoing} {
		if err := ipt.ClearChain("nat", chain); err != nil {
			return err
		}
	}
	// First priority, scoped to this guest and only addresses it previously
	// owned. Preserve existing user/Docker firewall chains.
	for _, rule := range []struct{ chain, target string }{{"PREROUTING", incoming}, {"OUTPUT", incoming}, {"POSTROUTING", outgoing}} {
		if exists, err := ipt.Exists("nat", rule.chain, "-j", rule.target); err != nil {
			return err
		} else if !exists {
			if err := ipt.Insert("nat", rule.chain, 1, "-j", rule.target); err != nil {
				return err
			}
		}
	}
	ports := map[int]string{}
	for _, address := range aliases {
		for _, port := range listeners[address] {
			if previous := ports[port]; previous != "" && previous != address {
				return fmt.Errorf("multiple paused interfaces listen on TCP port %d", port)
			}
			ports[port] = address
		}
		if err := ipt.Append("nat", outgoing, "-s", address, "-o", nic, "-j", "SNAT", "--to-source", current.String()); err != nil {
			return err
		}
	}
	for _, port := range listeners[current.String()] {
		delete(ports, port)
	}
	for port, address := range ports {
		if err := ipt.Append("nat", incoming, "-d", current.String(), "-p", "tcp", "--dport", strconv.Itoa(port), "-j", "DNAT", "--to-destination", net.JoinHostPort(address, strconv.Itoa(port))); err != nil {
			return err
		}
	}
	return nil
}
