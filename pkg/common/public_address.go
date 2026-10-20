package common

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"syscall"
)

var ErrPrivateAddress = errors.New("destination must resolve to a public address")

// Special-use ranges not covered by net.IP.IsPrivate or IsGlobalUnicast.
var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("192.0.2.0/24"),
	netip.MustParsePrefix("192.88.99.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("198.51.100.0/24"),
	netip.MustParsePrefix("203.0.113.0/24"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("2001::/23"),
	netip.MustParsePrefix("2001:db8::/32"),
	netip.MustParsePrefix("2002::/16"), // 6to4.
	netip.MustParsePrefix("3ffe::/16"), // Former 6bone allocation.
	netip.MustParsePrefix("3fff::/20"),
}

// Globally reachable exceptions within the protocol-assignment blocks.
var publicSpecialPrefixes = []netip.Prefix{
	netip.MustParsePrefix("192.0.0.9/32"),
	netip.MustParsePrefix("192.0.0.10/32"),
	netip.MustParsePrefix("2001:1::1/128"),
	netip.MustParsePrefix("2001:1::2/128"),
	netip.MustParsePrefix("2001:1::3/128"),
	netip.MustParsePrefix("2001:3::/32"),
	netip.MustParsePrefix("2001:4:112::/48"),
	netip.MustParsePrefix("2001:20::/28"),
	netip.MustParsePrefix("2001:30::/28"),
}

var (
	nat64Prefix      = netip.MustParsePrefix("64:ff9b::/96")
	globalIPv6Prefix = netip.MustParsePrefix("2000::/3")
)

// IsPublicIP excludes local and special-use address space. IPv4-mapped and
// well-known NAT64 addresses are checked against their embedded IPv4 address.
func IsPublicIP(ip net.IP) bool {
	address, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	address = address.Unmap()
	if nat64Prefix.Contains(address) {
		bytes := address.As16()
		address = netip.AddrFrom4([4]byte(bytes[12:]))
	}
	if !address.IsGlobalUnicast() || address.IsPrivate() {
		return false
	}
	if address.Is6() && !globalIPv6Prefix.Contains(address) {
		return false
	}
	for _, prefix := range publicSpecialPrefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(address) {
			return false
		}
	}
	return true
}

// CheckPublicAddress is a net.Dialer.ControlContext hook. It checks the actual
// resolved IP immediately before connecting, preventing DNS rebinding.
func CheckPublicAddress(_ context.Context, _, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil || !IsPublicIP(net.ParseIP(host)) {
		return ErrPrivateAddress
	}
	return nil
}

// ValidatePublicHTTPURL checks both URL syntax and every resolved address.
// Callers must also enforce CheckPublicAddress when dialing the destination.
func ValidatePublicHTTPURL(ctx context.Context, raw string) error {
	return validatePublicHTTPURL(ctx, raw, net.DefaultResolver.LookupIPAddr)
}

func validatePublicHTTPURL(ctx context.Context, raw string, lookup func(context.Context, string) ([]net.IPAddr, error)) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return errors.New("url must be an absolute http(s) URL without credentials")
	}
	if port := u.Port(); port != "" {
		number, err := strconv.ParseUint(port, 10, 16)
		if err != nil || number == 0 {
			return errors.New("url port must be between 1 and 65535")
		}
	}
	addresses, err := lookup(ctx, u.Hostname())
	if err != nil {
		return err
	}
	if len(addresses) == 0 {
		return ErrPrivateAddress
	}
	for _, address := range addresses {
		if address.Zone != "" || !IsPublicIP(address.IP) {
			return ErrPrivateAddress
		}
	}
	return nil
}
