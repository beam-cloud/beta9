package common

import (
	"context"
	"errors"
	"net"
	"net/url"
	"strings"
	"syscall"
)

var ErrPrivateAddress = errors.New("destination must resolve to a public address")

// IsPublicIP excludes local, private, link-local, and shared address space,
// including IPv4 destinations embedded in the well-known NAT64 prefix.
func IsPublicIP(ip net.IP) bool {
	if ip == nil || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	if strings.HasPrefix(ip.String(), "64:ff9b::") {
		return IsPublicIP(net.IP(ip.To16()[12:]))
	}
	v4 := ip.To4()
	return v4 == nil || !(v4[0] == 100 && v4[1] >= 64 && v4[1] <= 127)
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
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
		return errors.New("url must be an absolute http(s) URL without credentials")
	}
	addresses, err := lookup(ctx, u.Hostname())
	if err != nil {
		return err
	}
	if len(addresses) == 0 {
		return ErrPrivateAddress
	}
	for _, address := range addresses {
		if !IsPublicIP(address.IP) {
			return ErrPrivateAddress
		}
	}
	return nil
}
