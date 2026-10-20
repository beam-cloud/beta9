package common

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPublicAddressGuard(t *testing.T) {
	for _, ip := range []string{
		"127.0.0.1", "10.0.0.1", "172.16.0.1", "192.168.0.1", "169.254.169.254",
		"100.64.0.1", "100.127.255.254", "0.0.0.0", "224.0.0.1", "::", "::1",
		"fd00::1", "fe80::1", "ff02::1", "::ffff:127.0.0.1", "64:ff9b::a00:1", "64:ff9b::7f00:1",
	} {
		t.Run(ip, func(t *testing.T) {
			require.False(t, IsPublicIP(net.ParseIP(ip)))
			require.ErrorIs(t, CheckPublicAddress(context.Background(), "tcp", net.JoinHostPort(ip, "80"), nil), ErrPrivateAddress)
		})
	}
	for _, ip := range []string{"8.8.8.8", "2606:4700:4700::1111", "64:ff9b::808:808"} {
		require.True(t, IsPublicIP(net.ParseIP(ip)))
		require.NoError(t, CheckPublicAddress(context.Background(), "tcp", net.JoinHostPort(ip, "443"), nil))
	}
	require.ErrorIs(t, CheckPublicAddress(context.Background(), "tcp", "example.com:80", nil), ErrPrivateAddress)
}

func TestValidatePublicHTTPURL(t *testing.T) {
	for _, tc := range []struct {
		name, url string
		ips       []string
		lookupErr error
		allowed   bool
	}{
		{name: "public", url: "https://example.com/hook", ips: []string{"8.8.8.8"}, allowed: true},
		{name: "public IPv6", url: "http://[2606:4700:4700::1111]/hook", ips: []string{"2606:4700:4700::1111"}, allowed: true},
		{name: "mixed DNS", url: "https://example.com/hook", ips: []string{"8.8.8.8", "10.0.0.1"}},
		{name: "private DNS", url: "https://internal.example/hook", ips: []string{"169.254.169.254"}},
		{name: "no addresses", url: "https://empty.example/hook"},
		{name: "DNS failure", url: "https://missing.example/hook", lookupErr: errors.New("DNS failed")},
		{name: "relative", url: "/hook"},
		{name: "missing hostname", url: "http://:80/hook"},
		{name: "file", url: "file:///tmp/hook"},
		{name: "credentials", url: "https://user:password@example.com/hook"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePublicHTTPURL(context.Background(), tc.url, func(context.Context, string) ([]net.IPAddr, error) {
				var addresses []net.IPAddr
				for _, ip := range tc.ips {
					addresses = append(addresses, net.IPAddr{IP: net.ParseIP(ip)})
				}
				return addresses, tc.lookupErr
			})
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
