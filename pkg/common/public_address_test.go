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
		"0.0.0.1", "192.0.0.8", "192.0.0.170", "192.0.2.1", "192.88.99.2", "198.18.0.1", "198.19.255.254",
		"198.51.100.1", "203.0.113.1", "240.0.0.1", "::ffff:0.0.0.1", "64:ff9b::c000:201",
		"::a00:1", "64:ff9b:1::a00:1", "100::1", "100:0:0:1::1", "2001::1", "2001:2::1",
		"2001:10::1", "2001:1::4", "2001:db8::1", "2002:a00:1::", "3ffe::1", "3fff::1", "4000::1", "5f00::1", "fec0::1",
	} {
		t.Run(ip, func(t *testing.T) {
			require.False(t, IsPublicIP(net.ParseIP(ip)))
			require.ErrorIs(t, CheckPublicAddress(context.Background(), "tcp", net.JoinHostPort(ip, "80"), nil), ErrPrivateAddress)
		})
	}
	for _, ip := range []string{
		"8.8.8.8", "100.63.255.254", "100.128.0.1", "192.0.0.9", "192.0.0.10", "192.0.1.1", "198.20.0.1",
		"2606:4700:4700::1111", "2001:4860:4860::8888", "64:ff9b::808:808", "::ffff:8.8.8.8",
		"2001:1::1", "2001:1::2", "2001:1::3", "2001:3::1", "2001:4:112::1", "2001:20::1", "2001:30::1", "2001:200::1",
	} {
		require.True(t, IsPublicIP(net.ParseIP(ip)))
		require.NoError(t, CheckPublicAddress(context.Background(), "tcp", net.JoinHostPort(ip, "443"), nil))
	}
	require.ErrorIs(t, CheckPublicAddress(context.Background(), "tcp", "example.com:80", nil), ErrPrivateAddress)
}

func TestValidatePublicHTTPURL(t *testing.T) {
	for _, tc := range []struct {
		name, url string
		ips       []string
		zone      string
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
		{name: "maximum port", url: "https://example.com:65535/hook", ips: []string{"8.8.8.8"}, allowed: true},
		{name: "out of range port", url: "https://example.com:65536/hook", ips: []string{"8.8.8.8"}},
		{name: "zero port", url: "https://example.com:0/hook", ips: []string{"8.8.8.8"}},
		{name: "oversized port", url: "https://example.com:18446744073709551616/hook", ips: []string{"8.8.8.8"}},
		{name: "scoped IPv6", url: "http://[2606:4700:4700::1111%25eth0]/hook", ips: []string{"2606:4700:4700::1111"}, zone: "eth0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validatePublicHTTPURL(context.Background(), tc.url, func(context.Context, string) ([]net.IPAddr, error) {
				var addresses []net.IPAddr
				for _, ip := range tc.ips {
					addresses = append(addresses, net.IPAddr{IP: net.ParseIP(ip), Zone: tc.zone})
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

func TestValidatePublicHTTPURLHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := validatePublicHTTPURL(ctx, "https://8.8.8.8/hook", func(context.Context, string) ([]net.IPAddr, error) {
		t.Fatal("canceled validation must not resolve the destination")
		return nil, nil
	})
	require.ErrorIs(t, err, context.Canceled)
}
