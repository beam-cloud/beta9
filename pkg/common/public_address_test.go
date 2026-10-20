package common

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

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
		"2606:4700:4700::1111", "2001:4860:4860::8888", "2606:4700::127.0.0.1", "64:ff9b::808:808", "::ffff:8.8.8.8",
		"2001:1::1", "2001:1::2", "2001:1::3", "2001:3::1", "2001:4:112::1", "2001:20::1", "2001:30::1", "2001:200::1",
	} {
		require.True(t, IsPublicIP(net.ParseIP(ip)))
		require.NoError(t, CheckPublicAddress(context.Background(), "tcp", net.JoinHostPort(ip, "443"), nil))
	}
	require.ErrorIs(t, CheckPublicAddress(context.Background(), "tcp", "example.com:80", nil), ErrPrivateAddress)
}

func TestPublicAddressRangeBoundaries(t *testing.T) {
	for _, tc := range []struct {
		ip      string
		allowed bool
	}{
		{"100.64.0.0", false},
		{"100.127.255.255", false},
		{"172.15.255.255", true},
		{"172.16.0.0", false},
		{"172.31.255.255", false},
		{"172.32.0.0", true},
		{"2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff", false},
		{"2001:200::", true},
		{"2001:db7:ffff:ffff:ffff:ffff:ffff:ffff", true},
		{"2001:db8::", false},
		{"2001:db8:ffff:ffff:ffff:ffff:ffff:ffff", false},
		{"2001:db9::", true},
		{"3fff:fff:ffff:ffff:ffff:ffff:ffff:ffff", false},
		{"3fff:1000::", true},
		{"3fff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", true},
		{"4000::", false},
		{"fc00::", false},
		{"fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false},
		{"fe80::", false},
		{"febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff", false},
	} {
		t.Run(tc.ip, func(t *testing.T) {
			require.Equal(t, tc.allowed, IsPublicIP(net.ParseIP(tc.ip)))
		})
	}
}

func TestPublicAddressDialer(t *testing.T) {
	stopBeforeConnect := errors.New("address checked; stop before connecting")
	for _, tc := range []struct {
		network, address string
		allowed          bool
	}{
		{"tcp4", "8.8.8.8:443", true},
		{"tcp6", "[2606:4700:4700::1111]:443", true},
		{"tcp6", "[2606:4700:4700:0:0:0:0:1111]:443", true},
		{"tcp", "[::ffff:8.8.8.8]:443", true},
		{"tcp6", "[64:ff9b::808:808]:443", true},
		{"tcp4", "127.0.0.1:443", false},
		{"tcp6", "[::1]:443", false},
		{"tcp6", "[fd00::1]:443", false},
		{"tcp", "[::ffff:127.0.0.1]:443", false},
		{"tcp6", "[64:ff9b::7f00:1]:443", false},
	} {
		t.Run(tc.network+"/"+tc.address, func(t *testing.T) {
			dialer := net.Dialer{
				Timeout: time.Second,
				ControlContext: func(ctx context.Context, network, address string, conn syscall.RawConn) error {
					if err := CheckPublicAddress(ctx, network, address, conn); err != nil {
						return err
					}
					return stopBeforeConnect
				},
			}
			conn, err := dialer.DialContext(context.Background(), tc.network, tc.address)
			if conn != nil {
				conn.Close()
				t.Fatal("dialer must stop before connecting")
			}
			if tc.network == "tcp6" && errors.Is(err, syscall.EAFNOSUPPORT) {
				t.Skip("IPv6 sockets are unavailable")
			}
			if tc.allowed {
				require.ErrorIs(t, err, stopBeforeConnect)
			} else {
				require.ErrorIs(t, err, ErrPrivateAddress)
			}
		})
	}
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
		{name: "public dual stack", url: "https://example.com/hook", ips: []string{"8.8.8.8", "2606:4700:4700::1111"}, allowed: true},
		{name: "mixed DNS", url: "https://example.com/hook", ips: []string{"8.8.8.8", "10.0.0.1"}},
		{name: "private IPv6 in dual stack", url: "https://example.com/hook", ips: []string{"8.8.8.8", "fd00::1"}},
		{name: "private IPv4 in dual stack", url: "https://example.com/hook", ips: []string{"2606:4700:4700::1111", "10.0.0.1"}},
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

func TestValidatePublicHTTPURLLiterals(t *testing.T) {
	for _, tc := range []struct {
		url     string
		allowed bool
	}{
		{"https://8.8.8.8:443/hook", true},
		{"https://[2606:4700:4700::1111]:443/hook", true},
		{"http://[2606:4700:4700:0:0:0:0:1111]/hook", true},
		{"https://[::ffff:8.8.8.8]/hook", true},
		{"https://[64:ff9b::808:808]/hook", true},
		{"http://127.0.0.1/hook", false},
		{"http://[::1]/hook", false},
		{"http://[fd00::1]/hook", false},
		{"http://[::ffff:169.254.169.254]/hook", false},
		{"http://[64:ff9b::a9fe:a9fe]/hook", false},
		{"http://[2606:4700:4700::1111%25eth0]/hook", false},
	} {
		t.Run(tc.url, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := ValidatePublicHTTPURL(ctx, tc.url)
			if tc.allowed {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrPrivateAddress)
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

func FuzzPublicAddressRepresentations(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte{1, 2, 3})
	f.Add([]byte{8, 8, 8, 8})
	f.Add([]byte{127, 0, 0, 1})
	f.Add([]byte{169, 254, 169, 254})
	for _, ip := range []string{"::ffff:8.8.8.8", "2606:4700:4700::1111", "fd00::1", "64:ff9b::808:808", "64:ff9b::a00:1"} {
		f.Add([]byte(net.ParseIP(ip)))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		ip := net.IP(data)
		allowed := IsPublicIP(ip)
		if len(ip) != net.IPv4len && len(ip) != net.IPv6len {
			require.False(t, allowed, "invalid IP length")
			return
		}
		require.Equal(t, allowed, IsPublicIP(net.ParseIP(ip.String())), "text representation")
		if v4 := ip.To4(); v4 != nil {
			require.Equal(t, allowed, IsPublicIP(v4), "four-byte representation")
			require.Equal(t, allowed, IsPublicIP(v4.To16()), "mapped representation")
			nat64 := net.ParseIP("64:ff9b::")
			copy(nat64[12:], v4)
			require.Equal(t, allowed, IsPublicIP(nat64), "well-known NAT64 representation")
		}
	})
}
