package cache

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	publicIPLookupTimeout = 2 * time.Second
	publicIPResponseLimit = 128
)

func getDefaultInterface() (string, error) {
	file, err := os.Open("/proc/net/route")
	if err != nil {
		return "", err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if fields[1] == "00000000" { // Destination of default route
			return fields[0], nil
		}
	}

	return "", fmt.Errorf("default route not found")
}

func isTLSEnabled(addr string) bool {
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	return port == "443"
}

func GetPublicIpAddr() (string, error) {
	client := &http.Client{Timeout: publicIPLookupTimeout}
	resp, err := client.Get("https://api.ipify.org?format=text")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("public IP lookup failed with status %d", resp.StatusCode)
	}

	ip, err := io.ReadAll(io.LimitReader(resp.Body, publicIPResponseLimit))
	if err != nil {
		return "", err
	}

	ipAddr := strings.TrimSpace(string(ip))
	if net.ParseIP(ipAddr) == nil {
		return "", fmt.Errorf("public IP lookup returned invalid address: %q", ipAddr)
	}

	return ipAddr, nil
}

func GetPrivateIpAddr() (string, error) {
	ifaceName, err := getDefaultInterface()
	if err != nil {
		return "", err
	}

	iface, err := net.InterfaceByName(ifaceName)
	if err != nil {
		return "", err
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", err
	}

	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}

		if ip == nil || ip.IsLoopback() {
			continue
		}

		ipv4 := ip.To4()
		if ipv4 != nil && ipv4.IsPrivate() {
			return ipv4.String(), nil
		}
	}

	return "", fmt.Errorf("no active network interface found with a private IP address")
}

func DialWithTimeout(ctx context.Context, addr string) (net.Conn, error) {
	timeout := time.Second
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(timeoutCtx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
