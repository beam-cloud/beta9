package worker

import (
	"errors"
	"fmt"
	"net"
	"os"
)

func GetRandomFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port, nil
}

// GetPodIP gets the IP from the POD_IP env var.
// Returns an error if it fails to retrieve an IP.
func GetPodIP() (string, error) {
	return getIPFromEnv("POD_IP")
}

// getIPFromInterface gets the IP address from a given interface.
// Returns an error if the interface doesn't exist or no IP is found.
func getIPFromInterface(ifname string) (string, error) {
	iface, err := net.InterfaceByName(ifname)
	if err != nil {
		return "", err
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", err
	}

	if len(addrs) < 1 {
		return "", fmt.Errorf("no ip addresses found on <%s> interface", ifname)
	}

	ip, _, err := net.ParseCIDR(addrs[0].String())
	if err != nil {
		return "", err
	}

	return ip.String(), nil
}

// getIPFromEnv gets the IP address from an environment variable.
func getIPFromEnv(varName string) (string, error) {
	addr := os.Getenv(varName)
	if addr == "" {
		return "", fmt.Errorf("no ip found in environment variable")
	}

	ip := net.ParseIP(addr)
	if ip == nil {
		return "", errors.New("failed to parse ip address")
	}

	return ip.String(), nil
}
