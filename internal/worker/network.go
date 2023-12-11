package worker

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
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

func GetPodFQDN() (string, error) {
	podIP := os.Getenv("POD_IP")
	podNamespace := os.Getenv("POD_NAMESPACE")
	clusterDomain := os.Getenv("CLUSTER_DOMAIN")

	if podIP == "" || podNamespace == "" || clusterDomain == "" {
		return "", fmt.Errorf("environment variables POD_IP, POD_NAMESPACE, or CLUSTER_DOMAIN are not set")
	}

	dnsName := fmt.Sprintf(
		"%s.%s.pod.%s",
		strings.ReplaceAll(podIP, ".", "-"),
		podNamespace,
		clusterDomain,
	)

	return dnsName, nil
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
		return "", fmt.Errorf("no IP addresses found on <%s> interface", ifname)
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
		return "", fmt.Errorf("no IP found in environment variable")
	}

	ip := net.ParseIP(addr)
	if ip == nil {
		return "", errors.New("failed to parse ip address")
	}

	return ip.String(), nil
}
