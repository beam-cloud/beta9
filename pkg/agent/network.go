package agent

import (
	"context"
	"net"
	"net/http"
	"strings"
	"time"

	pb "github.com/beam-cloud/beta9/proto"
)

const (
	networkRefreshInterval  = time.Minute
	networkPublicIPInterval = 5 * time.Minute
	networkPublicIPLookup   = "https://api.ipify.org?format=text"
)

// networkReporter tells the gateway how this machine is reachable: its public
// IP and its tailnet node. Tailnet status is local and read every minute; the
// public IP lookup is cached and keeps its last good answer.
type networkReporter struct {
	telemetry   *agentTelemetry
	client      tsnetStatusClient
	httpClient  *http.Client
	publicIPURL string
	hostname    string
	sshEnabled  func() bool

	publicIP   string
	publicIPAt time.Time
}

func newNetworkReporter(telemetry *agentTelemetry, client tsnetStatusClient, hostname string, sshEnabled func() bool) *networkReporter {
	return &networkReporter{
		telemetry:   telemetry,
		client:      client,
		httpClient:  &http.Client{Timeout: 3 * time.Second},
		publicIPURL: networkPublicIPLookup,
		hostname:    hostname,
		sshEnabled:  sshEnabled,
	}
}

func (r *networkReporter) run(ctx context.Context) {
	ticker := time.NewTicker(networkRefreshInterval)
	defer ticker.Stop()
	for {
		r.telemetry.setNetwork(r.collect(ctx))
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *networkReporter) collect(ctx context.Context) *pb.AgentNetworkInfo {
	info := &pb.AgentNetworkInfo{TailnetHostname: r.hostname, TailnetSsh: r.sshEnabled()}
	statusCtx, cancel := context.WithTimeout(ctx, tsnetSnapshotTimeout)
	defer cancel()
	if status, err := r.client.StatusWithoutPeers(statusCtx); err == nil && status != nil {
		for _, ip := range status.TailscaleIPs {
			if info.TailnetIp == "" || ip.Is4() {
				info.TailnetIp = ip.String()
			}
		}
		// MagicDNS may suffix the requested hostname on a collision; the
		// first DNS label is the name that resolves.
		if status.Self != nil {
			if label, _, _ := strings.Cut(status.Self.DNSName, "."); label != "" {
				info.TailnetHostname = label
			}
		}
	}
	if time.Since(r.publicIPAt) >= networkPublicIPInterval {
		if ip := discoverPublicIP(ctx, r.httpClient, r.publicIPURL); ip != "" {
			r.publicIP, r.publicIPAt = ip, time.Now()
		}
	}
	info.PublicIp = r.publicIP
	return info
}

func discoverPublicIP(ctx context.Context, client *http.Client, url string) string {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ""
	}
	response, err := client.Do(request)
	if err != nil {
		return ""
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return ""
	}
	var buffer [64]byte
	n, _ := response.Body.Read(buffer[:])
	value := strings.TrimSpace(string(buffer[:n]))
	if net.ParseIP(value) == nil {
		return ""
	}
	return value
}
