package compute

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	routePrewarmInterval = 30 * time.Second
	routePrewarmTimeout  = 3 * time.Second
)

type routePrewarmer struct {
	mu          sync.Mutex
	lastAttempt map[string]time.Time
}

func (p *routePrewarmer) shouldAttempt(proxyTarget string, now time.Time) bool {
	proxyTarget = strings.TrimSpace(proxyTarget)
	if proxyTarget == "" {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lastAttempt == nil {
		p.lastAttempt = map[string]time.Time{}
	}
	if last := p.lastAttempt[proxyTarget]; !last.IsZero() && now.Sub(last) < routePrewarmInterval {
		return false
	}
	p.lastAttempt[proxyTarget] = now
	return true
}

func (s *Service) prewarmRoute(route types.BackendRoute, agentState *model.AgentTokenState) {
	if s.tailscale == nil || route.Transport != types.BackendRouteTransportTSNet || strings.TrimSpace(route.ProxyTarget) == "" {
		return
	}
	if !s.routePrewarm.shouldAttempt(route.ProxyTarget, time.Now()) {
		return
	}

	go s.prewarmRouteOnce(route, agentState)
}

func (s *Service) prewarmRouteOnce(route types.BackendRoute, agentState *model.AgentTokenState) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), routePrewarmTimeout)
	defer cancel()

	conn, err := network.NewBackendDialer(
		s.tailscale,
		s.appConfig.Tailscale,
		s.containerRepo,
		routePrewarmTimeout,
	).Dial(ctx, types.BackendRouteAddress(route.RouteID))
	dialLatency := time.Since(start)
	if conn != nil {
		_ = conn.Close()
	}

	attrs := map[string]string{
		"proxy_target": route.ProxyTarget,
		"dial_ms":      strconv.FormatInt(dialLatency.Milliseconds(), 10),
	}
	for key, value := range s.routePeerAttrs(route.ProxyTarget) {
		attrs[key] = value
	}

	status := "ready"
	message := ""
	if err != nil {
		status = "error"
		message = err.Error()
		attrs["reason"] = message
	}

	s.emitComputeEvent(types.EventComputeTransport, types.EventComputeSchema{
		WorkspaceID: agentState.WorkspaceID,
		PoolName:    agentState.PoolName,
		MachineID:   agentState.MachineID,
		WorkerID:    route.WorkerID,
		ContainerID: route.ContainerID,
		RouteID:     route.RouteID,
		Action:      types.EventComputeActionTransportPrewarm,
		Status:      status,
		Transport:   route.Transport,
		Message:     message,
		Attrs:       attrs,
	})
}

func (s *Service) routePeerAttrs(proxyTarget string) map[string]string {
	if s.tailscale == nil || s.tailscale.GetServer() == nil {
		return nil
	}
	host, _, err := net.SplitHostPort(proxyTarget)
	if err != nil {
		host = proxyTarget
	}
	host = strings.TrimSuffix(strings.TrimSpace(host), ".")
	if host == "" {
		return nil
	}

	client, err := s.tailscale.GetServer().LocalClient()
	if err != nil {
		return map[string]string{"peer_status_error": err.Error()}
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	status, err := client.Status(ctx)
	if err != nil {
		return map[string]string{"peer_status_error": err.Error()}
	}

	for _, peer := range status.Peer {
		if peer == nil || !peerMatchesHost(peer.HostName, peer.DNSName, host) {
			continue
		}
		attrs := map[string]string{
			"peer_online": strconv.FormatBool(peer.Online),
			"peer_active": strconv.FormatBool(peer.Active),
			"peer_direct": strconv.FormatBool(peer.CurAddr != ""),
		}
		if peer.Relay != "" {
			attrs["peer_relay"] = peer.Relay
		}
		if !peer.LastHandshake.IsZero() {
			age := time.Since(peer.LastHandshake)
			if age < 0 {
				age = 0
			}
			attrs["peer_last_handshake_age_ms"] = fmt.Sprintf("%d", age.Milliseconds())
		}
		return attrs
	}
	return map[string]string{"peer_status": "not_found"}
}

func peerMatchesHost(hostName, dnsName, target string) bool {
	target = strings.TrimSuffix(target, ".")
	hostName = strings.TrimSuffix(hostName, ".")
	dnsName = strings.TrimSuffix(dnsName, ".")
	return target == hostName || target == dnsName || strings.HasPrefix(dnsName, target+".")
}
