package compute

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
)

const (
	routePrewarmTimeout     = 3 * time.Second
	routePrewarmConcurrency = 4
)

type routePrewarmer struct {
	mu            sync.Mutex
	activeTargets map[string]struct{}
}

func (p *routePrewarmer) tryStart(proxyTarget string) bool {
	proxyTarget = strings.TrimSpace(proxyTarget)
	if proxyTarget == "" {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.activeTargets == nil {
		p.activeTargets = map[string]struct{}{}
	}
	if _, active := p.activeTargets[proxyTarget]; active {
		return false
	}
	if len(p.activeTargets) >= routePrewarmConcurrency {
		return false
	}
	p.activeTargets[proxyTarget] = struct{}{}
	return true
}

func (p *routePrewarmer) finish(proxyTarget string) {
	proxyTarget = strings.TrimSpace(proxyTarget)
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.activeTargets, proxyTarget)
}

func routeEligibleForPrewarm(route types.BackendRoute) bool {
	return route.State == types.BackendRouteStateReady &&
		route.Transport == types.BackendRouteTransportTSNet &&
		strings.TrimSpace(route.ProxyTarget) != ""
}

func (s *Service) prewarmRoute(route types.BackendRoute, agentState *model.AgentTokenState) {
	if s.tailscale == nil || !routeEligibleForPrewarm(route) {
		return
	}
	// All routes on an agent share one tailnet proxy target. Deduplicate
	// concurrent ready updates so each admitted dial warms that shared path.
	if !s.routePrewarm.tryStart(route.ProxyTarget) {
		return
	}

	go func() {
		defer s.routePrewarm.finish(route.ProxyTarget)
		s.prewarmRouteOnce(route, agentState)
	}()
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
		network.WithBackgroundDialSlots(),
	).Dial(ctx, types.BackendRouteAddress(route.RouteID))
	dialLatency := time.Since(start)
	if conn != nil {
		_ = conn.Close()
	}

	attrs := map[string]string{
		"proxy_target": route.ProxyTarget,
		"dial_ms":      strconv.FormatInt(dialLatency.Milliseconds(), 10),
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
