package agent

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	routeProxyPrefaceTimeout   = 10 * time.Second
	routeProxyLocalDialTimeout = 2 * time.Second
	routeProxyReadyDialTimeout = 250 * time.Millisecond

	// routeProxyMaxConsecutiveFailures is how many local dials must fail in a
	// row before the route is dropped and reported degraded.
	routeProxyMaxConsecutiveFailures = 3
)

func newRouteProxy(client pb.GatewayServiceClient, agentToken string, listener net.Listener, proxyTarget string, workers *workerRuntimeManager, stdout, stderr io.Writer) *routeProxy {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	return &routeProxy{
		agentToken:      agentToken,
		client:          client,
		listener:        listener,
		proxyTarget:     proxyTarget,
		workers:         workers,
		stdout:          stdout,
		stderr:          stderr,
		routes:          map[string]string{},
		readinessChecks: map[string]string{},
		failureCounts:   map[string]int{},
	}
}

type routeProxy struct {
	client      pb.GatewayServiceClient
	agentToken  string
	listener    net.Listener
	proxyTarget string
	workers     *workerRuntimeManager
	stdout      io.Writer
	stderr      io.Writer
	mu          sync.Mutex
	routes      map[string]string

	// routeID -> local target currently being probed for readiness.
	readinessChecks map[string]string

	// routeID -> consecutive local dial failures.
	failureCounts map[string]int
}

func (p *routeProxy) run(ctx context.Context) error {
	acceptErr := make(chan error, 1)
	go func() {
		acceptErr <- p.accept(ctx)
	}()

	streamErr := make(chan error, 1)
	go func() {
		streamErr <- p.watchRoutes(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-acceptErr:
			return err
		case err := <-streamErr:
			return err
		}
	}
}

func (p *routeProxy) watchRoutes(ctx context.Context) error {
	backoff := time.Second
	for {
		if err := p.watchRoutesOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			fmt.Fprintf(p.stderr, "agent stream disconnected: %v\n", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff = nextBackoff(backoff, 5*time.Second)
			continue
		}
		backoff = time.Second
	}
}

func (p *routeProxy) watchRoutesOnce(ctx context.Context) error {
	stream, err := p.client.StreamAgent(ctx, &pb.StreamAgentRequest{AgentToken: p.agentToken})
	if err != nil {
		return err
	}
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}
		if !msg.Ok {
			return fmt.Errorf("%s", msg.ErrMsg)
		}
		if err := p.reconcileRoutes(ctx, msg.Routes); err != nil {
			return err
		}
		if p.workers != nil {
			// A worker reconcile failure should not tear down the route stream.
			if err := p.workers.reconcile(ctx, msg.Slots); err != nil {
				fmt.Fprintf(p.stderr, "worker slot reconcile failed: %v\n", err)
			}
		}
	}
}

func (p *routeProxy) reconcileRoutes(ctx context.Context, routes []*pb.AgentRoute) error {
	seen := map[string]struct{}{}
	for _, route := range routes {
		if route.LocalTarget == "" {
			continue
		}
		seen[route.RouteId] = struct{}{}
		p.setRoute(route.RouteId, route.LocalTarget)
		if route.State == types.BackendRouteStateReady && route.ProxyTarget == p.proxyTarget {
			p.clearReadinessCheck(route.RouteId, route.LocalTarget)
			continue
		}
		p.ensureRouteReady(ctx, route.RouteId, route.LocalTarget)
	}
	p.deleteRoutesNotIn(seen)
	return nil
}

func (p *routeProxy) setRoute(routeID, localTarget string) {
	p.mu.Lock()
	if p.routes == nil {
		p.routes = map[string]string{}
	}
	if p.readinessChecks == nil {
		p.readinessChecks = map[string]string{}
	}
	if previous := p.routes[routeID]; previous != "" && previous != localTarget {
		delete(p.readinessChecks, routeID)
	}
	p.routes[routeID] = localTarget
	p.mu.Unlock()
}

func (p *routeProxy) deleteRoute(routeID string) {
	p.mu.Lock()
	delete(p.routes, routeID)
	delete(p.readinessChecks, routeID)
	delete(p.failureCounts, routeID)
	p.mu.Unlock()
}

func (p *routeProxy) deleteRoutesNotIn(seen map[string]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for routeID := range p.routes {
		if _, ok := seen[routeID]; !ok {
			delete(p.routes, routeID)
			delete(p.readinessChecks, routeID)
			delete(p.failureCounts, routeID)
		}
	}
}

func (p *routeProxy) localTarget(routeID string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	target, ok := p.routes[routeID]
	return target, ok
}

func (p *routeProxy) ensureRouteReady(ctx context.Context, routeID, localTarget string) {
	p.mu.Lock()
	if p.routes[routeID] != localTarget {
		p.mu.Unlock()
		return
	}
	if p.readinessChecks[routeID] == localTarget {
		p.mu.Unlock()
		return
	}
	p.readinessChecks[routeID] = localTarget
	p.mu.Unlock()

	go p.waitForRouteReady(ctx, routeID, localTarget)
}

func (p *routeProxy) waitForRouteReady(ctx context.Context, routeID, localTarget string) {
	defer p.clearReadinessCheck(routeID, localTarget)

	backoff := 100 * time.Millisecond
	for {
		if ctx.Err() != nil || !p.routeTargetMatches(routeID, localTarget) {
			return
		}

		dialLatency, err := checkLocalTargetReady(localTarget)
		if err != nil {
			backoff = p.waitBeforeNextReadinessCheck(ctx, backoff)
			continue
		}

		attrs := map[string]string{
			"local_target":  localTarget,
			"proxy_target":  p.proxyTarget,
			"local_dial_ms": fmt.Sprintf("%d", dialLatency.Milliseconds()),
		}
		if err := updateRouteStatus(ctx, p.client, p.agentToken, routeID, types.BackendRouteStateReady, p.proxyTarget, "", attrs); err != nil {
			if ctx.Err() != nil {
				return
			}
			fmt.Fprintf(p.stderr, "route %s ready status update failed: %v\n", routeID, err)
			backoff = p.waitBeforeNextReadinessCheck(ctx, backoff)
			continue
		}

		return
	}
}

func (p *routeProxy) waitBeforeNextReadinessCheck(ctx context.Context, backoff time.Duration) time.Duration {
	select {
	case <-ctx.Done():
	case <-time.After(backoff):
	}
	return nextBackoff(backoff, time.Second)
}

func (p *routeProxy) routeTargetMatches(routeID, localTarget string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.routes[routeID] == localTarget
}

func (p *routeProxy) clearReadinessCheck(routeID, localTarget string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.readinessChecks[routeID] == localTarget {
		delete(p.readinessChecks, routeID)
	}
}

func (p *routeProxy) accept(ctx context.Context) error {
	backoff := 100 * time.Millisecond
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !temporaryAcceptError(err) {
				return err
			}
			fmt.Fprintf(p.stderr, "agent route listener accept failed: %v\n", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff = nextBackoff(backoff, time.Second)
			continue
		}
		backoff = 100 * time.Millisecond
		go p.handleConn(conn)
	}
}

func temporaryAcceptError(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "too many open files") ||
		strings.Contains(msg, "resource temporarily unavailable")
}

func (p *routeProxy) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(routeProxyPrefaceTimeout))
	lineBytes, err := reader.ReadSlice('\n')
	_ = conn.SetReadDeadline(time.Time{})
	if err != nil {
		return
	}

	line := strings.TrimRight(string(lineBytes), "\r\n")
	if !strings.HasPrefix(line, network.BackendRoutePreface) {
		return
	}

	routeID := strings.TrimSpace(strings.TrimPrefix(line, network.BackendRoutePreface))
	localTarget, ok := p.localTarget(routeID)
	if !ok || localTarget == "" {
		return
	}
	if dialLatency, err := proxyConn(conn, reader, localTarget); err != nil {
		p.recordRouteFailure(routeID, localTarget, dialLatency, err)
		if !isLocalTargetUnavailable(err) {
			fmt.Fprintf(p.stderr, "route %s proxy failed: %v\n", routeID, err)
		}
	} else {
		p.resetRouteFailures(routeID)
	}
}

// recordRouteFailure drops the route only after several consecutive local
// dial failures.
func (p *routeProxy) recordRouteFailure(routeID, localTarget string, dialLatency time.Duration, cause error) {
	p.mu.Lock()
	if p.routes[routeID] != localTarget {
		p.mu.Unlock()
		return
	}
	if p.failureCounts == nil {
		p.failureCounts = map[string]int{}
	}
	p.failureCounts[routeID]++
	count := p.failureCounts[routeID]
	p.mu.Unlock()

	if count < routeProxyMaxConsecutiveFailures {
		return
	}
	p.markRouteDegraded(routeID, localTarget, dialLatency, cause)
}

func (p *routeProxy) resetRouteFailures(routeID string) {
	p.mu.Lock()
	delete(p.failureCounts, routeID)
	p.mu.Unlock()
}

func (p *routeProxy) markRouteDegraded(routeID, localTarget string, dialLatency time.Duration, cause error) {
	p.deleteRoute(routeID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	attrs := map[string]string{
		"local_target": localTarget,
		"proxy_target": p.proxyTarget,
		"reason":       cause.Error(),
	}
	if dialLatency > 0 {
		attrs["local_dial_ms"] = fmt.Sprintf("%d", dialLatency.Milliseconds())
	}
	if err := updateRouteStatus(ctx, p.client, p.agentToken, routeID, types.BackendRouteStateDegraded, p.proxyTarget, cause.Error(), attrs); err != nil && !isLocalTargetUnavailable(cause) {
		fmt.Fprintf(p.stderr, "route %s status update failed: %v\n", routeID, err)
	}
}

func proxyConn(conn net.Conn, source io.Reader, localTarget string) (time.Duration, error) {
	defer conn.Close()
	start := time.Now()
	local, err := dialLocalTarget(localTarget)
	dialLatency := time.Since(start)
	if err != nil {
		return dialLatency, err
	}
	defer local.Close()

	copyBuffered(conn, source, local)
	return dialLatency, nil
}

func copyBuffered(conn net.Conn, source io.Reader, local net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, _ = io.Copy(local, source)
		closeWrite(local)
	}()
	go func() {
		defer wg.Done()
		_, _ = io.Copy(conn, local)
		closeWrite(conn)
	}()
	wg.Wait()
}

func copyBoth(a, b net.Conn) {
	copyBuffered(a, a, b)
}

func checkLocalTargetReady(localTarget string) (time.Duration, error) {
	start := time.Now()
	conn, err := dialLocalTargetWithTimeout(localTarget, routeProxyReadyDialTimeout)
	dialLatency := time.Since(start)
	if err != nil {
		return dialLatency, err
	}
	return dialLatency, conn.Close()
}

func dialLocalTarget(localTarget string) (net.Conn, error) {
	return dialLocalTargetWithTimeout(localTarget, routeProxyLocalDialTimeout)
}

func dialLocalTargetWithTimeout(localTarget string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", localTarget, timeout)
	if err == nil {
		return conn, nil
	}

	_, port, splitErr := net.SplitHostPort(localTarget)
	if splitErr != nil {
		return nil, err
	}
	fallbackTarget := net.JoinHostPort("127.0.0.1", port)
	if fallbackTarget == localTarget {
		return nil, err
	}

	fallbackConn, fallbackErr := net.DialTimeout("tcp", fallbackTarget, timeout)
	if fallbackErr == nil {
		return fallbackConn, nil
	}
	return nil, fmt.Errorf("dial %s failed: %w; loopback fallback %s failed: %v", localTarget, err, fallbackTarget, fallbackErr)
}

func isLocalTargetUnavailable(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "i/o timeout") ||
		strings.Contains(msg, "deadline exceeded")
}

type closeWriter interface {
	CloseWrite() error
}

func closeWrite(conn net.Conn) {
	if cw, ok := conn.(closeWriter); ok {
		_ = cw.CloseWrite()
	}
}

func updateRouteStatus(ctx context.Context, client pb.GatewayServiceClient, agentToken, routeID, state, proxyTarget, errMsg string, attrs map[string]string) error {
	res, err := client.UpdateAgentRouteStatus(ctx, &pb.UpdateAgentRouteStatusRequest{
		AgentToken:  agentToken,
		RouteId:     routeID,
		State:       state,
		ProxyTarget: proxyTarget,
		Error:       errMsg,
		Attrs:       attrs,
	})
	if err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("%s", res.ErrMsg)
	}
	return nil
}
