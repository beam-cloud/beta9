package agent

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
	pb "github.com/beam-cloud/beta9/proto"
)

func newRouteProxy(client pb.GatewayServiceClient, agentToken string, listener net.Listener, proxyTarget string, workers *workerRuntimeManager, stderr io.Writer) *routeProxy {
	if stderr == nil {
		stderr = io.Discard
	}
	return &routeProxy{
		agentToken:  agentToken,
		client:      client,
		listener:    listener,
		proxyTarget: proxyTarget,
		workers:     workers,
		stderr:      stderr,
		routes:      map[string]string{},
	}
}

type routeProxy struct {
	client      pb.GatewayServiceClient
	agentToken  string
	listener    net.Listener
	proxyTarget string
	workers     *workerRuntimeManager
	stderr      io.Writer
	mu          sync.Mutex
	routes      map[string]string
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
			if backoff < 5*time.Second {
				backoff *= 2
			}
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
			if err := p.workers.reconcile(ctx, msg.Slots); err != nil {
				return err
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
		if route.State == "ready" && route.ProxyTarget == p.proxyTarget {
			continue
		}
		if err := checkLocalTargetReady(route.LocalTarget); err != nil {
			if route.State == "ready" {
				_ = updateRouteStatus(ctx, p.client, p.agentToken, route.RouteId, "degraded", p.proxyTarget, err.Error())
			}
			continue
		}
		if err := updateRouteStatus(ctx, p.client, p.agentToken, route.RouteId, "ready", p.proxyTarget, ""); err != nil {
			p.deleteRoute(route.RouteId)
			_ = updateRouteStatus(ctx, p.client, p.agentToken, route.RouteId, "degraded", p.proxyTarget, err.Error())
			return err
		}
	}
	p.deleteRoutesNotIn(seen)
	return nil
}

func (p *routeProxy) setRoute(routeID, localTarget string) {
	p.mu.Lock()
	p.routes[routeID] = localTarget
	p.mu.Unlock()
}

func (p *routeProxy) deleteRoute(routeID string) {
	p.mu.Lock()
	delete(p.routes, routeID)
	p.mu.Unlock()
}

func (p *routeProxy) deleteRoutesNotIn(seen map[string]struct{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for routeID := range p.routes {
		if _, ok := seen[routeID]; !ok {
			delete(p.routes, routeID)
		}
	}
}

func (p *routeProxy) localTarget(routeID string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	target, ok := p.routes[routeID]
	return target, ok
}

func (p *routeProxy) accept(ctx context.Context) error {
	for {
		conn, err := p.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return err
			}
		}
		go p.handleConn(conn)
	}
}

func (p *routeProxy) handleConn(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
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
	if err := proxyConn(conn, reader, localTarget); err != nil {
		p.markRouteDegraded(routeID, err)
		if !isLocalTargetUnavailable(err) {
			fmt.Fprintf(p.stderr, "route %s proxy failed: %v\n", routeID, err)
		}
	}
}

func (p *routeProxy) markRouteDegraded(routeID string, cause error) {
	p.deleteRoute(routeID)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := updateRouteStatus(ctx, p.client, p.agentToken, routeID, "degraded", p.proxyTarget, cause.Error()); err != nil && !isLocalTargetUnavailable(cause) {
		fmt.Fprintf(p.stderr, "route %s status update failed: %v\n", routeID, err)
	}
}

func proxyConn(conn net.Conn, source io.Reader, localTarget string) error {
	defer conn.Close()
	local, err := dialLocalTarget(localTarget)
	if err != nil {
		return err
	}
	defer local.Close()

	copyBuffered(conn, source, local)
	return nil
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

func checkLocalTargetReady(localTarget string) error {
	conn, err := dialLocalTargetWithTimeout(localTarget, 250*time.Millisecond)
	if err != nil {
		return err
	}
	return conn.Close()
}

func dialLocalTarget(localTarget string) (net.Conn, error) {
	return dialLocalTargetWithTimeout(localTarget, 30*time.Second)
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
		strings.Contains(msg, "connection reset by peer")
}

type closeWriter interface {
	CloseWrite() error
}

func closeWrite(conn net.Conn) {
	if cw, ok := conn.(closeWriter); ok {
		_ = cw.CloseWrite()
	}
}

func updateRouteStatus(ctx context.Context, client pb.GatewayServiceClient, agentToken, routeID, state, proxyTarget, errMsg string) error {
	res, err := client.UpdateAgentRouteStatus(ctx, &pb.UpdateAgentRouteStatusRequest{
		AgentToken:  agentToken,
		RouteId:     routeID,
		State:       state,
		ProxyTarget: proxyTarget,
		Error:       errMsg,
	})
	if err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("%s", res.ErrMsg)
	}
	return nil
}
