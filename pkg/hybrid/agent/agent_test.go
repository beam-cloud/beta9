package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
)

func TestRouteProxySingleListenerRoutesByPreface(t *testing.T) {
	backend := startEchoListener(t, "127.0.0.1:0")
	proxyListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxyListener.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	proxy := &routeProxy{
		listener: proxyListener,
		routes: map[string]string{
			"route-one": backend.Addr().String(),
		},
	}

	go func() {
		_ = proxy.accept(ctx)
	}()

	conn, err := net.DialTimeout("tcp", proxyListener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(conn, "%sroute-one\n", network.BackendRoutePreface); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "ping" {
		t.Fatalf("expected ping echo, got %q", string(buf))
	}
}

func TestDialLocalTargetFallsBackToLoopback(t *testing.T) {
	backend := startEchoListener(t, "127.0.0.1:0")
	_, port, err := net.SplitHostPort(backend.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	conn, err := dialLocalTargetWithTimeout(net.JoinHostPort("127.0.0.2", port), 50*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
}

func startEchoListener(t *testing.T, addr string) net.Listener {
	t.Helper()

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	t.Cleanup(func() {
		_ = listener.Close()
		wg.Wait()
	})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return listener
}
