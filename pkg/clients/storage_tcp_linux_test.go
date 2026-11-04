//go:build linux

package clients

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"sync"
	"testing"

	"github.com/beam-cloud/beta9/pkg/common"
	"golang.org/x/sys/unix"
)

func TestStorageTCPDefaultRetainsSharedPool(t *testing.T) {
	for _, mode := range []string{"", "invalid", "cubic"} {
		client, err := storageTCPHTTPClient(mode)
		if err != nil || client != common.AWSHTTPClient() {
			t.Fatalf("default pool changed for %q: %v", mode, err)
		}
	}
}

func TestStorageTCPUsesBBRAndReusesConnection(t *testing.T) {
	client, err := storageTCPHTTPClient("bbr")
	if err != nil {
		if os.Getenv("BETA9_REQUIRE_STORAGE_BBR") == "1" {
			t.Fatal(err)
		}
		t.Skipf("BBR unavailable: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "ok") }))
	defer server.Close()
	var mu sync.Mutex
	var algorithms []string
	var reused []bool
	for range 2 {
		ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{GotConn: func(info httptrace.GotConnInfo) {
			conn := info.Conn.(*net.TCPConn)
			raw, err := conn.SyscallConn()
			if err != nil {
				t.Error(err)
				return
			}
			var algorithm string
			err = raw.Control(func(fd uintptr) {
				algorithm, err = unix.GetsockoptString(int(fd), unix.IPPROTO_TCP, unix.TCP_CONGESTION)
			})
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			algorithms = append(algorithms, algorithm)
			reused = append(reused, info.Reused)
			mu.Unlock()
		}})
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || string(data) != "ok" {
			t.Fatalf("HTTP response changed: %q, %v", data, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(algorithms) != 2 || algorithms[0] != "bbr" || algorithms[1] != "bbr" || !reused[1] {
		t.Fatalf("socket policy/pool not preserved: %v, %v", algorithms, reused)
	}
}
