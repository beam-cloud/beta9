package microvm

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPortForwardsLoopbackWithIndependentHalfCloses(t *testing.T) {
	for _, serverFirst := range []bool{false, true} {
		name := "client finishes first"
		if serverFirst {
			name = "server finishes first"
		}
		t.Run(name, func(t *testing.T) {
			target, err := net.Listen("tcp4", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			payload := bytes.Repeat([]byte{0, 1, 127, 128, 255}, 20_000)
			banner := []byte("server-first\x00\xff\r\n")
			received := make(chan []byte, 1)
			go func() {
				conn, err := target.Accept()
				if err != nil {
					received <- nil
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(5 * time.Second))
				conn.Write(banner)
				if serverFirst {
					conn.(*net.TCPConn).CloseWrite()
				}
				data, _ := io.ReadAll(conn)
				if !serverFirst {
					conn.Write(data)
				}
				received <- data
			}()
			client, done := portTestStream(t)
			if err := ConnectPort(client, uint32(target.Addr().(*net.TCPAddr).Port)); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, len(banner))
			if _, err := io.ReadFull(client, got); err != nil || !bytes.Equal(banner, got) {
				t.Fatalf("banner: %q, %v", got, err)
			}
			if serverFirst {
				if _, err := client.Read(make([]byte, 1)); err != io.EOF {
					t.Fatalf("server output must end before client input: %v", err)
				}
			}
			if _, err := client.Write(payload); err != nil {
				t.Fatal(err)
			}
			if err := client.CloseWrite(); err != nil {
				t.Fatal(err)
			}
			if !serverFirst {
				got, err := io.ReadAll(client)
				if err != nil || !bytes.Equal(payload, got) {
					t.Fatalf("response: %d bytes, %v", len(got), err)
				}
			}
			if got := <-received; !bytes.Equal(payload, got) {
				t.Fatalf("request: got %d bytes, want %d", len(got), len(payload))
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPortForwardsIPv6Loopback(t *testing.T) {
	target, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("IPv6 loopback unavailable: %v", err)
	}
	defer target.Close()
	// Reserve the IPv4 port until the tunnel fixture is ready, so neither it
	// nor an unrelated listener can intercept the IPv6 fallback test.
	ipv4, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: target.Addr().(*net.TCPAddr).Port})
	if err != nil {
		t.Skipf("IPv4 port already occupied: %v", err)
	}
	defer ipv4.Close()
	go func() {
		conn, err := target.Accept()
		if err == nil {
			defer conn.Close()
			conn.Write([]byte("ipv6"))
		}
	}()
	client, done := portTestStream(t)
	if err := ipv4.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ConnectPort(client, uint32(target.Addr().(*net.TCPAddr).Port)); err != nil {
		t.Fatal(err)
	}
	client.CloseWrite()
	if got, err := io.ReadAll(client); err != nil || string(got) != "ipv6" {
		t.Fatalf("IPv6 response: %q, %v", got, err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPortRejectsFailedConnections(t *testing.T) {
	target, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := uint32(target.Addr().(*net.TCPAddr).Port)
	client, done := portTestStream(t)
	target.Close()
	if err := ConnectPort(client, port); err == nil {
		t.Fatal("a failed TCP connection must fail the handshake")
	}
	if err := <-done; err == nil {
		t.Fatal("guest must report the dial failure")
	}
	for _, port := range []uint32{0, 65536} {
		var stream bytes.Buffer
		if err := ConnectPort(&stream, port); err == nil || stream.Len() != 0 {
			t.Fatalf("invalid port %d reached the stream", port)
		}
	}
}

// A headless guest's process manager listens on a socket, which the host
// reaches the way it reaches a port, and can tell from one that is not there.
func TestSocketForwardsToUnixSocketAndReportsMissingOne(t *testing.T) {
	// Socket paths are short, and macOS's temporary directory is not.
	dir, err := os.MkdirTemp("/tmp", "beam-socket")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "manager.sock")

	client, done := socketTestStream(t, path)
	if err := ConnectProcess(client); !errors.Is(err, ErrProcessUnavailable) {
		t.Fatalf("no listener on the socket: %v", err)
	}
	if err := <-done; err == nil {
		t.Fatal("guest must report the dial failure")
	}

	target, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	payload := bytes.Repeat([]byte{0, 1, 127, 128, 255}, 20_000)
	received := make(chan []byte, 1)
	go func() {
		conn, err := target.Accept()
		if err != nil {
			received <- nil
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		data, _ := io.ReadAll(conn)
		conn.Write(data)
		received <- data
	}()
	client, done = socketTestStream(t, path)
	if err := ConnectProcess(client); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write(payload); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(client)
	if err != nil || !bytes.Equal(payload, got) {
		t.Fatalf("response: %d bytes, %v", len(got), err)
	}
	if got := <-received; !bytes.Equal(payload, got) {
		t.Fatalf("request: got %d bytes, want %d", len(got), len(payload))
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func socketTestStream(t *testing.T, path string) (*net.TCPConn, <-chan error) {
	t.Helper()
	return testStream(t, func(conn *net.TCPConn) error { return ServeSocket(conn, path) })
}

func portTestStream(t *testing.T) (*net.TCPConn, <-chan error) {
	t.Helper()
	return testStream(t, func(conn *net.TCPConn) error { return ServePort(conn) })
}

// testStream stands in for a vsock connection: serve runs the guest's end.
func testStream(t *testing.T, serve func(*net.TCPConn) error) (*net.TCPConn, <-chan error) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		done <- serve(conn.(*net.TCPConn))
	}()
	conn, err := net.Dial("tcp4", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	return conn.(*net.TCPConn), done
}
