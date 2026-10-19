package microvm

import (
	"bytes"
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
	go func() {
		conn, err := target.Accept()
		if err == nil {
			defer conn.Close()
			conn.Write([]byte("ipv6"))
		}
	}()
	client, done := portTestStream(t)
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
	target.Close()
	client, done := portTestStream(t)
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

func portTestStream(t *testing.T) (portConn, <-chan error) {
	t.Helper()
	// Model the vsock stream without occupying a loopback TCP port. A TCP
	// listener here can collide with the IPv6 target and accept its IPv4 probe.
	dir, err := os.MkdirTemp("", "b9-port-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	listener, err := net.Listen("unix", filepath.Join(dir, "port.sock"))
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
		done <- ServePort(conn.(*net.UnixConn))
	}()
	conn, err := net.Dial("unix", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	return conn.(*net.UnixConn), done
}
