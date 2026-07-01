package abstractions

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestConnIdleDeadlineAppliesToNonTCPConn(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	deadline := NewConnIdleDeadline(10*time.Millisecond, client)
	defer deadline.Clear()

	_, err := client.Read(make([]byte, 1))
	if err == nil {
		t.Fatal("expected idle read deadline to fire")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("read error = %v, want timeout", err)
	}
}

func TestCopyWithProxyBufferActivityRefreshesIdleDeadline(t *testing.T) {
	src, srcWriter := net.Pipe()
	dstReader, dst := net.Pipe()
	defer src.Close()
	defer srcWriter.Close()
	defer dstReader.Close()
	defer dst.Close()

	idle := NewConnIdleDeadline(40*time.Millisecond, src)
	defer idle.Clear()

	copyDone := make(chan error, 1)
	go func() {
		_, err := CopyWithProxyBufferActivity(dst, src, idle.Refresh)
		_ = dst.Close()
		copyDone <- err
	}()

	readDone := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(dstReader)
		readDone <- data
	}()

	for _, b := range []byte("abc") {
		if _, err := srcWriter.Write([]byte{b}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	_ = srcWriter.Close()

	select {
	case err := <-copyDone:
		if err != nil {
			t.Fatalf("copy error = %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("copy did not finish")
	}

	select {
	case got := <-readDone:
		if !bytes.Equal(got, []byte("abc")) {
			t.Fatalf("copied bytes = %q, want abc", string(got))
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("reader did not finish")
	}
}

func TestCopyWithProxyBufferActivityReturnsIdleTimeout(t *testing.T) {
	src, srcWriter := net.Pipe()
	defer src.Close()
	defer srcWriter.Close()

	idle := NewConnIdleDeadline(10*time.Millisecond, src)
	defer idle.Clear()

	_, err := CopyWithProxyBufferActivity(io.Discard, src, idle.Refresh)
	if err == nil {
		t.Fatal("expected idle timeout")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("copy error = %v, want timeout", err)
	}
}
