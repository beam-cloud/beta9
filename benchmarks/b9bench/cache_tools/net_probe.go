package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	mode := flag.String("mode", "", "server or client")
	addr := flag.String("addr", "", "listen or dial address")
	totalBytes := flag.Int64("bytes", 0, "total bytes to transfer")
	concurrency := flag.Int("concurrency", 1, "connection count")
	chunkBytes := flag.Int("chunk-bytes", 4*1024*1024, "transfer chunk size")
	flag.Parse()

	if *mode == "server" {
		if err := runServer(*addr, *concurrency, *chunkBytes); err != nil {
			fail(err)
		}
		return
	}
	if *mode == "client" {
		if err := runClient(*addr, *totalBytes, *concurrency, *chunkBytes); err != nil {
			fail(err)
		}
		return
	}
	fail(fmt.Errorf("--mode must be server or client"))
}

func runServer(addr string, concurrency int, chunkBytes int) error {
	if addr == "" {
		addr = "[::]:39099"
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if chunkBytes <= 0 {
		chunkBytes = 4 * 1024 * 1024
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer ln.Close()
	fmt.Printf("ready %s\n", ln.Addr().String())
	os.Stdout.Sync()

	started := time.Now()
	var total int64
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			defer conn.Close()
			n, err := readRequestedBytes(conn)
			if err != nil {
				return
			}
			buf := make([]byte, chunkBytes)
			var sent int64
			for sent < n {
				toWrite := int64(len(buf))
				if remaining := n - sent; remaining < toWrite {
					toWrite = remaining
				}
				written, err := conn.Write(buf[:toWrite])
				if written > 0 {
					sent += int64(written)
					atomic.AddInt64(&total, int64(written))
				}
				if err != nil {
					return
				}
			}
		}(conn)
	}
	wg.Wait()
	durationMs := float64(time.Since(started).Nanoseconds()) / 1_000_000
	bytes := atomic.LoadInt64(&total)
	mb := float64(bytes) / 1048576
	out := map[string]any{
		"ok":          true,
		"mode":        "server",
		"bytes":       bytes,
		"durationMs":  durationMs,
		"mbps":        mb / (durationMs / 1000),
		"concurrency": concurrency,
		"chunkBytes":  chunkBytes,
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}

func runClient(addr string, totalBytes int64, concurrency int, chunkBytes int) error {
	if addr == "" {
		return fmt.Errorf("--addr is required")
	}
	if totalBytes <= 0 {
		return fmt.Errorf("--bytes must be > 0")
	}
	if concurrency <= 0 {
		concurrency = 1
	}
	if chunkBytes <= 0 {
		chunkBytes = 4 * 1024 * 1024
	}

	perConn := splitBytes(totalBytes, concurrency)
	started := time.Now()
	var received int64
	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)
	for _, n := range perConn {
		wg.Add(1)
		go func(n int64) {
			defer wg.Done()
			conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetNoDelay(true)
				_ = tcp.SetReadBuffer(16 * 1024 * 1024)
				_ = tcp.SetWriteBuffer(16 * 1024 * 1024)
			}
			if err := writeRequestedBytes(conn, n); err != nil {
				errCh <- err
				return
			}
			buf := make([]byte, chunkBytes)
			var got int64
			for got < n {
				toRead := int64(len(buf))
				if remaining := n - got; remaining < toRead {
					toRead = remaining
				}
				read, err := io.ReadFull(conn, buf[:toRead])
				if read > 0 {
					got += int64(read)
					atomic.AddInt64(&received, int64(read))
				}
				if err != nil {
					errCh <- err
					return
				}
			}
		}(n)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	durationMs := float64(time.Since(started).Nanoseconds()) / 1_000_000
	bytes := atomic.LoadInt64(&received)
	mb := float64(bytes) / 1048576
	out := map[string]any{
		"ok":          bytes == totalBytes,
		"mode":        "client",
		"bytes":       bytes,
		"durationMs":  durationMs,
		"mbps":        mb / (durationMs / 1000),
		"concurrency": concurrency,
		"chunkBytes":  chunkBytes,
	}
	return json.NewEncoder(os.Stdout).Encode(out)
}

func splitBytes(total int64, concurrency int) []int64 {
	out := make([]int64, concurrency)
	base := total / int64(concurrency)
	rem := total % int64(concurrency)
	for i := range out {
		out[i] = base
		if int64(i) < rem {
			out[i]++
		}
	}
	return out
}

func writeRequestedBytes(w io.Writer, n int64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(n))
	_, err := w.Write(buf[:])
	return err
}

func readRequestedBytes(r io.Reader) (int64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(r, buf[:]); err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(buf[:])), nil
}

func fail(err error) {
	out := map[string]any{"ok": false, "error": err.Error()}
	_ = json.NewEncoder(os.Stdout).Encode(out)
	os.Exit(1)
}
