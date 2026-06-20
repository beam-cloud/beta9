package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	magic    = "B9CR\x01"
	version  = byte(1)
	statusOK = byte(0)
)

type task struct {
	offset int64
	length int
}

type result struct {
	offset int64
	body   []byte
	readNS int64
	err    error
}

func parseAddr(addr string) (string, string, error) {
	if strings.HasPrefix(addr, "[") {
		i := strings.LastIndex(addr, "]:")
		if i < 0 {
			return "", "", fmt.Errorf("invalid address %q", addr)
		}
		return addr[1:i], addr[i+2:], nil
	}
	i := strings.LastIndex(addr, ":")
	if i < 0 {
		return "", "", fmt.Errorf("invalid address %q", addr)
	}
	return addr[:i], addr[i+1:], nil
}

func dial(addr string, timeout time.Duration) (net.Conn, error) {
	host, port, err := parseAddr(addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), timeout)
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
		_ = tcp.SetReadBuffer(16 * 1024 * 1024)
		_ = tcp.SetWriteBuffer(16 * 1024 * 1024)
	}
	if _, err := conn.Write([]byte(magic)); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func writeRequest(w io.Writer, hash string, offset int64, length int) error {
	if len(hash) == 0 || len(hash) > 512 {
		return fmt.Errorf("invalid hash length %d", len(hash))
	}
	var hdr [19]byte
	hdr[0] = version
	binary.BigEndian.PutUint16(hdr[1:3], uint16(len(hash)))
	binary.BigEndian.PutUint64(hdr[3:11], uint64(offset))
	binary.BigEndian.PutUint64(hdr[11:19], uint64(length))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write([]byte(hash))
	return err
}

func readOne(conn net.Conn, hash string, t task) result {
	started := time.Now()
	if err := writeRequest(conn, hash, t.offset, t.length); err != nil {
		return result{offset: t.offset, err: err}
	}
	var rh [9]byte
	if _, err := io.ReadFull(conn, rh[:]); err != nil {
		return result{offset: t.offset, err: err}
	}
	if rh[0] != statusOK {
		return result{offset: t.offset, err: fmt.Errorf("raw status=%d", rh[0])}
	}
	n := int(binary.BigEndian.Uint64(rh[1:9]))
	if n != t.length {
		return result{offset: t.offset, err: fmt.Errorf("length mismatch got=%d want=%d", n, t.length)}
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(conn, body); err != nil {
		return result{offset: t.offset, err: err}
	}
	return result{offset: t.offset, body: body, readNS: time.Since(started).Nanoseconds()}
}

func main() {
	addr := flag.String("addr", "", "cache host address")
	hash := flag.String("hash", "", "content hash")
	size := flag.Int64("size", 0, "bytes to read")
	expected := flag.String("expected-sha256", "", "expected sha256")
	chunkBytes := flag.Int("chunk-bytes", 1024*1024, "chunk size")
	concurrency := flag.Int("concurrency", 1, "concurrency")
	connectTimeout := flag.Duration("connect-timeout", 10*time.Second, "connect timeout")
	flag.Parse()
	if *addr == "" || *hash == "" || *size <= 0 {
		panic("addr, hash, and size are required")
	}
	if *chunkBytes <= 0 {
		*chunkBytes = 1024 * 1024
	}
	if *concurrency <= 0 {
		*concurrency = 1
	}

	var tasks []task
	for off := int64(0); off < *size; {
		n := *chunkBytes
		if remaining := int(*size - off); remaining < n {
			n = remaining
		}
		tasks = append(tasks, task{offset: off, length: n})
		off += int64(n)
	}

	taskCh := make(chan task)
	resultCh := make(chan result, max(1, *concurrency*2))
	var readWaitNS int64
	var wg sync.WaitGroup
	started := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := dial(*addr, *connectTimeout)
			if err != nil {
				resultCh <- result{err: err}
				return
			}
			defer conn.Close()
			for t := range taskCh {
				waitStarted := time.Now()
				r := readOne(conn, *hash, t)
				atomic.AddInt64(&readWaitNS, time.Since(waitStarted).Nanoseconds())
				resultCh <- r
				if r.err != nil {
					return
				}
			}
		}()
	}
	go func() {
		for _, t := range tasks {
			taskCh <- t
		}
		close(taskCh)
		wg.Wait()
		close(resultCh)
	}()

	h := sha256.New()
	pending := make(map[int64][]byte, max(1, *concurrency*2))
	nextOffset := int64(0)
	var readWallNS int64
	for r := range resultCh {
		if r.err != nil {
			out := map[string]any{"ok": false, "error": r.err.Error(), "addr": *addr}
			_ = json.NewEncoder(io.Discard).Encode(out)
			b, _ := json.Marshal(out)
			fmt.Println(string(b))
			return
		}
		pending[r.offset] = r.body
		readWallNS += r.readNS
		for {
			body, ok := pending[nextOffset]
			if !ok {
				break
			}
			h.Write(body)
			delete(pending, nextOffset)
			nextOffset += int64(len(body))
		}
	}
	if nextOffset != *size {
		out := map[string]any{
			"ok":        false,
			"error":     fmt.Sprintf("incomplete ordered read got=%d want=%d pending=%d", nextOffset, *size, len(pending)),
			"addr":      *addr,
			"bytes":     *size,
			"readBytes": nextOffset,
		}
		b, _ := json.Marshal(out)
		fmt.Println(string(b))
		return
	}
	digest := hex.EncodeToString(h.Sum(nil))
	durationMs := float64(time.Since(started).Nanoseconds()) / 1_000_000
	socketReadMs := float64(readWallNS) / 1_000_000
	mb := float64(*size) / 1048576
	out := map[string]any{
		"ok":             digest == *expected,
		"addr":           *addr,
		"hash":           *hash,
		"bytes":          *size,
		"chunks":         len(tasks),
		"chunkBytes":     *chunkBytes,
		"concurrency":    *concurrency,
		"durationMs":     durationMs,
		"socketReadMs":   socketReadMs,
		"socketWaitMs":   float64(atomic.LoadInt64(&readWaitNS)) / 1_000_000,
		"mbps":           mb / (durationMs / 1000),
		"socketReadMBps": mb / (socketReadMs / 1000),
		"sha256":         digest,
		"expectedSha256": *expected,
	}
	_ = json.NewEncoder(io.Discard).Encode(out)
	b, _ := json.Marshal(out)
	fmt.Println(string(b))
}
