package cache

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	rawReadMagic                    = "B9CR\x01"
	rawReadVersion             byte = 1
	rawReadStatusOK            byte = 0
	rawReadStatusMiss          byte = 1
	rawReadStatusError         byte = 2
	rawReadHeaderSize               = 19
	rawReadRespHeaderSize           = 9
	defaultRawReadChunkBytes        = 1024 * 1024
	rawReadSocketBufferBytes        = 16 * 1024 * 1024
	cacheMuxInitialReadTimeout      = 5 * time.Second
)

type cacheMuxListener struct {
	base       net.Listener
	rawHandler func(net.Conn)
	grpcConns  chan net.Conn
	done       chan struct{}
	closeOnce  sync.Once
}

func newCacheMuxListener(base net.Listener, rawHandler func(net.Conn)) *cacheMuxListener {
	l := &cacheMuxListener{
		base:       base,
		rawHandler: rawHandler,
		grpcConns:  make(chan net.Conn, 64),
		done:       make(chan struct{}),
	}
	go l.acceptLoop()
	return l
}

func (l *cacheMuxListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.grpcConns:
		if conn == nil {
			return nil, net.ErrClosed
		}
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *cacheMuxListener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		err = l.base.Close()
		close(l.done)
	})
	return err
}

func (l *cacheMuxListener) Addr() net.Addr {
	return l.base.Addr()
}

func (l *cacheMuxListener) acceptLoop() {
	for {
		conn, err := l.base.Accept()
		if err != nil {
			_ = l.Close()
			return
		}
		go l.dispatch(conn)
	}
}

func (l *cacheMuxListener) dispatch(conn net.Conn) {
	_ = conn.SetReadDeadline(time.Now().Add(cacheMuxInitialReadTimeout))
	prefix := make([]byte, len(rawReadMagic))
	n, err := io.ReadFull(conn, prefix)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			_ = conn.Close()
			return
		}
		if n == 0 {
			_ = conn.Close()
			return
		}
	}
	_ = conn.SetReadDeadline(time.Time{})

	if n == len(rawReadMagic) && string(prefix) == rawReadMagic {
		l.rawHandler(conn)
		return
	}

	wrapped := &prefixConn{Conn: conn, prefix: prefix[:n]}
	select {
	case l.grpcConns <- wrapped:
	case <-l.done:
		_ = wrapped.Close()
	}
}

type prefixConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixConn) Read(p []byte) (int, error) {
	if len(c.prefix) == 0 {
		return c.Conn.Read(p)
	}
	n := copy(p, c.prefix)
	c.prefix = c.prefix[n:]
	return n, nil
}

type rawReadRequest struct {
	hash   string
	offset int64
	length int64
}

type rawReadPageRegion struct {
	path       string
	pageOffset int64
	length     int
}

func readRawReadRequest(r io.Reader) (rawReadRequest, error) {
	var hdr [rawReadHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return rawReadRequest{}, err
	}
	if hdr[0] != rawReadVersion {
		return rawReadRequest{}, fmt.Errorf("unsupported raw read version: %d", hdr[0])
	}
	hashLen := int(binary.BigEndian.Uint16(hdr[1:3]))
	if hashLen <= 0 || hashLen > 512 {
		return rawReadRequest{}, fmt.Errorf("invalid raw read hash length: %d", hashLen)
	}
	offset := int64(binary.BigEndian.Uint64(hdr[3:11]))
	length := int64(binary.BigEndian.Uint64(hdr[11:19]))
	if offset < 0 || length < 0 {
		return rawReadRequest{}, errors.New("negative raw read range")
	}
	hashBytes := make([]byte, hashLen)
	if _, err := io.ReadFull(r, hashBytes); err != nil {
		return rawReadRequest{}, err
	}
	return rawReadRequest{hash: string(hashBytes), offset: offset, length: length}, nil
}

func writeRawReadRequest(w io.Writer, hash string, offset int64, length int64) error {
	if len(hash) == 0 || len(hash) > 512 {
		return fmt.Errorf("invalid raw read hash length: %d", len(hash))
	}
	if offset < 0 || length < 0 {
		return errors.New("negative raw read range")
	}
	var hdr [rawReadHeaderSize]byte
	hdr[0] = rawReadVersion
	binary.BigEndian.PutUint16(hdr[1:3], uint16(len(hash)))
	binary.BigEndian.PutUint64(hdr[3:11], uint64(offset))
	binary.BigEndian.PutUint64(hdr[11:19], uint64(length))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write([]byte(hash))
	return err
}

func writeRawReadResponseHeader(w io.Writer, status byte, length int64) error {
	var hdr [rawReadRespHeaderSize]byte
	hdr[0] = status
	binary.BigEndian.PutUint64(hdr[1:9], uint64(length))
	_, err := w.Write(hdr[:])
	return err
}

func readRawReadResponseHeader(r io.Reader) (byte, int64, error) {
	var hdr [rawReadRespHeaderSize]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return 0, 0, err
	}
	return hdr[0], int64(binary.BigEndian.Uint64(hdr[1:9])), nil
}

func (cs *Server) handleRawReadConn(conn net.Conn) {
	defer conn.Close()
	tuneRawReadConn(conn)
	for {
		req, err := readRawReadRequest(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				Logger.Debugf("raw cache read request failed: %v", err)
			}
			return
		}
		cs.serveRawRead(conn, req)
	}
}

func (cs *Server) serveRawRead(conn net.Conn, req rawReadRequest) {
	atomic.AddInt64(&cachePathStats.serverRawRequests, 1)
	maxLength := int64(defaultRawReadChunkBytes)
	if cs.globalConfig.GRPCMessageSizeBytes > 0 {
		maxLength = int64(cs.globalConfig.GRPCMessageSizeBytes)
	}
	if req.length < 0 || req.length > maxLength {
		atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
		_ = writeRawReadResponseHeader(conn, rawReadStatusError, 0)
		return
	}

	regions, ok, err := cs.rawReadPageRegions(req)
	if err == nil && ok {
		if err := writeRawReadResponseHeader(conn, rawReadStatusOK, req.length); err != nil {
			return
		}
		usedSendfile := false
		usedCopy := false
		for _, region := range regions {
			file, err := os.Open(region.path)
			if err != nil {
				Logger.Warnf("raw cache read open failed: hash=%s offset=%d length=%d path=%s err=%v", req.hash, req.offset, req.length, region.path, err)
				atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
				_ = conn.Close()
				return
			}
			copyOffset := region.pageOffset
			copyLength := int64(region.length)
			_ = fadviseSequential(file.Fd())
			_ = fadviseWillneed(file.Fd(), copyOffset, copyLength)
			if cs.serverConfig.ReadTransport.Sendfile {
				sent, err := sendFileToConn(conn, file, region.pageOffset, int64(region.length))
				if sent > 0 {
					usedSendfile = true
					copyOffset += sent
					copyLength -= sent
				}
				if err != nil {
					Logger.Debugf("raw cache read sendfile partial: hash=%s offset=%d length=%d path=%s page_offset=%d region_length=%d sent=%d remaining=%d err=%v", req.hash, req.offset, req.length, region.path, region.pageOffset, region.length, sent, copyLength, err)
				}
				if copyLength == 0 {
					_ = file.Close()
					continue
				}
			}
			if _, err := file.Seek(copyOffset, io.SeekStart); err != nil {
				Logger.Warnf("raw cache read seek failed: hash=%s offset=%d length=%d path=%s page_offset=%d remaining=%d err=%v", req.hash, req.offset, req.length, region.path, copyOffset, copyLength, err)
				_ = file.Close()
				atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
				_ = conn.Close()
				return
			}
			if _, err := io.CopyN(conn, file, copyLength); err != nil {
				if isRawReadClientAbort(err) {
					Logger.Debugf("raw cache read client aborted: hash=%s offset=%d length=%d path=%s page_offset=%d remaining=%d err=%v", req.hash, req.offset, req.length, region.path, copyOffset, copyLength, err)
					_ = file.Close()
					_ = conn.Close()
					return
				}
				Logger.Warnf("raw cache read copy failed: hash=%s offset=%d length=%d path=%s page_offset=%d remaining=%d err=%v", req.hash, req.offset, req.length, region.path, copyOffset, copyLength, err)
				_ = file.Close()
				atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
				_ = conn.Close()
				return
			}
			_ = file.Close()
			usedCopy = true
		}
		if usedSendfile {
			cacheReadRawSendfileTotal.Inc()
			atomic.AddInt64(&cachePathStats.serverRawSendfileHits, 1)
		}
		if usedCopy {
			atomic.AddInt64(&cachePathStats.serverRawCopyHits, 1)
		}
		return
	}

	buf := make([]byte, req.length)
	n64, err := cs.cas.ReadAt(req.hash, req.offset, buf)
	if err != nil || n64 != req.length {
		atomic.AddInt64(&cachePathStats.serverRawMisses, 1)
		_ = writeRawReadResponseHeader(conn, rawReadStatusMiss, 0)
		return
	}
	if err := writeRawReadResponseHeader(conn, rawReadStatusOK, n64); err != nil {
		atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
		return
	}
	if _, err := conn.Write(buf[:n64]); err != nil {
		atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
		return
	}
	atomic.AddInt64(&cachePathStats.serverRawReadAtHits, 1)
}

func isRawReadClientAbort(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "broken pipe") || strings.Contains(msg, "connection reset by peer")
}

func (cs *Server) rawReadPageRegions(req rawReadRequest) ([]rawReadPageRegion, bool, error) {
	pageSize := cs.serverConfig.PageSizeBytes
	if pageSize <= 0 || req.length <= 0 {
		return nil, false, nil
	}
	regions := make([]rawReadPageRegion, 0, 1)
	remaining := req.length
	currentOffset := req.offset
	for remaining > 0 {
		pageRemaining := pageSize - currentOffset%pageSize
		readLength := min(remaining, pageRemaining)
		path, pageOffset, n, ok, err := cs.cas.PageRegion(req.hash, currentOffset, readLength)
		if err != nil || !ok || n <= 0 {
			return nil, false, err
		}
		if int64(n) != readLength {
			return nil, false, ErrContentNotFound
		}
		regions = append(regions, rawReadPageRegion{path: path, pageOffset: pageOffset, length: n})
		currentOffset += int64(n)
		remaining -= int64(n)
	}
	return regions, true, nil
}

type rawReadConnPool struct {
	addr      string
	maxActive int
	maxIdle   int
	mu        sync.Mutex
	idle      []net.Conn
	tokens    chan struct{}
}

func newRawReadConnPool(addr string, maxActive int, maxIdle int) *rawReadConnPool {
	if maxActive <= 0 {
		maxActive = 64
	}
	if maxIdle <= 0 {
		maxIdle = 16
	}
	return &rawReadConnPool{
		addr:      addr,
		maxActive: maxActive,
		maxIdle:   maxIdle,
		tokens:    make(chan struct{}, maxActive),
	}
}

func (p *rawReadConnPool) get(ctxDone <-chan struct{}) (net.Conn, error) {
	if err := p.acquire(ctxDone); err != nil {
		return nil, err
	}

	p.mu.Lock()
	last := len(p.idle) - 1
	if last >= 0 {
		conn := p.idle[last]
		p.idle = p.idle[:last]
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	dialer := &net.Dialer{Timeout: time.Second}
	type dialResult struct {
		conn net.Conn
		err  error
	}
	ch := make(chan dialResult, 1)
	go func() {
		conn, err := dialer.Dial("tcp", p.addr)
		if err == nil {
			tuneRawReadConn(conn)
			_, err = conn.Write([]byte(rawReadMagic))
			if err != nil {
				_ = conn.Close()
				conn = nil
			}
		}
		ch <- dialResult{conn: conn, err: err}
	}()
	select {
	case res := <-ch:
		if res.err != nil {
			p.release()
		}
		return res.conn, res.err
	case <-ctxDone:
		p.release()
		return nil, ErrUnableToReachHost
	}
}

func tuneRawReadConn(conn net.Conn) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcpConn.SetNoDelay(true)
	_ = tcpConn.SetReadBuffer(rawReadSocketBufferBytes)
	_ = tcpConn.SetWriteBuffer(rawReadSocketBufferBytes)
}

func (p *rawReadConnPool) acquire(ctxDone <-chan struct{}) error {
	select {
	case p.tokens <- struct{}{}:
		return nil
	case <-ctxDone:
		return ErrUnableToReachHost
	}
}

func (p *rawReadConnPool) release() {
	select {
	case <-p.tokens:
	default:
	}
}

func (p *rawReadConnPool) put(conn net.Conn) {
	if conn == nil {
		p.release()
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.idle) >= p.maxIdle {
		_ = conn.Close()
		p.release()
		return
	}
	p.idle = append(p.idle, conn)
	p.release()
}

func (p *rawReadConnPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.idle {
		_ = conn.Close()
	}
	p.idle = nil
}
