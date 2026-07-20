package cache

import (
	"bufio"
	"context"
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

	"golang.org/x/sync/semaphore"
)

const (
	rawReadMagic                            = "B9CR\x01"
	rawReadVersion                     byte = 1
	rawReadStatusOK                    byte = 0
	rawReadStatusMiss                  byte = 1
	rawReadStatusError                 byte = 2
	rawReadStatusBusy                  byte = 3
	rawReadHeaderSize                       = 19
	rawReadRespHeaderSize                   = 9
	defaultRawReadChunkBytes                = 1024 * 1024
	rawReadSocketBufferBytes                = 16 * 1024 * 1024
	cacheMuxInitialReadTimeout              = 5 * time.Second
	rawReadFallbackBufferBytes              = 4 * 1024 * 1024
	defaultRawReadMaxRequestBytes           = 64 * 1024 * 1024
	defaultRawReadMaxInflightBytes          = 8 * defaultRawReadMaxRequestBytes
	defaultRawReadMaxConcurrent             = 64
	defaultRawReadAdmissionWait             = 60 * time.Second
	defaultRawReadWriteProgressTimeout      = 30 * time.Second
)

var errRawReadAdmissionBusy = ErrRawReadBusy

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
		connCount := atomic.AddInt64(&cachePathStats.serverRawConns, 1)
		if shouldTraceCachePath(connCount, 0, false) {
			Logger.Debugf("cache raw server connection accepted: seq=%d remote=%s local=%s", connCount, conn.RemoteAddr(), conn.LocalAddr())
		}
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

// rawReadAdmission bounds server-side raw transport work independently of the
// gRPC message limit. The byte budget deliberately admits eight maximum-sized
// requests so the coordinated GeeseFS pipeline can retain its full fanout. The
// count gate rejects overload immediately, and byte-budget waits are time-bound.
type rawReadAdmission struct {
	maxRequestBytes int64
	inflightBytes   *semaphore.Weighted
	concurrent      chan struct{}
	waitTimeout     time.Duration
}

func newRawReadAdmission(config ServerReadTransportConfig) *rawReadAdmission {
	maxRequestBytes := config.MaxRequestSizeBytes
	if maxRequestBytes <= 0 {
		maxRequestBytes = defaultRawReadMaxRequestBytes
	}
	maxInflightBytes := config.MaxInflightBytes
	if maxInflightBytes <= 0 {
		maxInflightBytes = defaultRawReadMaxInflightBytes
	}
	if maxRequestBytes > maxInflightBytes {
		maxRequestBytes = maxInflightBytes
	}
	maxConcurrent := config.MaxConcurrentRequests
	if maxConcurrent <= 0 {
		maxConcurrent = defaultRawReadMaxConcurrent
	}
	return &rawReadAdmission{
		maxRequestBytes: maxRequestBytes,
		inflightBytes:   semaphore.NewWeighted(maxInflightBytes),
		concurrent:      make(chan struct{}, maxConcurrent),
		waitTimeout:     defaultRawReadAdmissionWait,
	}
}

type rawReadWaitContextFactory func(context.Context) (context.Context, func())

func (a *rawReadAdmission) acquire(ctx context.Context, length int64) (func(), error) {
	return a.acquireWithWaitContext(ctx, length, nil)
}

func (a *rawReadAdmission) acquireWithWaitContext(ctx context.Context, length int64, waitContextFactory rawReadWaitContextFactory) (func(), error) {
	if length < 0 || length > a.maxRequestBytes {
		return nil, fmt.Errorf("raw read length %d exceeds server limit %d", length, a.maxRequestBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case a.concurrent <- struct{}{}:
	default:
		return nil, errRawReadAdmissionBusy
	}
	if length > 0 && !a.inflightBytes.TryAcquire(length) {
		waitCtx := ctx
		stopWaitContext := func() {}
		if waitContextFactory != nil {
			waitCtx, stopWaitContext = waitContextFactory(ctx)
		}
		cancelTimeout := func() {}
		if a.waitTimeout > 0 {
			waitCtx, cancelTimeout = context.WithTimeout(waitCtx, a.waitTimeout)
		}
		err := a.inflightBytes.Acquire(waitCtx, length)
		cancelTimeout()
		stopWaitContext()
		if err != nil {
			<-a.concurrent
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if errors.Is(err, context.DeadlineExceeded) {
				return nil, errRawReadAdmissionBusy
			}
			return nil, err
		}
	}
	return func() {
		if length > 0 {
			a.inflightBytes.Release(length)
		}
		<-a.concurrent
	}, nil
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

type rawReadProgressWriter struct {
	conn    net.Conn
	timeout time.Duration
}

func (w rawReadProgressWriter) Write(p []byte) (int, error) {
	if w.timeout > 0 {
		if err := w.conn.SetWriteDeadline(time.Now().Add(w.timeout)); err != nil {
			return 0, err
		}
	}
	return w.conn.Write(p)
}

func (cs *Server) rawReadProgressTimeout() time.Duration {
	if cs != nil && cs.rawReadWriteTimeout > 0 {
		return cs.rawReadWriteTimeout
	}
	return defaultRawReadWriteProgressTimeout
}

func (cs *Server) trackRawReadConnection(conn net.Conn) bool {
	if cs == nil || conn == nil {
		return false
	}
	cs.rawReadMu.Lock()
	defer cs.rawReadMu.Unlock()
	if cs.rawReadClosing {
		return false
	}
	if cs.rawReadConns == nil {
		cs.rawReadConns = make(map[net.Conn]struct{})
	}
	cs.rawReadConns[conn] = struct{}{}
	cs.rawReadHandlers.Add(1)
	return true
}

func (cs *Server) untrackRawReadConnection(conn net.Conn) {
	cs.rawReadMu.Lock()
	delete(cs.rawReadConns, conn)
	cs.rawReadMu.Unlock()
	cs.rawReadHandlers.Done()
}

func (cs *Server) closeRawReadConnections() {
	if cs == nil {
		return
	}
	cs.rawReadMu.Lock()
	cs.rawReadClosing = true
	conns := make([]net.Conn, 0, len(cs.rawReadConns))
	for conn := range cs.rawReadConns {
		conns = append(conns, conn)
	}
	cs.rawReadMu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	cs.rawReadHandlers.Wait()
}

func (cs *Server) rawReadConnectionCount() int {
	if cs == nil {
		return 0
	}
	cs.rawReadMu.Lock()
	defer cs.rawReadMu.Unlock()
	return len(cs.rawReadConns)
}

func (cs *Server) handleRawReadConn(conn net.Conn) {
	if !cs.trackRawReadConnection(conn) {
		_ = conn.Close()
		return
	}
	defer cs.untrackRawReadConnection(conn)
	defer conn.Close()
	tuneRawReadConn(conn)
	reader := bufio.NewReader(conn)
	for {
		req, err := readRawReadRequest(reader)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				Logger.Debugf("raw cache read request failed: %v", err)
			}
			return
		}
		cs.serveRawReadWithReader(conn, reader, req)
	}
}

func (cs *Server) serveRawRead(conn net.Conn, req rawReadRequest) {
	cs.serveRawReadWithReader(conn, nil, req)
}

func (cs *Server) serveRawReadWithReader(conn net.Conn, reader *bufio.Reader, req rawReadRequest) {
	progressTimeout := cs.rawReadProgressTimeout()
	responseWriter := rawReadProgressWriter{conn: conn, timeout: progressTimeout}
	defer func() { _ = conn.SetWriteDeadline(time.Time{}) }()
	started := time.Now()
	reqCount := atomic.AddInt64(&cachePathStats.serverRawRequests, 1)
	if shouldTraceCachePath(reqCount, 0, false) {
		Logger.Debugf("cache raw server request received: seq=%d hash=%s offset=%d length=%d", reqCount, req.hash, req.offset, req.length)
	}
	status := "unknown"
	method := "none"
	var responseBytes int64
	var regionElapsed time.Duration
	var openElapsed time.Duration
	var sendElapsed time.Duration
	var admissionElapsed time.Duration
	var regionCount int
	defer func() {
		elapsed := time.Since(started)
		if shouldTraceCachePath(reqCount, elapsed, status != "ok") {
			Logger.Debugf(
				"cache raw server read trace: seq=%d status=%s method=%s hash=%s offset=%d length=%d bytes=%d regions=%d elapsed=%s admission=%s region=%s open=%s send=%s",
				reqCount,
				status,
				method,
				req.hash,
				req.offset,
				req.length,
				responseBytes,
				regionCount,
				elapsed.Truncate(time.Millisecond),
				admissionElapsed.Truncate(time.Microsecond),
				regionElapsed.Truncate(time.Microsecond),
				openElapsed.Truncate(time.Microsecond),
				sendElapsed.Truncate(time.Microsecond),
			)
		}
	}()
	admission := cs.rawReadLimits
	if admission == nil {
		admission = newRawReadAdmission(cs.serverConfig.ReadTransport)
	}
	if req.length < 0 || req.length > admission.maxRequestBytes {
		status = "invalid_length"
		atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
		_ = writeRawReadResponseHeader(responseWriter, rawReadStatusError, 0)
		return
	}
	admissionStarted := time.Now()
	admissionCtx := cs.rawReadCtx
	if admissionCtx == nil {
		admissionCtx = cs.ctx
	}
	if admissionCtx == nil {
		admissionCtx = context.Background()
	}
	var waitContextFactory rawReadWaitContextFactory
	if reader != nil {
		waitContextFactory = func(ctx context.Context) (context.Context, func()) {
			return rawReadDisconnectContext(ctx, conn, reader)
		}
	}
	releaseAdmission, err := admission.acquireWithWaitContext(admissionCtx, req.length, waitContextFactory)
	admissionElapsed = time.Since(admissionStarted)
	if err != nil {
		if errors.Is(err, errRawReadAdmissionBusy) {
			status = "busy"
			_ = writeRawReadResponseHeader(responseWriter, rawReadStatusBusy, 0)
			return
		}
		status = "admission_error"
		atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
		_ = writeRawReadResponseHeader(responseWriter, rawReadStatusError, 0)
		return
	}
	defer releaseAdmission()

	regionStarted := time.Now()
	regions, responseLength, ok, err := cs.rawReadPageRegions(req)
	regionElapsed = time.Since(regionStarted)
	regionCount = len(regions)
	atomic.AddInt64(&cachePathStats.serverRawRegionNanos, regionElapsed.Nanoseconds())
	if err == nil && ok {
		if err := writeRawReadResponseHeader(responseWriter, rawReadStatusOK, responseLength); err != nil {
			status = "write_header_error"
			return
		}
		responseBytes = responseLength
		atomic.AddInt64(&cachePathStats.serverRawBytes, responseLength)
		usedSendfile := false
		usedCopy := false
		useSendfile := cs.serverConfig.ReadTransport.Sendfile
		if threshold := cs.serverConfig.SmallRangeCopyThresholdBytes; threshold > 0 && req.length <= threshold {
			useSendfile = false
		}
		for _, region := range regions {
			openStarted := time.Now()
			file, err := os.Open(region.path)
			if err != nil {
				status = "open_error"
				Logger.Warnf("raw cache read open failed: hash=%s offset=%d length=%d path=%s err=%v", req.hash, req.offset, req.length, region.path, err)
				atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
				_ = conn.Close()
				return
			}
			copyOffset := region.pageOffset
			copyLength := int64(region.length)
			if useSendfile {
				_ = fadviseSequential(file.Fd())
				_ = fadviseWillneed(file.Fd(), copyOffset, copyLength)
			}
			openDuration := time.Since(openStarted)
			openElapsed += openDuration
			atomic.AddInt64(&cachePathStats.serverRawOpenNanos, openDuration.Nanoseconds())
			if useSendfile {
				sendStarted := time.Now()
				sent, err := sendFileToConn(conn, file, region.pageOffset, int64(region.length), progressTimeout)
				sendDuration := time.Since(sendStarted)
				sendElapsed += sendDuration
				atomic.AddInt64(&cachePathStats.serverRawSendNanos, sendDuration.Nanoseconds())
				if sent > 0 {
					usedSendfile = true
					method = "sendfile"
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
				status = "seek_error"
				Logger.Warnf("raw cache read seek failed: hash=%s offset=%d length=%d path=%s page_offset=%d remaining=%d err=%v", req.hash, req.offset, req.length, region.path, copyOffset, copyLength, err)
				_ = file.Close()
				atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
				_ = conn.Close()
				return
			}
			copyStarted := time.Now()
			if _, err := io.CopyN(responseWriter, file, copyLength); err != nil {
				copyDuration := time.Since(copyStarted)
				sendElapsed += copyDuration
				atomic.AddInt64(&cachePathStats.serverRawSendNanos, copyDuration.Nanoseconds())
				if isRawReadClientAbort(err) {
					status = "client_abort"
					Logger.Debugf("raw cache read client aborted: hash=%s offset=%d length=%d path=%s page_offset=%d remaining=%d err=%v", req.hash, req.offset, req.length, region.path, copyOffset, copyLength, err)
					_ = file.Close()
					_ = conn.Close()
					return
				}
				status = "copy_error"
				Logger.Warnf("raw cache read copy failed: hash=%s offset=%d length=%d path=%s page_offset=%d remaining=%d err=%v", req.hash, req.offset, req.length, region.path, copyOffset, copyLength, err)
				_ = file.Close()
				atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
				_ = conn.Close()
				return
			}
			copyDuration := time.Since(copyStarted)
			sendElapsed += copyDuration
			atomic.AddInt64(&cachePathStats.serverRawSendNanos, copyDuration.Nanoseconds())
			_ = file.Close()
			usedCopy = true
			if method == "none" {
				method = "copy"
			} else if method != "copy" {
				method = "sendfile+copy"
			}
		}
		if usedSendfile {
			cacheReadRawSendfileTotal.Inc()
			atomic.AddInt64(&cachePathStats.serverRawSendfileHits, 1)
		}
		if usedCopy {
			atomic.AddInt64(&cachePathStats.serverRawCopyHits, 1)
		}
		status = "ok"
		return
	}

	if req.length == 0 {
		if err := writeRawReadResponseHeader(responseWriter, rawReadStatusOK, 0); err != nil {
			status = "write_header_error"
			atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
			return
		}
		atomic.AddInt64(&cachePathStats.serverRawReadAtHits, 1)
		method = "readat_stream"
		status = "ok"
		return
	}

	bufferLength := min(req.length, int64(rawReadFallbackBufferBytes))
	buf := make([]byte, int(bufferLength))
	remaining := req.length
	readOffset := req.offset
	headerWritten := false
	for remaining > 0 {
		chunkLength := min(remaining, int64(len(buf)))
		n64, readErr := cs.cas.ReadAt(req.hash, readOffset, buf[:chunkLength])
		if readErr != nil || n64 != chunkLength {
			if !headerWritten {
				status = "miss"
				atomic.AddInt64(&cachePathStats.serverRawMisses, 1)
				_ = writeRawReadResponseHeader(responseWriter, rawReadStatusMiss, 0)
				return
			}
			status = "readat_stream_error"
			atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
			_ = conn.Close()
			return
		}
		if !headerWritten {
			if err := writeRawReadResponseHeader(responseWriter, rawReadStatusOK, req.length); err != nil {
				status = "write_header_error"
				atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
				return
			}
			headerWritten = true
		}
		sendStarted := time.Now()
		writeErr := writeRawReadBytes(responseWriter, buf[:n64])
		writeDuration := time.Since(sendStarted)
		sendElapsed += writeDuration
		atomic.AddInt64(&cachePathStats.serverRawSendNanos, writeDuration.Nanoseconds())
		if writeErr != nil {
			if isRawReadClientAbort(writeErr) {
				status = "client_abort"
			} else {
				status = "write_body_error"
				atomic.AddInt64(&cachePathStats.serverRawErrors, 1)
			}
			return
		}
		responseBytes += n64
		readOffset += n64
		remaining -= n64
	}
	atomic.AddInt64(&cachePathStats.serverRawBytes, responseBytes)
	atomic.AddInt64(&cachePathStats.serverRawReadAtHits, 1)
	method = "readat_stream"
	status = "ok"
}

func rawReadDisconnectContext(parent context.Context, conn net.Conn, reader *bufio.Reader) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			_, err := reader.Peek(1)
			if err == nil {
				// A pipelined request byte remains buffered for the next protocol
				// read. Stop probing rather than consuming or spinning on it.
				return
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				select {
				case <-stop:
					return
				case <-ctx.Done():
					return
				default:
					continue
				}
			}
			cancel()
			return
		}
	}()

	return ctx, func() {
		close(stop)
		_ = conn.SetReadDeadline(time.Now())
		<-done
		_ = conn.SetReadDeadline(time.Time{})
		cancel()
	}
}

func writeRawReadBytes(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
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

func (cs *Server) rawReadPageRegions(req rawReadRequest) ([]rawReadPageRegion, int64, bool, error) {
	pageSize := cs.serverConfig.PageSizeBytes
	if pageSize <= 0 || req.length <= 0 {
		return nil, 0, false, nil
	}
	regions := make([]rawReadPageRegion, 0, 1)
	remaining := req.length
	currentOffset := req.offset
	var responseLength int64
	for remaining > 0 {
		pageRemaining := pageSize - currentOffset%pageSize
		readLength := min(remaining, pageRemaining)
		path, pageOffset, n, ok, err := cs.cas.PageRegion(req.hash, currentOffset, readLength)
		if err != nil || !ok || n <= 0 {
			return nil, 0, false, err
		}
		regions = append(regions, rawReadPageRegion{path: path, pageOffset: pageOffset, length: n})
		responseLength += int64(n)
		if int64(n) != readLength {
			// A short page-region read is the CAS representation for the final
			// partial page. Exact-read clients reject the short response; page
			// promotion clients use it to install the EOF tail locally.
			return regions, responseLength, true, nil
		}
		currentOffset += int64(n)
		remaining -= int64(n)
	}
	return regions, responseLength, true, nil
}

type rawReadConnPool struct {
	addr      string
	maxActive int
	maxIdle   int
	mu        sync.Mutex
	idle      []net.Conn
	tokens    chan struct{}
	closed    bool
	closedCh  chan struct{}
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
		closedCh:  make(chan struct{}),
	}
}

func (p *rawReadConnPool) get(ctx context.Context) (net.Conn, error) {
	if err := p.acquire(ctx); err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		p.release()
		return nil, ErrUnableToReachHost
	}
	last := len(p.idle) - 1
	if last >= 0 {
		conn := p.idle[last]
		p.idle = p.idle[:last]
		p.mu.Unlock()
		return conn, nil
	}
	p.mu.Unlock()

	dialCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	dialer := &net.Dialer{}
	conn, err := dialer.DialContext(dialCtx, "tcp", p.addr)
	if err != nil {
		p.release()
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		_ = conn.Close()
		p.release()
		return nil, err
	}
	tuneRawReadConn(conn)
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	err = writeRawReadBytes(conn, []byte(rawReadMagic))
	_ = conn.SetWriteDeadline(time.Time{})
	if err != nil {
		_ = conn.Close()
		p.release()
		return nil, err
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed || ctx.Err() != nil {
		_ = conn.Close()
		p.release()
		return nil, ErrUnableToReachHost
	}
	return conn, nil
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

func (p *rawReadConnPool) acquire(ctx context.Context) error {
	select {
	case p.tokens <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ErrUnableToReachHost
	case <-p.closedCh:
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
	if p.closed {
		_ = conn.Close()
		p.release()
		return
	}
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
	if p.closed {
		return
	}
	p.closed = true
	close(p.closedCh)
	for _, conn := range p.idle {
		_ = conn.Close()
	}
	p.idle = nil
}
