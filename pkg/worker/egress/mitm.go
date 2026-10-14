package egress

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	"golang.org/x/net/http2"
)

func (f *Firewall) serveTLS(conn net.Conn, c *container, host string, port int) {
	c.mu.Lock()
	ca := c.ca
	c.mu.Unlock()
	if ca == nil {
		return
	}
	cert, err := ca.leaf(host)
	if err != nil {
		return
	}
	tlsConn := tls.Server(conn, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{*cert},
		NextProtos:   []string{"h2", "http/1.1"},
	})
	ctx, cancel := context.WithTimeout(f.ctx, peekTimeout)
	err = tlsConn.HandshakeContext(ctx)
	cancel()
	if err != nil {
		return
	}
	handler := f.newSecretProxy(c, host, port)
	defer handler.transport.CloseIdleConnections()
	if tlsConn.ConnectionState().NegotiatedProtocol == "h2" {
		server := &http2.Server{MaxConcurrentStreams: 32, IdleTimeout: time.Minute}
		server.ServeConn(tlsConn, &http2.ServeConnOpts{
			Context:    f.ctx,
			Handler:    handler,
			BaseConfig: &http.Server{MaxHeaderBytes: 1 << 20},
		})
		return
	}
	listener := &singleConnListener{conn: tlsConn, done: make(chan struct{})}
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler.ServeHTTP(w, r)
			if r.Header.Get("Upgrade") != "" {
				listener.Close()
			}
		}),
		ReadHeaderTimeout: peekTimeout,
		IdleTimeout:       time.Minute,
		MaxHeaderBytes:    1 << 20,
		ConnState: func(_ net.Conn, state http.ConnState) {
			if state == http.StateClosed {
				listener.Close()
			}
		},
	}
	_ = server.Serve(listener)
}

type secretProxy struct {
	firewall  *Firewall
	container *container
	host      string
	address   string
	transport *http.Transport
}

func (f *Firewall) newSecretProxy(c *container, host string, port int) *secretProxy {
	address := net.JoinHostPort(host, strconv.Itoa(port))
	var roots *x509.CertPool
	if len(f.deps.RootCertificates) > 0 {
		roots = x509.NewCertPool()
		roots.AppendCertsFromPEM(f.deps.RootCertificates)
	}
	transport := &http.Transport{
		TLSClientConfig:       &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		DialContext:           f.deps.DialContext,
		ForceAttemptHTTP2:     true,
		DisableCompression:    true,
		TLSHandshakeTimeout:   peekTimeout,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       time.Minute,
		MaxIdleConnsPerHost:   8,
	}

	return &secretProxy{
		firewall:  f,
		container: c,
		host:      host,
		address:   address,
		transport: transport,
	}
}

func (p *secretProxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// HTTP/2 authority and HTTP Host must agree with the authenticated SNI.
	if cleanHost(req.Host) != p.host || req.Method == http.MethodConnect {
		http.Error(w, "egress host mismatch", http.StatusForbidden)
		return
	}
	bindings, err := p.firewall.bindings(req.Context(), p.container)
	if err != nil {
		http.Error(w, "secret binding unavailable", http.StatusBadGateway)
		return
	}
	p.container.mu.Lock()
	allowed := !p.container.policy.BindingsOnly || common.HostPatternCovered(p.host, p.container.policy.AllowList)
	p.container.mu.Unlock()
	if !allowed {
		http.Error(w, "egress host denied", http.StatusForbidden)
		return
	}
	proxy := &httputil.ReverseProxy{
		Transport:     p.transport,
		FlushInterval: -1,
		Rewrite: func(request *httputil.ProxyRequest) {
			request.Out.URL.Scheme, request.Out.URL.Host, request.Out.Host = "https", p.address, p.address
			request.Out.Header.Del("Accept-Encoding")
			// Extensions would make WebSocket frames opaque to the scrubber.
			request.Out.Header.Del("Sec-WebSocket-Extensions")
			substituteHeaders(request.Out.Header, bindings, p.host)
		},
		ModifyResponse: func(resp *http.Response) error {
			scrubHeaders(resp.Header, bindings)
			if resp.Header.Get("Content-Encoding") != "" && resp.Header.Get("Content-Encoding") != "identity" {
				return fmt.Errorf("encoded upstream response cannot be scrubbed")
			}
			if resp.StatusCode == http.StatusSwitchingProtocols {
				upstream, ok := resp.Body.(io.ReadWriteCloser)
				if !ok {
					return fmt.Errorf("invalid upgrade stream")
				}
				resp.Body = &webSocketScrubber{ReadWriteCloser: upstream, bindings: bindings}
				return nil
			}
			resp.Header.Del("Content-Length")
			resp.ContentLength = -1
			resp.Body = &scrubBody{
				ReadCloser: resp.Body,
				reader:     &scrubber{source: resp.Body, bindings: bindings},
				trailers:   resp.Trailer,
			}
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			http.Error(w, "egress upstream unavailable", http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, req)
}

func substituteHeaders(headers http.Header, bindings []types.SecretBinding, host string) {
	for _, binding := range bindings {
		if !common.HostPatternCovered(host, binding.Hosts) {
			continue
		}
		for _, values := range headers {
			for i, value := range values {
				values[i] = strings.ReplaceAll(value, binding.Placeholder, binding.Value)
			}
		}
	}
}

func scrubHeaders(headers http.Header, bindings []types.SecretBinding) {
	for _, values := range headers {
		for i, value := range values {
			values[i] = scrubString(value, bindings)
		}
	}
}

func scrubString(value string, bindings []types.SecretBinding) string {
	for _, binding := range bindings {
		if binding.Value != "" {
			value = strings.ReplaceAll(value, binding.Value, binding.Placeholder)
		}
	}
	return value
}

type scrubBody struct {
	io.ReadCloser
	reader   *scrubber
	trailers http.Header
}

func (s *scrubBody) Read(p []byte) (int, error) {
	n, err := s.reader.Read(p)
	if err != nil {
		scrubHeaders(s.trailers, s.reader.bindings)
	}
	return n, err
}

// Retain possible secret prefixes across reads without delaying ordinary SSE.
type scrubber struct {
	source          io.Reader
	bindings        []types.SecretBinding
	pending, output []byte
	err             error
	buffer          [32 << 10]byte
}

func (s *scrubber) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(s.output) == 0 {
		if s.err != nil && len(s.pending) == 0 {
			return 0, s.err
		}
		if s.err == nil {
			n, err := s.source.Read(s.buffer[:])
			s.pending = append(s.pending, s.buffer[:n]...)
			s.err = err
			if n == 0 && err == nil {
				return 0, nil
			}
		}
		consumed := 0
		for consumed < len(s.pending) {
			rest := s.pending[consumed:]
			matched, partial := false, false
			for _, binding := range s.bindings {
				value := []byte(binding.Value)
				if len(value) == 0 {
					continue
				}
				if len(rest) < len(value) && bytes.HasPrefix(value, rest) && s.err == nil {
					partial = true
					break
				}
				if bytes.HasPrefix(rest, value) {
					s.output = append(s.output, binding.Placeholder...)
					consumed += len(value)
					matched = true
					break
				}
			}
			if partial {
				break
			}
			if !matched {
				s.output = append(s.output, rest[0])
				consumed++
			}
		}
		s.pending = append(s.pending[:0], s.pending[consumed:]...)
	}
	n := copy(p, s.output)
	s.output = s.output[n:]
	return n, nil
}

type singleConnListener struct {
	conn  net.Conn
	done  chan struct{}
	taken bool
	once  sync.Once
}

func (l *singleConnListener) Accept() (net.Conn, error) {
	if !l.taken {
		l.taken = true
		return l.conn, nil
	}
	<-l.done
	return nil, net.ErrClosed
}

func (l *singleConnListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *singleConnListener) Addr() net.Addr { return l.conn.LocalAddr() }

// Upgrade responses need frame lengths rewritten after replacing payloads.
type webSocketScrubber struct {
	io.ReadWriteCloser
	bindings        []types.SecretBinding
	output, message []byte
	opcode          byte
}

func (s *webSocketScrubber) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(s.output) == 0 {
		var header [2]byte
		if _, err := io.ReadFull(s.ReadWriteCloser, header[:]); err != nil {
			return 0, err
		}
		if header[0]&0x70 != 0 || header[1]&0x80 != 0 {
			return 0, fmt.Errorf("unsupported upstream WebSocket encoding")
		}
		length := uint64(header[1] & 127)
		switch length {
		case 126:
			var b [2]byte
			if _, err := io.ReadFull(s.ReadWriteCloser, b[:]); err != nil {
				return 0, err
			}
			length = uint64(binary.BigEndian.Uint16(b[:]))
		case 127:
			var b [8]byte
			if _, err := io.ReadFull(s.ReadWriteCloser, b[:]); err != nil {
				return 0, err
			}
			length = binary.BigEndian.Uint64(b[:])
		}
		if length > 16<<20 || uint64(len(s.message))+length > 16<<20 {
			return 0, fmt.Errorf("WebSocket message exceeds 16 MiB")
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(s.ReadWriteCloser, payload); err != nil {
			return 0, err
		}
		opcode, final := header[0]&15, header[0]&0x80 != 0
		if opcode >= 8 {
			payload = []byte(scrubString(string(payload), s.bindings))
			if !final || len(payload) > 125 {
				return 0, fmt.Errorf("invalid WebSocket control frame")
			}
			s.output = webSocketFrame(opcode, payload)
			continue
		}
		if opcode == 1 || opcode == 2 {
			if s.opcode != 0 {
				return 0, fmt.Errorf("invalid WebSocket fragmentation")
			}
			s.opcode = opcode
		} else if opcode != 0 || s.opcode == 0 {
			return 0, fmt.Errorf("invalid WebSocket opcode")
		}
		s.message = append(s.message, payload...)
		if final {
			s.output = webSocketFrame(s.opcode, []byte(scrubString(string(s.message), s.bindings)))
			s.message = nil
			s.opcode = 0
		}
	}
	n := copy(p, s.output)
	s.output = s.output[n:]
	return n, nil
}

func webSocketFrame(opcode byte, payload []byte) []byte {
	frame := []byte{0x80 | opcode}
	switch {
	case len(payload) < 126:
		frame = append(frame, byte(len(payload)))
	case len(payload) <= 65535:
		frame = append(frame, 126, byte(len(payload)>>8), byte(len(payload)))
	default:
		frame = append(frame, 127)
		frame = binary.BigEndian.AppendUint64(frame, uint64(len(payload)))
	}
	return append(frame, payload...)
}
