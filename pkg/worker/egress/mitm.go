package egress

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
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

const bindingTTL = 15 * time.Second

func (f *Firewall) bindings(ctx context.Context, c *container) ([]types.SecretBinding, error) {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.mu.Lock()
	bindings, generation := c.policy.Bindings, c.generation
	fresh, closed := f.deps.RefreshBindings == nil || time.Since(c.refreshed) < bindingTTL, c.closed
	c.mu.Unlock()
	if closed {
		return nil, net.ErrClosed
	}
	if fresh {
		return bindings, nil
	}
	ctx, cancel := context.WithTimeout(ctx, peekTimeout)
	defer cancel()
	bindings, err := f.deps.RefreshBindings(ctx, c.id, types.CloneSecretBindings(bindings))
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, net.ErrClosed
	}
	if generation == c.generation {
		c.policy.Bindings = bindings
		if c.policy.BindingsOnly {
			c.policy.AllowList = nil
			for _, binding := range bindings {
				c.policy.AllowList = append(c.policy.AllowList, binding.Hosts...)
			}
		}
		c.refreshed = time.Now()
	}
	return c.policy.Bindings, nil
}

func (f *Firewall) intercept(conn net.Conn, c *container, host string, port int) {
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
	tlsConn := tls.Server(conn, &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{*cert}, NextProtos: []string{"h2", "http/1.1"}})
	ctx, cancel := context.WithTimeout(f.ctx, peekTimeout)
	err = tlsConn.HandshakeContext(ctx)
	cancel()
	if err != nil {
		return
	}
	conn = tlsConn
	handler := f.handler(c, host, port)
	if tlsConn.ConnectionState().NegotiatedProtocol == "h2" {
		server := &http2.Server{MaxConcurrentStreams: 32, IdleTimeout: time.Minute}
		server.ServeConn(conn, &http2.ServeConnOpts{Context: f.ctx, Handler: handler, BaseConfig: &http.Server{MaxHeaderBytes: 1 << 20}})
		return
	}
	listener := &singleConnListener{conn: conn, done: make(chan struct{})}
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler.ServeHTTP(w, r)
		if r.Header.Get("Upgrade") != "" {
			listener.Close()
		}
	}), ReadHeaderTimeout: peekTimeout, IdleTimeout: time.Minute, MaxHeaderBytes: 1 << 20, ConnState: func(_ net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			listener.Close()
		}
	}}
	_ = server.Serve(listener)
}

func (f *Firewall) handler(c *container, host string, port int) http.Handler {
	address := net.JoinHostPort(host, strconv.Itoa(port))
	var roots *x509.CertPool
	if len(f.deps.RootCertificates) > 0 {
		roots = x509.NewCertPool()
		roots.AppendCertsFromPEM(f.deps.RootCertificates)
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12},
		DialContext:     f.deps.DialContext, ForceAttemptHTTP2: true, DisableCompression: true,
		TLSHandshakeTimeout: peekTimeout, ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout: time.Minute, MaxIdleConnsPerHost: 8,
	}

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// HTTP/2 authority and HTTP Host must agree with the authenticated SNI.
		if cleanHost(req.Host) != host || req.Method == http.MethodConnect {
			http.Error(w, "egress host mismatch", http.StatusForbidden)
			return
		}
		bindings, err := f.bindings(req.Context(), c)
		if err != nil {
			http.Error(w, "secret binding unavailable", http.StatusBadGateway)
			return
		}
		c.mu.Lock()
		allowed := !c.policy.BindingsOnly || common.HostPatternCovered(host, c.policy.AllowList)
		c.mu.Unlock()
		if !allowed {
			http.Error(w, "egress host denied", http.StatusForbidden)
			return
		}
		proxy := &httputil.ReverseProxy{
			Transport: transport, FlushInterval: -1,
			Rewrite: func(p *httputil.ProxyRequest) {
				p.Out.URL.Scheme, p.Out.URL.Host, p.Out.Host = "https", address, address
				p.Out.Header.Del("Accept-Encoding")
				// Extensions would make WebSocket frames opaque to the scrubber.
				p.Out.Header.Del("Sec-WebSocket-Extensions")
				substituteHeaders(p.Out.Header, bindings, host)
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
					resp.Body = newWebSocketScrubber(upstream, bindings)
					return nil
				}
				resp.Header.Del("Content-Length")
				resp.ContentLength = -1
				resp.Body = &scrubBody{ReadCloser: resp.Body, reader: newScrubber(resp.Body, bindings), trailers: resp.Trailer, bindings: bindings}
				return nil
			},
			ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
				http.Error(w, "egress upstream unavailable", http.StatusBadGateway)
			},
		}
		proxy.ServeHTTP(w, req)
	})
}

func substituteHeaders(headers http.Header, bindings []types.SecretBinding, host string) {
	for _, values := range headers {
		for i, value := range values {
			for _, binding := range bindings {
				if common.HostPatternCovered(host, binding.Hosts) {
					value = strings.ReplaceAll(value, binding.Placeholder, binding.Value)
				}
			}
			values[i] = value
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
	reader   io.Reader
	trailers http.Header
	bindings []types.SecretBinding
}

func (s *scrubBody) Read(p []byte) (int, error) {
	n, err := s.reader.Read(p)
	if err != nil {
		scrubHeaders(s.trailers, s.bindings)
	}
	return n, err
}

// Retain possible secret prefixes across reads without delaying ordinary SSE.
type scrubber struct {
	source          io.Reader
	bindings        []types.SecretBinding
	pending, output []byte
	err             error
}

func newScrubber(source io.Reader, bindings []types.SecretBinding) *scrubber {
	return &scrubber{source: source, bindings: bindings}
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
			buffer := make([]byte, 32<<10)
			n, err := s.source.Read(buffer)
			s.pending = append(s.pending, buffer[:n]...)
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
