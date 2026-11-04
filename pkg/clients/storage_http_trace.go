package clients

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/beam-cloud/beta9/pkg/common"
)

// Keep instrumentation per HTTP request: a multipart upload and its retries
// can have several concurrent requests. Never record URLs, headers, or bodies.
type storageTracingHTTPClient struct{ base aws.HTTPClient }

var storageBaseHTTPClient aws.HTTPClient = common.AWSHTTPClient()

func storageHTTPClient(ctx context.Context) aws.HTTPClient {
	var base aws.HTTPClient = storageBaseHTTPClient
	if storageUploadBudget != nil {
		base = storageUploadHTTPClient{base: base, budget: storageUploadBudget}
	}
	if common.StartupTraceFromContext(ctx) == nil {
		return base
	}
	return storageTracingHTTPClient{base: base}
}

func (c storageTracingHTTPClient) Do(request *http.Request) (*http.Response, error) {
	ctx := request.Context()
	if common.StartupTraceFromContext(ctx) == nil {
		return c.base.Do(request)
	}
	started := time.Now()
	defer func() { common.RecordStartupSpan(ctx, "storage.http.total", started, time.Now()) }()
	var mu sync.Mutex
	var connection, dns, tlsStart, headers, waitContinue time.Time
	var localPort string
	record := func(name string, start time.Time) {
		if !start.IsZero() {
			common.RecordStartupSpan(ctx, "storage.http."+name, start, time.Now())
		}
	}
	hooks := &httptrace.ClientTrace{
		GetConn: func(string) { mu.Lock(); connection = time.Now(); mu.Unlock() },
		GotConn: func(info httptrace.GotConnInfo) {
			mu.Lock()
			defer mu.Unlock()
			name := "new_connection"
			if info.Reused {
				name = "reused_connection"
			}
			if info.Conn != nil {
				_, localPort, _ = net.SplitHostPort(info.Conn.LocalAddr().String())
			}
			record(name, connection)
		},
		DNSStart:          func(httptrace.DNSStartInfo) { mu.Lock(); dns = time.Now(); mu.Unlock() },
		DNSDone:           func(httptrace.DNSDoneInfo) { mu.Lock(); defer mu.Unlock(); record("dns", dns) },
		TLSHandshakeStart: func() { mu.Lock(); tlsStart = time.Now(); mu.Unlock() },
		TLSHandshakeDone:  func(tls.ConnectionState, error) { mu.Lock(); defer mu.Unlock(); record("tls", tlsStart) },
		WroteHeaders:      func() { mu.Lock(); headers = time.Now(); mu.Unlock() },
		Wait100Continue:   func() { mu.Lock(); waitContinue = time.Now(); mu.Unlock() },
		Got100Continue:    func() { mu.Lock(); defer mu.Unlock(); record("continue_response", waitContinue) },
		WroteRequest: func(httptrace.WroteRequestInfo) {
			mu.Lock()
			defer mu.Unlock()
			record("headers_to_request_written", headers)
		},
		GotFirstResponseByte: func() { record("first_response_byte", started) },
	}
	request = request.Clone(httptrace.WithClientTrace(ctx, hooks))
	if request.Body != nil {
		request.Body = &storageTracingBody{ReadCloser: request.Body, firstRead: func() {
			mu.Lock()
			defer mu.Unlock()
			record("headers_to_first_body_read", headers)
			record("continue_to_first_body_read", waitContinue)
		}}
	}
	response, err := c.base.Do(request)
	// Distinguish object-store throttling from connection failures and retry
	// backoff without recording an object key, endpoint, or error message.
	result := "transport_error"
	switch {
	case errors.Is(err, context.Canceled):
		result = "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		result = "timeout"
	case response != nil && response.StatusCode >= 100 && response.StatusCode <= 599:
		result = "status_" + strconv.Itoa(response.StatusCode)
	}
	mu.Lock()
	port := localPort
	mu.Unlock()
	var attributes map[string]string
	if port != "" {
		attributes = map[string]string{"tcp_local_port": port}
	}
	common.RecordStartupSpanWithAttributes(ctx, "storage.http."+result, started, time.Now(), attributes)
	return response, err
}

type storageTracingBody struct {
	io.ReadCloser
	once      sync.Once
	firstRead func()
}

func (b *storageTracingBody) Read(data []byte) (int, error) {
	b.once.Do(b.firstRead)
	return b.ReadCloser.Read(data)
}
