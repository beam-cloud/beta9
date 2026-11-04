package clients

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestStorageUploadBudgetIsSharedAcrossConcurrentClients(t *testing.T) {
	payload := bytes.Repeat([]byte("durable"), 4096)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(r.Body)
		if err != nil || !bytes.Equal(payload, data) {
			t.Errorf("upload changed: bytes=%d err=%v", len(data), err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	budget := rate.NewLimiter(256*1024, 4096)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := storageUploadHTTPClient{base: server.Client(), budget: budget}
			request, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL, bytes.NewReader(payload))
			if err != nil {
				t.Error(err)
				return
			}
			response, err := client.Do(request)
			if err != nil {
				t.Error(err)
				return
			}
			response.Body.Close()
		}()
	}
	wg.Wait()
	minimum := time.Duration(float64(4*len(payload)-budget.Burst()) / float64(budget.Limit()) * float64(time.Second))
	require.GreaterOrEqual(t, time.Since(start), minimum-10*time.Millisecond)
}

func TestStorageUploadBudgetCancellationAndReplay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	budget := rate.NewLimiter(1, 1)
	require.True(t, budget.Allow())
	body := &storageUploadBody{ReadCloser: io.NopCloser(bytes.NewReader([]byte("payload"))), ctx: ctx, budget: budget}
	cancel()
	n, err := body.Read(make([]byte, 64))
	require.Zero(t, n)
	require.ErrorIs(t, err, context.Canceled)

	request, err := http.NewRequest(http.MethodPut, "http://example.invalid", bytes.NewReader([]byte("payload")))
	require.NoError(t, err)
	originalBody := request.Body
	client := storageUploadHTTPClient{base: storageUploadReplayClient{t}, budget: rate.NewLimiter(rate.Inf, 2)}
	_, err = client.Do(request)
	require.NoError(t, err)
	require.Equal(t, originalBody, request.Body, "wrapping must not mutate the caller's request")
}

type storageUploadReplayClient struct{ t *testing.T }

func (c storageUploadReplayClient) Do(r *http.Request) (*http.Response, error) {
	for _, source := range []func() (io.ReadCloser, error){func() (io.ReadCloser, error) { return r.Body, nil }, r.GetBody} {
		body, err := source()
		require.NoError(c.t, err)
		_, ok := body.(*storageUploadBody)
		require.True(c.t, ok, "replayed requests must retain pacing")
		data, err := io.ReadAll(body)
		require.NoError(c.t, err)
		require.Equal(c.t, "payload", string(data))
		require.NoError(c.t, body.Close())
	}
	return &http.Response{StatusCode: 204, Body: http.NoBody}, nil
}

func TestStorageUploadRateConfiguration(t *testing.T) {
	for _, value := range []string{"", "0", "-1", "NaN", "+Inf", "100001", "invalid"} {
		require.Nil(t, storageUploadRate(value), value)
	}
	require.Equal(t, rate.Limit(1062500000), storageUploadRate("8500").Limit())
}

func TestStorageUploadCloseInterruptsBudgetWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	budget := rate.NewLimiter(1, 1)
	require.True(t, budget.Allow())
	read := make(chan struct{})
	body := &storageUploadBody{ReadCloser: &storageUploadSignalBody{read: read}, ctx: ctx, cancel: cancel, budget: budget}
	done := make(chan error, 1)
	go func() { _, err := body.Read(make([]byte, 1)); done <- err }()
	<-read
	require.NoError(t, body.Close())
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("closing a request body left its upload waiting for budget")
	}
}

type storageUploadSignalBody struct{ read chan struct{} }

func (b *storageUploadSignalBody) Read(data []byte) (int, error) {
	data[0] = 'a'
	close(b.read)
	return 1, nil
}
func (*storageUploadSignalBody) Close() error { return nil }
