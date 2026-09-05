package apiv1

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/require"
)

type fakeEventStream struct {
	records []types.ContainerEventRecord
	err     error
	closed  chan struct{}
	index   int
}

func (s *fakeEventStream) Next() bool {
	if s.index < len(s.records) {
		s.index++
		return true
	}
	if s.err == nil {
		<-s.closed // a live stream blocks until closed
	}
	return false
}
func (s *fakeEventStream) Record() types.ContainerEventRecord { return s.records[s.index-1] }
func (s *fakeEventStream) Err() error                         { return s.err }
func (s *fakeEventStream) Close() error                       { close(s.closed); return nil }

func TestStreamSSEWritesRecordsWithSeqNumIDsAndErrors(t *testing.T) {
	stream := &fakeEventStream{
		records: []types.ContainerEventRecord{{SeqNum: 7, Type: "container.event"}, {SeqNum: 8}},
		err:     errors.New("store went away"),
		closed:  make(chan struct{}),
	}
	recorder := httptest.NewRecorder()
	ctx := echo.New().NewContext(httptest.NewRequest(http.MethodGet, "/", nil), recorder)

	require.NoError(t, streamSSE(ctx, stream, encodeEventRecord("event")))

	body := recorder.Body.String()
	require.Equal(t, "text/event-stream", recorder.Header().Get("Content-Type"))
	require.Contains(t, body, ": connected\n\n")
	require.Contains(t, body, "id: 7\nevent: container.event\n")
	require.Contains(t, body, "id: 8\nevent: event\n")
	require.Contains(t, body, "event: error\ndata: {\"error\":\"store went away\"}\n")
}

func TestStreamSSEStopsWhenClientDisconnects(t *testing.T) {
	stream := &fakeEventStream{closed: make(chan struct{})}
	defer stream.Close()
	requestCtx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(requestCtx)
	ctx := echo.New().NewContext(request, httptest.NewRecorder())

	finished := make(chan error, 1)
	go func() { finished <- streamSSE(ctx, stream, encodeEventRecord("event")) }()
	cancel()
	require.NoError(t, <-finished)
}
