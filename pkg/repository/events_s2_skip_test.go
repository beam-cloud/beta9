package repository

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	s2pb "github.com/s2-streamstore/s2-sdk-go/generated"
	"github.com/s2-streamstore/s2-sdk-go/s2"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const historySkipTestEpochMs = 1_700_000_000_000

// historySkipTestStream serves one stream over S2's unary read API, like S2:
// a read at the tail returns no records and a read past it returns 416. It
// counts the records it serves.
type historySkipTestStream struct {
	mu      sync.Mutex
	records []*s2pb.SequencedRecord
	served  int
}

func (s *historySkipTestStream) append(t *testing.T, eventType string, n int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for range n {
		seq := uint64(len(s.records))
		eventTime := time.UnixMilli(historySkipTestEpochMs + int64(seq)).UTC()
		body, err := json.Marshal(map[string]any{
			"type":        eventType,
			"time":        eventTime,
			"workspaceid": "workspace",
			"stubid":      "stub",
			"data":        map[string]any{},
		})
		require.NoError(t, err)
		s.records = append(s.records, &s2pb.SequencedRecord{
			SeqNum:    seq,
			Timestamp: uint64(eventTime.UnixMilli()),
			Headers:   []*s2pb.Header{{Name: []byte("type"), Value: []byte(eventType)}},
			Body:      body,
		})
	}
}

func (s *historySkipTestStream) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = nil
}

func (s *historySkipTestStream) takeServed() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	served := s.served
	s.served = 0
	return served
}

func (s *historySkipTestStream) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	params := request.URL.Query()
	start := 0
	if value := params.Get("seq_num"); value != "" {
		seq, _ := strconv.Atoi(value)
		if seq > len(s.records) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		start = seq
	} else if value := params.Get("timestamp"); value != "" {
		timestamp, _ := strconv.ParseUint(value, 10, 64)
		start = sort.Search(len(s.records), func(i int) bool { return s.records[i].Timestamp >= timestamp })
	}
	count, _ := strconv.Atoi(params.Get("count"))
	end := min(len(s.records), start+count)
	s.served += end - start

	body, err := proto.Marshal(&s2pb.ReadBatch{
		Records: s.records[start:end],
		Tail:    &s2pb.StreamPosition{SeqNum: uint64(len(s.records))},
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

func newHistorySkipTestRepository(t *testing.T) (*S2EventRepository, *historySkipTestStream) {
	t.Helper()
	stream := &historySkipTestStream{}
	server := httptest.NewServer(stream)
	t.Cleanup(server.Close)
	repo := &S2EventRepository{basin: s2.New("test", &s2.ClientOptions{
		HTTPClient:       server.Client(),
		MakeBasinBaseURL: func(string) string { return server.URL },
	}).Basin("test")}
	return repo, stream
}

func pollHistorySkipTestStream(t *testing.T, repo *S2EventRepository, query types.EventQuery) []types.ContainerEventRecord {
	t.Helper()
	response := &types.EventHistoryResponse{}
	require.NoError(t, repo.readEventHistoryStream(context.Background(), "stream", query, query.Limit, response))
	return response.Events
}

// The service status panel polls an app's recent container metrics every
// five seconds with limit 100. For an app that never runs a container, each
// poll used to read every record from its start position to the tail.
func TestEventHistoryPollsReadOnlyRecordsNotAlreadyChecked(t *testing.T) {
	repo, stream := newHistorySkipTestRepository(t)
	stream.append(t, types.EventContainerLifecycle, 5000)

	start := time.UnixMilli(historySkipTestEpochMs).UTC()
	metrics := types.EventQuery{
		WorkspaceID: "workspace",
		StubID:      "stub",
		EventTypes:  []string{types.EventContainerMetrics},
		StartTime:   &start,
		Limit:       100,
	}

	require.Empty(t, pollHistorySkipTestStream(t, repo, metrics))
	require.Equal(t, 5000, stream.takeServed())

	// Later polls read the batch their start position seeks to, then only
	// the records appended since.
	stream.append(t, types.EventContainerLifecycle, 50)
	later := start.Add(time.Millisecond)
	metrics.StartTime = &later
	require.Empty(t, pollHistorySkipTestStream(t, repo, metrics))
	require.Equal(t, 100+50, stream.takeServed())

	stream.append(t, types.EventContainerMetrics, 1)
	stream.append(t, types.EventContainerLifecycle, 10)
	events := pollHistorySkipTestStream(t, repo, metrics)
	require.Len(t, events, 1)
	require.EqualValues(t, 5050, events[0].SeqNum)
	require.Equal(t, 100+11, stream.takeServed())

	// A match splits the skipped records into two ranges; both are kept.
	require.Len(t, pollHistorySkipTestStream(t, repo, metrics), 1)
	require.Equal(t, 100+1, stream.takeServed())

	// Other filters are unaffected.
	lifecycle := metrics
	lifecycle.EventTypes = []string{types.EventContainerLifecycle}
	require.Len(t, pollHistorySkipTestStream(t, repo, lifecycle), 100)
}

func TestEventHistorySkipIsDroppedWhenItOutlivesTheStream(t *testing.T) {
	zero := uint64(0)
	start := time.UnixMilli(historySkipTestEpochMs).UTC()
	metrics := types.EventQuery{StubID: "stub", EventTypes: []string{types.EventContainerMetrics}, Limit: 100}
	fromSeq, fromTime := metrics, metrics
	fromSeq.SeqNum, fromTime.StartTime = &zero, &start

	for name, query := range map[string]types.EventQuery{"sequence start": fromSeq, "time start": fromTime} {
		t.Run(name, func(t *testing.T) {
			repo, stream := newHistorySkipTestRepository(t)
			stream.append(t, types.EventContainerLifecycle, 3000)
			require.Empty(t, pollHistorySkipTestStream(t, repo, query))

			// Recreated under the same name, shorter than the remembered ranges.
			stream.reset()
			stream.append(t, types.EventContainerLifecycle, 500)
			stream.append(t, types.EventContainerMetrics, 1)
			events := pollHistorySkipTestStream(t, repo, query)
			require.Len(t, events, 1)
			require.EqualValues(t, 500, events[0].SeqNum)
		})
	}
}

func TestRememberHistorySkipMergesRangesAndKeepsTheLongest(t *testing.T) {
	repo := &S2EventRepository{}
	key := "stream\x00{}"

	repo.rememberHistorySkip(key, 10, 20)
	repo.rememberHistorySkip(key, 20, 30)
	repo.rememberHistorySkip(key, 40, 50)
	repo.rememberHistorySkip(key, 45, 60)
	require.Equal(t, []s2SeqRange{{10, 30}, {40, 60}}, repo.historySkip(key).ranges)

	skip := repo.historySkip(key)
	for _, tc := range []struct {
		seq, start, nextRange uint64
		bounded               bool
	}{
		{seq: 0, start: 0, nextRange: 10, bounded: true},
		{seq: 15, start: 30, nextRange: 40, bounded: true},
		{seq: 30, start: 30, nextRange: 40, bounded: true},
		{seq: 55, start: 60},
		{seq: 70, start: 70},
	} {
		start, nextRange, bounded := skip.resume(tc.seq)
		require.Equal(t, []any{tc.start, tc.nextRange, tc.bounded}, []any{start, nextRange, bounded}, "resume(%d)", tc.seq)
	}

	for i := range uint64(s2HistorySkipMaxRanges) {
		repo.rememberHistorySkip(key, 1000+100*i, 1000+100*i+1+i)
	}
	ranges := repo.historySkip(key).ranges
	require.Len(t, ranges, s2HistorySkipMaxRanges)
	require.NotContains(t, ranges, s2SeqRange{1000, 1001}, "the shortest range is dropped first")
	require.Contains(t, ranges, s2SeqRange{10, 30})
}

func TestHistorySkipKeyIgnoresOnlyPositionAndWindow(t *testing.T) {
	start, end := time.Now(), time.Now().Add(time.Minute)
	seq := uint64(7)
	base := types.EventQuery{StubID: "stub", EventTypes: []string{types.EventContainerMetrics}}
	positioned := base
	positioned.StartTime, positioned.EndTime, positioned.SeqNum, positioned.Limit = &start, &end, &seq, 100
	require.Equal(t, historySkipKey("stream", base), historySkipKey("stream", positioned))

	otherStub := base
	otherStub.StubID = "other"
	require.NotEqual(t, historySkipKey("stream", base), historySkipKey("stream", otherStub))
	excluded := base
	excluded.ExcludeActions = []string{"heartbeat"}
	require.NotEqual(t, historySkipKey("stream", base), historySkipKey("stream", excluded))
	require.NotEqual(t, historySkipKey("stream", base), historySkipKey("other", base))
}
