package repository

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/beam-cloud/beta9/pkg/providers/watch"
	"github.com/beam-cloud/beta9/pkg/types"
	s2pb "github.com/s2-streamstore/s2-sdk-go/generated"
	"github.com/s2-streamstore/s2-sdk-go/s2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// marketTestS2 serves streams over S2's API, like S2 as the market's log
// uses it: a stream is made with PUT and not by its first append, a read
// returns no more than a batch, a read at the tail returns no records, and
// a read past it, or of a stream that does not exist, fails.
type marketTestS2 struct {
	mu      sync.Mutex
	streams map[string]*marketTestStream
	// batch is the most records a read returns.
	batch int
	// made counts the times a stream was made or changed.
	made int
}

type marketTestStream struct {
	config  s2.StreamConfig
	records []*s2pb.SequencedRecord
}

func newMarketTestLog(t *testing.T) (*S2MarketLog, *marketTestS2) {
	t.Helper()
	fake := &marketTestS2{streams: map[string]*marketTestStream{}, batch: 1000}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	basin := s2.New("test", &s2.ClientOptions{
		HTTPClient:       server.Client(),
		MakeBasinBaseURL: func(string) string { return server.URL },
	}).Basin("test")
	return newS2MarketLog(basin, ""), fake
}

func (s *marketTestS2) stream(name string) *marketTestStream {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[name]
}

func (s *marketTestS2) names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.streams))
	for name := range s.streams {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (s *marketTestS2) delete(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.streams, name)
}

func (s *marketTestS2) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	name, records := strings.CutSuffix(strings.TrimPrefix(request.URL.Path, "/v1/streams/"), "/records")
	stream := s.streams[name]
	if !records {
		if request.Method != http.MethodPut {
			http.Error(w, "unexpected "+request.Method, http.StatusMethodNotAllowed)
			return
		}
		var config s2.StreamConfig
		if err := json.NewDecoder(request.Body).Decode(&config); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		status := http.StatusOK
		if stream == nil {
			stream = &marketTestStream{}
			s.streams[name] = stream
			status = http.StatusCreated
		}
		stream.config = config
		s.made++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(s2.StreamInfo{Name: s2.StreamName(name), CreatedAt: time.Now()})
		return
	}
	if stream == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"code":"stream_not_found","message":"stream not found"}`))
		return
	}

	var answer proto.Message
	switch request.Method {
	case http.MethodPost:
		body, _ := io.ReadAll(request.Body)
		var input s2pb.AppendInput
		if err := proto.Unmarshal(body, &input); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		start := uint64(len(stream.records))
		for _, record := range input.Records {
			// A stream's timestamps never go back.
			timestamp := record.GetTimestamp()
			if n := len(stream.records); n > 0 {
				timestamp = max(timestamp, stream.records[n-1].Timestamp)
			}
			stream.records = append(stream.records, &s2pb.SequencedRecord{SeqNum: uint64(len(stream.records)), Timestamp: timestamp, Body: record.Body})
		}
		tail := &s2pb.StreamPosition{SeqNum: uint64(len(stream.records))}
		answer = &s2pb.AppendAck{Start: &s2pb.StreamPosition{SeqNum: start}, End: tail, Tail: tail}
	case http.MethodGet:
		params := request.URL.Query()
		start := 0
		if value := params.Get("seq_num"); value != "" {
			seq, _ := strconv.Atoi(value)
			if seq > len(stream.records) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			start = seq
		} else if value := params.Get("timestamp"); value != "" {
			timestamp, _ := strconv.ParseUint(value, 10, 64)
			start, _ = slices.BinarySearchFunc(stream.records, timestamp, func(record *s2pb.SequencedRecord, timestamp uint64) int {
				if record.Timestamp < timestamp {
					return -1
				}
				return 1
			})
			if start >= len(stream.records) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
		}
		end := min(len(stream.records), start+s.batch)
		if value := params.Get("until"); value != "" {
			until, _ := strconv.ParseUint(value, 10, 64)
			for i := start; i < end; i++ {
				if stream.records[i].Timestamp >= until {
					end = i
					break
				}
			}
		}
		answer = &s2pb.ReadBatch{Records: stream.records[start:end], Tail: &s2pb.StreamPosition{SeqNum: uint64(len(stream.records))}}
	default:
		http.Error(w, "unexpected "+request.Method, http.StatusMethodNotAllowed)
		return
	}
	body, err := proto.Marshal(answer)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(body)
}

var marketTestNoon = time.Date(2026, 11, 17, 12, 0, 0, 0, time.UTC)

func TestTheMarketsHistoryIsReadFromS2ByTime(t *testing.T) {
	log, fake := newMarketTestLog(t)
	fake.batch = 100
	ctx := context.Background()
	samples := watch.Stream{Name: "series/a10g/1m", Keep: 14 * 24 * time.Hour}

	// A day and a half of samples, a minute apart.
	var records []watch.Record
	for minute := range 36 * 60 {
		records = append(records, watch.Record{At: marketTestNoon.Add(time.Duration(minute) * time.Minute), Body: []byte(strconv.Itoa(minute))})
	}
	for batch := range slices.Chunk(records, 500) {
		require.NoError(t, log.Append(ctx, samples, batch...))
	}

	read, err := log.Read(ctx, samples, marketTestNoon.Add(time.Hour), marketTestNoon.Add(7*time.Hour))
	require.NoError(t, err)
	require.Len(t, read, 360, "from the start, until the end, however many a read returns at once")
	assert.Equal(t, watch.Record{At: marketTestNoon.Add(time.Hour), Body: []byte("60")}, read[0])
	assert.Equal(t, []byte("419"), read[359].Body)

	// The end of the stream, and what is past it.
	read, err = log.Read(ctx, samples, marketTestNoon.Add(35*time.Hour), marketTestNoon.Add(48*time.Hour))
	require.NoError(t, err)
	assert.Len(t, read, 60)
	read, err = log.Read(ctx, samples, marketTestNoon.Add(40*time.Hour), marketTestNoon.Add(48*time.Hour))
	require.NoError(t, err)
	assert.Empty(t, read)
	read, err = log.Read(ctx, samples, marketTestNoon.Add(-time.Hour), marketTestNoon)
	require.NoError(t, err)
	assert.Empty(t, read)

	// A stream that was never written to holds nothing, and is not made by
	// being read.
	read, err = log.Read(ctx, watch.Stream{Name: "series/h100/1m"}, marketTestNoon, marketTestNoon.Add(time.Hour))
	require.NoError(t, err)
	assert.Empty(t, read)
	assert.Equal(t, []string{"events/market/series/a10g/1m"}, fake.names())
}

func TestAStreamIsMadeToKeepItsRecordsForAsLongAsItSays(t *testing.T) {
	log, fake := newMarketTestLog(t)
	ctx := context.Background()
	record := watch.Record{At: marketTestNoon, Body: []byte("{}")}

	require.NoError(t, log.Append(ctx, watch.Stream{Name: "series/a10g/1m", Keep: 14 * 24 * time.Hour}, record))
	require.NoError(t, log.Append(ctx, watch.Stream{Name: "series/a10g/1m", Keep: 14 * 24 * time.Hour}, record))
	require.NoError(t, log.Append(ctx, watch.Stream{Name: "series/a10g/1h"}, record))

	samples := fake.stream("events/market/series/a10g/1m")
	require.NotNil(t, samples)
	require.NotNil(t, samples.config.RetentionPolicy)
	assert.Equal(t, int64(14*24*3600), *samples.config.RetentionPolicy.Age)
	assert.Nil(t, samples.config.RetentionPolicy.Infinite)
	assert.Equal(t, s2.TimestampingModeClientPrefer, *samples.config.Timestamping.Mode, "a record is of the time it says")
	assert.Len(t, samples.records, 2)

	hours := fake.stream("events/market/series/a10g/1h")
	require.NotNil(t, hours)
	assert.NotNil(t, hours.config.RetentionPolicy.Infinite, "kept for good, and not for the week a stream is kept by default")
	assert.Nil(t, hours.config.RetentionPolicy.Age)
	assert.Equal(t, 2, fake.made, "a stream is made once, not before every append")

	// A change of how long it is kept reaches a stream that exists.
	require.NoError(t, log.Append(ctx, watch.Stream{Name: "series/a10g/1m", Keep: 30 * 24 * time.Hour}, record))
	assert.Equal(t, int64(30*24*3600), *fake.stream("events/market/series/a10g/1m").config.RetentionPolicy.Age)

	// A stream that was deleted is made again as it should be.
	fake.delete("events/market/series/a10g/1h")
	require.NoError(t, log.Append(ctx, watch.Stream{Name: "series/a10g/1h"}, record))
	hours = fake.stream("events/market/series/a10g/1h")
	require.NotNil(t, hours)
	assert.NotNil(t, hours.config.RetentionPolicy.Infinite)
	assert.Len(t, hours.records, 1)
}

func TestARecordThatS2WouldRefuseIsNotSent(t *testing.T) {
	log, fake := newMarketTestLog(t)

	err := log.Append(context.Background(), watch.Stream{Name: "snapshots/vast"}, watch.Record{At: marketTestNoon, Body: make([]byte, s2MaxRecordBytes+1)})

	require.ErrorContains(t, err, "more than s2 stream")
	assert.Empty(t, fake.names())
}

func TestManyRecordsAreAppendedInAsManyBatchesAsS2Takes(t *testing.T) {
	log, fake := newMarketTestLog(t)
	var records []watch.Record
	for i := range 5 {
		records = append(records, watch.Record{At: marketTestNoon.Add(time.Duration(i) * time.Second), Body: make([]byte, 400_000)})
	}

	require.NoError(t, log.Append(context.Background(), watch.Stream{Name: "outcomes"}, records...))

	stream := fake.stream("events/market/outcomes")
	require.Len(t, stream.records, 5)
	for i, record := range stream.records {
		assert.Equal(t, uint64(marketTestNoon.Add(time.Duration(i)*time.Second).UnixMilli()), record.Timestamp)
	}
}

func TestS2IsNotUsedForTheMarketUnlessItIsConfigured(t *testing.T) {
	log, err := NewS2MarketLog(types.S2Config{})
	require.NoError(t, err)
	assert.Nil(t, log)

	_, err = NewS2MarketLog(types.S2Config{ApiKey: "key"})
	require.ErrorContains(t, err, "basin")

	log, err = NewS2MarketLog(types.S2Config{ApiKey: "key", Basin: "basin", StreamPrefix: "/staging/"})
	require.NoError(t, err)
	assert.Equal(t, s2.StreamName("staging/market/outcomes"), log.name(watch.Stream{Name: "outcomes"}))
}

// The sampler writes to S2 and the reader draws a chart from what it wrote:
// the two ends of the market's history, with S2 between them.
func TestAChartIsDrawnFromWhatTheSamplerWroteToS2(t *testing.T) {
	log, fake := newMarketTestLog(t)
	fake.batch = 25
	server := miniredis.RunT(t)
	rdb, err := common.NewRedisClient(types.RedisConfig{Addrs: []string{server.Addr()}, Mode: types.RedisModeSingle})
	require.NoError(t, err)
	t.Cleanup(func() { rdb.Close() })

	seller := providers.NewFake("shadeform", "test")
	registry := providers.RegistryOf(seller)
	clock := common.NewFakeClock(marketTestNoon)
	sampler := watch.NewSampler(watch.Options{Providers: registry, Redis: rdb, Log: log, Clock: clock, ID: "gw-1"})
	ctx := context.Background()
	// The price rises by a cent a minute, for two hours and a half.
	for minute := range 151 {
		seller.SetOffers(providers.Offer{ID: "A10G_1x", Region: "newyork-usa-1", GPU: "A10G", GPUCount: 1, HourlyCostMicros: 1_000_000 + int64(minute)*10_000})
		sampler.Sample(ctx, marketTestNoon.Add(time.Duration(minute)*time.Minute))
		clock.Advance(time.Minute)
	}

	assert.Equal(t, []string{
		"events/market/series/a10g/1h",
		"events/market/series/a10g/1m",
		"events/market/snapshots/shadeform",
	}, fake.names())
	assert.Len(t, fake.stream("events/market/series/a10g/1m").records, 151)
	assert.Len(t, fake.stream("events/market/series/a10g/1h").records, 2, "one for each hour that has ended")
	assert.NotNil(t, fake.stream("events/market/series/a10g/1h").config.RetentionPolicy.Infinite)

	reader := watch.NewReader(watch.ReaderOptions{Providers: registry, Redis: rdb, Log: log, Clock: clock})
	answer, err := reader.Series(ctx, watch.SeriesRequest{GPU: "A10G", Start: marketTestNoon})
	require.NoError(t, err)
	require.Len(t, answer.Series, 1)
	assert.Equal(t, watch.TierMinute, answer.Tier)
	points := answer.Series[0].Points
	require.Len(t, points, 151)
	assert.JSONEq(t, asJSON([]any{marketTestNoon.UnixMilli(), 1_000_000, 1_000_000, 1_000_000, nil, 1, 1}), asJSON(points[0]))
	assert.JSONEq(t, asJSON([]any{marketTestNoon.Add(150 * time.Minute).UnixMilli(), 2_500_000, 2_500_000, 2_500_000, nil, 1, 1}), asJSON(points[150]))

	// A week is drawn from the hours: the two that ended, and the one that
	// has not, from its samples.
	answer, err = reader.Series(ctx, watch.SeriesRequest{GPU: "A10G", Start: marketTestNoon.Add(-7 * 24 * time.Hour)})
	require.NoError(t, err)
	assert.Equal(t, watch.TierHour, answer.Tier)
	points = answer.Series[0].Points
	require.Len(t, points, 3)
	assert.JSONEq(t, asJSON([]any{marketTestNoon.UnixMilli(), 1_590_000, 1_000_000, 1_590_000, nil, 1, 1}), asJSON(points[0]))
	assert.JSONEq(t, asJSON([]any{marketTestNoon.Add(2 * time.Hour).UnixMilli(), 2_500_000, 2_200_000, 2_500_000, nil, 1, 1}), asJSON(points[2]))
}

// asJSON is a value as it is sent: a point of a series is compared so,
// since a price that is there is a number and one that is not is null.
func asJSON(value any) string {
	body, _ := json.Marshal(value)
	return string(body)
}
