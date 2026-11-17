package market

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWhatBecameOfAMachineIsWrittenToTheHistory(t *testing.T) {
	market := newTestMarket(t)
	recorder := NewRecorder(market.log, StreamsOf(market.config), market.clock)
	offer := a10g("us-east-1b/g5.xlarge", "us-east-1", 1, 380_000, spot)
	offer.Provider, offer.InstanceType = "aws", "g5.xlarge"
	bought := OutcomeOf(offer)
	bought.Source = SourceReservation

	recorder.Record(bought.Bought(nil, 1200*time.Millisecond))
	market.clock.Advance(90 * time.Second)
	recorder.Record(bought.CameUp(90 * time.Second))
	market.clock.Advance(time.Hour)
	recorder.Record(bought.Ended(ResultInterrupted, time.Hour+90*time.Second))
	recorder.Flush(context.Background())

	outcomes := read[Outcome](t, market.log, "outcomes")
	require.Len(t, outcomes, 3)
	assert.Equal(t, Outcome{
		At: noon, Event: OutcomeCreate, Result: ResultOK, Source: SourceReservation,
		Provider: "aws", Cloud: "aws", Region: "us-east-1", Geo: "us-east", Offer: "g5.xlarge", Zone: "us-east-1b",
		Spot: true, GPU: "A10G", GPUs: 1, HourlyMicros: 380_000, Took: 1200 * time.Millisecond,
	}, outcomes[0])
	assert.Equal(t, OutcomeActive, outcomes[1].Event)
	assert.Equal(t, 90*time.Second, outcomes[1].Took)
	assert.Equal(t, noon.Add(90*time.Second), outcomes[1].At)
	assert.Equal(t, ResultInterrupted, outcomes[2].Result)
	assert.Zero(t, market.log.Kept("outcomes"), "outcomes are kept for good")
}

func TestARefusedPurchaseIsRecordedByWhyItWasRefused(t *testing.T) {
	bought := OutcomeOf(a10g("A10G_1x", "newyork-usa-1", 1, 1_000_000))
	for result, err := range map[string]error{
		ResultOutOfStock: errors.Join(providers.ErrOutOfStock, errors.New("no capacity")),
		ResultQuota:      providers.ErrQuota,
		ResultUnanswered: &providers.StatusError{Status: 502},
		ResultError:      &providers.StatusError{Status: 400, Body: "bad image"},
	} {
		outcome := bought.Bought(err, time.Second)
		assert.Equal(t, result, outcome.Result)
		assert.NotEmpty(t, outcome.Err)
	}
}

func TestWithoutAHistoryNothingIsRecordedAndNothingFails(t *testing.T) {
	recorder := NewRecorder(nil, Streams{}, nil)
	assert.Nil(t, recorder)
	recorder.Start(context.Background())
	recorder.Record(Outcome{Event: OutcomeCreate})
	recorder.Flush(context.Background())
}
