package watch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/providers"
	"github.com/redis/go-redis/v9"
)

const (
	// latestFor is how long what a provider last answered is kept for the
	// gateways. A provider that has not answered for this long is asked
	// again by whoever wants its offers.
	latestFor = 10 * time.Minute

	// seenFor is how long a listing is remembered after it was last seen
	// for sale. Until then a sample says of it that there is none, which a
	// provider that lists only what it has does not say itself.
	seenFor = 7 * 24 * time.Hour
)

// Shared is what the gateways share about the market, in Redis: what every
// provider last answered, which every gateway reads, and what the sampler
// must know that a gateway taking the sampling over has not seen itself.
type Shared struct {
	rdb *common.RedisClient
}

func NewShared(rdb *common.RedisClient) *Shared {
	return &Shared{rdb: rdb}
}

// Latest is what a provider sold when it last answered.
type Latest struct {
	At     time.Time
	Offers []providers.Offer
}

type latestWire struct {
	V      int             `json:"v"`
	TS     int64           `json:"ts"`
	Offers json.RawMessage `json:"offers"`
}

// Publish keeps what a provider answered for the gateways to read.
func (s *Shared) Publish(ctx context.Context, provider string, latest Latest) error {
	encoded, err := encodeOffers(latest.Offers)
	if err != nil {
		return err
	}
	body, err := json.Marshal(latestWire{V: recordVersion, TS: latest.At.UnixMilli(), Offers: encoded})
	if err != nil {
		return err
	}
	return s.rdb.Set(ctx, common.RedisKeys.MarketOffers(provider), compress(body), latestFor).Err()
}

// Latest returns what a provider sold when it last answered, and whether
// there is such an answer.
func (s *Shared) Latest(ctx context.Context, provider string) (Latest, bool, error) {
	packed, err := s.rdb.Get(ctx, common.RedisKeys.MarketOffers(provider)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Latest{}, false, nil
	}
	if err != nil {
		return Latest{}, false, err
	}
	body, err := decompress(packed)
	if err != nil {
		return Latest{}, false, fmt.Errorf("read what %s last sold: %w", provider, err)
	}
	var in latestWire
	if err := json.Unmarshal(body, &in); err != nil {
		return Latest{}, false, fmt.Errorf("read what %s last sold: %w", provider, err)
	}
	if in.V != recordVersion {
		// Written by a gateway of another version. It is asked for anew.
		return Latest{}, false, nil
	}
	offers, err := decodeOffers(provider, in.Offers)
	if err != nil {
		return Latest{}, false, fmt.Errorf("read what %s last sold: %w", provider, err)
	}
	return Latest{At: time.UnixMilli(in.TS).UTC(), Offers: offers}, true, nil
}

// PollAt is how a provider answered, and when it was asked.
type PollAt struct {
	Poll
	TS int64 `json:"ts"`
}

// Polled keeps how each provider answered when it was last asked.
func (s *Shared) Polled(ctx context.Context, at time.Time, polls map[string]Poll) error {
	if len(polls) == 0 {
		return nil
	}
	fields := make([]any, 0, 2*len(polls))
	for provider, poll := range polls {
		body, err := json.Marshal(PollAt{Poll: poll, TS: at.UnixMilli()})
		if err != nil {
			return err
		}
		fields = append(fields, provider, body)
	}
	return s.rdb.HSet(ctx, common.RedisKeys.MarketPolls(), fields...).Err()
}

// Polls returns how each provider answered when it was last asked. A
// provider that has not been asked since latestFor ago is left out: it is
// no longer sampled.
func (s *Shared) Polls(ctx context.Context, now time.Time) (map[string]PollAt, error) {
	stored, err := s.rdb.HGetAll(ctx, common.RedisKeys.MarketPolls()).Result()
	if err != nil {
		return nil, err
	}
	polls := map[string]PollAt{}
	for provider, body := range stored {
		var poll PollAt
		if json.Unmarshal([]byte(body), &poll) != nil {
			continue
		}
		if now.Sub(time.UnixMilli(poll.TS)) > latestFor {
			continue
		}
		polls[provider] = poll
	}
	return polls, nil
}

// ClaimTick marks the sample of a time as taken by a gateway, and reports
// whether no gateway had taken it. A gateway that takes the sampling over
// in the middle of a minute finds the minute's sample taken already.
func (s *Shared) ClaimTick(ctx context.Context, tick time.Time, by string) (bool, error) {
	return s.rdb.SetNX(ctx, common.RedisKeys.MarketTick(tick.UnixMilli()), by, latestFor).Result()
}

// seenSeparator stands between the parts of what a listing is remembered
// by. No provider puts it in a name.
const seenSeparator = "\x1f"

func seenMember(gpu string, listing Listing) string {
	return strings.Join([]string{
		gpu, listing.Provider, listing.Cloud, listing.Region,
		strconv.Itoa(btoi(listing.Spot)), strconv.FormatUint(uint64(listing.GPUs), 10),
	}, seenSeparator)
}

func parseSeen(member string) (gpu string, listing Listing, ok bool) {
	parts := strings.Split(member, seenSeparator)
	if len(parts) != 6 {
		return "", Listing{}, false
	}
	gpus, err := strconv.ParseUint(parts[5], 10, 32)
	if err != nil {
		return "", Listing{}, false
	}
	return parts[0], Listing{
		Provider: parts[1],
		Cloud:    parts[2],
		Region:   parts[3],
		Geo:      Geo(parts[3]),
		Spot:     parts[4] == "1",
		GPUs:     uint32(gpus),
	}, true
}

// Seen is a listing that has been for sale, and the GPU it is of.
type Seen struct {
	GPU string
	Listing
}

// See remembers that the listings were for sale at a time, and forgets what
// has not been for seenFor.
func (s *Shared) See(ctx context.Context, at time.Time, listings []Seen) error {
	key := common.RedisKeys.MarketSeen()
	const batch = 500
	for start := 0; start < len(listings); start += batch {
		members := make([]redis.Z, 0, batch)
		for _, seen := range listings[start:min(start+batch, len(listings))] {
			members = append(members, redis.Z{Score: float64(at.UnixMilli()), Member: seenMember(seen.GPU, seen.Listing)})
		}
		if err := s.rdb.ZAdd(ctx, key, members...).Err(); err != nil {
			return err
		}
	}
	return s.rdb.ZRemRangeByScore(ctx, key, "-inf", "("+strconv.FormatInt(at.Add(-seenFor).UnixMilli(), 10)).Err()
}

// Seen returns every listing that has been for sale within seenFor.
func (s *Shared) Seen(ctx context.Context, now time.Time) ([]Seen, error) {
	members, err := s.rdb.ZRangeByScore(ctx, common.RedisKeys.MarketSeen(), &redis.ZRangeBy{
		Min: strconv.FormatInt(now.Add(-seenFor).UnixMilli(), 10),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, err
	}
	seen := make([]Seen, 0, len(members))
	for _, member := range members {
		if gpu, listing, ok := parseSeen(member); ok {
			seen = append(seen, Seen{GPU: gpu, Listing: listing})
		}
	}
	return seen, nil
}

// RolledUp returns the hours of each GPU's samples that were last put
// together, by GPU.
func (s *Shared) RolledUp(ctx context.Context) (map[string]time.Time, error) {
	stored, err := s.rdb.HGetAll(ctx, common.RedisKeys.MarketRollups()).Result()
	if err != nil {
		return nil, err
	}
	hours := make(map[string]time.Time, len(stored))
	for gpu, value := range stored {
		if unix, err := strconv.ParseInt(value, 10, 64); err == nil {
			hours[gpu] = time.Unix(unix, 0).UTC()
		}
	}
	return hours, nil
}

// SetRolledUp records the hour of a GPU's samples that was last put
// together.
func (s *Shared) SetRolledUp(ctx context.Context, gpu string, hour time.Time) error {
	return s.rdb.HSet(ctx, common.RedisKeys.MarketRollups(), gpu, hour.Unix()).Err()
}
