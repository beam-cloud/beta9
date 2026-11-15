package providers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestLive buys a real machine. It checks a provider's client against the
// provider itself, without the rest of Beam: it buys the cheapest spot
// machine on offer, reads its status, deletes it, and confirms that nothing
// is left.
//
// It runs for the providers named in BEAM_LIVE_PROVIDERS, a comma-separated
// list, and nowhere else: credentials alone do not turn it on, because a
// shell often holds cloud credentials for other reasons. Each provider reads
// its credentials from the variables in liveProviders.
//
//	BEAM_LIVE_PROVIDERS=verda VERDA_CLIENT_ID=... VERDA_CLIENT_SECRET=... \
//	  go test ./pkg/providers -run TestLive -v -count=1 -timeout 30m
//
// The price limit is also the most the machine may come to cost, where the
// provider can be told: this is what makes Nebius's pricing policies.
//
// Optional:
//
//	BEAM_LIVE_GPU               buy a machine with this GPU, such as L40S
//	BEAM_LIVE_MAX_HOURLY_CENTS  never buy above this price (default 200)
//	BEAM_LIVE_ON_DEMAND=1       buy on demand where there is no spot stock
//	BEAM_LIVE_WAIT              wait this long for the machine to run, such
//	                            as 10m (default: read the status once)
func TestLive(t *testing.T) {
	names := strings.FieldsFunc(os.Getenv("BEAM_LIVE_PROVIDERS"), func(r rune) bool { return r == ',' || r == ' ' })
	if len(names) == 0 {
		t.Skip("BEAM_LIVE_PROVIDERS names no provider; this test buys real machines")
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			open, known := liveProviders[name]
			require.True(t, known, "no live test for provider %q", name)
			// The owner is this run's alone, so that what the provider lists
			// for it is what this run bought.
			owner := "beam-live-" + randomHex(t, 4)
			provider := open(t, owner)
			require.Equal(t, name, provider.Name())
			liveCheck{provider: provider, owner: owner, poll: 15 * time.Second}.run(t)
		})
	}
}

// liveProviders builds each provider's client from the environment.
var liveProviders = map[string]func(t *testing.T, owner string) Provider{
	"shadeform": func(t *testing.T, owner string) Provider {
		return NewShadeform(types.ShadeformProviderConfig{ApiKey: liveEnv(t, "SHADEFORM_API_KEY")}, owner)
	},
	// The account needs an SSH key: Vast starts no VM without one.
	"vast": func(t *testing.T, owner string) Provider {
		return NewVast(types.VastProviderConfig{ApiKey: liveEnv(t, "VAST_API_KEY")}, owner)
	},
	"verda": func(t *testing.T, owner string) Provider {
		config := types.VerdaProviderConfig{ClientID: liveEnv(t, "VERDA_CLIENT_ID"), ClientSecret: liveEnv(t, "VERDA_CLIENT_SECRET")}
		if keys := os.Getenv("VERDA_SSH_KEY_IDS"); keys != "" {
			config.SSHKeyIDs = strings.Split(keys, ",")
		}
		client, err := NewVerda(config, owner)
		require.NoError(t, err)
		return client
	},
	// NEBIUS_PRIVATE_KEY_FILE is the PEM file of the service account's
	// authorized key, and NEBIUS_PROJECT_IDS the projects to buy in, one for
	// a region, separated by commas.
	"nebius": func(t *testing.T, owner string) Provider {
		key, err := os.ReadFile(liveEnv(t, "NEBIUS_PRIVATE_KEY_FILE"))
		require.NoError(t, err)
		config := types.NebiusProviderConfig{
			ServiceAccountID: liveEnv(t, "NEBIUS_SERVICE_ACCOUNT_ID"),
			PublicKeyID:      liveEnv(t, "NEBIUS_PUBLIC_KEY_ID"),
			PrivateKey:       string(key),
		}
		for _, project := range strings.Split(liveEnv(t, "NEBIUS_PROJECT_IDS"), ",") {
			config.Projects = append(config.Projects, types.NebiusProjectConfig{ID: strings.TrimSpace(project)})
		}
		client, err := NewNebius(config, owner)
		require.NoError(t, err)
		return client
	},
	// The keys are the ones the AWS SDK finds by itself, such as
	// AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or AWS_PROFILE.
	// AWS_LIVE_REGIONS are the regions to buy in, separated by commas; each
	// needs a default VPC.
	"aws": func(t *testing.T, owner string) Provider {
		config := types.AWSProviderConfig{}
		for _, region := range strings.Split(liveEnv(t, "AWS_LIVE_REGIONS"), ",") {
			config.Regions = append(config.Regions, types.AWSProviderRegionConfig{Name: strings.TrimSpace(region)})
		}
		client, err := NewAWS(config, owner)
		require.NoError(t, err)
		return client
	},
	// GCP_SERVICE_ACCOUNT_KEY_FILE is the service account's JSON key file,
	// and GCP_LIVE_REGIONS the regions to buy in, separated by commas. The
	// project is the service account's unless GCP_PROJECT names another,
	// and needs a network named default.
	"gcp": func(t *testing.T, owner string) Provider {
		key, err := os.ReadFile(liveEnv(t, "GCP_SERVICE_ACCOUNT_KEY_FILE"))
		require.NoError(t, err)
		config := types.GCPProviderConfig{ServiceAccountKey: string(key), Project: os.Getenv("GCP_PROJECT")}
		for _, region := range strings.Split(liveEnv(t, "GCP_LIVE_REGIONS"), ",") {
			config.Regions = append(config.Regions, types.GCPProviderRegionConfig{Name: strings.TrimSpace(region)})
		}
		client, err := NewGCP(config, owner)
		require.NoError(t, err)
		return client
	},
}

func liveEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	require.NotEmpty(t, value, "%s is not set", name)
	return value
}

func randomHex(t *testing.T, bytes int) string {
	t.Helper()
	raw := make([]byte, bytes)
	_, err := rand.Read(raw)
	require.NoError(t, err)
	return hex.EncodeToString(raw)
}

// liveCheck is one purchase and its undoing.
type liveCheck struct {
	provider Provider
	owner    string
	// poll is the wait between two reads of something that is to change.
	poll time.Duration
	// finishDelete is set where a stand-in plays the provider, which then
	// has to be told to finish deleting.
	finishDelete func(id string)
}

func (c liveCheck) run(t *testing.T) {
	ctx := context.Background()
	provider, owner := c.provider, c.owner
	maxCents := int64(200)
	if text := os.Getenv("BEAM_LIVE_MAX_HOURLY_CENTS"); text != "" {
		var err error
		maxCents, err = strconv.ParseInt(text, 10, 64)
		require.NoError(t, err, "BEAM_LIVE_MAX_HOURLY_CENTS")
	}
	var wait time.Duration
	if text := os.Getenv("BEAM_LIVE_WAIT"); text != "" {
		var err error
		wait, err = time.ParseDuration(text)
		require.NoError(t, err, "BEAM_LIVE_WAIT")
	}

	before, err := provider.List(ctx)
	require.NoError(t, err)
	require.Empty(t, before, "owner %s is new and already has instances", owner)

	filter := OfferFilter{}
	if gpu := os.Getenv("BEAM_LIVE_GPU"); gpu != "" {
		filter.GPUs = []string{gpu}
	}
	offers, err := provider.Offers(ctx, filter)
	require.NoError(t, err)
	t.Logf("%d offers, %d of them spot", len(offers), len(slices.DeleteFunc(slices.Clone(offers), func(o Offer) bool { return !o.Spot })))

	offer, ok := cheapest(offers, true, maxCents)
	if !ok && os.Getenv("BEAM_LIVE_ON_DEMAND") != "" {
		offer, ok = cheapest(offers, false, maxCents)
	}
	if !ok {
		t.Skipf("nothing on offer at or under %d cents an hour (spot only unless BEAM_LIVE_ON_DEMAND is set)", maxCents)
	}
	t.Logf("buying %s in %s (%s), spot=%t, $%.4f an hour", offer.InstanceType, offer.Region, offer.Cloud, offer.Spot, float64(offer.HourlyCostMicros)/1e6)

	name := owner + "-" + randomHex(t, 4)
	tags := map[string]string{"beam-live": "smoke"}
	created, err := provider.Create(ctx, CreateRequest{
		Offer:         offer,
		Name:          name,
		StartupScript: "echo beam live smoke test",
		Tags:          tags,
		// A provider that deletes by itself does, should this test die.
		Until:               time.Now().Add(time.Hour),
		MaxHourlyCostMicros: maxCents * 10_000,
	})
	if errors.Is(err, ErrOutOfStock) || errors.Is(err, ErrQuota) {
		// The client named the refusal, which is what it is for.
		t.Skipf("the provider would not sell: %v", err)
	}
	require.NoError(t, err)
	require.NotEmpty(t, created.ID)
	t.Logf("bought %s, status %s", created.ID, created.Status)

	deleted := false
	t.Cleanup(func() {
		if deleted {
			return
		}
		// Whatever failed, the machine must not be left running.
		for attempt := 1; attempt <= 5; attempt++ {
			err := provider.Delete(context.Background(), created.ID)
			if err == nil {
				return
			}
			t.Logf("delete %s, attempt %d: %v", created.ID, attempt, err)
			time.Sleep(c.poll)
		}
		t.Errorf("INSTANCE %s OF %s MAY STILL BE RUNNING: delete it by hand", created.ID, provider.Name())
	})

	found, err := provider.Get(ctx, created.ID)
	require.NoError(t, err)
	require.Equal(t, created.ID, found.ID)
	require.Equal(t, tags, found.Tags)
	require.Contains(t, []Status{StatusPending, StatusActive}, found.Status, "detail: %s", found.StatusDetail)
	for deadline := time.Now().Add(wait); found.Status == StatusPending && time.Now().Before(deadline); {
		time.Sleep(c.poll)
		found, err = provider.Get(ctx, created.ID)
		require.NoError(t, err)
		t.Logf("status %s %s", found.Status, found.StatusDetail)
	}
	if wait > 0 {
		require.Equal(t, StatusActive, found.Status, "not running after %s; detail: %s", wait, found.StatusDetail)
		require.NotEmpty(t, found.PublicIP)
	}

	listed, err := provider.List(ctx)
	require.NoError(t, err)
	require.Len(t, listed, 1)
	require.Equal(t, created.ID, listed[0].ID)
	require.Equal(t, tags, listed[0].Tags)

	require.NoError(t, provider.Extend(ctx, created.ID, time.Now().Add(2*time.Hour)))

	require.NoError(t, provider.Delete(ctx, created.ID))
	deleted = true
	require.NoError(t, provider.Delete(ctx, created.ID), "deleting twice succeeds")
	if c.finishDelete != nil {
		c.finishDelete(created.ID)
	}

	// A provider may show the instance for a while as it deletes it. Some
	// cannot tell a spot machine they took back from one that was deleted.
	ended := []Status{StatusDeleted, StatusInterrupted}
	for deadline := time.Now().Add(10 * time.Minute); ; {
		listed, err = provider.List(ctx)
		require.NoError(t, err)
		if len(listed) == 0 {
			break
		}
		for _, instance := range listed {
			require.Contains(t, ended, instance.Status, "instance %s is listed and is not being deleted", instance.ID)
		}
		require.True(t, time.Now().Before(deadline), "the provider still lists %d instances ten minutes after the delete", len(listed))
		time.Sleep(c.poll)
	}

	if found, err := provider.Get(ctx, created.ID); err == nil {
		t.Logf("after the delete the provider reports %s %s", found.Status, found.StatusDetail)
		require.Contains(t, ended, found.Status)
	} else {
		require.ErrorIs(t, err, ErrNotFound)
	}
}

// cheapest returns the offer that costs least among those bought the given
// way, at or under the price limit.
func cheapest(offers []Offer, spot bool, maxCents int64) (Offer, bool) {
	var best Offer
	found := false
	for _, offer := range offers {
		if offer.Spot != spot || offer.HourlyCostMicros > maxCents*10_000 {
			continue
		}
		if !found || offer.HourlyCostMicros < best.HourlyCostMicros {
			best, found = offer, true
		}
	}
	return best, found
}

// The live check is run rarely, and with money. This runs it against the
// stand-ins, so that a mistake in the check itself shows up here.
func TestTheLiveCheckPassesAgainstEveryStandIn(t *testing.T) {
	t.Setenv("BEAM_LIVE_MAX_HOURLY_CENTS", "10000")
	t.Setenv("BEAM_LIVE_ON_DEMAND", "1")
	for name, open := range conformanceSites {
		t.Run(name, func(t *testing.T) {
			s := open(t)
			liveCheck{provider: s.provider, owner: testOwner, poll: time.Millisecond, finishDelete: s.lose}.run(t)
		})
	}
}

func TestCheapestOfferRespectsTheWayOfBuyingAndThePriceLimit(t *testing.T) {
	offers := []Offer{
		{ID: "spot-dear", Spot: true, HourlyCostMicros: 3_000_000},
		{ID: "spot-cheap", Spot: true, HourlyCostMicros: 500_000},
		{ID: "on-demand", HourlyCostMicros: 100_000},
	}
	best, ok := cheapest(offers, true, 200)
	require.True(t, ok)
	require.Equal(t, "spot-cheap", best.ID)

	_, ok = cheapest(offers, true, 40)
	require.False(t, ok, "no spot offer costs 40 cents or less")

	best, ok = cheapest(offers, false, 200)
	require.True(t, ok)
	require.Equal(t, "on-demand", best.ID)
}
