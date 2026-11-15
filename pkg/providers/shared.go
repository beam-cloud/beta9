package providers

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// dollars is a price a provider writes in dollars, as a number or as a
// string such as "1.50". It holds the price in micros. A price the provider
// left empty is zero.
type dollars int64

func (d *dollars) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	if text == "" || text == "null" {
		*d = 0
		return nil
	}
	amount, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return fmt.Errorf("price %s is not a number of dollars", data)
	}
	*d = dollars(DollarsToMicros(amount))
	return nil
}

// count is a whole number a provider writes as a number or as a string such
// as "8". One the provider left empty is zero.
type count int64

func (c *count) UnmarshalJSON(data []byte) error {
	text := strings.Trim(string(data), `"`)
	if text == "" || text == "null" {
		*c = 0
		return nil
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("%s is not a whole number", data)
	}
	*c = count(n)
	return nil
}

const (
	// defaultDiskGB is the size of a machine's disk where the config names
	// none.
	defaultDiskGB = 200
	// hoursPerMonth is the month that a disk's price is for.
	hoursPerMonth = 730
)

// hourlyDiskMicros is what a disk of gb gigabytes costs an hour, in micros,
// at a price for a gigabyte for a month. It is for the providers that have
// no API for the price of a disk, where the price is their list's.
func hourlyDiskMicros(gb, perGBMonth int64) int64 {
	return gb * perGBMonth / hoursPerMonth
}

// cached remembers an answer that changes rarely.
type cached[T any] struct {
	mu     sync.Mutex
	value  T
	loaded time.Time
}

// get returns the remembered answer, and loads it when there is none or it
// is older than maxAge.
func (c *cached[T]) get(now time.Time, maxAge time.Duration, load func() (T, error)) (T, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.loaded.IsZero() && now.Sub(c.loaded) < maxAge {
		return c.value, nil
	}
	value, err := load()
	if err != nil {
		return value, err
	}
	c.value, c.loaded = value, now
	return value, nil
}

func (c *cached[T]) forget() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.loaded = time.Time{}
}

// noDeletionTimer is part of the client of a provider that has no time at
// which it deletes an instance by itself, or none that can be moved. For
// such a provider Extend does nothing.
type noDeletionTimer struct{}

func (noDeletionTimer) Extend(ctx context.Context, id string, until time.Time) error { return nil }

// unknownStatus is what a status this client has no mapping for is read as.
// It is logged, so that the mapping gets written.
func unknownStatus(provider, status string) Status {
	log.Warn().Str("provider", provider).Str("status", status).Msg("provider reported a status this client does not know")
	return StatusUnknown
}

// statusDetail is the provider's own wording, kept for a machine that
// failed, was taken back or is in a status this client does not know: the
// ones somebody has to look into.
func statusDetail(status Status, wording string) string {
	switch status {
	case StatusFailed, StatusInterrupted, StatusUnknown:
		return wording
	}
	return ""
}

// sshPort is where a machine's SSH server listens, unless its provider says
// otherwise.
const sshPort = 22

// reachableAt records a machine's public address, where its SSH server
// listens too. A machine that has no address yet is left without one.
func (i *Instance) reachableAt(address string) {
	if address != "" {
		i.PublicIP, i.SSHHost, i.SSHPort = address, address, sshPort
	}
}

// maxPages is how many pages of one list are read. No account has a list
// that long: a provider that sends more is sending pages without end, and a
// client that read on would never return.
const maxPages = 1000

// tooManyPages is the error of a list that went past maxPages.
func tooManyPages(list string) error {
	return fmt.Errorf("%s: the provider sent more than %d pages of a list that should have ended", list, maxPages)
}

// offersByRegion gathers what each of the regions sells that matches the
// filter. A region that cannot be read is logged and left out, so that the
// others can still be bought in. When none of them can be read, that is the
// error, and it says what each lacks.
func offersByRegion(provider string, regions []string, filter OfferFilter, read func(region string) ([]Offer, error)) ([]Offer, error) {
	var offers []Offer
	var unread []error
	asked := 0
	for _, region := range regions {
		if len(filter.Regions) > 0 && !slices.Contains(filter.Regions, region) {
			continue
		}
		asked++
		sold, err := read(region)
		if err != nil {
			log.Warn().Err(err).Str("provider", provider).Str("region", region).Msg("could not read what a region sells; its offers are left out")
			unread = append(unread, err)
			continue
		}
		for _, offer := range sold {
			if filter.Matches(offer) {
				offers = append(offers, offer)
			}
		}
	}
	if asked > 0 && len(unread) == asked {
		return nil, errors.Join(unread...)
	}
	return offers, nil
}

// sortOffers puts offers in the order a client lists them in: by region,
// GPU and number of GPUs, and among those the cheapest first. A buyer
// chooses by its own rules; the order is there so that the same stock is
// listed the same way twice.
func sortOffers(offers []Offer) {
	slices.SortFunc(offers, func(a, b Offer) int {
		return cmp.Or(
			cmp.Compare(a.Region, b.Region),
			cmp.Compare(a.GPU, b.GPU),
			cmp.Compare(a.GPUCount, b.GPUCount),
			cmp.Compare(a.HourlyCostMicros, b.HourlyCostMicros),
			cmp.Compare(a.ID, b.ID),
		)
	})
}

func containsAny(text string, words ...string) bool {
	return slices.ContainsFunc(words, func(word string) bool { return strings.Contains(text, word) })
}

// bootScript wraps a create request's startup commands as the script a
// provider runs when the machine boots.
func bootScript(commands string) string {
	return fmt.Sprintf("#!/usr/bin/env bash\nset -euo pipefail\n%s\n", commands)
}
