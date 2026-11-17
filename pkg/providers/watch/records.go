package watch

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/beam-cloud/beta9/pkg/providers"
)

// recordVersion is written in every record. A reader that meets a version it
// does not know leaves the record out.
const recordVersion = 1

// Records name their columns once and then list their rows as arrays, which
// takes a fraction of the room that an object for each row does. A reader
// finds a column by its name, so a column can be added without breaking the
// readers that came before it.
type wire[P any, C any] struct {
	V     int          `json:"v"`
	TS    int64        `json:"ts"`
	GPU   string       `json:"gpu"`
	Polls map[string]P `json:"polls"`
	Cols  []string     `json:"cols"`
	Rows  [][]C        `json:"rows"`
}

// cells reads one row by the names of its columns. A column the record does
// not have reads as nothing.
type cells struct {
	at  map[string]int
	row []json.RawMessage
}

func columnIndex(cols []string) map[string]int {
	at := make(map[string]int, len(cols))
	for i, col := range cols {
		at[col] = i
	}
	return at
}

func (c cells) read(col string, into any) {
	if i, ok := c.at[col]; ok && i < len(c.row) {
		_ = json.Unmarshal(c.row[i], into)
	}
}

func (c cells) str(col string) (value string) {
	c.read(col, &value)
	return value
}

func (c cells) int(col string) (value int64) {
	var number json.Number
	c.read(col, &number)
	if whole, err := number.Int64(); err == nil {
		return whole
	}
	fraction, _ := number.Float64()
	return int64(fraction)
}

func (c cells) float(col string) (value float64) {
	c.read(col, &value)
	return value
}

func (c cells) flag(col string) bool {
	return c.int(col) != 0
}

// Poll is how a provider answered when it was asked what it sells. It is in
// every sample, so that a time with no rows for a provider reads as the
// provider not answering, and not as the provider having nothing.
type Poll struct {
	OK bool `json:"ok"`
	// MS is how long the provider took, in milliseconds.
	MS int64 `json:"ms"`
	// N is how many offers it listed, of every kind.
	N   int    `json:"n,omitempty"`
	Err string `json:"err,omitempty"`
}

// Sample is what one GPU type sold for, everywhere, at one time.
type Sample struct {
	At    time.Time
	GPU   string
	Polls map[string]Poll
	Rows  []Row
}

var sampleColumns = []string{"provider", "cloud", "region", "geo", "spot", "gpus", "n", "avail", "avail_kind", "min", "p50", "max", "offer", "zone"}

func (s Sample) MarshalJSON() ([]byte, error) {
	rows := make([][]any, 0, len(s.Rows))
	for _, row := range s.Rows {
		rows = append(rows, []any{
			row.Provider, row.Cloud, row.Region, row.Geo, btoi(row.Spot), row.GPUs,
			row.Offers, row.Stock, row.Kind, row.Min, row.Median, row.Max, row.Offer, row.Zone,
		})
	}
	return json.Marshal(wire[Poll, any]{
		V: recordVersion, TS: s.At.UnixMilli(), GPU: s.GPU, Polls: s.Polls, Cols: sampleColumns, Rows: rows,
	})
}

func (s *Sample) UnmarshalJSON(data []byte) error {
	var in wire[Poll, json.RawMessage]
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	if in.V != recordVersion {
		return fmt.Errorf("sample of version %d", in.V)
	}
	*s = Sample{At: time.UnixMilli(in.TS).UTC(), GPU: in.GPU, Polls: in.Polls, Rows: make([]Row, 0, len(in.Rows))}
	at := columnIndex(in.Cols)
	for _, raw := range in.Rows {
		c := cells{at, raw}
		s.Rows = append(s.Rows, Row{
			Listing: listingFrom(c),
			Offers:  int(c.int("n")),
			Stock:   uint32(c.int("avail")),
			Kind:    StockKind(c.str("avail_kind")),
			Min:     c.int("min"),
			Median:  c.int("p50"),
			Max:     c.int("max"),
			Offer:   c.str("offer"),
			Zone:    c.str("zone"),
		})
	}
	return nil
}

func listingFrom(c cells) Listing {
	return Listing{
		Provider: c.str("provider"),
		Cloud:    c.str("cloud"),
		Region:   c.str("region"),
		Geo:      c.str("geo"),
		Spot:     c.flag("spot"),
		GPUs:     uint32(c.int("gpus")),
	}
}

// PollHour is how a provider answered over an hour.
type PollHour struct {
	// Polls is how often it was asked, and Answered how often it answered.
	Polls    int `json:"n"`
	Answered int `json:"ok"`
	// MS is how long it took to answer, on average.
	MS int64 `json:"ms"`
}

// Hourly is what an hour of one GPU type's samples comes to.
type Hourly struct {
	// At is when the hour began.
	At    time.Time
	GPU   string
	Polls map[string]PollHour
	Rows  []HourRow
}

// HourRow is a listing over an hour. Its prices are of the cheapest offer,
// for one GPU for an hour, over the samples the listing was in stock in.
type HourRow struct {
	Listing
	Kind StockKind
	// Open and Close are the first and the last price of the hour. Mean
	// weighs each price by how long it stood.
	Open, Close, Min, Max, Mean int64
	// InStock is the share of the provider's answers in which the listing
	// was in stock.
	InStock float64
	// MinStock and MaxStock are the least and the most machines that could
	// be bought, for a provider that counts them.
	MinStock, MaxStock uint32
	// Answers is how often the provider answered.
	Answers int
	// Offer is the instance type that was cheapest last.
	Offer string
}

var hourColumns = []string{"provider", "cloud", "region", "geo", "spot", "gpus", "avail_kind", "open", "close", "min", "max", "mean", "in_stock", "min_avail", "max_avail", "polls", "offer"}

func (h Hourly) MarshalJSON() ([]byte, error) {
	rows := make([][]any, 0, len(h.Rows))
	for _, row := range h.Rows {
		rows = append(rows, []any{
			row.Provider, row.Cloud, row.Region, row.Geo, btoi(row.Spot), row.GPUs, row.Kind,
			row.Open, row.Close, row.Min, row.Max, row.Mean, row.InStock, row.MinStock, row.MaxStock, row.Answers, row.Offer,
		})
	}
	return json.Marshal(wire[PollHour, any]{
		V: recordVersion, TS: h.At.UnixMilli(), GPU: h.GPU, Polls: h.Polls, Cols: hourColumns, Rows: rows,
	})
}

func (h *Hourly) UnmarshalJSON(data []byte) error {
	var in wire[PollHour, json.RawMessage]
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	if in.V != recordVersion {
		return fmt.Errorf("hour of version %d", in.V)
	}
	*h = Hourly{At: time.UnixMilli(in.TS).UTC(), GPU: in.GPU, Polls: in.Polls, Rows: make([]HourRow, 0, len(in.Rows))}
	at := columnIndex(in.Cols)
	for _, raw := range in.Rows {
		c := cells{at, raw}
		h.Rows = append(h.Rows, HourRow{
			Listing:  listingFrom(c),
			Kind:     StockKind(c.str("avail_kind")),
			Open:     c.int("open"),
			Close:    c.int("close"),
			Min:      c.int("min"),
			Max:      c.int("max"),
			Mean:     c.int("mean"),
			InStock:  c.float("in_stock"),
			MinStock: uint32(c.int("min_avail")),
			MaxStock: uint32(c.int("max_avail")),
			Answers:  int(c.int("polls")),
			Offer:    c.str("offer"),
		})
	}
	return nil
}

// offerTable is a provider's offers, column by column, with every field an
// offer has.
type offerTable struct {
	Cols []string            `json:"cols"`
	Rows [][]json.RawMessage `json:"rows"`
}

var offerColumns = []string{
	"id", "zone", "type", "cloud", "region", "geo", "gpu", "gpus", "cpu_m", "mem_mb", "disk_mb",
	"micros", "spot", "avail", "rel", "name", "category", "region_name", "lat", "lng",
}

// sortedOffers puts offers in one order, so that the same stock is written
// the same way whatever order its provider listed it in.
func sortedOffers(offers []providers.Offer) []providers.Offer {
	sorted := slices.Clone(offers)
	slices.SortFunc(sorted, func(a, b providers.Offer) int {
		return cmp.Or(
			cmp.Compare(a.Region, b.Region),
			cmp.Compare(a.ID, b.ID),
			cmp.Compare(a.Cloud, b.Cloud),
			cmp.Compare(btoi(a.Spot), btoi(b.Spot)),
			cmp.Compare(a.HourlyCostMicros, b.HourlyCostMicros),
		)
	})
	return sorted
}

// encodeOffers writes offers column by column.
func encodeOffers(offers []providers.Offer) ([]byte, error) {
	rows := make([][]any, 0, len(offers))
	for _, offer := range sortedOffers(offers) {
		// The zone is in the ID of a provider that sells by zone. It is
		// written out for whoever reads the record without this code.
		zone, _ := place(offer)
		rows = append(rows, []any{
			offer.ID, zone, offer.InstanceType, offer.Cloud, offer.Region, Geo(offer.Region), offer.GPU, offer.GPUCount,
			offer.CPUMillicores, offer.MemoryMB, offer.StorageMB, offer.HourlyCostMicros, btoi(offer.Spot),
			offer.Available, offer.Reliability, offer.DisplayName, offer.Category, offer.RegionDisplayName,
			offer.Latitude, offer.Longitude,
		})
	}
	return json.Marshal(struct {
		Cols []string `json:"cols"`
		Rows [][]any  `json:"rows"`
	}{offerColumns, rows})
}

// decodeOffers reads back what encodeOffers wrote of a provider's offers.
func decodeOffers(provider string, data []byte) ([]providers.Offer, error) {
	var table offerTable
	if err := json.Unmarshal(data, &table); err != nil {
		return nil, err
	}
	at := columnIndex(table.Cols)
	offers := make([]providers.Offer, 0, len(table.Rows))
	for _, raw := range table.Rows {
		c := cells{at, raw}
		offers = append(offers, providers.Offer{
			ID:                c.str("id"),
			Provider:          provider,
			Cloud:             c.str("cloud"),
			InstanceType:      c.str("type"),
			Region:            c.str("region"),
			GPU:               c.str("gpu"),
			GPUCount:          uint32(c.int("gpus")),
			CPUMillicores:     c.int("cpu_m"),
			MemoryMB:          c.int("mem_mb"),
			StorageMB:         c.int("disk_mb"),
			HourlyCostMicros:  c.int("micros"),
			Spot:              c.flag("spot"),
			Available:         uint32(c.int("avail")),
			Reliability:       c.float("rel"),
			DisplayName:       c.str("name"),
			Category:          c.str("category"),
			RegionDisplayName: c.str("region_name"),
			Latitude:          c.float("lat"),
			Longitude:         c.float("lng"),
		})
	}
	return offers, nil
}

// hashOf names what was encoded, so that two samples of the same stock are
// known to be the same without being compared.
func hashOf(encoded []byte) string {
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:12])
}

func compress(data []byte) []byte {
	var packed bytes.Buffer
	writer := gzip.NewWriter(&packed)
	_, _ = writer.Write(data)
	_ = writer.Close()
	return packed.Bytes()
}

func decompress(packed []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(packed))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// The kinds of record in a provider's snapshots.
const (
	// SnapshotFull holds every offer the provider listed.
	SnapshotFull = "full"
	// SnapshotSame says the provider listed what it listed in the full
	// record before it.
	SnapshotSame = "same"
	// SnapshotError says the provider did not answer.
	SnapshotError = "error"
)

// Snapshot is everything a provider said when it was asked what it sells.
// Its offers are written in full when they differ from the last that were,
// and at least once an hour, so that a reader never goes far back for them.
type Snapshot struct {
	At       time.Time
	Provider string
	Kind     string
	Poll     Poll
	// Hash names the offers, and Since is when they were last written in
	// full.
	Hash  string
	Since time.Time
	// Sampler is the gateway that asked.
	Sampler string
	Offers  []providers.Offer
}

type snapshotWire struct {
	V        int    `json:"v"`
	TS       int64  `json:"ts"`
	Provider string `json:"provider"`
	Kind     string `json:"kind"`
	Poll
	Hash    string `json:"hash,omitempty"`
	Since   int64  `json:"since,omitempty"`
	Sampler string `json:"sampler,omitempty"`
	// Offers is the offers column by column, compressed with gzip and
	// written in base64.
	Offers string `json:"offers,omitempty"`
}

func (s Snapshot) MarshalJSON() ([]byte, error) {
	out := snapshotWire{
		V: recordVersion, TS: s.At.UnixMilli(), Provider: s.Provider, Kind: s.Kind, Poll: s.Poll,
		Hash: s.Hash, Sampler: s.Sampler,
	}
	if !s.Since.IsZero() {
		out.Since = s.Since.UnixMilli()
	}
	if s.Kind == SnapshotFull {
		encoded, err := encodeOffers(s.Offers)
		if err != nil {
			return nil, err
		}
		out.Offers = base64.StdEncoding.EncodeToString(compress(encoded))
	}
	return json.Marshal(out)
}

func (s *Snapshot) UnmarshalJSON(data []byte) error {
	var in snapshotWire
	if err := json.Unmarshal(data, &in); err != nil {
		return err
	}
	if in.V != recordVersion {
		return fmt.Errorf("snapshot of version %d", in.V)
	}
	*s = Snapshot{
		At: time.UnixMilli(in.TS).UTC(), Provider: in.Provider, Kind: in.Kind, Poll: in.Poll,
		Hash: in.Hash, Sampler: in.Sampler,
	}
	if in.Since > 0 {
		s.Since = time.UnixMilli(in.Since).UTC()
	}
	if in.Offers == "" {
		return nil
	}
	packed, err := base64.StdEncoding.DecodeString(in.Offers)
	if err != nil {
		return err
	}
	encoded, err := decompress(packed)
	if err != nil {
		return err
	}
	s.Offers, err = decodeOffers(in.Provider, encoded)
	return err
}
