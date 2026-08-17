package types

import (
	"fmt"
	"testing"
)

func TestBuildScopedCacheRequiredContentRevisionMultipartAndTombstone(t *testing.T) {
	const (
		scope      = "36eb1f5c-e9ed-464a-bd98-cc35d5d068bc"
		revisionID = "12614665-148e-405b-9cc3-6e1b06f659d9"
	)
	items := make([]CacheRequiredContentItem, 0, CacheRequiredContentMaxItemsPerPart+1)
	for index := 0; index < CacheRequiredContentMaxItemsPerPart+1; index++ {
		kind := CacheContentKindStateChunk
		if index%17 == 0 {
			kind = CacheContentKindStateManifest
		}
		hash := fmt.Sprintf("hash-%04d", index)
		items = append(items, CacheRequiredContentItem{
			Hash: hash, RoutingKey: hash, ExpectedHash: hash, SizeBytes: int64(index + 1),
			Kind: kind, VolumeID: scope, GenerationID: revisionID,
		})
	}
	records, err := BuildScopedCacheRequiredContentRevision("workspace", "stub", "node", scope, 9, revisionID, items, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 || len(records[0].Items) != CacheRequiredContentMaxItemsPerPart || len(records[1].Items) != 1 || !records[2].Commit {
		t.Fatalf("unexpected multipart revision: %+v", records)
	}
	for index, record := range records {
		if record.Scope != scope || record.RevisionGeneration != 9 || record.RevisionID != revisionID ||
			!record.Replace || record.Kind != "" || record.PartCount != 2 || record.ItemCount != len(items) || record.SetDigest == "" {
			t.Fatalf("record %d changed whole-set metadata: %+v", index, record)
		}
	}
	if len(records[2].Items) != 0 {
		t.Fatalf("commit marker contains items: %+v", records[2])
	}

	tombstone, err := BuildScopedCacheRequiredContentRevision("workspace", "stub", "node", scope, 10,
		"1670704b-7589-49b8-be95-0015315125f7", nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(tombstone) != 1 || !tombstone[0].Commit || !tombstone[0].Tombstone || tombstone[0].PartCount != 0 ||
		tombstone[0].ItemCount != 0 || tombstone[0].TotalBytes != 0 || tombstone[0].SetDigest == "" {
		t.Fatalf("invalid scoped tombstone: %+v", tombstone)
	}
}
