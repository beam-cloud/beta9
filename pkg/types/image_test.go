package types

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSnapshotImageStub(t *testing.T) {
	const stubID = "11111111-2222-3333-4444-555555555555"
	imageID := SnapshotImageID(stubID, time.Unix(1700000000, 0))
	require.Equal(t, stubID+"-1700000000", imageID)

	got, ok := SnapshotImageStub(imageID)
	require.True(t, ok)
	require.Equal(t, stubID, got)

	for _, imageID := range []string{"0123456789abcdef", "pending-image-lookup", "stub-0", "stub-+1700000000", "-1700000000", "stub-", ""} {
		_, ok := SnapshotImageStub(imageID)
		require.False(t, ok, imageID)
	}
}
