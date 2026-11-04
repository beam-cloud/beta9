package worker

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/stretchr/testify/require"
)

func TestCheckpointMultipartPolicyIsBoundedAndOptIn(t *testing.T) {
	for _, value := range []string{"", "bad", "4", "65", "9223372036854775808"} {
		t.Setenv("BETA9_CHECKPOINT_MULTIPART_MIB", value)
		require.Equal(t, clients.StorageUploadOptions{}, checkpointUploadOptions())
	}
	t.Setenv("BETA9_CHECKPOINT_MULTIPART_MIB", "8")
	for _, value := range []string{"", "0", "9", "bad"} {
		t.Setenv("BETA9_CHECKPOINT_MULTIPART_CONCURRENCY", value)
		require.Equal(t, clients.StorageUploadOptions{PartSize: 8 << 20, Concurrency: 2}, checkpointUploadOptions())
	}
	t.Setenv("BETA9_CHECKPOINT_MULTIPART_CONCURRENCY", "1")
	require.Equal(t, 1, checkpointUploadOptions().Concurrency)
}
