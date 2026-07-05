package types

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSafeDurableDiskName(t *testing.T) {
	require.Equal(t, "pg-data", SafeDurableDiskName("pg-data"))
	require.Equal(t, "pg-data", SafeDurableDiskName("pg/data"))
	require.Equal(t, "disk", SafeDurableDiskName(".."))
}

func TestCheckpointModelCacheVolumeName(t *testing.T) {
	require.Equal(t, "checkpoint-model-cache-qwen", CheckpointModelCacheVolumeName("qwen"))
	require.Equal(t, "checkpoint-model-cache-qwen-prod", CheckpointModelCacheVolumeName("qwen/prod"))
	require.Equal(t, "checkpoint-model-cache", CheckpointModelCacheVolumeName(".."))
}

func TestDatabaseServingConfigKindHelpers(t *testing.T) {
	require.Equal(t, DatabaseKindPostgres, NormalizeDatabaseKind(" PostgreSQL "))
	require.True(t, (&DatabaseServingConfig{Kind: "postgresql"}).IsPostgres())
	require.True(t, (&DatabaseServingConfig{Kind: "valkey"}).IsRedisCompatible())
	require.False(t, (&DatabaseServingConfig{Kind: "mysql"}).IsRedisCompatible())
}
