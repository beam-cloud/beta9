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
