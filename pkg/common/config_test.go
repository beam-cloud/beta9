package common

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfigUsesClipV2(t *testing.T) {
	cm := &ConfigManager[types.AppConfig]{
		kf:  koanf.New("."),
		tag: "key",
	}

	require.NoError(t, cm.LoadConfig(YAMLConfigFormat, rawbytes.Provider(defaultConfig)))

	config := cm.GetConfig()
	require.Equal(t, uint32(types.ClipVersion2), config.ImageService.ClipVersion)
}
