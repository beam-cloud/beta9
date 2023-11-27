package repository

import (
	"strings"
	"testing"

	"github.com/beam-cloud/beam/internal/types"
	"github.com/tj/assert"
)

func TestIdentityQuotaRedis(t *testing.T) {
	test := []struct {
		name     string
		identity string
		quota    *types.IdentityQuota
	}{
		{
			name:     "quota",
			identity: "test-identity",
			quota: &types.IdentityQuota{
				CpuConcurrencyLimit: 1,
			},
		},
	}

	for _, tc := range test {
		t.Run(tc.name, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			assert.NotNil(t, rdb)
			assert.Nil(t, err)

			repo := NewIdentityRedisRepositoryForTest(rdb)
			assert.NotNil(t, repo)

			err = repo.SetIdentityQuota(tc.identity, tc.quota)
			assert.Nil(t, err)

			quota, err := repo.GetIdentityQuota(tc.identity)
			assert.Nil(t, err)
			assert.Equal(t, tc.quota, quota)
		})
	}
}

func TestSetActiveContainerWithQuota(t *testing.T) {
	tests := []struct {
		name                string
		identity            string
		quota               *types.IdentityQuota
		currentContainerIds []string
		gpuType             string
		shouldFail          bool
	}{
		{
			name:     "quota-should-pass-cpu",
			identity: "test-identity",
			quota: &types.IdentityQuota{
				CpuConcurrencyLimit: 1,
			},
			currentContainerIds: []string{"test-container-1"},
			gpuType:             strings.ToLower(types.NO_GPU.String()),
			shouldFail:          true,
		},
		{
			name:     "quota-should-fail-cpu",
			identity: "test-identity",
			quota: &types.IdentityQuota{
				CpuConcurrencyLimit: 2,
			},
			currentContainerIds: []string{"test-container-1"},
			gpuType:             strings.ToLower(types.NO_GPU.String()),
			shouldFail:          false,
		},
		{
			name:     "quota-should-pass-gpu",
			identity: "test-identity",
			quota: &types.IdentityQuota{
				GpuConcurrencyLimit: 2,
			},
			currentContainerIds: []string{"test-container-1"},
			gpuType:             strings.ToLower(types.GPU_A10G.String()),
			shouldFail:          false,
		},
		{
			name:     "quota-should-fail-gpu",
			identity: "test-identity",
			quota: &types.IdentityQuota{
				GpuConcurrencyLimit: 2,
			},
			currentContainerIds: []string{"test-container-1", "test-container-2"},
			gpuType:             strings.ToLower(types.GPU_A10G.String()),
			shouldFail:          true,
		},
		{
			name:     "quota-should-pass-gpu-and-cpu",
			identity: "test-identity",
			quota: &types.IdentityQuota{
				CpuConcurrencyLimit: 2,
				GpuConcurrencyLimit: 2,
			},
			currentContainerIds: []string{"test-container-1"},
			gpuType:             strings.ToLower(types.GPU_A10G.String()),
			shouldFail:          false,
		},
		{
			name:     "quota-should-pass-gpu-and-cpu",
			identity: "test-identity",
			quota: &types.IdentityQuota{
				CpuConcurrencyLimit: 2,
				GpuConcurrencyLimit: 2,
			},
			currentContainerIds: []string{"test-container-1", "test-container-2"},
			gpuType:             strings.ToLower(types.GPU_A10G.String()),
			shouldFail:          true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rdb, err := NewRedisClientForTest()
			assert.NotNil(t, rdb)
			assert.Nil(t, err)

			repo := NewIdentityRedisRepositoryForTest(rdb)
			assert.NotNil(t, repo)

			err = repo.SetIdentityQuota(tc.identity, tc.quota)
			assert.Nil(t, err)

			for _, containerId := range tc.currentContainerIds {
				err := repo.SetIdentityActiveContainer(tc.identity, tc.quota, containerId, tc.gpuType)
				assert.Nil(t, err)
			}

			// Now try adding 1 more container
			err = repo.SetIdentityActiveContainer(tc.identity, tc.quota, "adding-new-test-container-for-test", tc.gpuType)
			if tc.shouldFail {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
