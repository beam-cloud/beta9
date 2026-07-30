package scheduler

import (
	"testing"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/tj/assert"
)

func TestWorkerNodeSelector(t *testing.T) {
	jobSpec := types.WorkerPoolJobSpecConfig{
		NodeSelector: map[string]string{
			"karpenter.sh/nodepool": "gpu",
			"kubernetes.io/arch":    "amd64",
		},
		SingleGPUNodeSelector: map[string]string{
			"karpenter.sh/nodepool":                "gpu-single",
			"karpenter.k8s.aws/instance-gpu-count": "1",
		},
	}

	tests := []struct {
		name     string
		gpuCount uint32
		want     map[string]string
	}{
		{
			name: "CPU uses base selector",
			want: map[string]string{
				"karpenter.sh/nodepool": "gpu",
				"kubernetes.io/arch":    "amd64",
			},
		},
		{
			name:     "single GPU overlays selector",
			gpuCount: 1,
			want: map[string]string{
				"karpenter.sh/nodepool":                "gpu-single",
				"kubernetes.io/arch":                   "amd64",
				"karpenter.k8s.aws/instance-gpu-count": "1",
			},
		},
		{
			name:     "multiple GPUs use base selector",
			gpuCount: 2,
			want: map[string]string{
				"karpenter.sh/nodepool": "gpu",
				"kubernetes.io/arch":    "amd64",
			},
		},
		{
			name:     "four GPUs use base selector",
			gpuCount: 4,
			want: map[string]string{
				"karpenter.sh/nodepool": "gpu",
				"kubernetes.io/arch":    "amd64",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := workerNodeSelector(jobSpec, test.gpuCount)
			assert.Equal(t, test.want, got)

			got["mutated"] = "true"
			assert.Equal(t, "gpu", jobSpec.NodeSelector["karpenter.sh/nodepool"])
			assert.Equal(t, "gpu-single", jobSpec.SingleGPUNodeSelector["karpenter.sh/nodepool"])
			_, baseMutated := jobSpec.NodeSelector["mutated"]
			_, singleMutated := jobSpec.SingleGPUNodeSelector["mutated"]
			assert.False(t, baseMutated)
			assert.False(t, singleMutated)
		})
	}
}

func TestWorkerNodeSelectorSingleGPUWithoutBase(t *testing.T) {
	jobSpec := types.WorkerPoolJobSpecConfig{
		SingleGPUNodeSelector: map[string]string{
			"karpenter.sh/nodepool": "gpu-single",
		},
	}

	assert.Equal(t, map[string]string{
		"karpenter.sh/nodepool": "gpu-single",
	}, workerNodeSelector(jobSpec, 1))
	assert.Nil(t, workerNodeSelector(jobSpec, 0))
}

func TestWorkerJobBuildersUseGPUCountSelectorsWithoutMutatingConfig(t *testing.T) {
	jobSpec := types.WorkerPoolJobSpecConfig{
		NodeSelector: map[string]string{
			"karpenter.sh/nodepool": "gpu",
			"kubernetes.io/arch":    "amd64",
		},
		SingleGPUNodeSelector: map[string]string{
			"karpenter.sh/nodepool":                "gpu-single",
			"karpenter.k8s.aws/instance-gpu-count": "1",
		},
	}
	workerConfig := types.WorkerConfig{
		DefaultWorkerCPURequest:    1000,
		DefaultWorkerMemoryRequest: 1024,
	}
	localController := &LocalKubernetesWorkerPoolController{
		name:             "local-gpu",
		config:           types.AppConfig{Worker: workerConfig},
		workerPoolConfig: types.WorkerPoolConfig{JobSpec: jobSpec},
	}
	providerController := &ProviderWorkerPoolController{
		name:             "provider-gpu",
		config:           types.AppConfig{Worker: workerConfig},
		workerPoolConfig: types.WorkerPoolConfig{JobSpec: jobSpec},
	}

	builders := []struct {
		name  string
		build func(uint32) map[string]string
	}{
		{
			name: "local Job",
			build: func(gpuCount uint32) map[string]string {
				job, _ := localController.createWorkerJob("worker-local", 1000, 1024, "A10G", gpuCount, "token")
				return job.Spec.Template.Spec.NodeSelector
			},
		},
		{
			name: "provider Job",
			build: func(gpuCount uint32) map[string]string {
				job, _ := providerController.buildWorkerJob("worker-provider", "machine-provider", 1000, 1024, "A10G", gpuCount, nil)
				return job.Spec.Template.Spec.NodeSelector
			},
		},
	}
	tests := []struct {
		name     string
		gpuCount uint32
		want     map[string]string
	}{
		{
			name:     "one GPU overlays selector",
			gpuCount: 1,
			want: map[string]string{
				"karpenter.sh/nodepool":                "gpu-single",
				"kubernetes.io/arch":                   "amd64",
				"karpenter.k8s.aws/instance-gpu-count": "1",
			},
		},
		{
			name:     "two GPUs retain base selector",
			gpuCount: 2,
			want: map[string]string{
				"karpenter.sh/nodepool": "gpu",
				"kubernetes.io/arch":    "amd64",
			},
		},
		{
			name:     "four GPUs retain base selector",
			gpuCount: 4,
			want: map[string]string{
				"karpenter.sh/nodepool": "gpu",
				"kubernetes.io/arch":    "amd64",
			},
		},
	}

	for _, builder := range builders {
		t.Run(builder.name, func(t *testing.T) {
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					selector := builder.build(test.gpuCount)
					assert.Equal(t, test.want, selector)

					selector["mutated"] = "true"
					assert.Equal(t, "gpu", jobSpec.NodeSelector["karpenter.sh/nodepool"])
					assert.Equal(t, "gpu-single", jobSpec.SingleGPUNodeSelector["karpenter.sh/nodepool"])
					_, baseMutated := jobSpec.NodeSelector["mutated"]
					_, singleMutated := jobSpec.SingleGPUNodeSelector["mutated"]
					assert.False(t, baseMutated)
					assert.False(t, singleMutated)
				})
			}
		})
	}
}
