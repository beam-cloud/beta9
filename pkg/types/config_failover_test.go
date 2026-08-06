package types

import (
	"strings"
	"testing"
	"time"
)

func TestFailoverConfigValidate(t *testing.T) {
	base := func() FailoverConfig {
		return FailoverConfig{
			Enabled: true,
			Chains: map[string]FailoverChain{
				"A10G": {
					Pools: []string{"rtx4090"},
					OnDemand: &FailoverOnDemandStep{
						MaxNodes: 2,
					},
				},
			},
		}
	}
	pools := map[string]WorkerPoolConfig{
		"rtx4090": {GPUType: "RTX4090"},
	}

	tests := []struct {
		name    string
		mutate  func(*FailoverConfig, map[string]WorkerPoolConfig)
		wantErr string
	}{
		{name: "valid"},
		{
			name: "case insensitive duplicate chain",
			mutate: func(config *FailoverConfig, _ map[string]WorkerPoolConfig) {
				config.Chains["a10g"] = FailoverChain{Pools: []string{"other"}}
			},
			wantErr: "same GPU",
		},
		{
			name: "configured on demand pool collision",
			mutate: func(_ *FailoverConfig, pools map[string]WorkerPoolConfig) {
				pools["ondemand-a10g"] = WorkerPoolConfig{GPUType: "A10G"}
			},
			wantErr: "conflicts with a configured worker pool",
		},
		{
			name: "explicit on demand pool",
			mutate: func(config *FailoverConfig, _ map[string]WorkerPoolConfig) {
				chain := config.Chains["A10G"]
				chain.Pools = append(chain.Pools, "ondemand-a10g")
				config.Chains["A10G"] = chain
			},
			wantErr: "lists managed onDemand pool",
		},
		{
			name: "negative hourly budget",
			mutate: func(config *FailoverConfig, _ map[string]WorkerPoolConfig) {
				config.OnDemand.Budget.MaxHourlyCents = -1
			},
			wantErr: "budgets cannot be negative",
		},
		{
			name: "negative idle window",
			mutate: func(config *FailoverConfig, _ map[string]WorkerPoolConfig) {
				config.OnDemand.ScaleDownAfterIdle = -time.Minute
			},
			wantErr: "scaleDownAfterIdle cannot be negative",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := base()
			testPools := make(map[string]WorkerPoolConfig, len(pools))
			for name, pool := range pools {
				testPools[name] = pool
			}
			if test.mutate != nil {
				test.mutate(&config, testPools)
			}
			err := config.Validate(testPools)
			if test.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Validate() error = %v, want substring %q", err, test.wantErr)
			}
		})
	}
}

func TestFailoverOnDemandPoolName(t *testing.T) {
	if got := FailoverOnDemandPoolName(" A10G "); got != "ondemand-a10g" {
		t.Fatalf("FailoverOnDemandPoolName() = %q", got)
	}
}
