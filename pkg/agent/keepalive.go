package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// KeepalivePayload is the request body for keepalive updates
type KeepalivePayload struct {
	MachineID    string           `json:"machine_id"`
	ProviderName string           `json:"provider_name"`
	PoolName     string           `json:"pool_name"`
	AgentVersion string           `json:"agent_version"`
	Metrics      *MachineMetrics  `json:"metrics"`
	Inference    *InferenceStatus `json:"inference,omitempty"`
}

type InferenceStatus struct {
	Status string   `json:"status"` // stopped, starting, running, error
	IP     string   `json:"ip,omitempty"`
	Port   int      `json:"port,omitempty"`
	Models []string `json:"models,omitempty"`
}

// KeepaliveLoop manages periodic keepalive updates to the gateway
type KeepaliveLoop struct {
	config              *AgentConfig
	metricsCollector    *MetricsCollector
	state               *AgentState
	mu                  sync.RWMutex
	lastMetrics         *MachineMetrics
	consecutiveFailures int32
	maxFailures         int32
	stopCh              chan struct{}
	doneCh              chan struct{}
}

// NewKeepaliveLoop creates a new keepalive loop
func NewKeepaliveLoop(config *AgentConfig) *KeepaliveLoop {
	return &KeepaliveLoop{
		config:           config,
		metricsCollector: NewMetricsCollector(),
		maxFailures:      3,
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
	}
}

// NewKeepaliveLoopWithState creates a keepalive loop that updates agent state
func NewKeepaliveLoopWithState(config *AgentConfig, state *AgentState) *KeepaliveLoop {
	return &KeepaliveLoop{
		config:           config,
		metricsCollector: NewMetricsCollector(),
		state:            state,
		maxFailures:      3,
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
	}
}

// GetLastMetrics returns the last collected metrics
func (k *KeepaliveLoop) GetLastMetrics() *MachineMetrics {
	k.mu.RLock()
	defer k.mu.RUnlock()
	if k.lastMetrics != nil {
		return k.lastMetrics
	}
	return &MachineMetrics{}
}

// Start begins the keepalive loop in a goroutine
func (k *KeepaliveLoop) Start(ctx context.Context) {
	go k.run(ctx)
}

func (k *KeepaliveLoop) run(ctx context.Context) {
	defer close(k.doneCh)

	log.Info().
		Dur("interval", k.config.KeepaliveInterval).
		Msg("Started keepalive loop")

	// Send first keepalive immediately
	k.sendKeepalive(ctx)

	ticker := time.NewTicker(k.config.KeepaliveInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("Keepalive loop stopped (context cancelled)")
			return
		case <-k.stopCh:
			log.Info().Msg("Keepalive loop stopped (stop signal)")
			return
		case <-ticker.C:
			k.sendKeepalive(ctx)
		}
	}
}

// Stop signals the keepalive loop to stop and waits for it to finish
func (k *KeepaliveLoop) Stop() {
	close(k.stopCh)
	// Wait for loop to finish with timeout
	select {
	case <-k.doneCh:
	case <-time.After(5 * time.Second):
		log.Warn().Msg("Keepalive loop did not stop within timeout")
	}
}

// IsHealthy returns true if recent keepalives succeeded
func (k *KeepaliveLoop) IsHealthy() bool {
	return atomic.LoadInt32(&k.consecutiveFailures) < k.maxFailures
}

func (k *KeepaliveLoop) sendKeepalive(ctx context.Context) bool {
	metrics, err := k.metricsCollector.Collect()
	if err != nil {
		log.Warn().Err(err).Msg("Failed to collect metrics")
		metrics = &MachineMetrics{}
	}

	// Store last metrics for TUI (protected by mutex)
	k.mu.Lock()
	k.lastMetrics = metrics
	k.mu.Unlock()

	payload := &KeepalivePayload{
		MachineID:    k.config.MachineID,
		ProviderName: k.config.ProviderName,
		PoolName:     k.config.PoolName,
		AgentVersion: "0.2.0-go",
		Metrics:      metrics,
	}

	// Add inference status if state is available
	if k.state != nil {
		inferenceObj := &InferenceStatus{
			Status: k.state.InferenceStatus,
			Port:   k.state.InferencePort,
			Models: k.state.InferenceModels,
		}
		// If inference is running, use Tailscale IP
		if k.state.InferenceStatus == "running" && k.state.InferenceIP != "" {
			inferenceObj.IP = k.state.InferenceIP
		}

		payload.Inference = inferenceObj
	}

	log.Debug().
		Float64("cpu_pct", metrics.CpuUtilizationPct).
		Float64("mem_pct", metrics.MemoryUtilizationPct).
		Int("free_gpu", metrics.FreeGpuCount).
		Msg("Sending keepalive")

	if k.config.DryRun {
		log.Info().Msg("Dry run - skipping keepalive")
		return true
	}

	body, err := json.Marshal(payload)
	if err != nil {
		log.Error().Err(err).Msg("Failed to marshal keepalive payload")
		return false
	}

	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(ctx, "POST", k.config.KeepaliveURL(), bytes.NewReader(body))
	if err != nil {
		log.Error().Err(err).Msg("Failed to create keepalive request")
		return false
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", k.config.Token))
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		failures := atomic.AddInt32(&k.consecutiveFailures, 1)
		log.Warn().
			Err(err).
			Int32("failure_count", failures).
			Msg("Keepalive connection failed")
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		atomic.StoreInt32(&k.consecutiveFailures, 0)
		log.Debug().
			Str("machine_id", k.config.MachineID).
			Msg("Keepalive successful")
		if k.state != nil {
			k.state.UpdateHeartbeat(true)
		}
		return true
	}

	failures := atomic.AddInt32(&k.consecutiveFailures, 1)
	log.Warn().
		Int("status", resp.StatusCode).
		Int32("failure_count", failures).
		Int32("max_failures", k.maxFailures).
		Msg("Keepalive failed")
	if k.state != nil {
		k.state.UpdateHeartbeat(false)
	}
	return false
}

// SendSingleKeepalive sends a single keepalive update (for testing/once mode)
func SendSingleKeepalive(ctx context.Context, config *AgentConfig) bool {
	loop := NewKeepaliveLoop(config)
	return loop.sendKeepalive(ctx)
}
