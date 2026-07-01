package vast

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type lease struct {
	GPUUUID  string
	Hostname string
	PID      int
	SeenAt   time.Time
}

type serviceState int

const (
	// The sidecar may restart while per-GPU agent units are still running.
	// Unknown units are stopped once if no active Vast sentinel lease appears.
	serviceStateUnknown serviceState = iota
	serviceStateStopped
	serviceStateRunning
)

type hostServer struct {
	opts          HostOptions
	token         string
	gpus          []GPU
	leases        map[string]lease
	serviceStates map[string]serviceState
	mu            sync.Mutex
}

type sentinelEvent struct {
	GPUUUID  string `json:"gpuUuid"`
	Hostname string `json:"hostname"`
	PID      int    `json:"pid"`
}

func RunHost(ctx context.Context, opts HostOptions) error {
	server, err := newHostServer(ctx, opts)
	if err != nil {
		return err
	}
	return server.run(ctx)
}

func newHostServer(ctx context.Context, opts HostOptions) (*hostServer, error) {
	if opts.Stdout == nil {
		opts.Stdout = io.Discard
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	opts.GatewayURL = strings.TrimRight(strings.TrimSpace(opts.GatewayURL), "/")
	if opts.GatewayURL == "" {
		return nil, fmt.Errorf("gateway is required")
	}
	opts.StateDir = firstNonEmpty(strings.TrimSpace(opts.StateDir), DefaultStateDir)
	opts.ListenAddr = firstNonEmpty(strings.TrimSpace(opts.ListenAddr), DefaultListenAddr)
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = DefaultLeaseTTL
	}
	if opts.ReconcilePeriod <= 0 {
		opts.ReconcilePeriod = DefaultReconcilePeriod
	}
	opts.ServiceTemplate = firstNonEmpty(strings.TrimSpace(opts.ServiceTemplate), DefaultGPUServiceName)
	if opts.Services == nil {
		opts.Services = SystemdController{}
	}
	if opts.Cleaner == nil {
		opts.Cleaner = AgentContainerCleaner{}
	}
	if opts.DetectGPUs == nil {
		opts.DetectGPUs = DetectNvidiaGPUs
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}

	token, err := readSecret(opts.SentinelToken, opts.SentinelTokenFile)
	if err != nil {
		return nil, err
	}
	if token == "" {
		return nil, fmt.Errorf("sentinel token is required")
	}

	gpus, err := opts.DetectGPUs(ctx)
	if err != nil {
		return nil, err
	}
	serviceStates := make(map[string]serviceState, len(gpus))
	for _, gpu := range gpus {
		serviceStates[gpu.UUID] = serviceStateUnknown
	}
	return &hostServer{
		opts:          opts,
		token:         token,
		gpus:          gpus,
		leases:        map[string]lease{},
		serviceStates: serviceStates,
	}, nil
}

func (h *hostServer) run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/heartbeat", h.handleHeartbeat)
	mux.HandleFunc("/preempt", h.handlePreempt)

	httpServer := &http.Server{Addr: h.opts.ListenAddr, Handler: mux}
	errCh := make(chan error, 1)
	go func() {
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	fmt.Fprintf(h.opts.Stdout, "vast host listening on %s\n", h.opts.ListenAddr)
	ticker := time.NewTicker(h.opts.ReconcilePeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = httpServer.Shutdown(shutdownCtx)
			return nil
		case err := <-errCh:
			return err
		case <-ticker.C:
			h.reconcile(ctx, time.Now().UTC())
		}
	}
}

func (h *hostServer) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	event, ok := h.decodeEvent(w, r)
	if !ok {
		return
	}
	if _, found := gpuByUUID(h.gpus, event.GPUUUID); !found {
		http.Error(w, "unknown gpu", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	h.leases[event.GPUUUID] = lease{
		GPUUUID:  event.GPUUUID,
		Hostname: event.Hostname,
		PID:      event.PID,
		SeenAt:   time.Now().UTC(),
	}
	h.mu.Unlock()
	writeJSON(w, map[string]any{"ok": true})
}

func (h *hostServer) handlePreempt(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	event, ok := h.decodeEvent(w, r)
	if !ok {
		return
	}
	gpu, found := gpuByUUID(h.gpus, event.GPUUUID)
	if !found {
		http.Error(w, "unknown gpu", http.StatusBadRequest)
		return
	}
	h.mu.Lock()
	delete(h.leases, event.GPUUUID)
	h.mu.Unlock()
	if err := h.withdrawGPU(r.Context(), gpu, "vast_preempt", true); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]any{"ok": true})
}

func (h *hostServer) decodeEvent(w http.ResponseWriter, r *http.Request) (sentinelEvent, bool) {
	if !h.authorized(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return sentinelEvent{}, false
	}
	defer r.Body.Close()
	var event sentinelEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return sentinelEvent{}, false
	}
	event.GPUUUID = strings.TrimSpace(event.GPUUUID)
	if event.GPUUUID == "" {
		http.Error(w, "gpuUuid is required", http.StatusBadRequest)
		return sentinelEvent{}, false
	}
	return event, true
}

func (h *hostServer) authorized(r *http.Request) bool {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
		return strings.TrimSpace(auth[len("bearer "):]) == h.token
	}
	return false
}

func (h *hostServer) reconcile(ctx context.Context, now time.Time) {
	for _, gpu := range h.gpus {
		if h.leaseActive(gpu.UUID, now) {
			if err := h.ensureGPU(ctx, gpu); err != nil {
				fmt.Fprintf(h.opts.Stderr, "vast gpu %s start failed: %v\n", gpu.Index, err)
			}
			continue
		}
		if err := h.withdrawGPU(ctx, gpu, "vast_sentinel_lost", false); err != nil {
			fmt.Fprintf(h.opts.Stderr, "vast gpu %s withdraw failed: %v\n", gpu.Index, err)
		}
	}
}

func (h *hostServer) leaseActive(uuid string, now time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	current, ok := h.leases[uuid]
	if !ok {
		return false
	}
	if now.Sub(current.SeenAt) > h.opts.LeaseTTL {
		delete(h.leases, uuid)
		return false
	}
	return true
}

func (h *hostServer) ensureGPU(ctx context.Context, gpu GPU) error {
	if !h.markServiceStarting(gpu.UUID) {
		return nil
	}
	unit := h.serviceUnit(gpu)
	if err := h.opts.Services.Start(ctx, unit); err != nil {
		h.noteServiceStopped(gpu.UUID)
		return err
	}
	fmt.Fprintf(h.opts.Stdout, "started %s for gpu %s\n", unit, gpu.Index)
	return nil
}

func (h *hostServer) withdrawGPU(ctx context.Context, gpu GPU, reason string, force bool) error {
	if !h.markServiceStopping(gpu.UUID, force) {
		return nil
	}

	stateDir := gpuStateDir(h.opts.StateDir, gpu)
	state, stateErr := loadRuntimeState(stateDir)
	if stateErr != nil {
		fmt.Fprintf(h.opts.Stderr, "failed to read agent state for gpu %s: %v\n", gpu.Index, stateErr)
	}
	gatewayURL := h.opts.GatewayURL
	if state != nil && state.GatewayURL != "" {
		gatewayURL = state.GatewayURL
	}
	if state != nil && state.AgentToken != "" {
		if err := updateAvailability(ctx, h.opts.HTTPClient, gatewayURL, state.AgentToken, false, reason); err != nil {
			fmt.Fprintf(h.opts.Stderr, "failed to update availability for gpu %s: %v\n", gpu.Index, err)
		}
	}

	unit := h.serviceUnit(gpu)
	stopErr := h.opts.Services.Stop(ctx, unit)
	if stopErr != nil {
		fmt.Fprintf(h.opts.Stderr, "failed to stop %s: %v\n", unit, stopErr)
	}
	if state != nil && state.MachineID != "" {
		if err := h.opts.Cleaner.RemoveManagedWorkerContainersForMachine(state.MachineID); err != nil {
			fmt.Fprintf(h.opts.Stderr, "failed to clean beam containers for machine %s: %v\n", state.MachineID, err)
		}
	}
	fmt.Fprintf(h.opts.Stdout, "withdrew %s for gpu %s: %s\n", unit, gpu.Index, reason)
	return stopErr
}

func (h *hostServer) markServiceStarting(gpuUUID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.serviceStates[gpuUUID] == serviceStateRunning {
		return false
	}
	h.serviceStates[gpuUUID] = serviceStateRunning
	return true
}

func (h *hostServer) markServiceStopping(gpuUUID string, force bool) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.serviceStates[gpuUUID] == serviceStateStopped && !force {
		return false
	}
	h.serviceStates[gpuUUID] = serviceStateStopped
	return true
}

func (h *hostServer) noteServiceStopped(gpuUUID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.serviceStates[gpuUUID] = serviceStateStopped
}

func (h *hostServer) serviceUnit(gpu GPU) string {
	return fmt.Sprintf(h.opts.ServiceTemplate, gpu.Index)
}

func readSecret(value, file string) (string, error) {
	value = strings.TrimSpace(value)
	if value != "" {
		return value, nil
	}
	file = strings.TrimSpace(file)
	if file == "" {
		return "", nil
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
}

func publicHostFromListen(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "<host-ip>"
	}
	return net.JoinHostPort(host, port)
}

func ensureDir(path string, perm os.FileMode) error {
	return os.MkdirAll(filepath.Clean(path), perm)
}
