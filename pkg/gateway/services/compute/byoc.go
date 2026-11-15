package compute

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	byocBootstrapJoinTokenHashLabel            = "join_token_hash"
	byocBootstrapJoinTokenHashesLabel          = "join_token_hashes"
	byocDeletedPoolRetention                   = 5 * time.Minute
	byocProviderControlUnavailableCleanupGrace = 30 * time.Minute
)

var (
	errBYOCProviderControlUnavailable = errors.New("BYOC provider control plane is unavailable")
	errBYOCProviderResourceNotFound   = errors.New("BYOC provider resource not found")
)

// A BYOC pool runs on machines in the tenant's own cloud account. AWS is the
// one cloud there is; byoc_aws.go has what is particular to it, and this file
// the pool's record, its join tokens, its events and its place in the
// scheduler.

// byocPoolRequest is what a tenant asks for when setting up or resizing a
// BYOC pool: as it was sent, and then as normalizeAWSBYOCPoolRequest returns
// it.
type byocPoolRequest struct {
	poolName     string
	region       string
	instanceType string
	desiredNodes uint32
	maxNodes     uint32
	accountID    string
	gpu          string
	gpuCount     uint32
}

type byocProviderResource struct {
	Provider          string
	AccountID         string
	Region            string
	ResourceName      string
	ResourceURL       string
	DestroyURL        string
	InstanceType      string
	GPU               string
	GPUCount          uint32
	DesiredNodes      uint32
	MaxNodes          uint32
	TargetSandboxes   uint32
	SandboxesPerNode  uint32
	HourlyCostMicros  int64
	TotalHourlyMicros int64
	DirectScale       bool
}

func (s *Service) CreateBYOCPool(ctx context.Context, in *pb.CreateBYOCPoolRequest) (*pb.CreateBYOCPoolResponse, error) {
	switch strings.ToLower(strings.TrimSpace(in.Provider)) {
	case "":
		return &pb.CreateBYOCPoolResponse{Ok: false, ErrMsg: "provider is required"}, nil
	case string(model.SourceAWS):
	default:
		return &pb.CreateBYOCPoolResponse{Ok: false, ErrMsg: fmt.Sprintf("unsupported BYOC provider %q", in.Provider)}, nil
	}
	state, setupURL, err := s.prepareBYOCPoolSetup(ctx, byocPoolRequest{
		poolName:     in.PoolName,
		region:       in.Region,
		instanceType: in.InstanceType,
		desiredNodes: in.DesiredNodes,
		maxNodes:     in.MaxNodes,
		accountID:    in.AccountId,
		gpu:          in.Gpu,
		gpuCount:     in.GpuCount,
	})
	if err != nil {
		return &pb.CreateBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	pool := s.privatePoolStateToProto(state)
	return &pb.CreateBYOCPoolResponse{
		Ok:           true,
		Pool:         pool,
		SetupUrl:     setupURL,
		ResourceName: state.BYOC.ResourceName,
		ResourceUrl:  state.BYOC.ResourceURL,
		Byoc:         pool.GetByoc(),
	}, nil
}

// prepareBYOCPoolSetup saves the pool a tenant asked for and returns it with
// the link at which the tenant creates its stack.
func (s *Service) prepareBYOCPoolSetup(ctx context.Context, raw byocPoolRequest) (*model.PoolState, string, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	actorTokenID := computeActorTokenID(authInfo)
	if workspaceID == "" || actorTokenID == "" {
		return nil, "", fmt.Errorf("missing workspace auth")
	}

	req, err := normalizeAWSBYOCPoolRequest(raw)
	if err != nil {
		return nil, "", err
	}

	var state *model.PoolState
	var joinToken string
	lockErr := s.withPoolStateLock(ctx, workspaceID, req.poolName, func(lockCtx context.Context) error {
		var err error
		state, joinToken, err = s.createOrUpdateBYOCPool(lockCtx, authInfo, req)
		return err
	})
	if lockErr != nil {
		return nil, "", lockErr
	}

	setupURL, err := s.awsBYOCSetupURL(workspaceID, joinToken, req, state.BYOC)
	if err != nil {
		return nil, "", err
	}

	s.emitComputeEvent(types.EventComputePool, types.EventComputeSchema{
		WorkspaceID: workspaceID,
		PoolName:    state.Name,
		Action:      types.EventComputeActionPoolCreated,
		Status:      "byoc_pool_configured",
		Source:      string(model.SourceAWS),
		Transport:   state.Transport,
		Fallback:    state.Fallback,
		NodeCount:   req.desiredNodes,
		Attrs: map[string]string{
			"provider":      string(model.SourceAWS),
			"cloud":         string(model.SourceAWS),
			"region":        req.region,
			"instance_type": req.instanceType,
			"gpu":           req.gpu,
			"gpu_count":     strconv.FormatUint(uint64(req.gpuCount), 10),
		},
	})
	return state, setupURL, nil
}

func (s *Service) GetBYOCPool(ctx context.Context, in *pb.GetBYOCPoolRequest) (*pb.GetBYOCPoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}

	state, err := s.getWorkspacePrivatePoolState(ctx, authInfo, strings.TrimSpace(in.GetPoolName()))
	if err != nil {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if state == nil {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}

	machines, deleted, err := s.poolMachines(ctx, workspaceID, state)
	if err != nil {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if deleted {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: "pool not found"}, nil
	}
	pool := s.privatePoolStateToProtoWithMachines(state, machines)
	if pool.GetByoc() == nil {
		return &pb.GetBYOCPoolResponse{Ok: false, ErrMsg: "pool is not a BYOC pool"}, nil
	}
	ready := pool.GetByoc().Phase == "ready"
	return &pb.GetBYOCPoolResponse{
		Ok:                true,
		Pool:              pool,
		Ready:             ready,
		ReadyMachineCount: pool.ReadyMachineCount,
		MachineCount:      pool.MachineCount,
		Byoc:              pool.GetByoc(),
	}, nil
}

func (s *Service) ScaleBYOCPool(ctx context.Context, in *pb.ScaleBYOCPoolRequest) (*pb.ScaleBYOCPoolResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	workspaceID := computeWorkspaceID(authInfo)
	if workspaceID == "" {
		return &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: "missing workspace auth"}, nil
	}
	poolName := strings.TrimSpace(in.GetPoolName())
	if poolName == "" {
		return &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: "pool name is required"}, nil
	}

	var response *pb.ScaleBYOCPoolResponse
	lockErr := s.withPoolStateLock(ctx, workspaceID, poolName, func(lockCtx context.Context) error {
		state, err := s.getWorkspacePrivatePoolState(lockCtx, authInfo, poolName)
		if err != nil {
			return err
		}
		if state == nil {
			return errors.New("pool not found")
		}
		if state.BYOC == nil {
			return errors.New("missing BYOC provider metadata")
		}
		resource, err := awsBYOCResource(state.BYOC, s.appConfig.ManagedCompute)
		if err != nil {
			return err
		}
		req, err := normalizeAWSBYOCPoolRequest(byocPoolRequest{
			poolName:     state.Name,
			region:       resource.Region,
			instanceType: resource.InstanceType,
			desiredNodes: in.GetDesiredNodes(),
			maxNodes:     firstNonZeroUint32(in.GetMaxNodes(), in.GetDesiredNodes()),
			accountID:    resource.AccountID,
			gpu:          resource.GPU,
			gpuCount:     resource.GPUCount,
		})
		if err != nil {
			return err
		}
		if err := awsBYOCScale(lockCtx, state.BYOC, req); err != nil {
			return err
		}
		updateAWSBYOCScaleState(state, req, s.appConfig.ManagedCompute)
		state.UpdatedAt = time.Now().UTC()
		if err := s.savePrivatePoolState(lockCtx, workspaceID, state); err != nil {
			return err
		}
		machines, err := s.computeRepo.ListAgentTokenStates(lockCtx, workspaceID, state.Name)
		if err != nil {
			return err
		}
		machines, err = s.pruneBYOCPoolMachinesAboveDesired(lockCtx, workspaceID, state, machines, time.Now().UTC())
		if err != nil {
			return err
		}
		pool := s.privatePoolStateToProtoWithMachines(state, machines)
		s.emitComputeEvent(types.EventComputePool, computePoolEvent(workspaceID, state, types.EventComputeActionPoolExtended, "byoc_scaled"))
		response = &pb.ScaleBYOCPoolResponse{
			Ok:   true,
			Pool: pool,
			Byoc: pool.GetByoc(),
		}
		return nil
	})
	if lockErr != nil {
		return &pb.ScaleBYOCPoolResponse{Ok: false, ErrMsg: lockErr.Error()}, nil
	}
	return response, nil
}

func byocPoolStateToProto(state *model.PoolState, pool *pb.PrivatePool, config types.ManagedComputeConfig) *pb.BYOCPoolState {
	if state == nil || state.BYOC == nil {
		return nil
	}
	resource, err := awsBYOCResource(state.BYOC, config)
	if err != nil {
		return nil
	}
	phase, message := byocPoolPhase(resource, pool)
	return &pb.BYOCPoolState{
		Provider:              resource.Provider,
		AccountId:             resource.AccountID,
		Region:                resource.Region,
		ResourceName:          resource.ResourceName,
		ResourceUrl:           resource.ResourceURL,
		DestroyUrl:            resource.DestroyURL,
		Phase:                 phase,
		DesiredNodes:          resource.DesiredNodes,
		MaxNodes:              resource.MaxNodes,
		TargetSandboxes:       resource.TargetSandboxes,
		SandboxesPerNode:      resource.SandboxesPerNode,
		InstanceType:          resource.InstanceType,
		Gpu:                   resource.GPU,
		GpuCount:              resource.GPUCount,
		MachineCount:          pool.MachineCount,
		ReadyMachineCount:     pool.ReadyMachineCount,
		Message:               message,
		HourlyCostMicros:      resource.HourlyCostMicros,
		TotalHourlyCostMicros: resource.TotalHourlyMicros,
		DirectScaleEnabled:    resource.DirectScale,
	}
}

func byocPoolGPUs(req byocPoolRequest) []string {
	if req.gpu == "" || req.gpuCount == 0 {
		return nil
	}
	return []string{req.gpu}
}

func (s *Service) cleanupDeletedBYOCPoolIfNeeded(ctx context.Context, workspaceID string, state *model.PoolState, machines []*model.AgentTokenState, now time.Time) (bool, error) {
	if !byocPoolDeletedForCleanup(ctx, state, machines, now) {
		return false, nil
	}

	deleted := false
	err := s.withPoolStateLock(ctx, workspaceID, state.Name, func(lockCtx context.Context) error {
		fresh, err := s.getPrivatePoolState(lockCtx, workspaceID, state.Name)
		if err != nil || fresh == nil {
			return err
		}
		freshMachines, err := s.computeRepo.ListAgentTokenStates(lockCtx, workspaceID, fresh.Name)
		if err != nil {
			return err
		}
		if !byocPoolDeletedForCleanup(ctx, fresh, freshMachines, now) {
			return nil
		}
		deleted = true
		return s.deleteBYOCPoolLocalState(lockCtx, workspaceID, fresh, freshMachines, "cloud_resource_deleted")
	})
	return deleted, err
}

func (s *Service) pruneBYOCPoolMachinesAboveDesired(ctx context.Context, workspaceID string, state *model.PoolState, machines []*model.AgentTokenState, now time.Time) ([]*model.AgentTokenState, error) {
	if state == nil || state.BYOC == nil || len(machines) == 0 {
		return machines, nil
	}
	resource, err := awsBYOCResource(state.BYOC, s.appConfig.ManagedCompute)
	if err != nil {
		return machines, err
	}
	desiredNodes := int(resource.DesiredNodes)
	if desiredNodes < 1 || len(machines) <= desiredNodes {
		return machines, nil
	}

	stale := staleBYOCMachinesAboveDesired(machines, desiredNodes, now)
	if len(stale) == 0 {
		return machines, nil
	}
	remove := map[string]bool{}
	for _, machine := range stale {
		if err := s.removePrivateMachine(ctx, machine); err != nil {
			return machines, err
		}
		remove[machine.MachineID] = true
	}
	kept := machines[:0]
	for _, machine := range machines {
		if machine == nil || remove[machine.MachineID] {
			continue
		}
		kept = append(kept, machine)
	}
	return kept, nil
}

func staleBYOCMachinesAboveDesired(machines []*model.AgentTokenState, desiredNodes int, now time.Time) []*model.AgentTokenState {
	excess := len(machines) - desiredNodes
	candidates := make([]*model.AgentTokenState, 0, excess)
	for _, machine := range machines {
		if machine == nil || model.AgentMachineConnected(machine, now) {
			continue
		}
		candidates = append(candidates, machine)
	}
	if len(candidates) == 0 {
		return nil
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		left := model.AgentMachineLastSeen(candidates[i])
		right := model.AgentMachineLastSeen(candidates[j])
		if left.Equal(right) {
			return candidates[i].MachineID < candidates[j].MachineID
		}
		if left.IsZero() {
			return false
		}
		if right.IsZero() {
			return true
		}
		return left.Before(right)
	})
	if len(candidates) > excess {
		candidates = candidates[:excess]
	}
	return candidates
}

// byocPoolDeletedForCleanup reports whether the pool's cloud resource is
// gone, so that what Beam holds for it is to be cleaned up. A provider that
// can be asked decides: machines fall silent for many reasons, and a pool
// that is cleaned up takes the token in the tenant's stack with it.
func byocPoolDeletedForCleanup(ctx context.Context, state *model.PoolState, machines []*model.AgentTokenState, now time.Time) bool {
	if state == nil || state.BYOC == nil {
		return false
	}
	deleted, err := awsBYOCStackDeleted(ctx, state.BYOC)
	switch {
	case err == nil:
		return deleted
	case errors.Is(err, errBYOCDirectScaleUnavailable):
		// The stack gave Beam no role to ask with. All there is to go by is
		// that its machines stay silent.
		return byocPoolLooksDeleted(machines, now)
	case errors.Is(err, errBYOCProviderControlUnavailable), errors.Is(err, errBYOCProviderResourceNotFound):
		return byocProviderResourceUnavailableLooksDeleted(state, machines, now)
	default:
		// The provider did not answer. The next read asks again.
		return false
	}
}

func byocProviderResourceUnavailableLooksDeleted(state *model.PoolState, machines []*model.AgentTokenState, now time.Time) bool {
	if byocPoolMachinesAllDisconnected(machines, now) {
		return true
	}
	if byocPoolLooksDeleted(machines, now) {
		return true
	}
	if len(machines) > 0 || state.CreatedAt.IsZero() {
		return false
	}
	return now.Sub(state.CreatedAt) >= byocProviderControlUnavailableCleanupGrace
}

func byocPoolMachinesAllDisconnected(machines []*model.AgentTokenState, now time.Time) bool {
	if len(machines) == 0 {
		return false
	}
	for _, machine := range machines {
		if machine == nil || model.AgentMachineConnected(machine, now) || machine.LastDisconnectAt.IsZero() {
			return false
		}
	}
	return true
}

// Without provider credentials, external stack deletion is observable as all
// previously joined BYOC machines disconnecting and staying gone.
func byocPoolLooksDeleted(machines []*model.AgentTokenState, now time.Time) bool {
	if len(machines) == 0 {
		return false
	}
	latestSeen := time.Time{}
	for _, machine := range machines {
		if model.AgentMachineConnected(machine, now) {
			return false
		}
		lastSeen := model.AgentMachineLastSeen(machine)
		if lastSeen.IsZero() {
			return false
		}
		if lastSeen.After(latestSeen) {
			latestSeen = lastSeen
		}
	}
	return now.Sub(latestSeen) >= byocDeletedPoolRetention
}

func (s *Service) deleteBYOCPoolLocalState(ctx context.Context, workspaceID string, state *model.PoolState, machines []*model.AgentTokenState, status string) error {
	for _, machine := range machines {
		if err := s.removePrivateMachine(ctx, machine); err != nil {
			return err
		}
	}
	if err := s.revokeBYOCBootstrapJoinTokens(ctx, state); err != nil {
		return err
	}
	if err := s.deletePrivatePoolState(ctx, workspaceID, state.Name); err != nil {
		return err
	}
	if s.scheduler != nil {
		s.scheduler.DeleteAgentPool(workspaceID, state)
	}
	event := computePoolEvent(workspaceID, state, types.EventComputeActionPoolDeleted, status)
	event.MachineCount = uint32(len(machines))
	event.Message = "BYOC cloud resource is gone; cleaned up local pool state"
	s.emitComputeEvent(types.EventComputePool, event)
	return nil
}

// createOrUpdateBYOCPool saves the pool's record together with a new token for
// its stack to join with, and returns both. The caller holds the pool lock,
// so the one save cannot undo what the reconciler wrote in between.
func (s *Service) createOrUpdateBYOCPool(ctx context.Context, authInfo *auth.AuthInfo, req byocPoolRequest) (*model.PoolState, string, error) {
	workspaceID := computeWorkspaceID(authInfo)
	actorTokenID := computeActorTokenID(authInfo)
	if _, exists := s.appConfig.Worker.Pools[req.poolName]; exists {
		return nil, "", fmt.Errorf("pool %q conflicts with a configured worker pool", req.poolName)
	}
	existing, err := s.getPrivatePoolState(ctx, workspaceID, req.poolName)
	if err != nil {
		return nil, "", err
	}
	if existing != nil && !poolStateIsPrivate(existing) {
		return nil, "", fmt.Errorf("pool already exists in this workspace")
	}
	if existing != nil && existing.Source.Canonical() != model.SourceAWS {
		return nil, "", fmt.Errorf("pool already exists with source %s", existing.Source)
	}
	if err := validateExistingAWSBYOCPool(existing, req); err != nil {
		return nil, "", err
	}
	if err := validateAWSBYOCSetupConfig(s.appConfig.ManagedCompute); err != nil {
		return nil, "", err
	}

	config := normalizePoolConfig(&pb.PoolConfig{
		Name:      req.poolName,
		Regions:   []string{req.region},
		Gpu:       byocPoolGPUs(req),
		Mode:      string(types.PoolModePrivate),
		Transport: defaultPrivateTransport,
		Fallback:  defaultPrivateFallback,
		Priority:  defaultPrivatePriority,
	})
	if _, err := computePoolFromProto(config, 0, false); err != nil {
		return nil, "", err
	}

	byocState := awsBYOCPoolState(workspaceID, req, s.appConfig.ManagedCompute)
	if existing != nil {
		for _, label := range []string{byocBootstrapJoinTokenHashLabel, byocBootstrapJoinTokenHashesLabel} {
			value := byocLabelValue(existing.BYOC, label)
			if value == "" {
				continue
			}
			if byocState.Labels == nil {
				byocState.Labels = map[string]string{}
			}
			byocState.Labels[label] = value
		}
	}
	state := nextPoolState(existing, config, actorTokenID, time.Now())
	state.Source = model.SourceAWS
	state.BYOC = byocState

	joinToken, tokenState, err := s.createPersistentPrivatePoolJoinToken(ctx, workspaceID, req.poolName, state.CreatedAt, "")
	if err != nil {
		return nil, "", err
	}
	recordBYOCBootstrapJoinTokenHash(state.BYOC, tokenState.TokenHash)
	if err := s.savePrivatePoolState(ctx, workspaceID, state); err != nil {
		_ = s.revokeComputeJoinTokenHash(ctx, tokenState.TokenHash)
		return nil, "", err
	}
	if s.scheduler != nil {
		if err := s.scheduler.EnsureAgentPool(workspaceID, state); err != nil {
			// The token was given to nobody, and must admit nobody.
			_ = s.revokeComputeJoinTokenHash(ctx, tokenState.TokenHash)
			return nil, "", s.rollbackPoolState(ctx, workspaceID, state.Name, existing, err)
		}
	}
	return state, joinToken, nil
}

func (s *Service) revokeBYOCBootstrapJoinTokens(ctx context.Context, state *model.PoolState) error {
	for _, tokenHash := range byocBootstrapJoinTokenHashes(state) {
		if err := s.revokeComputeJoinTokenHash(ctx, tokenHash); err != nil {
			return err
		}
	}
	return nil
}

func recordBYOCBootstrapJoinTokenHash(state *model.BYOCProviderState, tokenHash string) {
	tokenHash = strings.TrimSpace(tokenHash)
	if state == nil || tokenHash == "" {
		return
	}
	if state.Labels == nil {
		state.Labels = map[string]string{}
	}
	hashes := appendUniqueTokenHash(byocBootstrapJoinTokenHashesFromLabels(state.Labels), tokenHash)
	state.Labels[byocBootstrapJoinTokenHashLabel] = tokenHash
	state.Labels[byocBootstrapJoinTokenHashesLabel] = strings.Join(hashes, ",")
}

func byocBootstrapJoinTokenHashes(state *model.PoolState) []string {
	if state == nil || state.BYOC == nil {
		return nil
	}
	return byocBootstrapJoinTokenHashesFromLabels(state.BYOC.Labels)
}

func byocBootstrapJoinTokenHashesFromLabels(labels map[string]string) []string {
	if labels == nil {
		return nil
	}
	hashes := make([]string, 0, 2)
	for _, tokenHash := range strings.Split(labels[byocBootstrapJoinTokenHashesLabel], ",") {
		hashes = appendUniqueTokenHash(hashes, tokenHash)
	}
	return appendUniqueTokenHash(hashes, labels[byocBootstrapJoinTokenHashLabel])
}

func appendUniqueTokenHash(hashes []string, tokenHash string) []string {
	tokenHash = strings.TrimSpace(tokenHash)
	if tokenHash == "" {
		return hashes
	}
	for _, existing := range hashes {
		if existing == tokenHash {
			return hashes
		}
	}
	return append(hashes, tokenHash)
}

func byocLabelValue(state *model.BYOCProviderState, key string) string {
	if state == nil || state.Labels == nil {
		return ""
	}
	return strings.TrimSpace(state.Labels[key])
}

func byocPoolPhase(resource *byocProviderResource, pool *pb.PrivatePool) (string, string) {
	desiredNodes := resource.DesiredNodes
	if desiredNodes == 0 {
		desiredNodes = 1
	}
	switch {
	case pool.ReadyMachineCount >= desiredNodes:
		return "ready", "Pool is ready to run sandboxes"
	case pool.ReadyMachineCount > 0:
		return "partially_ready", fmt.Sprintf("%d/%d nodes connected", pool.ReadyMachineCount, desiredNodes)
	case pool.MachineCount > 0:
		return "nodes_booting", fmt.Sprintf("%d node%s detected", pool.MachineCount, pluralSuffix(pool.MachineCount))
	default:
		return "waiting_for_provider", "Waiting for cloud resources to launch nodes"
	}
}

func pluralSuffix(count uint32) string {
	if count == 1 {
		return ""
	}
	return "s"
}
