package compute

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
	shellpkg "github.com/beam-cloud/beta9/pkg/abstractions/shell"
	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/clients"
	"github.com/beam-cloud/beta9/pkg/common"
	model "github.com/beam-cloud/beta9/pkg/compute"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

const (
	rentalWorkloadKindPod      = "pod"
	rentalWorkloadKindShell    = "shell"
	marketplaceRentalUsageKind = "rental"

	rentalErrNotFound        = "rental not found"
	rentalShellWaitTimeout   = 5 * time.Minute
	rentalUsageEmitTimeout   = 30 * time.Second
	rentalWorkloadMinCPU     = int64(1000)
	rentalWorkloadMinMemory  = int64(1024)
	rentalKeepWarmForever    = -1
	rentalContainerWaitPoll  = time.Second
	rentalWorkloadNamePrefix = "rental"
)

// CreateMarketplaceRental locks N GPUs on one seller machine for the buyer.
// The lock is exclusive: serverless marketplace scheduling stops seeing that
// capacity, and only the buyer's machine-pinned workloads consume it.
func (s *Service) CreateMarketplaceRental(ctx context.Context, in *pb.CreateMarketplaceRentalRequest) (*pb.CreateMarketplaceRentalResponse, error) {
	authCtx := marketplaceAuthFromContext(ctx)
	if !authCtx.hasOwner() {
		return &pb.CreateMarketplaceRentalResponse{Ok: false, ErrMsg: marketplaceErrMissingAuth}, nil
	}

	listing, err := s.computeRepo.GetMarketplaceListingByID(ctx, strings.TrimSpace(in.GetListingId()))
	if err != nil {
		return &pb.CreateMarketplaceRentalResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if listing == nil || listing.Status != model.MarketplaceListingStatusActive {
		return &pb.CreateMarketplaceRentalResponse{Ok: false, ErrMsg: marketplaceErrListingNotFound}, nil
	}

	gpuCount := in.GetGpuCount()
	if gpuCount == 0 {
		gpuCount = 1
	}

	machine, errMsg := s.rentalMachineForListing(ctx, listing, strings.TrimSpace(in.GetMachineId()), gpuCount)
	if errMsg != "" {
		return &pb.CreateMarketplaceRentalResponse{Ok: false, ErrMsg: errMsg}, nil
	}
	machineID := machine.MachineID

	// The capacity check and the rental write must be atomic per machine, or
	// two concurrent reserves could both claim the same free GPUs.
	if err := s.computeRepo.LockMachineRentals(ctx, machineID); err != nil {
		return &pb.CreateMarketplaceRentalResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	defer func() {
		// A failed release blocks reserves on this machine until the lock's
		// TTL lapses; surface it instead of stalling silently.
		if err := s.computeRepo.UnlockMachineRentals(machineID); err != nil {
			log.Warn().Err(err).Str("machine_id", machineID).Msg("failed to release machine rental lock")
		}
	}()

	unrented, errMsg := s.unrentedGPUsOnMachine(ctx, machine)
	if errMsg != "" {
		return &pb.CreateMarketplaceRentalResponse{Ok: false, ErrMsg: errMsg}, nil
	}
	if gpuCount > unrented {
		return &pb.CreateMarketplaceRentalResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("requested %d GPUs but only %d are unrented on this machine", gpuCount, unrented),
		}, nil
	}

	now := time.Now().UTC()
	rental := &model.MarketplaceRentalState{
		ID:                   model.MarketplaceRentalID(authCtx.workspaceID, machineID),
		BuyerWorkspaceID:     authCtx.workspaceID,
		SellerWorkspaceID:    listing.SellerWorkspaceID,
		ListingID:            listing.ID,
		PoolName:             listing.PoolName,
		MachineID:            machineID,
		GPU:                  listing.GPU,
		GPUCount:             gpuCount,
		PricePerGPUHourCents: listing.PricePerGPUHourCents,
		CreatedAt:            now,
		LastBilledAt:         now,
	}
	if err := s.computeRepo.SaveMarketplaceRental(ctx, rental); err != nil {
		return &pb.CreateMarketplaceRentalResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.CreateMarketplaceRentalResponse{Ok: true, Rental: s.marketplaceRentalToProto(ctx, rental)}, nil
}

func (s *Service) ListMarketplaceRentals(ctx context.Context, in *pb.ListMarketplaceRentalsRequest) (*pb.ListMarketplaceRentalsResponse, error) {
	authCtx := marketplaceAuthFromContext(ctx)
	if !authCtx.hasWorkspace() {
		return &pb.ListMarketplaceRentalsResponse{Ok: false, ErrMsg: marketplaceErrMissingAuth}, nil
	}
	rentals, err := s.computeRepo.ListMarketplaceRentals(ctx, authCtx.workspaceID)
	if err != nil {
		return &pb.ListMarketplaceRentalsResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	out := make([]*pb.MarketplaceRental, 0, len(rentals))
	for _, rental := range rentals {
		out = append(out, s.marketplaceRentalToProto(ctx, rental))
	}
	return &pb.ListMarketplaceRentalsResponse{Ok: true, Rentals: out}, nil
}

// DeleteMarketplaceRental releases the hold: the buyer's containers on the
// machine are stopped and the GPUs return to the serverless pool.
func (s *Service) DeleteMarketplaceRental(ctx context.Context, in *pb.DeleteMarketplaceRentalRequest) (*pb.DeleteMarketplaceRentalResponse, error) {
	authCtx := marketplaceAuthFromContext(ctx)
	if !authCtx.hasOwner() {
		return &pb.DeleteMarketplaceRentalResponse{Ok: false, ErrMsg: marketplaceErrMissingAuth}, nil
	}
	rental, err := s.computeRepo.GetMarketplaceRental(ctx, authCtx.workspaceID, strings.TrimSpace(in.GetRentalId()))
	if err != nil {
		return &pb.DeleteMarketplaceRentalResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if rental == nil {
		return &pb.DeleteMarketplaceRentalResponse{Ok: true}, nil
	}

	s.stopRentalContainers(ctx, rental)
	if err := s.computeRepo.DeleteMarketplaceRental(ctx, rental); err != nil {
		return &pb.DeleteMarketplaceRentalResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	return &pb.DeleteMarketplaceRentalResponse{Ok: true}, nil
}

// LaunchRentalWorkload starts a pod or an SSH-able shell on the rental's
// machine. The gateway creates the machine-pinned stub itself so the pin is
// only reachable through an owned rental, never through the public stub API.
func (s *Service) LaunchRentalWorkload(ctx context.Context, in *pb.LaunchRentalWorkloadRequest) (*pb.LaunchRentalWorkloadResponse, error) {
	authInfo, authenticated := auth.AuthInfoFromContext(ctx)
	if !authenticated || authInfo.Workspace == nil || authInfo.Token == nil {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: marketplaceErrMissingAuth}, nil
	}

	rental, err := s.computeRepo.GetMarketplaceRental(ctx, authInfo.Workspace.ExternalId, strings.TrimSpace(in.GetRentalId()))
	if err != nil {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if rental == nil {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: rentalErrNotFound}, nil
	}

	machine, err := s.computeRepo.GetAgentMachineState(ctx, rental.SellerWorkspaceID, rental.PoolName, rental.MachineID)
	if err != nil {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if machine == nil || !model.AgentMachineConnected(machine, time.Now()) {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: "machine is not connected"}, nil
	}

	kind := strings.ToLower(strings.TrimSpace(in.GetKind()))
	if kind != rentalWorkloadKindPod && kind != rentalWorkloadKindShell {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: "kind must be \"pod\" or \"shell\""}, nil
	}
	if strings.TrimSpace(in.GetImageId()) == "" {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: "image id is required"}, nil
	}
	if kind == rentalWorkloadKindPod && len(in.GetCommand()) == 0 {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: "command is required for pod workloads"}, nil
	}

	gpuCount := in.GetGpuCount()
	if gpuCount == 0 || gpuCount > rental.GPUCount {
		gpuCount = rental.GPUCount
	}

	stub, stubConfig, err := s.createRentalStub(ctx, authInfo.Workspace, rental, machine, kind, in, gpuCount)
	if err != nil {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	switch kind {
	case rentalWorkloadKindShell:
		return s.launchRentalShell(ctx, authInfo, rental, stub, stubConfig)
	default:
		return s.launchRentalPod(ctx, authInfo, rental, stub, stubConfig, in.GetCommand())
	}
}

// rentalMachineForListing resolves the machine a rental locks. Buyers usually
// reserve from an offer without seeing individual machines, so an empty
// machine id picks the connected machine with the most unrented GPUs.
func (s *Service) rentalMachineForListing(ctx context.Context, listing *model.MarketplaceListingState, machineID string, gpuCount uint32) (*model.AgentTokenState, string) {
	now := time.Now()

	if machineID != "" {
		machine, err := s.computeRepo.GetAgentMachineState(ctx, listing.SellerWorkspaceID, listing.PoolName, machineID)
		if err != nil {
			return nil, err.Error()
		}
		if machine == nil || !model.AgentMachineConnected(machine, now) {
			return nil, "machine is not connected"
		}
		// Capacity is validated by the caller under the machine's rental lock.
		return machine, ""
	}

	machines, err := s.computeRepo.ListAgentTokenStates(ctx, listing.SellerWorkspaceID, listing.PoolName)
	if err != nil {
		return nil, err.Error()
	}
	var best *model.AgentTokenState
	bestUnrented := uint32(0)
	for _, machine := range machines {
		if machine == nil || !model.AgentMachineConnected(machine, now) {
			continue
		}
		unrented, errMsg := s.unrentedGPUsOnMachine(ctx, machine)
		if errMsg != "" {
			return nil, errMsg
		}
		if unrented >= gpuCount && (best == nil || unrented > bestUnrented) {
			best = machine
			bestUnrented = unrented
		}
	}
	if best == nil {
		return nil, fmt.Sprintf("no connected machine has %d unrented GPUs available", gpuCount)
	}
	return best, ""
}

func (s *Service) unrentedGPUsOnMachine(ctx context.Context, machine *model.AgentTokenState) (uint32, string) {
	rented, err := s.rentedGPUsOnMachine(ctx, machine.MachineID)
	if err != nil {
		return 0, err.Error()
	}
	if rented >= machine.GPUCount {
		return 0, ""
	}
	return machine.GPUCount - rented, ""
}

// rentedGPUsOnMachine sums every buyer's active rental GPUs on one machine.
func (s *Service) rentedGPUsOnMachine(ctx context.Context, machineID string) (uint32, error) {
	rentals, err := s.computeRepo.ListMarketplaceRentalsForMachine(ctx, machineID)
	if err != nil {
		return 0, err
	}
	total := uint32(0)
	for _, rental := range rentals {
		total += rental.GPUCount
	}
	return total, nil
}

func (s *Service) marketplaceRentalToProto(ctx context.Context, rental *model.MarketplaceRentalState) *pb.MarketplaceRental {
	if rental == nil {
		return nil
	}
	out := &pb.MarketplaceRental{
		Id:                   rental.ID,
		ListingId:            rental.ListingID,
		PoolName:             rental.PoolName,
		MachineId:            rental.MachineID,
		Gpu:                  rental.GPU,
		GpuCount:             rental.GPUCount,
		PricePerGpuHourCents: rental.PricePerGPUHourCents,
		CreatedAt:            timestampOrNil(rental.CreatedAt),
	}
	if listing, err := s.computeRepo.GetMarketplaceListing(ctx, rental.SellerWorkspaceID, rental.ListingID); err == nil && listing != nil {
		out.ListingName = listing.DisplayName
		out.Region = listing.Region
	}
	if machine, err := s.computeRepo.GetAgentMachineState(ctx, rental.SellerWorkspaceID, rental.PoolName, rental.MachineID); err == nil && machine != nil {
		out.MachineConnected = model.AgentMachineConnected(machine, time.Now())
	}
	return out
}

// stopRentalContainers stops the released rental's workloads — and only
// those. The buyer may hold other rentals on the same machine or have
// serverless containers there; matching on the rental's stub-name prefix
// keeps them running.
func (s *Service) stopRentalContainers(ctx context.Context, rental *model.MarketplaceRentalState) {
	if s.containerRepo == nil || s.scheduler == nil {
		return
	}
	containers, err := s.containerRepo.GetActiveContainersByWorkerId(model.AgentMachineWorkerID(rental.MachineID))
	if err != nil {
		return
	}
	stubPrefix := fmt.Sprintf("%s-%s-", rentalWorkloadNamePrefix, rental.ID)
	for _, container := range containers {
		if container.WorkspaceId != rental.BuyerWorkspaceID {
			continue
		}
		if !s.stubBelongsToRental(ctx, container.StubId, stubPrefix) {
			continue
		}
		_ = s.scheduler.Stop(&types.StopContainerArgs{
			ContainerId: container.ContainerId,
			Force:       true,
			Reason:      types.StopContainerReasonUser,
		})
	}
}

func (s *Service) stubBelongsToRental(ctx context.Context, stubExternalID, stubPrefix string) bool {
	if s.backendRepo == nil || stubExternalID == "" {
		return false
	}
	stub, err := s.backendRepo.GetStubByExternalId(ctx, stubExternalID)
	if err != nil || stub == nil {
		return false
	}
	return strings.HasPrefix(stub.Name, stubPrefix)
}

// createRentalStub registers the machine-pinned stub backing a rental
// workload. CPU and memory follow the rented GPU share of the machine.
func (s *Service) createRentalStub(
	ctx context.Context,
	workspace *types.Workspace,
	rental *model.MarketplaceRentalState,
	machine *model.AgentTokenState,
	kind string,
	in *pb.LaunchRentalWorkloadRequest,
	gpuCount uint32,
) (*types.StubWithRelated, *types.StubConfigV1, error) {
	cpu, memory := rentalWorkloadResources(machine, gpuCount)
	stubConfig := types.StubConfigV1{
		Runtime: types.Runtime{
			Cpu:      cpu,
			Memory:   memory,
			Gpus:     []types.GpuType{types.GpuType(rental.GPU)},
			GpuCount: gpuCount,
			ImageId:  strings.TrimSpace(in.GetImageId()),
		},
		AllowMarketplace: true,
		MachineID:        rental.MachineID,
		Ports:            in.GetPorts(),
		Env:              in.GetEnv(),
		Authorized:       false,
		TaskPolicy:       types.TaskPolicy{},
	}
	if kind == rentalWorkloadKindPod {
		stubConfig.EntryPoint = in.GetCommand()
		stubConfig.KeepWarmSeconds = rentalKeepWarmForever
	}

	name := fmt.Sprintf("%s-%s-%s", rentalWorkloadNamePrefix, rental.ID, kind)
	app, err := s.backendRepo.GetOrCreateApp(ctx, workspace.Id, name)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare app: %w", err)
	}
	object, err := abstractions.EnsureEmptyStubObject(ctx, s.backendRepo, workspace)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare stub object: %w", err)
	}

	stubType := types.StubTypePod
	if kind == rentalWorkloadKindShell {
		stubType = types.StubTypeShell
	}
	stub, err := s.backendRepo.GetOrCreateStub(ctx, name, stubType, stubConfig, object.Id, workspace.Id, true, app.Id)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create stub: %w", err)
	}

	return &types.StubWithRelated{Stub: stub, Workspace: *workspace, App: app, Object: object}, &stubConfig, nil
}

// rentalWorkloadResources sizes CPU and memory proportionally to the rented
// GPU share of the machine, so renting 2 of 8 GPUs also grants a quarter of
// the machine's CPU and memory.
func rentalWorkloadResources(machine *model.AgentTokenState, gpuCount uint32) (int64, int64) {
	cpu := machine.CPUMillicores
	if cpu == 0 {
		cpu = int64(machine.CPUCount) * 1000
	}
	memory := int64(machine.MemoryMB)
	if machine.GPUCount > 0 && gpuCount < machine.GPUCount {
		cpu = cpu * int64(gpuCount) / int64(machine.GPUCount)
		memory = memory * int64(gpuCount) / int64(machine.GPUCount)
	}
	return max(cpu, rentalWorkloadMinCPU), max(memory, rentalWorkloadMinMemory)
}

func (s *Service) launchRentalPod(
	ctx context.Context,
	authInfo *auth.AuthInfo,
	rental *model.MarketplaceRentalState,
	stub *types.StubWithRelated,
	stubConfig *types.StubConfigV1,
	command []string,
) (*pb.LaunchRentalWorkloadResponse, error) {
	containerId := fmt.Sprintf("pod-%s-%s", stub.ExternalId, uuid.New().String()[:8])
	request := s.rentalContainerRequest(authInfo, rental, stub, stubConfig, containerId, command, nil)

	if err := s.containerRepo.SetPodKeepWarmLock(ctx, authInfo.Workspace.Name, stub.ExternalId, containerId, rentalKeepWarmForever); err != nil {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if err := s.scheduler.Run(request); err != nil {
		// The keep-warm lock has no TTL; clear it so failed launches don't
		// leave permanent keys behind (setting 0 deletes the lock).
		_ = s.containerRepo.SetPodKeepWarmLock(ctx, authInfo.Workspace.Name, stub.ExternalId, containerId, 0)
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	return &pb.LaunchRentalWorkloadResponse{
		Ok:          true,
		ContainerId: containerId,
		StubId:      stub.ExternalId,
		Url:         common.BuildPodURL(s.appConfig.GatewayService.HTTP.GetExternalURL(), common.InvokeUrlTypePath, stub, stubConfig),
	}, nil
}

func (s *Service) launchRentalShell(
	ctx context.Context,
	authInfo *auth.AuthInfo,
	rental *model.MarketplaceRentalState,
	stub *types.StubWithRelated,
	stubConfig *types.StubConfigV1,
) (*pb.LaunchRentalWorkloadResponse, error) {
	if s.redisClient == nil {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: "shell launches are unavailable"}, nil
	}

	username, password := shellpkg.CredentialsForToken(*authInfo.Token)
	containerId := shellpkg.ContainerIDForStub(stub.ExternalId)
	env := []string{
		fmt.Sprintf("USERNAME=%s", username),
		fmt.Sprintf("PASSWORD=%s", password),
	}
	request := s.rentalContainerRequest(authInfo, rental, stub, stubConfig, containerId, shellpkg.StandaloneEntryPoint(), env)

	// The running shell service owns the container's lifecycle through this
	// TTL key, exactly like shells it creates itself.
	if err := shellpkg.SetInitialContainerTTL(ctx, s.redisClient, containerId); err != nil {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if err := s.scheduler.Run(request); err != nil {
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: err.Error()}, nil
	}
	if err := s.waitForRentalContainer(ctx, containerId, rentalShellWaitTimeout); err != nil {
		s.scheduler.Stop(&types.StopContainerArgs{ContainerId: containerId, Force: true, Reason: types.StopContainerReasonTtl})
		return &pb.LaunchRentalWorkloadResponse{Ok: false, ErrMsg: err.Error()}, nil
	}

	return &pb.LaunchRentalWorkloadResponse{
		Ok:           true,
		ContainerId:  containerId,
		StubId:       stub.ExternalId,
		ShellCommand: fmt.Sprintf("beam shell --container-id %s", containerId),
		Username:     username,
		Password:     password,
	}, nil
}

func (s *Service) rentalContainerRequest(
	authInfo *auth.AuthInfo,
	rental *model.MarketplaceRentalState,
	stub *types.StubWithRelated,
	stubConfig *types.StubConfigV1,
	containerId string,
	entryPoint []string,
	extraEnv []string,
) *types.ContainerRequest {
	env := append([]string{}, stubConfig.Env...)
	env = append(env,
		fmt.Sprintf("BETA9_TOKEN=%s", authInfo.Token.Key),
		fmt.Sprintf("STUB_ID=%s", stub.ExternalId),
	)
	env = append(env, extraEnv...)

	appId := ""
	if stub.App != nil {
		appId = stub.App.ExternalId
	}

	return &types.ContainerRequest{
		ContainerId:      containerId,
		StubId:           stub.ExternalId,
		AppId:            appId,
		Env:              env,
		Cpu:              stubConfig.Runtime.Cpu,
		Memory:           stubConfig.Runtime.Memory,
		GpuRequest:       []string{rental.GPU},
		GpuCount:         stubConfig.Runtime.GpuCount,
		ImageId:          stubConfig.Runtime.ImageId,
		WorkspaceId:      authInfo.Workspace.ExternalId,
		Workspace:        *authInfo.Workspace,
		EntryPoint:       entryPoint,
		Ports:            stubConfig.Ports,
		Stub:             *stub,
		AllowMarketplace: true,
		MachineId:        rental.MachineID,
	}
}

// emitRentalUsage bills held rental time — idle included, since rentals are
// wall-clock on-demand holds. Runs on the managed-compute reconcile interval;
// the billing service prices the intervals.
func (s *Service) emitRentalUsage(ctx context.Context, now time.Time) {
	if s.rentalUsage == nil || s.computeRepo == nil {
		return
	}
	rentals, err := s.computeRepo.ListAllMarketplaceRentals(ctx)
	if err != nil {
		log.Warn().Err(err).Msg("failed to list rentals for usage emission")
		return
	}
	for _, rental := range rentals {
		start := rental.LastBilledAt
		if start.IsZero() {
			start = rental.CreatedAt
		}
		if !now.After(start) {
			continue
		}
		duration := now.Sub(start)
		err := s.rentalUsage.Record(ctx, clients.MarketplaceUsageRequest{
			BuyerWorkspaceID:  rental.BuyerWorkspaceID,
			SellerWorkspaceID: rental.SellerWorkspaceID,
			ListingID:         rental.ListingID,
			MachineID:         rental.MachineID,
			UsageKind:         marketplaceRentalUsageKind,
			GPU:               rental.GPU,
			GPUCount:          rental.GPUCount,
			DurationSeconds:   duration.Seconds(),
			BuyerCostCents:    rentalIntervalCostCents(rental, duration),
			StartAt:           start.UTC(),
			EndAt:             now.UTC(),
			ContainerType:     marketplaceRentalUsageKind,
			RuntimeMetadata: map[string]interface{}{
				"pool_name":                rental.PoolName,
				"rental_id":                rental.ID,
				"price_per_gpu_hour_cents": rental.PricePerGPUHourCents,
			},
		})
		if err != nil {
			log.Warn().Err(err).Str("rental_id", rental.ID).Msg("failed to record rental usage")
			continue
		}
		rental.LastBilledAt = now
		if err := s.computeRepo.SaveMarketplaceRental(ctx, rental); err != nil {
			log.Warn().Err(err).Str("rental_id", rental.ID).Msg("failed to advance rental billing cursor")
		}
	}
}

// rentalIntervalCostCents prices a held interval at the rental's snapshotted
// per-GPU-hour rate. Zero when the seller listed without a price (billing
// service falls back to its own rates).
func rentalIntervalCostCents(rental *model.MarketplaceRentalState, duration time.Duration) float64 {
	if rental.PricePerGPUHourCents == 0 {
		return 0
	}
	return float64(rental.PricePerGPUHourCents) * float64(rental.GPUCount) * duration.Hours()
}

func (s *Service) waitForRentalContainer(ctx context.Context, containerId string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for container to start")
		}
		state, err := s.containerRepo.GetContainerState(containerId)
		if err == nil && state != nil && state.Status == types.ContainerStatusRunning {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(rentalContainerWaitPoll):
		}
	}
}
