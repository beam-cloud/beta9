package gatewayservices

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	common "github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"k8s.io/utils/ptr"
)

func (gws *GatewayService) ListDeployments(ctx context.Context, in *pb.ListDeploymentsRequest) (*pb.ListDeploymentsResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	filter := types.DeploymentFilter{
		WorkspaceID: authInfo.Workspace.Id,
	}

	limit := uint32(1000)
	if in.Limit > 0 && in.Limit < limit {
		limit = in.Limit
	}
	filter.Limit = limit

	for field, value := range in.Filters {
		switch field {
		case "name":
			filter.Name = value.Values[0]
		case "active":
			v := strings.ToLower(value.Values[0])

			if v == "yes" || v == "y" {
				filter.Active = ptr.To(true)
			} else if v == "no" || v == "n" {
				filter.Active = ptr.To(false)
			} else if val, err := strconv.ParseBool(v); err == nil {
				filter.Active = ptr.To(val)
			}
		case "version":
			val, err := strconv.ParseUint(value.Values[0], 10, 32)
			if err == nil {
				filter.Version = uint(val)
			}
		}
	}

	deploymentsWithRelated, err := gws.backendRepo.ListDeploymentsWithRelated(ctx, filter)
	if err != nil {
		return &pb.ListDeploymentsResponse{
			Ok:     false,
			ErrMsg: "Unable to list deployments",
		}, nil
	}

	deployments := make([]*pb.Deployment, len(deploymentsWithRelated))
	for i, deployment := range deploymentsWithRelated {
		deployments[i] = &pb.Deployment{
			Id:            deployment.ExternalId,
			Name:          deployment.Name,
			Active:        deployment.Active,
			StubId:        deployment.Stub.ExternalId,
			StubName:      deployment.Stub.Name,
			StubType:      string(deployment.Stub.Type),
			Version:       uint32(deployment.Version),
			WorkspaceId:   deployment.Workspace.ExternalId,
			WorkspaceName: deployment.Workspace.Name,
			CreatedAt:     timestamppb.New(deployment.CreatedAt.Time),
			UpdatedAt:     timestamppb.New(deployment.UpdatedAt.Time),
			AppId:         deployment.App.ExternalId,
		}
	}

	return &pb.ListDeploymentsResponse{
		Ok:          true,
		Deployments: deployments,
	}, nil
}

func (gws *GatewayService) StopDeployment(ctx context.Context, in *pb.StopDeploymentRequest) (*pb.StopDeploymentResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.StopDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	// Get deployment
	deploymentWithRelated, err := gws.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, in.Id)
	if err != nil {
		return &pb.StopDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unable to get deployment",
		}, nil
	}

	if deploymentWithRelated == nil {
		return &pb.StopDeploymentResponse{
			Ok:     false,
			ErrMsg: "Deployment not found",
		}, nil
	}

	// Stop deployment
	if err := gws.stopDeployments([]types.DeploymentWithRelated{*deploymentWithRelated}, ctx); err != nil {
		return &pb.StopDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unable to stop deployment",
		}, nil
	}

	return &pb.StopDeploymentResponse{
		Ok: true,
	}, nil
}

func (gws *GatewayService) ScaleDeployment(ctx context.Context, in *pb.ScaleDeploymentRequest) (*pb.ScaleDeploymentResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.ScaleDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	// Get deployment
	deploymentWithRelated, err := gws.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, in.Id)
	if err != nil {
		return &pb.ScaleDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unable to get deployment",
		}, nil
	}

	if deploymentWithRelated == nil {
		return &pb.ScaleDeploymentResponse{
			Ok:     false,
			ErrMsg: "Deployment not found",
		}, nil
	}
	if workspace := authInfo.Workspace; workspace != nil {
		if deploymentWithRelated.Workspace.Id == 0 {
			deploymentWithRelated.Workspace.Id = workspace.Id
		}
		if deploymentWithRelated.Workspace.ExternalId == "" {
			deploymentWithRelated.Workspace.ExternalId = workspace.ExternalId
		}
		if deploymentWithRelated.Workspace.Name == "" {
			deploymentWithRelated.Workspace.Name = workspace.Name
		}
		if deploymentWithRelated.Workspace.Storage == nil {
			deploymentWithRelated.Workspace.Storage = workspace.Storage
		}
	}

	// For now, we only support direct scaling of pod deployments
	if deploymentWithRelated.Stub.Type != types.StubType(types.StubTypePodDeployment) {
		return &pb.ScaleDeploymentResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("This type of deployment cannot be scaled directly."),
		}, nil
	}

	if maxReplicas := gws.appConfig.GatewayService.StubLimits.MaxReplicas; maxReplicas > 0 && uint64(in.Containers) > maxReplicas {
		return &pb.ScaleDeploymentResponse{
			Ok:     false,
			ErrMsg: fmt.Sprintf("replicas must be %d or less", maxReplicas),
		}, nil
	}

	// Scale deployment
	if err := gws.scaleDeployment(ctx, *deploymentWithRelated, uint(in.Containers)); err != nil {
		return &pb.ScaleDeploymentResponse{
			Ok:     false,
			ErrMsg: err.Error(),
		}, nil
	}

	return &pb.ScaleDeploymentResponse{
		Ok: true,
	}, nil
}

func (gws *GatewayService) BindService(ctx context.Context, in *pb.BindServiceRequest) (*pb.BindServiceResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.BindServiceResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	deploymentWithRelated, err := gws.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, in.Id)
	if err != nil {
		return &pb.BindServiceResponse{
			Ok:     false,
			ErrMsg: "Unable to get deployment",
		}, nil
	}
	if deploymentWithRelated == nil {
		return &pb.BindServiceResponse{
			Ok:     false,
			ErrMsg: "Deployment not found",
		}, nil
	}

	stubConfig := &types.StubConfigV1{}
	if err := json.Unmarshal([]byte(deploymentWithRelated.Stub.Config), stubConfig); err != nil {
		return &pb.BindServiceResponse{
			Ok:     false,
			ErrMsg: "Unable to parse deployment config",
		}, nil
	}

	for key, value := range in.Env {
		stubConfig.Env = mergeEnvVar(stubConfig.Env, key, value)
	}

	aliasIP := gws.serviceBindAliasHost(ctx, authInfo.Workspace.ExternalId, stubConfig)
	for _, secretName := range in.Secrets {
		secret, err := gws.backendRepo.GetSecretByName(ctx, authInfo.Workspace, secretName)
		if err != nil {
			return &pb.BindServiceResponse{
				Ok:     false,
				ErrMsg: fmt.Sprintf("Secret %q not found", secretName),
			}, nil
		}
		stubConfig.Secrets = mergeSecret(stubConfig.Secrets, types.Secret{
			Name:      secret.Name,
			Value:     secret.Value,
			CreatedAt: secret.CreatedAt,
			UpdatedAt: secret.UpdatedAt,
		})
	}

	for envName, secretName := range in.SecretEnv {
		secret, err := gws.backendRepo.GetSecretByName(ctx, authInfo.Workspace, secretName)
		if err != nil {
			return &pb.BindServiceResponse{
				Ok:     false,
				ErrMsg: fmt.Sprintf("Secret %q not found", secretName),
			}, nil
		}
		stubConfig.Secrets = mergeSecret(stubConfig.Secrets, types.Secret{
			Name:      envName,
			Value:     secret.Value,
			CreatedAt: secret.CreatedAt,
			UpdatedAt: secret.UpdatedAt,
		})

		if aliasIP != "" {
			secretKey, err := common.ParseSecretKey(*authInfo.Workspace.SigningKey)
			if err != nil {
				return &pb.BindServiceResponse{
					Ok:     false,
					ErrMsg: "Unable to parse workspace signing key",
				}, nil
			}
			value, err := common.Decrypt(secretKey, secret.Value)
			if err != nil {
				return &pb.BindServiceResponse{
					Ok:     false,
					ErrMsg: fmt.Sprintf("Unable to decrypt secret %q", secretName),
				}, nil
			}
			if host := connectionStringHost(value); host != "" {
				stubConfig.HostAliases = mergeHostAlias(stubConfig.HostAliases, host, aliasIP)
			}
		}
	}

	if err := gws.backendRepo.UpdateStubConfig(ctx, deploymentWithRelated.Stub.Id, stubConfig); err != nil {
		return &pb.BindServiceResponse{
			Ok:     false,
			ErrMsg: "Unable to update deployment config",
		}, nil
	}

	eventBus := common.NewEventBus(gws.redisClient)
	eventBus.Send(&common.Event{Type: common.EventTypeReloadInstance, Retries: 3, LockAndDelete: false, Args: map[string]any{
		"stub_id":   deploymentWithRelated.Stub.ExternalId,
		"stub_type": deploymentWithRelated.StubType,
		"timestamp": time.Now().Unix(),
	}})

	return &pb.BindServiceResponse{Ok: true}, nil
}

func (gws *GatewayService) serviceBindAliasHost(ctx context.Context, workspaceID string, stubConfig *types.StubConfigV1) string {
	if gws.usesPrivatePool(ctx, workspaceID, stubConfig) {
		return ""
	}
	return gatewayTCPServiceHost()
}

func (gws *GatewayService) usesPrivatePool(ctx context.Context, workspaceID string, stubConfig *types.StubConfigV1) bool {
	if gws == nil || gws.computeRepo == nil || stubConfig == nil {
		return false
	}
	poolName := stubConfig.PoolSelector()
	if poolName == "" {
		return false
	}
	pool, err := gws.computeRepo.GetPoolState(ctx, workspaceID, poolName)
	return err == nil && pool != nil
}

func (gws *GatewayService) StartDeployment(ctx context.Context, in *pb.StartDeploymentRequest) (*pb.StartDeploymentResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.StartDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	// Get deployment
	deploymentWithRelated, err := gws.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, in.Id)
	if err != nil {
		return &pb.StartDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unable to get deployment",
		}, nil
	}

	if deploymentWithRelated == nil {
		return &pb.StartDeploymentResponse{
			Ok:     false,
			ErrMsg: "Deployment not found",
		}, nil
	}

	// start deployment
	deploymentWithRelated.Deployment.Active = true
	_, err = gws.backendRepo.UpdateDeployment(ctx, deploymentWithRelated.Deployment)
	if err != nil {
		return &pb.StartDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unable to start deployment",
		}, nil
	}

	// Publish reload instance event
	eventBus := common.NewEventBus(gws.redisClient)
	eventBus.Send(&common.Event{Type: common.EventTypeReloadInstance, Retries: 3, LockAndDelete: false, Args: map[string]any{
		"stub_id":   deploymentWithRelated.Stub.ExternalId,
		"stub_type": deploymentWithRelated.StubType,
		"timestamp": time.Now().Unix(),
	}})

	return &pb.StartDeploymentResponse{
		Ok: true,
	}, nil
}

func (gws *GatewayService) DeleteDeployment(ctx context.Context, in *pb.DeleteDeploymentRequest) (*pb.DeleteDeploymentResponse, error) {
	authInfo, _ := auth.AuthInfoFromContext(ctx)

	if !auth.HasPermission(authInfo) {
		return &pb.DeleteDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unauthorized Access",
		}, nil
	}

	// Get deployment
	deploymentWithRelated, err := gws.backendRepo.GetDeploymentByExternalId(ctx, authInfo.Workspace.Id, in.Id)
	if err != nil {
		return &pb.DeleteDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unable to get deployment",
		}, nil
	}

	if deploymentWithRelated == nil {
		return &pb.DeleteDeploymentResponse{
			Ok:     false,
			ErrMsg: "Deployment not found",
		}, nil
	}

	// Stop deployment first
	if err := gws.stopDeployments([]types.DeploymentWithRelated{*deploymentWithRelated}, ctx); err != nil {
		return &pb.DeleteDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unable to stop deployment",
		}, nil
	}

	// Delete deployment
	if err := gws.backendRepo.DeleteDeployment(ctx, deploymentWithRelated.Deployment); err != nil {
		return &pb.DeleteDeploymentResponse{
			Ok:     false,
			ErrMsg: "Unable to delete deployment",
		}, nil
	}

	return &pb.DeleteDeploymentResponse{
		Ok: true,
	}, nil
}

func (gws *GatewayService) stopDeployments(deployments []types.DeploymentWithRelated, ctx context.Context) error {
	for _, deployment := range deployments {
		// Stop scheduled job
		if deployment.StubType == types.StubTypeScheduledJobDeployment {
			if scheduledJob, err := gws.backendRepo.GetScheduledJob(ctx, deployment.Id); err == nil {
				gws.backendRepo.DeleteScheduledJob(ctx, scheduledJob)
			}
		}

		deployment.Active = false
		if _, err := gws.backendRepo.UpdateDeployment(ctx, deployment.Deployment); err != nil {
			return err
		}

		eventBus := common.NewEventBus(gws.redisClient)
		eventBus.Send(&common.Event{Type: common.EventTypeReloadInstance, Retries: 3, LockAndDelete: false, Args: map[string]any{
			"stub_id":   deployment.Stub.ExternalId,
			"stub_type": deployment.StubType,
			"timestamp": time.Now().Unix(),
		}})

		if err := gws.stopActiveDeploymentContainers(deployment, false); err != nil {
			return err
		}
	}

	return nil
}

func (gws *GatewayService) scaleDeployment(ctx context.Context, deployment types.DeploymentWithRelated, containers uint) error {
	stubConfigRaw := deployment.Stub.Config
	stubConfig := &types.StubConfigV1{}
	if err := json.Unmarshal([]byte(stubConfigRaw), stubConfig); err != nil {
		return err
	}

	stubConfig.SetReplicaCount(containers)
	if containers > 0 {
		if len(stubConfig.Disks) > 0 {
			if err := gws.configureDurableDiskPlacement(ctx, &deployment.Workspace, stubConfig); err != nil {
				return err
			}
		} else {
			if err := gws.configureUnavailablePrivatePoolFallback(ctx, &deployment.Workspace, stubConfig); err != nil {
				return err
			}
		}
	}

	err := gws.backendRepo.UpdateStubConfig(ctx, deployment.Stub.Id, stubConfig)
	if err != nil {
		return err
	}

	if containers == 0 {
		forceStop := len(stubConfig.Disks) == 0
		if err := gws.stopActiveDeploymentContainers(deployment, forceStop); err != nil {
			return err
		}
	}

	// Publish reload instance event
	eventBus := common.NewEventBus(gws.redisClient)
	eventBus.Send(&common.Event{Type: common.EventTypeReloadInstance, Retries: 3, LockAndDelete: false, Args: map[string]any{
		"stub_id":   deployment.Stub.ExternalId,
		"stub_type": deployment.StubType,
		"timestamp": time.Now().Unix(),
	}})

	return nil
}

func (gws *GatewayService) stopActiveDeploymentContainers(deployment types.DeploymentWithRelated, force bool) error {
	containers, err := gws.containerRepo.GetActiveContainersByStubId(deployment.Stub.ExternalId)
	if err != nil {
		return fmt.Errorf("list active containers for deployment %s: %w", deployment.Stub.ExternalId, err)
	}

	var stopErr error
	for _, container := range containers {
		if container.ContainerId == "" || container.Status == types.ContainerStatusStopping {
			continue
		}
		if err := gws.scheduler.Stop(&types.StopContainerArgs{
			ContainerId: container.ContainerId,
			Force:       force,
			Reason:      types.StopContainerReasonUser,
		}); err != nil {
			stopErr = errors.Join(stopErr, fmt.Errorf("stop container %s: %w", container.ContainerId, err))
		}
	}
	return stopErr
}

func mergeEnvVar(env []string, key, value string) []string {
	prefix := key + "="
	next := key + "=" + value
	for i, item := range env {
		if strings.HasPrefix(item, prefix) {
			env[i] = next
			return env
		}
	}
	return append(env, next)
}

func mergeSecret(secrets []types.Secret, secret types.Secret) []types.Secret {
	for i, existing := range secrets {
		if existing.Name == secret.Name {
			secrets[i] = secret
			return secrets
		}
	}
	return append(secrets, secret)
}

func gatewayTCPServiceHost() string {
	for _, key := range []string{
		"BETA9_GATEWAY_PROXY_TCP_SERVICE_HOST",
		"BETA9_GATEWAY_PROXY_TCP_PORT_443_TCP_ADDR",
		"BETA9_GATEWAY_PORT_1995_TCP_ADDR",
		"BETA9_GATEWAY_SERVICE_HOST",
	} {
		if value := strings.TrimSpace(os.Getenv(key)); value != "" {
			return value
		}
	}
	return ""
}

func connectionStringHost(value string) string {
	parsed, err := url.Parse(value)
	if err != nil {
		return ""
	}
	return parsed.Hostname()
}

func mergeHostAlias(aliases map[string]string, host string, ip string) map[string]string {
	host = strings.TrimSpace(host)
	ip = strings.TrimSpace(ip)
	if host == "" || ip == "" {
		return aliases
	}
	if aliases == nil {
		aliases = map[string]string{}
	}
	aliases[host] = ip
	return aliases
}
