package gatewayservices

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
)

// ActiveDeploymentByName is the newest active deployment with exactly this name.
func (gws *GatewayService) ActiveDeploymentByName(ctx context.Context, workspace *types.Workspace, name string) (*types.DeploymentWithRelated, error) {
	deployments, err := gws.deploymentsByName(ctx, workspace, name)
	if err != nil {
		return nil, err
	}
	matches := make([]types.DeploymentWithRelated, 0, len(deployments))
	for _, d := range deployments {
		if d.Name == name && d.Active {
			matches = append(matches, d)
		}
	}
	if len(matches) == 0 {
		return nil, fmt.Errorf("no active deployment named %q", name)
	}
	return newestDeployment(matches), nil
}

// RedeployWithConfig deploys a new version of an app's newest active
// deployment from its config after `mutate` edits it. This is how settings
// change without a code push; the previous version stays for rollback.
func (gws *GatewayService) RedeployWithConfig(ctx context.Context, authInfo *auth.AuthInfo, appName string, mutate func(*types.StubConfigV1) error) (*pb.DeployStubResponse, error) {
	deployment, err := gws.ActiveDeploymentByName(ctx, authInfo.Workspace, appName)
	if err != nil {
		return nil, err
	}
	stub, err := gws.backendRepo.GetStubByExternalId(ctx, deployment.Stub.ExternalId)
	if err != nil {
		return nil, fmt.Errorf("load stub: %w", err)
	}
	config, err := stub.UnmarshalConfig()
	if err != nil {
		return nil, fmt.Errorf("decode stub config: %w", err)
	}
	if err := mutate(config); err != nil {
		return nil, err
	}
	if valid, msg := types.ValidateCpuAndMemory(config.Runtime.Cpu, config.Runtime.Memory, gws.appConfig.GatewayService.StubLimits); !valid {
		return nil, errors.New(msg)
	}
	next, err := gws.backendRepo.GetOrCreateStub(ctx, stub.Name, string(stub.Type), *config, stub.ObjectId, authInfo.Workspace.Id, true, stub.AppId)
	if err != nil {
		return nil, fmt.Errorf("create stub: %w", err)
	}
	return gws.DeployStub(ctx, &pb.DeployStubRequest{StubId: next.ExternalId, Name: deployment.Name})
}

// SetDeploymentEnv redeploys with env entries set or removed. Values may hold
// `${{...}}` references and resolve the way a fresh deploy would; a variable
// set here replaces any binding it had.
func (gws *GatewayService) SetDeploymentEnv(ctx context.Context, authInfo *auth.AuthInfo, appName string, set map[string]string, unset []string) (*pb.DeployStubResponse, error) {
	return gws.RedeployWithConfig(ctx, authInfo, appName, func(config *types.StubConfigV1) error {
		drop := map[string]bool{}
		for key := range set {
			drop[key] = true
		}
		for _, key := range unset {
			drop[key] = true
		}
		env := make([]string, 0, len(config.Env)+len(set))
		for _, entry := range config.Env {
			if key, _, _ := strings.Cut(entry, "="); !drop[key] {
				env = append(env, entry)
			}
		}
		secrets := make([]types.Secret, 0, len(config.Secrets))
		for _, s := range config.Secrets {
			if !drop[s.EnvName] {
				secrets = append(secrets, s)
			}
		}

		entries := make([]string, 0, len(set))
		for key, value := range set {
			entries = append(entries, key+"="+value)
		}
		expanded, bindings, err := gws.expandReferences(ctx, authInfo, appName, entries)
		if err != nil {
			return err
		}
		for _, binding := range bindings {
			secret, err := gws.backendRepo.GetSecretByName(ctx, authInfo.Workspace, binding.Name)
			if err != nil {
				if err == sql.ErrNoRows {
					return fmt.Errorf("secret %q does not exist in this workspace", binding.Name)
				}
				return fmt.Errorf("resolve secret %q: %w", binding.Name, err)
			}
			secrets = append(secrets, types.Secret{Name: secret.Name, Value: secret.Value, EnvName: binding.EnvName, CreatedAt: secret.CreatedAt, UpdatedAt: secret.UpdatedAt})
		}
		config.Env = append(env, expanded...)
		config.Secrets = secrets
		return nil
	})
}
