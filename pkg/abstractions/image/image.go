package image

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/repository"
	"github.com/beam-cloud/beta9/pkg/scheduler"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type ImageService interface {
	pb.ImageServiceServer
	VerifyImageBuild(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error)
	BuildImage(in *pb.BuildImageRequest, stream pb.ImageService_BuildImageServer) error
}

type ContainerImageService struct {
	pb.UnimplementedImageServiceServer
	builder          *Builder
	config           types.AppConfig
	backendRepo      repository.BackendRepository
	containerRepo    repository.ContainerRepository
	keyEventChan     chan common.KeyEvent
	keyEventManager  *common.KeyEventManager
	baseImageDigests baseImageDigestCache
}

type ImageServiceOpts struct {
	Config        types.AppConfig
	ContainerRepo repository.ContainerRepository
	BackendRepo   repository.BackendRepository
	Scheduler     *scheduler.Scheduler
	Tailscale     *network.Tailscale
	RedisClient   *common.RedisClient
}

const buildContainerKeepAliveIntervalS int = 10
const imageContainerTtlS int = 60

func NewContainerImageService(
	ctx context.Context,
	opts ImageServiceOpts,
) (ImageService, error) {
	imgRegistry, err := reg.NewImageRegistry(opts.Config, opts.Config.ImageService.Registries.S3)
	if err != nil {
		return nil, err
	}

	builder, err := NewBuilder(opts.Config, imgRegistry, opts.Scheduler, opts.Tailscale, opts.ContainerRepo, opts.RedisClient)
	if err != nil {
		return nil, err
	}

	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	is := ContainerImageService{
		builder:          builder,
		config:           opts.Config,
		backendRepo:      opts.BackendRepo,
		containerRepo:    opts.ContainerRepo,
		keyEventChan:     make(chan common.KeyEvent),
		keyEventManager:  keyEventManager,
		baseImageDigests: newBaseImageDigestCache(),
	}

	go is.monitorImageContainers(ctx)
	go is.keyEventManager.ListenForPattern(ctx, common.RedisKeys.ImageBuildContainerTTL("*"), is.keyEventChan)
	go is.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerState(types.BuildContainerPrefix+"*"), is.keyEventChan)

	return &is, nil
}

func (is *ContainerImageService) VerifyImageBuild(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error) {
	result, err := is.verifyImage(ctx, in)
	if err != nil {
		return nil, err
	}

	return &pb.VerifyImageBuildResponse{
		ImageId: result.imageID,
		Exists:  result.exists,
		Valid:   result.valid,
	}, nil
}

func (is *ContainerImageService) BuildImage(in *pb.BuildImageRequest, stream pb.ImageService_BuildImageServer) error {
	log.Info().Interface("request", in).Msg("incoming image build request")

	verifyReq := &pb.VerifyImageBuildRequest{
		PythonVersion:    in.PythonVersion,
		PythonPackages:   in.PythonPackages,
		Commands:         in.Commands,
		ExistingImageUri: in.ExistingImageUri,
		BuildSteps:       in.BuildSteps,
		EnvVars:          in.EnvVars,
		Dockerfile:       in.Dockerfile,
		BuildCtxObject:   in.BuildCtxObject,
		Secrets:          in.Secrets,
		Gpu:              in.Gpu,
		IgnorePython:     in.IgnorePython,
	}

	verifyResult, err := is.verifyImage(stream.Context(), verifyReq)
	if err != nil {
		return err
	}

	if verifyResult.exists {
		_ = stream.Send(&pb.BuildImageResponse{Msg: "Image already exists\n", Done: false, Success: true, ImageId: verifyResult.imageID})
		_ = stream.Send(&pb.BuildImageResponse{Msg: "Build completed successfully\n", Done: true, Success: true, ImageId: verifyResult.imageID})
		return nil
	}

	clipVersion := is.config.ImageService.ClipVersion

	// Set ExistingImageCreds for credential processing
	buildOptions := verifyResult.opts
	if buildOptions == nil {
		return errors.New("missing image build options")
	}

	buildOptions.ExistingImageCreds = in.ExistingImageCreds
	buildOptions.ClipVersion = clipVersion

	// Process credentials for custom base image (if provided)
	if buildOptions.ExistingImageUri != "" && len(buildOptions.ExistingImageCreds) > 0 {
		baseImageCreds, err := reg.GetRegistryTokenForImage(buildOptions.ExistingImageUri, buildOptions.ExistingImageCreds)
		if err != nil {
			log.Error().Err(err).Str("image_id", verifyResult.imageID).Msg("failed to convert credentials to skopeo format")
			return err
		}
		buildOptions.BaseImageCreds = baseImageCreds
	}

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)

	go is.builder.Build(ctx, buildOptions, outputChan)

	var lastMessage common.OutputMsg
	for o := range outputChan {
		if err := stream.Send(&pb.BuildImageResponse{Msg: o.Msg, Done: o.Done, Success: o.Success, ImageId: o.ImageId, PythonVersion: o.PythonVersion, Warning: o.Warning}); err != nil {
			log.Error().Err(err).Msg("failed to complete build")
			lastMessage = o
			break
		}

		if o.Done {
			lastMessage = o
			break
		}
	}

	if !lastMessage.Success {
		return errors.New("build failed")
	}

	_, err = is.backendRepo.CreateImage(context.Background(), lastMessage.ImageId, clipVersion)
	if err != nil {
		log.Error().Err(err).Msg("failed to create image record")
		return errors.New("failed to create image record")
	}

	// Create credential secret ONLY for unmodified images (not pushed to build registry)
	// Modified images use build registry credentials instead
	if err := is.createCredentialSecretIfNeeded(stream.Context(), lastMessage.ImageId, buildOptions); err != nil {
		log.Error().Err(err).Msg("failed to create credential secret")
	}

	log.Info().Msg("build completed successfully")
	return nil
}

func (is *ContainerImageService) retrieveBuildSecrets(ctx context.Context, secrets []string, authInfo *auth.AuthInfo) ([]string, error) {
	var buildSecrets []string
	if secrets != nil {
		secrets, err := is.backendRepo.GetSecretsByNameDecrypted(ctx, authInfo.Workspace, secrets)
		if err != nil {
			return nil, err
		}

		for _, secret := range secrets {
			buildSecrets = append(buildSecrets, fmt.Sprintf("%s=%s", secret.Name, secret.Value))
		}
	}
	return buildSecrets, nil
}

func (is *ContainerImageService) monitorImageContainers(ctx context.Context) {
	for {
		select {
		case event := <-is.keyEventChan:
			switch event.Operation {
			case common.KeyOperationSet:
				if strings.Contains(event.Key, common.RedisKeys.SchedulerContainerState("")) {
					containerId := strings.TrimPrefix(is.keyEventManager.TrimKeyspacePrefix(event.Key), common.RedisKeys.SchedulerContainerState(""))

					if !is.containerRepo.HasBuildContainerTTL(containerId) {
						is.builder.scheduler.Stop(&types.StopContainerArgs{
							ContainerId: containerId,
							Force:       true,
							Reason:      types.StopContainerReasonTtl,
						})
					}
				}
			case common.KeyOperationExpired:
				if strings.Contains(event.Key, common.RedisKeys.ImageBuildContainerTTL("")) {
					containerId := strings.TrimPrefix(is.keyEventManager.TrimKeyspacePrefix(event.Key), common.RedisKeys.ImageBuildContainerTTL(""))
					is.builder.scheduler.Stop(&types.StopContainerArgs{
						ContainerId: containerId,
						Force:       true,
						Reason:      types.StopContainerReasonTtl,
					})
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func convertBuildSteps(buildSteps []*pb.BuildStep) []BuildStep {
	steps := make([]BuildStep, len(buildSteps))
	for i, s := range buildSteps {
		steps[i] = BuildStep{
			Command: s.Command,
			Type:    s.Type,
		}
	}
	return steps
}

// createCredentialSecretIfNeeded creates a workspace secret for OCI registry credentials
// ONLY for unmodified images that were not pushed to the build registry.
// Modified images use build registry credentials instead.
func (is *ContainerImageService) createCredentialSecretIfNeeded(ctx context.Context, imageId string, opts *BuildOpts) error {
	if opts == nil {
		return nil
	}

	// Check if this image was modified (built/pushed to build registry)
	wasModified := is.builder.hasWorkToDo(opts) || opts.Dockerfile != ""
	if wasModified {
		log.Debug().
			Str("image_id", imageId).
			Msg("image was modified and pushed to build registry, skipping credential secret creation")
		return nil
	}

	// Image was NOT modified - it's just an indexed base image in external registry
	// Store credentials as secret for runtime access
	log.Info().
		Str("image_id", imageId).
		Str("existing_image_uri", opts.ExistingImageUri).
		Msg("unmodified image - storing credentials for runtime access")

	// Only create secret if credentials were provided
	if opts.ExistingImageCreds == nil || len(opts.ExistingImageCreds) == 0 {
		return nil
	}

	if opts.ExistingImageUri == "" {
		return nil
	}

	// Get structured credentials
	registry, creds, err := reg.GetRegistryCredentialsForImage(opts.ExistingImageUri, opts.ExistingImageCreds)
	if err != nil {
		log.Warn().Err(err).Str("image_id", imageId).Msg("failed to get registry credentials")
		return nil
	}

	credType := reg.DetectCredentialType(registry, creds)
	if credType == reg.CredTypePublic || len(creds) == 0 {
		return nil
	}

	secretValue, err := reg.MarshalCredentials(registry, credType, creds)
	if err != nil {
		return fmt.Errorf("failed to marshal credentials: %w", err)
	}

	// Get workspace context
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo.Workspace == nil {
		return fmt.Errorf("no workspace found in context")
	}

	secretName := reg.CreateSecretName(registry)

	secret, err := is.upsertSecret(context.Background(), authInfo, secretName, secretValue, registry)
	if err != nil {
		return err
	}

	if err := is.backendRepo.SetImageCredentialSecret(context.Background(), imageId, secretName, secret.ExternalId); err != nil {
		return fmt.Errorf("failed to associate secret with image: %w", err)
	}

	log.Info().Str("image_id", imageId).Str("secret_name", secretName).Msg("stored credential secret")
	return nil
}

func (is *ContainerImageService) upsertSecret(ctx context.Context, authInfo *auth.AuthInfo, secretName, secretValue, registry string) (*types.Secret, error) {
	secret, err := is.backendRepo.GetSecretByName(ctx, authInfo.Workspace, secretName)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to check for existing secret: %w", err)
	}

	if secret != nil {
		secret, err = is.backendRepo.UpdateSecret(ctx, authInfo.Workspace, authInfo.Token.Id, secretName, secretValue)
		if err != nil {
			return nil, fmt.Errorf("failed to update secret: %w", err)
		}
	} else {
		secret, err = is.backendRepo.CreateSecret(ctx, authInfo.Workspace, authInfo.Token.Id, secretName, secretValue, false)
		if err != nil {
			return nil, fmt.Errorf("failed to create secret: %w", err)
		}
	}

	return secret, nil
}
