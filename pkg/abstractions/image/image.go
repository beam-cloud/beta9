package image

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"

	abstractions "github.com/beam-cloud/beta9/pkg/abstractions/common"
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
	builder.backendRepo = opts.BackendRepo

	is := ContainerImageService{
		builder:          builder,
		config:           opts.Config,
		backendRepo:      opts.BackendRepo,
		baseImageDigests: newBaseImageDigestCache(opts.RedisClient),
	}

	leases := abstractions.NewContainerLeaseManager(
		opts.RedisClient,
		opts.Scheduler,
		types.BuildContainerPrefix,
		common.RedisKeys.ImageBuildContainerTTL,
	)
	go func() {
		if err := leases.Run(ctx); err != nil {
			log.Error().Err(err).Msg("image container lease manager stopped")
		}
	}()

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
	// The request can contain registry credentials and secret environment
	// values. Log only non-sensitive build metadata.
	log.Info().Str("python_version", in.PythonVersion).Str("gpu", in.Gpu).Msg("incoming image build request")

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

	buildOptions := verifyResult.opts
	if buildOptions == nil {
		return errors.New("missing image build options")
	}
	buildOptions.ExistingImageCreds, err = is.registryCredentials(stream.Context(), in.ExistingImageCreds)
	if err != nil {
		return err
	}

	if verifyResult.exists {
		is.setImageCredentialSecretNames(verifyResult.imageID, buildOptions)
		_ = stream.Send(&pb.BuildImageResponse{Msg: "Image already exists\n", Done: false, Success: true, ImageId: verifyResult.imageID})
		_ = stream.Send(&pb.BuildImageResponse{Msg: "Build completed successfully\n", Done: true, Success: true, ImageId: verifyResult.imageID})
		return nil
	}

	clipVersion := is.config.ImageService.ClipVersion
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
	buildErrChan := make(chan error, 1)

	go func() {
		buildErrChan <- is.builder.Build(ctx, buildOptions, outputChan)
	}()

	lastMessage, err := streamImageBuildOutput(ctx, outputChan, buildErrChan, func(o common.OutputMsg) error {
		return stream.Send(&pb.BuildImageResponse{Msg: o.Msg, Done: o.Done, Success: o.Success, ImageId: o.ImageId, PythonVersion: o.PythonVersion, Warning: o.Warning})
	})
	if err != nil && !lastMessage.Success {
		return err
	}

	if !lastMessage.Success {
		return errors.New("build failed")
	}

	_, err = is.backendRepo.CreateImage(context.Background(), lastMessage.ImageId, clipVersion)
	if err != nil {
		log.Error().Err(err).Msg("failed to create image record")
		return errors.New("failed to create image record")
	}

	is.setImageCredentialSecretNames(lastMessage.ImageId, buildOptions)

	log.Info().Msg("build completed successfully")
	return nil
}

func streamImageBuildOutput(ctx context.Context, outputChan <-chan common.OutputMsg, buildErrChan <-chan error, send func(common.OutputMsg) error) (common.OutputMsg, error) {
	var lastMessage common.OutputMsg

	sendTerminalFailure := func(err error) (common.OutputMsg, error) {
		msg := "Build failed\n"
		if err != nil {
			msg = err.Error() + "\n"
		}

		lastMessage = common.OutputMsg{
			Msg:     msg,
			Done:    true,
			Success: false,
		}
		if sendErr := send(lastMessage); sendErr != nil {
			log.Error().Err(sendErr).Msg("failed to complete build")
			return lastMessage, sendErr
		}

		if err != nil {
			return lastMessage, err
		}
		return lastMessage, errors.New("build failed")
	}

	for {
		select {
		case o := <-outputChan:
			lastMessage = o
			if err := send(o); err != nil {
				log.Error().Err(err).Msg("failed to complete build")
				return lastMessage, err
			}

			if o.Done {
				select {
				case err := <-buildErrChan:
					return lastMessage, err
				case <-ctx.Done():
					return lastMessage, ctx.Err()
				}
			}

		case err := <-buildErrChan:
			if lastMessage.Done {
				return lastMessage, err
			}
			return sendTerminalFailure(err)

		case <-ctx.Done():
			return lastMessage, ctx.Err()
		}
	}
}

func (is *ContainerImageService) retrieveBuildSecrets(ctx context.Context, secrets []string, authInfo *auth.AuthInfo) ([]string, error) {
	var buildSecrets []string
	if secrets != nil {
		secrets, err := is.backendRepo.GetSecretsByNameDecrypted(ctx, authInfo.Workspace, secrets)
		if err != nil {
			return nil, err
		}

		// The repository query does not guarantee row order. Canonicalize it so
		// repeated builds render the same Dockerfile and resolve to the same image.
		sort.Slice(secrets, func(i, j int) bool {
			return secrets[i].Name < secrets[j].Name
		})

		for _, secret := range secrets {
			buildSecrets = append(buildSecrets, fmt.Sprintf("%s=%s", secret.Name, secret.Value))
		}
	}
	return buildSecrets, nil
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

// registryCredentials reads the workspace secrets named by the keys of creds.
// Values sent by the client are ignored; a secret is the only place they live.
func (is *ContainerImageService) registryCredentials(ctx context.Context, creds map[string]string) (map[string]string, error) {
	if len(creds) == 0 {
		return nil, nil
	}
	authInfo, ok := auth.AuthInfoFromContext(ctx)
	if !ok || authInfo.Workspace == nil {
		return nil, errors.New("no workspace found in context")
	}
	names := slices.Sorted(maps.Keys(creds))
	secrets, err := is.backendRepo.GetSecretsByNameDecrypted(ctx, authInfo.Workspace, names)
	if err != nil {
		return nil, err
	}
	resolved := make(map[string]string, len(secrets))
	for _, secret := range secrets {
		resolved[secret.Name] = secret.Value
	}
	for _, name := range names {
		if resolved[name] == "" {
			return nil, fmt.Errorf("registry credential %s is not a workspace secret; create it with `beta9 secret create %s <value>`", name, name)
		}
	}
	return resolved, nil
}

// setImageCredentialSecretNames records which workspace secrets the runtime
// reads to pull an unmodified image. Modified images live in the build registry.
func (is *ContainerImageService) setImageCredentialSecretNames(imageId string, opts *BuildOpts) {
	if len(opts.ExistingImageCreds) == 0 || is.builder.hasWorkToDo(opts) || opts.Dockerfile != "" {
		return
	}
	names := slices.Sorted(maps.Keys(opts.ExistingImageCreds))
	if err := is.backendRepo.SetImageCredentialSecretNames(context.Background(), imageId, names); err != nil {
		log.Error().Err(err).Str("image_id", imageId).Msg("failed to store image credential secret names")
	}
}
