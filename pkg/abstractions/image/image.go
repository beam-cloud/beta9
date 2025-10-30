package image

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
	"github.com/beam-cloud/beta9/pkg/registry"
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

type RuncImageService struct {
	pb.UnimplementedImageServiceServer
	builder         *Builder
	config          types.AppConfig
	backendRepo     repository.BackendRepository
	containerRepo   repository.ContainerRepository
	keyEventChan    chan common.KeyEvent
	keyEventManager *common.KeyEventManager
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

func NewRuncImageService(
	ctx context.Context,
	opts ImageServiceOpts,
) (ImageService, error) {
	registry, err := registry.NewImageRegistry(opts.Config, opts.Config.ImageService.Registries.S3)
	if err != nil {
		return nil, err
	}

	builder, err := NewBuilder(opts.Config, registry, opts.Scheduler, opts.Tailscale, opts.ContainerRepo, opts.RedisClient)
	if err != nil {
		return nil, err
	}

	keyEventManager, err := common.NewKeyEventManager(opts.RedisClient)
	if err != nil {
		return nil, err
	}

	is := RuncImageService{
		builder:         builder,
		config:          opts.Config,
		backendRepo:     opts.BackendRepo,
		containerRepo:   opts.ContainerRepo,
		keyEventChan:    make(chan common.KeyEvent),
		keyEventManager: keyEventManager,
	}

	go is.monitorImageContainers(ctx)
	go is.keyEventManager.ListenForPattern(ctx, common.RedisKeys.ImageBuildContainerTTL("*"), is.keyEventChan)
	go is.keyEventManager.ListenForPattern(ctx, common.RedisKeys.SchedulerContainerState(types.BuildContainerPrefix+"*"), is.keyEventChan)

	return &is, nil
}

func (is *RuncImageService) VerifyImageBuild(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error) {
	if in.ImageId != nil && *in.ImageId != "" {
		exists, err := is.builder.Exists(ctx, *in.ImageId)
		if err != nil {
			return nil, err
		}

		return &pb.VerifyImageBuildResponse{
			ImageId: *in.ImageId,
			Exists:  exists,
			Valid:   true,
		}, nil
	}

	imageId, exists, validResult, _, err := is.verifyImage(ctx, in)
	if err != nil {
		return nil, err
	}

	return &pb.VerifyImageBuildResponse{
		ImageId: imageId,
		Exists:  exists,
		Valid:   validResult,
	}, nil
}

func (is *RuncImageService) BuildImage(in *pb.BuildImageRequest, stream pb.ImageService_BuildImageServer) error {
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

    imageId, exists, _, buildOptions, err := is.verifyImage(stream.Context(), verifyReq)
	if err != nil {
		return err
	}

    if exists {
        // Emit a minimal success response consistent with SDK expectations
        _ = stream.Send(&pb.BuildImageResponse{Msg: "Image already exists\n", Done: false, Success: true, ImageId: imageId})
        _ = stream.Send(&pb.BuildImageResponse{Msg: "Build completed successfully\n", Done: true, Success: true, ImageId: imageId})
        return nil
    }

	clipVersion := is.config.ImageService.ClipVersion
	buildOptions.ExistingImageCreds = in.ExistingImageCreds
	buildOptions.ClipVersion = clipVersion

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)

    go is.builder.Build(ctx, buildOptions, outputChan)

	var lastMessage common.OutputMsg
	for o := range outputChan {
        // Stream all logs to the user (no archiving-stage filtering for v2)

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

	log.Info().Msg("build completed successfully")
	return nil
}

func (is *RuncImageService) verifyImage(ctx context.Context, in *pb.VerifyImageBuildRequest) (string, bool, bool, *BuildOpts, error) {
	var valid bool = true

	if in.ImageId != nil && *in.ImageId != "" {
		exists, err := is.builder.Exists(ctx, *in.ImageId)
		if err != nil {
			return "", false, false, nil, err
		}
		return *in.ImageId, exists, true, nil, nil
	}

	tag := in.PythonVersion
	if in.PythonVersion == types.Python3.String() {
		tag = is.config.ImageService.PythonVersion
	}

	baseImageTag, ok := is.config.ImageService.Runner.Tags[tag]
	if !ok {
		return "", false, false, nil, errors.Errorf("Python version not supported: %s", in.PythonVersion)
	}

	authInfo, _ := auth.AuthInfoFromContext(ctx)
	buildSecrets, err := is.retrieveBuildSecrets(ctx, in.Secrets, authInfo)
	if err != nil {
		return "", false, false, nil, err
	}

	opts := &BuildOpts{
		PythonVersion:  in.PythonVersion,
		PythonPackages: in.PythonPackages,
		Commands:       in.Commands,
		BuildSteps:     convertBuildSteps(in.BuildSteps),
		EnvVars:        in.EnvVars,
		Dockerfile:     in.Dockerfile,
		BuildCtxObject: in.BuildCtxObject,
		BuildSecrets:   buildSecrets,
		Gpu:            in.Gpu,
		ClipVersion:    is.config.ImageService.ClipVersion,
	}

	// Only set default beta9 base image if not using a custom Dockerfile
	// Custom Dockerfiles specify their own base image in the FROM instruction
	if in.Dockerfile == "" {
		opts.BaseImageTag = baseImageTag
		opts.BaseImageName = is.config.ImageService.Runner.BaseImageName
		opts.BaseImageRegistry = is.config.ImageService.Runner.BaseImageRegistry
	}

	if in.IgnorePython {
		opts.IgnorePython = true
	}

	// Handle custom base image (from Image.from_registry or base_image parameter)
	if in.ExistingImageUri != "" {
		opts.ExistingImageUri = in.ExistingImageUri
		opts.handleCustomBaseImage(nil)
	}

	if in.Dockerfile != "" {
		opts.addPythonRequirements()
	}

	// For v2 builds, render Dockerfile from build options if not already provided
	isV2 := is.config.ImageService.ClipVersion == 2
	if isV2 && opts.Dockerfile == "" && is.builder.hasWorkToDo(opts) {
		opts.Dockerfile, err = is.builder.RenderV2Dockerfile(opts)
		if err != nil {
			return "", false, false, nil, err
		}
	}

	// For V2 builds with custom Dockerfiles, ensure required directories are created
	// This guarantees /workspace and /volumes exist regardless of the base image
	if isV2 && opts.Dockerfile != "" {
		opts.Dockerfile = ensureRequiredDirectoriesInDockerfile(opts.Dockerfile)
	}

	imageId, err := getImageID(opts)
	if err != nil {
		valid = false
	}

	// Check registry for physical existence
	exists, err := is.builder.Exists(ctx, imageId)
	if err != nil {
		return "", false, false, nil, err
	}

	// Also check database to ensure image metadata is persisted
	// This prevents duplicate builds when registry has the file but DB record is missing
	_, err = is.backendRepo.GetImageClipVersion(ctx, imageId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Image not in database - needs to be built/recorded
			exists = false
		} else {
			return "", false, false, nil, err
		}
	}

	return imageId, exists, valid, opts, nil
}

func (is *RuncImageService) retrieveBuildSecrets(ctx context.Context, secrets []string, authInfo *auth.AuthInfo) ([]string, error) {
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

func (is *RuncImageService) monitorImageContainers(ctx context.Context) {
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
