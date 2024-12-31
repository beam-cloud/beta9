package image

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/network"
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
	builder     *Builder
	config      types.AppConfig
	backendRepo repository.BackendRepository
	rdb         *common.RedisClient
}

type ImageServiceOpts struct {
	Config        types.AppConfig
	ContainerRepo repository.ContainerRepository
	BackendRepo   repository.BackendRepository
	Scheduler     *scheduler.Scheduler
	Tailscale     *network.Tailscale
	RedisClient   *common.RedisClient
}

func NewRuncImageService(
	ctx context.Context,
	opts ImageServiceOpts,
) (ImageService, error) {
	registry, err := common.NewImageRegistry(opts.Config.ImageService)
	if err != nil {
		return nil, err
	}

	builder, err := NewBuilder(opts.Config, registry, opts.Scheduler, opts.Tailscale, opts.ContainerRepo, opts.RedisClient)
	if err != nil {
		return nil, err
	}

	is := RuncImageService{
		builder:     builder,
		config:      opts.Config,
		backendRepo: opts.BackendRepo,
		rdb:         opts.RedisClient,
	}

	go is.monitorImageContainers(ctx)

	return &is, nil
}

func (is *RuncImageService) VerifyImageBuild(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error) {
	var valid bool = true

	baseImageTag, ok := is.config.ImageService.Runner.Tags[in.PythonVersion]
	if !ok {
		return nil, errors.Errorf("Python version not supportted: %s", in.PythonVersion)
	}

	authInfo, _ := auth.AuthInfoFromContext(ctx)
	buildSecrets, err := is.retrieveBuildSecrets(ctx, in.Secrets, authInfo)
	if err != nil {
		return nil, err
	}

	opts := &BuildOpts{
		BaseImageTag:      baseImageTag,
		BaseImageName:     is.config.ImageService.Runner.BaseImageName,
		BaseImageRegistry: is.config.ImageService.Runner.BaseImageRegistry,
		PythonVersion:     in.PythonVersion,
		PythonPackages:    in.PythonPackages,
		Commands:          in.Commands,
		BuildSteps:        convertBuildSteps(in.BuildSteps),
		ExistingImageUri:  in.ExistingImageUri,
		EnvVars:           in.EnvVars,
		Dockerfile:        in.Dockerfile,
		BuildCtxObject:    in.BuildCtxObject,
		BuildSecrets:      buildSecrets,
	}

	if in.ExistingImageUri != "" {
		is.builder.handleCustomBaseImage(opts, nil)
	}

	if in.Dockerfile != "" {
		opts.addPythonRequirements()
	}

	imageId, err := is.builder.GetImageId(opts)
	if err != nil {
		valid = false
	}

	return &pb.VerifyImageBuildResponse{
		ImageId: imageId,
		Exists:  is.builder.Exists(ctx, imageId),
		Valid:   valid,
	}, nil
}

func (is *RuncImageService) BuildImage(in *pb.BuildImageRequest, stream pb.ImageService_BuildImageServer) error {
	log.Info().Interface("request", in).Msg("incoming image build request")

	authInfo, _ := auth.AuthInfoFromContext(stream.Context())

	buildSecrets, err := is.retrieveBuildSecrets(stream.Context(), in.Secrets, authInfo)
	if err != nil {
		return err
	}

	buildOptions := &BuildOpts{
		BaseImageTag:       is.config.ImageService.Runner.Tags[in.PythonVersion],
		BaseImageName:      is.config.ImageService.Runner.BaseImageName,
		BaseImageRegistry:  is.config.ImageService.Runner.BaseImageRegistry,
		PythonVersion:      in.PythonVersion,
		PythonPackages:     in.PythonPackages,
		Commands:           in.Commands,
		BuildSteps:         convertBuildSteps(in.BuildSteps),
		ExistingImageUri:   in.ExistingImageUri,
		ExistingImageCreds: in.ExistingImageCreds,
		EnvVars:            in.EnvVars,
		Dockerfile:         in.Dockerfile,
		BuildCtxObject:     in.BuildCtxObject,
		BuildSecrets:       buildSecrets,
	}

	ctx := stream.Context()
	outputChan := make(chan common.OutputMsg)

	go is.builder.Build(ctx, buildOptions, outputChan)

	// This is a switch to stop sending build log messages once the archiving stage is reached
	archivingStage := false
	var lastMessage common.OutputMsg
	for o := range outputChan {
		if archivingStage && !o.Archiving {
			continue
		}

		if err := stream.Send(&pb.BuildImageResponse{Msg: o.Msg, Done: o.Done, Success: o.Success, ImageId: o.ImageId}); err != nil {
			log.Error().Err(err).Msg("failed to complete build")
			lastMessage = o
			break
		}

		if o.Archiving {
			archivingStage = true
		}

		if o.Done {
			lastMessage = o
			break
		}
	}

	if !lastMessage.Success {
		return errors.New("build failed")
	}

	log.Info().Msg("build completed successfully")
	return nil
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
	ticker := time.NewTicker(5 * time.Second)

	for {
		select {
		case <-ticker.C:
			containerKeys, err := is.rdb.HKeys(ctx, imageBuildContainersCreatedAtKey).Result()
			if err != nil {
				log.Error().Err(err).Msg("failed to get active image build containers")
				return
			}

			for _, key := range containerKeys {
				createdAt := is.rdb.HGet(ctx, imageBuildContainersCreatedAtKey, key).Val()
				if createdAt == "" {
					log.Error().Msg("failed to get created at time for container")
					continue
				}

				createdAtInt, err := strconv.ParseInt(createdAt, 10, 64)
				if err != nil {
					log.Error().Err(err).Msg("failed to parse container created at time")
					continue
				}

				// See if the container still exists
				_, err = is.builder.containerRepo.GetContainerState(key)
				if err != nil {
					if errors.Is(err, &types.ErrCheckpointNotFound{}) {
						is.rdb.HDel(ctx, imageBuildContainersCreatedAtKey, key)
					}

					log.Error().Err(err).Msg("failed to get container state")
					continue
				}

				if createdAtInt < time.Now().Unix()-is.config.ImageService.ImageBuildTimeoutS {
					is.builder.scheduler.Stop(
						&types.StopContainerArgs{
							ContainerId: key,
							Force:       true,
						},
					)

					is.rdb.HDel(ctx, imageBuildContainersCreatedAtKey, key)
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

var imageBuildContainersCreatedAtKey = "image:containers:created_at"
