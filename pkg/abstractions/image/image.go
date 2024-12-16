package image

import (
	"context"

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
	builder *Builder
	config  types.AppConfig
}

type ImageServiceOpts struct {
	Config        types.AppConfig
	ContainerRepo repository.ContainerRepository
	Scheduler     *scheduler.Scheduler
	Tailscale     *network.Tailscale
}

func NewRuncImageService(
	ctx context.Context,
	opts ImageServiceOpts,
) (ImageService, error) {
	registry, err := common.NewImageRegistry(opts.Config.ImageService)
	if err != nil {
		return nil, err
	}

	builder, err := NewBuilder(opts.Config, registry, opts.Scheduler, opts.Tailscale, opts.ContainerRepo)
	if err != nil {
		return nil, err
	}

	return &RuncImageService{
		builder: builder,
		config:  opts.Config,
	}, nil
}

func (is *RuncImageService) VerifyImageBuild(ctx context.Context, in *pb.VerifyImageBuildRequest) (*pb.VerifyImageBuildResponse, error) {
	var valid bool = true

	baseImageTag, ok := is.config.ImageService.Runner.Tags[in.PythonVersion]
	if !ok {
		return nil, errors.Errorf("Python version not supportted: %s", in.PythonVersion)
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
