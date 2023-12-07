package image

import (
	"context"
	"fmt"
	"strings"

	"github.com/beam-cloud/beam/internal/common"
)

type ImageService struct {
	builder *Builder
}

func NewImageService(ctx context.Context) (*ImageService, error) {
	builder, err := NewBuilder()
	if err != nil {
		return nil, err
	}

	return &ImageService{
		builder: builder,
	}, nil
}

// Load list of images to cache on disk
func (is *ImageService) loadImageManifest() ([]BaseImageCacheOpt, error) {
	imageTagsToCache := strings.Split(common.Secrets().GetWithDefault("BEAM_IMAGESERVICE_IMAGE_TAGS_TO_CACHE", ""), ",")
	baseImageCacheSize := common.Secrets().GetInt("BEAM_IMAGESERVICE_BASE_IMAGE_CACHE_SIZE")

	// TODO: load from a yaml file or something
	allImages := []BaseImageCacheOpt{
		{
			SourceRegistry: common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_REGISTRY"),
			ImageName:      common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_NAME"),
			ImageTag:       fmt.Sprintf("py37-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG")),
			Copies:         baseImageCacheSize,
		},
		{
			SourceRegistry: common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_REGISTRY"),
			ImageName:      common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_NAME"),
			ImageTag:       fmt.Sprintf("py38-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG")),
			Copies:         baseImageCacheSize,
		},
		{
			SourceRegistry: common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_REGISTRY"),
			ImageName:      common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_NAME"),
			ImageTag:       fmt.Sprintf("py39-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG")),
			Copies:         baseImageCacheSize,
		},
		{
			SourceRegistry: common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_REGISTRY"),
			ImageName:      common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_NAME"),
			ImageTag:       fmt.Sprintf("py310-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG")),
			Copies:         baseImageCacheSize,
		},
	}

	if len(imageTagsToCache) == 0 {
		return allImages, nil
	}

	filteredImages := []BaseImageCacheOpt{}
	for _, image := range allImages {
		for _, tag := range imageTagsToCache {
			if strings.HasPrefix(image.ImageTag, tag) {
				filteredImages = append(filteredImages, image)
			}
		}
	}

	return filteredImages, nil
}

// // Verify image is valid / return whether or not it already exists
// func (is *ImageService) VerifyBuild(ctx context.Context, in *pb.VerifyBuildRequest) (*pb.VerifyBuildResponse, error) {
// 	var valid bool = true
// 	var exists bool

// 	imageTag, err := is.builder.GetImageTag(&BuildOpts{
// 		BaseImageName:    common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_NAME"),
// 		BaseImageTag:     is.getBaseImageTag(in.PythonVersion),
// 		PythonVersion:    in.PythonVersion,
// 		PythonPackages:   in.PythonPackages,
// 		Commands:         in.Commands,
// 		ExistingImageUri: in.ExistingImageUri,
// 	})
// 	if err != nil {
// 		valid = false
// 	}

// 	exists = is.builder.Exists(ctx, imageTag)

// 	return &pb.VerifyBuildResponse{
// 		ImageTag: imageTag,
// 		Exists:   exists,
// 		Valid:    valid,
// 	}, nil
// }

// // Build a new image
// func (is *ImageService) Build(in *pb.BuildRequest, stream pb.ImageService_BuildServer) error {
// 	log.Printf("incoming image build request: %+v", in)

// 	buildOptions := &BuildOpts{
// 		BaseImageName:    common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_NAME"),
// 		BaseImageTag:     is.getBaseImageTag(in.PythonVersion),
// 		UserImageTag:     in.ImageTag,
// 		PythonVersion:    in.PythonVersion,
// 		PythonPackages:   in.PythonPackages,
// 		Commands:         in.Commands,
// 		ExistingImageUri: in.ExistingImageUri,
// 	}

// 	defer is.builder.Clean(buildOptions)

// 	ctx := stream.Context()
// 	outputChan := make(chan common.OutputMsg)
// 	go is.builder.Build(ctx, buildOptions, outputChan)

// 	var lastMessage common.OutputMsg
// 	for o := range outputChan {
// 		if err := stream.Send(&pb.BuildResponse{Msg: o.Msg, Done: o.Done, Success: o.Success}); err != nil {
// 			log.Println("failed to complete build: ", err)
// 			lastMessage = o
// 			break
// 		}

// 		if o.Done {
// 			lastMessage = o
// 			break
// 		}
// 	}

// 	log.Println("Success: ", lastMessage.Success)
// 	return nil
// }

// // Get latest image tag from secrets
// func (is *ImageService) getBaseImageTag(pythonVersion string) string {
// 	var baseImageTag string

// 	switch pythonVersion {
// 	case "python3.7":
// 		baseImageTag = fmt.Sprintf("py37-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG"))

// 	case "python3.8":
// 		baseImageTag = fmt.Sprintf("py38-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG"))

// 	case "python3.9":
// 		baseImageTag = fmt.Sprintf("py39-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG"))

// 	case "python3.10":
// 		baseImageTag = fmt.Sprintf("py310-%s", common.Secrets().Get("BEAM_RUNNER_BASE_IMAGE_TAG"))
// 	}

// 	return baseImageTag
// }
