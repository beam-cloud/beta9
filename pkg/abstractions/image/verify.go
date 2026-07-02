package image

import (
	"context"
	"database/sql"

	"github.com/beam-cloud/beta9/pkg/auth"
	reg "github.com/beam-cloud/beta9/pkg/registry"
	"github.com/beam-cloud/beta9/pkg/types"
	pb "github.com/beam-cloud/beta9/proto"
	"github.com/pkg/errors"
)

type imageVerifyResult struct {
	imageID string
	exists  bool
	valid   bool
	opts    *BuildOpts
}

func (is *ContainerImageService) verifyImage(ctx context.Context, in *pb.VerifyImageBuildRequest) (result *imageVerifyResult, err error) {
	recorder := is.newImageVerifyRecorder(in)
	defer func() { recorder.total(result, err) }()

	if in.ImageId != nil && *in.ImageId != "" {
		return is.verifyExplicitImageID(ctx, recorder, *in.ImageId)
	}

	opts, err := measureImageVerifyValue(recorder, imageVerifyPhaseBuildOptions, func() (*BuildOpts, error) {
		return is.buildOptionsFromVerifyRequest(ctx, in)
	})
	if err != nil {
		return nil, err
	}

	if err := recorder.measure(imageVerifyPhasePrepareOptions, func() error {
		return is.prepareBuildOptionsForImageID(ctx, in, opts)
	}); err != nil {
		return nil, err
	}

	imageID, imageIDErr := measureImageVerifyValue(recorder, imageVerifyPhaseGetImageID, func() (string, error) {
		return getImageID(opts)
	})

	exists, err := measureImageVerifyValue(recorder, imageVerifyPhaseExists, func() (bool, error) {
		return is.imageExistsForComputedSpec(ctx, imageID)
	}, imageVerifyAttrMetadataAuthoritative, boolLabel(is.imageMetadataAuthoritative()))
	if err != nil {
		return nil, err
	}

	return &imageVerifyResult{
		imageID: imageID,
		exists:  exists,
		valid:   imageIDErr == nil,
		opts:    opts,
	}, nil
}

func (is *ContainerImageService) verifyExplicitImageID(ctx context.Context, recorder imageVerifyRecorder, imageID string) (*imageVerifyResult, error) {
	if is.imageMetadataAuthoritative() {
		current, err := measureImageVerifyValue(recorder, imageVerifyPhaseMetadata, func() (bool, error) {
			return is.imageMetadataCurrent(ctx, imageID)
		})
		if err != nil {
			return nil, err
		}
		if current {
			return &imageVerifyResult{imageID: imageID, exists: true, valid: true}, nil
		}
	}

	exists, err := measureImageVerifyValue(recorder, imageVerifyPhaseRegistryExists, func() (bool, error) {
		return is.builder.Exists(ctx, imageID)
	})
	if err != nil {
		return nil, err
	}

	return &imageVerifyResult{imageID: imageID, exists: exists, valid: true}, nil
}

func (is *ContainerImageService) buildOptionsFromVerifyRequest(ctx context.Context, in *pb.VerifyImageBuildRequest) (*BuildOpts, error) {
	tag := in.PythonVersion
	if in.PythonVersion == types.Python3.String() {
		tag = is.config.ImageService.PythonVersion
	}

	baseImageTag, ok := is.config.ImageService.Runner.Tags[tag]
	if !ok {
		return nil, errors.Errorf("Python version not supported: %s", in.PythonVersion)
	}

	authInfo, _ := auth.AuthInfoFromContext(ctx)
	buildSecrets, err := is.retrieveBuildSecrets(ctx, in.Secrets, authInfo)
	if err != nil {
		return nil, err
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

	if in.Dockerfile == "" {
		opts.BaseImageTag = baseImageTag
		opts.BaseImageName = is.config.ImageService.Runner.BaseImageName
		opts.BaseImageRegistry = is.config.ImageService.Runner.BaseImageRegistry
	}

	if in.IgnorePython {
		opts.IgnorePython = true
	}

	return opts, nil
}

func (is *ContainerImageService) prepareBuildOptionsForImageID(ctx context.Context, in *pb.VerifyImageBuildRequest, opts *BuildOpts) error {
	if in.ExistingImageUri != "" {
		opts.ExistingImageUri = in.ExistingImageUri

		baseImage, err := ExtractImageNameAndTag(opts.ExistingImageUri)
		if err != nil {
			return err
		}
		opts.BaseImageRegistry = baseImage.Registry
		opts.BaseImageName = baseImage.Repo
		opts.BaseImageTag = baseImage.Tag
		opts.BaseImageDigest = baseImage.Digest
		opts.addPythonRequirements()
	}

	is.resolveBaseImageDigest(ctx, opts, in.ExistingImageUri == "")

	if in.Dockerfile != "" {
		opts.addPythonRequirements()
	}

	if is.config.ImageService.ClipVersion != uint32(types.ClipVersion2) {
		return nil
	}

	if opts.Dockerfile == "" {
		if !is.builder.hasWorkToDo(opts) {
			return nil
		}

		dockerfile, err := is.builder.RenderV2Dockerfile(opts)
		if err != nil {
			return err
		}
		opts.Dockerfile = dockerfile
		return nil
	}

	if is.builder.hasWorkToDo(opts) {
		opts.Dockerfile = is.builder.appendToDockerfile(opts)
	}

	return nil
}

func (is *ContainerImageService) imageExistsForComputedSpec(ctx context.Context, imageID string) (bool, error) {
	if is.imageMetadataAuthoritative() {
		return is.imageMetadataCurrent(ctx, imageID)
	}

	exists, err := is.builder.Exists(ctx, imageID)
	if err != nil {
		return false, err
	}

	_, err = is.backendRepo.GetImageClipVersion(ctx, imageID)
	if err == nil {
		return exists, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}

	return false, nil
}

func (is *ContainerImageService) imageMetadataCurrent(ctx context.Context, imageID string) (bool, error) {
	clipVersion, err := is.backendRepo.GetImageClipVersion(ctx, imageID)
	if err == nil {
		return clipVersion == is.config.ImageService.ClipVersion, nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return false, err
}

func (is *ContainerImageService) imageMetadataAuthoritative() bool {
	return is.config.ImageService.RegistryStore == reg.S3ImageRegistryStore
}
