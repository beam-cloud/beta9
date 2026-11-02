package image

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/beam-cloud/beta9/pkg/auth"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/mitchellh/hashstructure/v2"
)

// getImageID calculates a deterministic image ID from build options.
// This is the single source of truth for image identity.
//
// For V2 builds with Dockerfiles (custom or generated), the Dockerfile content
// contains all build instructions, so we only need to hash the Dockerfile and
// build context. For V1 builds and V2 builds without Dockerfiles, we hash all
// the individual build options that will be used to construct the image.
func getImageID(opts *BuildOpts) (string, error) {
	hash, err := buildDefinitionHash(opts)
	if err != nil {
		return "", err
	}
	if opts.Scope != "" {
		// Hashed on top, so unscoped builds keep the ids they had.
		hash, err = hashstructure.Hash(struct {
			Build uint64
			Scope string
		}{hash, opts.Scope}, hashstructure.FormatV2, nil)
		if err != nil {
			return "", err
		}
	}
	return fmt.Sprintf("%016x", hash), nil
}

// imageScope keeps an image built from private inputs (build secrets,
// registry credentials or a git token) to the workspace that built it. An
// image ID otherwise names only the build definition, and a workspace that
// repeats the definition reuses the image, including what the private inputs
// fetched into it. The scope derives from a workspace secret, so another
// workspace can neither reproduce nor guess a scoped image ID.
func imageScope(ctx context.Context, opts *BuildOpts) (string, error) {
	if len(opts.BuildSecrets) == 0 && len(opts.ExistingImageCreds) == 0 && (opts.GitSource == nil || opts.GitSource.Token == "") {
		return "", nil
	}
	authInfo, _ := auth.AuthInfoFromContext(ctx)
	if authInfo == nil || authInfo.Workspace == nil || authInfo.Workspace.SigningKey == nil || *authInfo.Workspace.SigningKey == "" {
		return "", errors.New("workspace signing key is unavailable")
	}
	mac := hmac.New(sha256.New, []byte(*authInfo.Workspace.SigningKey))
	mac.Write([]byte("image scope"))
	return hex.EncodeToString(mac.Sum(nil)), nil
}

func buildDefinitionHash(opts *BuildOpts) (uint64, error) {
	if opts.GitSource != nil {
		return gitImageHash(opts)
	}

	// For V2 builds with a Dockerfile, the Dockerfile contains all the build instructions
	// Base the image ID primarily on the Dockerfile content and build context
	if opts.ClipVersion == uint32(types.ClipVersion2) && opts.Dockerfile != "" {
		return hashstructure.Hash(struct {
			ClipVersion    uint32
			Dockerfile     string
			BuildCtxObject string
			EnvVars        []string
		}{
			ClipVersion:    opts.ClipVersion,
			Dockerfile:     opts.Dockerfile,
			BuildCtxObject: opts.BuildCtxObject,
			EnvVars:        opts.EnvVars,
		}, hashstructure.FormatV2, nil)
	}

	// For V1 builds and V2 builds without Dockerfiles, hash all build options
	// Order matters for consistency, so we use a struct with defined field order
	return hashstructure.Hash(struct {
		ClipVersion       uint32
		BaseImageName     string
		BaseImageTag      string
		BaseImageDigest   string
		BaseImageRegistry string
		ExistingImageUri  string
		PythonVersion     string
		PythonPackages    []string
		Commands          []string
		BuildSteps        []BuildStep
		EnvVars           []string
		Dockerfile        string
		BuildCtxObject    string
	}{
		ClipVersion:       opts.ClipVersion,
		BaseImageName:     opts.BaseImageName,
		BaseImageTag:      opts.BaseImageTag,
		BaseImageDigest:   opts.BaseImageDigest,
		BaseImageRegistry: opts.BaseImageRegistry,
		ExistingImageUri:  opts.ExistingImageUri,
		PythonVersion:     opts.PythonVersion,
		PythonPackages:    opts.PythonPackages,
		Commands:          opts.Commands,
		BuildSteps:        opts.BuildSteps,
		EnvVars:           opts.EnvVars,
		Dockerfile:        opts.Dockerfile,
		BuildCtxObject:    opts.BuildCtxObject,
	}, hashstructure.FormatV2, nil)
}
