package image

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

type ImageIdHash struct {
	BaseImageName   string
	BaseImageTag    string
	UserImageTag    string
	PythonVersion   string
	PythonPackages  []string
	ExitingImageUri string
	CommandListHash string
}

func getImageID(opts *BuildOpts) (string, error) {
	h := sha1.New()
	h.Write([]byte(strings.Join(opts.Commands, "-")))
	if len(opts.BuildSteps) > 0 {
		for _, step := range opts.BuildSteps {
			fmt.Fprintf(h, "%s-%s", step.Type, step.Command)
		}
	}
	if len(opts.EnvVars) > 0 {
		for _, envVar := range opts.EnvVars {
			fmt.Fprint(h, envVar)
		}
	}
	if opts.Dockerfile != "" {
		fmt.Fprint(h, opts.Dockerfile)
	}
	if opts.BuildCtxObject != "" {
		fmt.Fprint(h, opts.BuildCtxObject)
	}
	commandListHash := hex.EncodeToString(h.Sum(nil))

	bodyToHash := &ImageIdHash{
		BaseImageName:   opts.BaseImageName,
		BaseImageTag:    getImageTagOrDigest(opts.BaseImageDigest, opts.BaseImageTag),
		PythonVersion:   opts.PythonVersion,
		PythonPackages:  opts.PythonPackages,
		ExitingImageUri: opts.ExistingImageUri,
		CommandListHash: commandListHash,
	}

	hash, err := hashstructure.Hash(bodyToHash, hashstructure.FormatV2, nil)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%016x", hash), nil
}
