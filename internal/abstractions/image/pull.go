package image

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"syscall"

	runc "github.com/slai-labs/go-runc"
)

const imagePullCommand string = "skopeo"

type ImagePuller struct {
	ImagePath      string
	PullCommand    string
	PdeathSignal   syscall.Signal
	CommandTimeout int
	Debug          bool
	Creds          string
}

func NewImagePuller(provider CredentialProvider) (*ImagePuller, error) {
	baseImagePath := "nonsense" // filepath.Join(ImageServiceConfig.BaseImageCachePath)
	os.MkdirAll(baseImagePath, os.ModePerm)

	creds, err := provider.GetAuthString()
	if err != nil {
		return nil, err
	}

	return &ImagePuller{
		ImagePath:      baseImagePath,
		PullCommand:    imagePullCommand,
		CommandTimeout: -1,
		Debug:          false,
		Creds:          creds,
	}, nil
}

func (i *ImagePuller) Pull(context context.Context, source string, dest string, creds *string) error {
	args := []string{"copy", source, dest}

	args = append(args, i.args(creds)...)
	cmd := exec.CommandContext(context, i.PullCommand, args...)
	cmd.Env = os.Environ()
	cmd.Dir = i.ImagePath
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	ec, err := i.startCommand(cmd)
	if err != nil {
		return err
	}

	status, err := runc.Monitor.Wait(cmd, ec)
	if err == nil && status != 0 {
		err = fmt.Errorf("unable to pull base image: %s", source)
	}

	return err
}

func (i *ImagePuller) startCommand(cmd *exec.Cmd) (chan runc.Exit, error) {
	if i.PdeathSignal != 0 {
		return runc.Monitor.StartLocked(cmd)
	}
	return runc.Monitor.Start(cmd)
}

func (i *ImagePuller) args(creds *string) (out []string) {
	if creds != nil && *creds != "" {
		out = append(out, "--src-creds", *creds)
	} else if creds != nil && *creds == "" {
		out = append(out, "--src-no-creds")
	} else {
		out = append(out, "--src-creds", i.Creds)
	}

	if i.CommandTimeout > 0 {
		out = append(out, "--command-timeout", fmt.Sprintf("%d", i.CommandTimeout))
	}

	if i.Debug {
		out = append(out, "--debug")
	}

	return out
}
