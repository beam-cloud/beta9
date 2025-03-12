package common

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"syscall"
	"time"

	types "github.com/beam-cloud/beta9/pkg/types"
	"github.com/beam-cloud/go-runc"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	imagePullCommand string = "skopeo"
	imageTmpDir      string = "/tmp"
)

type SkopeoClient interface {
	Inspect(ctx context.Context, sourceImage string, creds string, overrideLogger *slog.Logger) (ImageMetadata, error)
	InspectSizeInBytes(ctx context.Context, sourceImage string, creds string) (int64, error)
	Copy(ctx context.Context, sourceImage string, dest string, creds string, overrideLogger *slog.Logger) error
}

type skopeoClient struct {
	pullCommand    string
	commandTimeout int
	debug          bool
	enableTLS      bool
	creds          string
	pDeathSignal   syscall.Signal
}

type ImageMetadata struct {
	Name          string         `json:"Name"`
	Digest        string         `json:"Digest"`
	RepoTags      []string       `json:"RepoTags"`
	Created       time.Time      `json:"Created"`
	DockerVersion string         `json:"DockerVersion"`
	Labels        map[string]any `json:"Labels"`
	Architecture  string         `json:"Architecture"`
	Os            string         `json:"Os"`
	Layers        []string       `json:"Layers"`
	LayersData    []struct {
		MIMEType    string `json:"MIMEType"`
		Digest      string `json:"Digest"`
		Size        int    `json:"Size"`
		Annotations any    `json:"Annotations"`
	} `json:"LayersData"`
	Env []string `json:"Env"`
}

func NewSkopeoClient(config types.AppConfig) SkopeoClient {
	return &skopeoClient{
		pullCommand:    imagePullCommand,
		commandTimeout: -1,
		debug:          false,
		enableTLS:      config.ImageService.EnableTLS,
		creds:          "",
		pDeathSignal:   0,
	}
}

func (p *skopeoClient) Inspect(ctx context.Context, sourceImage string, creds string, overrideLogger *slog.Logger) (ImageMetadata, error) {
	var imageMetadata ImageMetadata
	args := []string{"inspect", fmt.Sprintf("docker://%s", sourceImage)}

	args = append(args, p.inspectArgs(creds)...)
	cmd := exec.CommandContext(ctx, p.pullCommand, args...)
	cmd.Stdout = &ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Info().Str("operation", fmt.Sprintf("%s inspect", p.pullCommand)) }}
	cmd.Stderr = &ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Error().Str("operation", fmt.Sprintf("%s inspect", p.pullCommand)) }}
	if overrideLogger != nil {
		cmd.Stdout = &ExecWriter{Logger: overrideLogger}
		cmd.Stderr = &ExecWriter{Logger: overrideLogger}
	}

	output, err := exec.CommandContext(ctx, p.pullCommand, args...).Output()
	if err != nil {
		return imageMetadata, &types.ExitCodeError{
			ExitCode: types.ContainerExitCodeInvalidCustomImage,
		}
	}

	err = json.Unmarshal(output, &imageMetadata)
	if err != nil {
		return imageMetadata, err
	}

	return imageMetadata, nil
}

func (p *skopeoClient) InspectSizeInBytes(ctx context.Context, sourceImage string, creds string) (int64, error) {
	imageMetadata, err := p.Inspect(ctx, sourceImage, creds, nil)
	if err != nil {
		return 0, err
	}

	size := int64(0)
	for _, layer := range imageMetadata.LayersData {
		size += int64(layer.Size)
	}

	return size, nil
}

func (p *skopeoClient) Copy(ctx context.Context, sourceImage string, dest string, creds string, overrideLogger *slog.Logger) error {
	args := []string{"copy", fmt.Sprintf("docker://%s", sourceImage), dest}

	args = append(args, p.copyArgs(creds)...)
	cmd := exec.CommandContext(ctx, p.pullCommand, args...)
	cmd.Env = os.Environ()
	cmd.Dir = imageTmpDir
	cmd.Stdout = &ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Info().Str("operation", fmt.Sprintf("%s copy", p.pullCommand)) }}
	cmd.Stderr = &ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Error().Str("operation", fmt.Sprintf("%s copy", p.pullCommand)) }}
	if overrideLogger != nil {
		cmd.Stdout = &ExecWriter{Logger: overrideLogger}
		cmd.Stderr = &ExecWriter{Logger: overrideLogger}
	}

	ec, err := p.startCommand(cmd)
	if err != nil {
		return err
	}

	status, err := runc.Monitor.Wait(cmd, ec)
	if err == nil && status != 0 {
		return fmt.Errorf("unable to copy image: %v", cmd.String())
	}
	return nil

}

func (p *skopeoClient) inspectArgs(creds string) (out []string) {
	if creds != "" {
		out = append(out, "--creds", creds)
	} else if creds == "" {
		out = append(out, "--no-creds")
	} else if p.creds != "" {
		out = append(out, "--creds", p.creds)
	}

	if p.commandTimeout > 0 {
		out = append(out, "--command-timeout", fmt.Sprintf("%d", p.commandTimeout))
	}

	if !p.enableTLS {
		out = append(out, []string{"--tls-verify=false"}...)
	}

	if p.debug {
		out = append(out, "--debug")
	}

	return out
}

func (p *skopeoClient) copyArgs(creds string) (out []string) {
	if creds != "" {
		out = append(out, "--src-creds", creds)
	} else if creds == "" {
		out = append(out, "--src-no-creds")
	} else if p.creds != "" {
		out = append(out, "--src-creds", p.creds)
	}

	if p.commandTimeout > 0 {
		out = append(out, "--command-timeout", fmt.Sprintf("%d", p.commandTimeout))
	}

	if !p.enableTLS {
		out = append(out, []string{"--src-tls-verify=false", "--dest-tls-verify=false"}...)
	}

	if p.debug {
		out = append(out, "--debug")
	}

	return out
}

func (p *skopeoClient) startCommand(cmd *exec.Cmd) (chan runc.Exit, error) {
	if p.pDeathSignal != 0 {
		return runc.Monitor.StartLocked(cmd)
	}
	return runc.Monitor.Start(cmd)
}
