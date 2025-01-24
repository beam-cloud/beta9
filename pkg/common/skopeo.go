package common

import (
	"context"
	"encoding/json"
	"fmt"
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

type SkopeoClient struct {
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

func NewSkopeoClient(config types.AppConfig) *SkopeoClient {
	return &SkopeoClient{
		pullCommand:    imagePullCommand,
		commandTimeout: -1,
		debug:          false,
		enableTLS:      config.ImageService.EnableTLS,
		creds:          "",
		pDeathSignal:   0,
	}
}

func (p *SkopeoClient) Inspect(ctx context.Context, sourceImage string, creds string) (ImageMetadata, error) {
	var imageMetadata ImageMetadata
	args := []string{"inspect", fmt.Sprintf("docker://%s", sourceImage)}

	args = append(args, p.inspectArgs(creds)...)
	cmd := exec.CommandContext(ctx, p.pullCommand, args...)
	cmd.Stdout = &ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Info().Str("operation", fmt.Sprintf("%s inspect", p.pullCommand)) }}
	cmd.Stderr = &ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Error().Str("operation", fmt.Sprintf("%s inspect", p.pullCommand)) }}

	output, err := exec.CommandContext(ctx, p.pullCommand, args...).Output()
	if err != nil {
		return imageMetadata, &types.ExitCodeError{
			ExitCode: types.WorkerContainerExitCodeInvalidCustomImage,
		}
	}

	err = json.Unmarshal(output, &imageMetadata)
	if err != nil {
		return imageMetadata, err
	}

	return imageMetadata, nil
}

func (p *SkopeoClient) Copy(ctx context.Context, sourceImage string, dest string, creds string) error {
	args := []string{"copy", fmt.Sprintf("docker://%s", sourceImage), dest}

	args = append(args, p.copyArgs(creds)...)
	cmd := exec.CommandContext(ctx, p.pullCommand, args...)
	cmd.Env = os.Environ()
	cmd.Dir = imageTmpDir
	cmd.Stdout = &ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Info().Str("operation", fmt.Sprintf("%s copy", p.pullCommand)) }}
	cmd.Stderr = &ZerologIOWriter{LogFn: func() *zerolog.Event { return log.Error().Str("operation", fmt.Sprintf("%s copy", p.pullCommand)) }}

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

func (p *SkopeoClient) inspectArgs(creds string) (out []string) {
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

func (p *SkopeoClient) copyArgs(creds string) (out []string) {
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

func (p *SkopeoClient) startCommand(cmd *exec.Cmd) (chan runc.Exit, error) {
	if p.pDeathSignal != 0 {
		return runc.Monitor.StartLocked(cmd)
	}
	return runc.Monitor.Start(cmd)
}
