package storage

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"text/template"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/cenkalti/backoff"
)

const cunoFsMountTimeout time.Duration = 30 * time.Second

type CunoFsStorage struct {
	mountCmd *exec.Cmd
	config   types.CunoFSConfig
}

func NewCunoFsStorage(config types.CunoFSConfig) (Storage, error) {
	return &CunoFsStorage{
		config: config,
	}, nil
}

func (s *CunoFsStorage) Mount(localPath string) error {
	slog.Info("cunofs filesystem mounting", "local_path", localPath)
	s.mountCmd = exec.Command(
		"cuno",
		"mount",
		localPath,
		"--root",
		fmt.Sprintf("s3://%s", s.config.S3BucketName),
		"--mkdir",
	)

	// Start the mount command in the background
	go func() {
		output, err := s.mountCmd.CombinedOutput()
		if err != nil {
			slog.Error("error executing cunofs mount", "error", err, "output", string(output))
		}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	timeout := time.After(cunoFsMountTimeout)

	done := make(chan bool)
	go func() {
		for {
			select {
			case <-timeout:
				done <- false
				return
			case <-ticker.C:
				if isMounted(localPath) {
					done <- true
					return
				}
			}
		}
	}()

	// Wait for confirmation or timeout
	if !<-done {
		return fmt.Errorf("failed to mount CunoFS filesystem to: '%s'", localPath)
	}

	slog.Info("cunofs filesystem mounted", "local_path", localPath)
	return nil
}

func (s *CunoFsStorage) Format(fsName string) error {
	activateCmd := exec.Command("sh", "-c", fmt.Sprintf("yes | echo %s | cuno creds activate", s.config.LicenseKey))
	_, err := activateCmd.CombinedOutput()
	if err != nil {
		return err
	}

	templateStr := `aws_access_key_id={{.S3AccessKey}}
aws_secret_access_key={{.S3SecretKey}}
endpoint={{.S3EndpointUrl}}
`
	tmpl, err := template.New("cunoTemplate").Parse(templateStr)
	if err != nil {
		return err
	}

	var creds bytes.Buffer
	err = tmpl.Execute(&creds, s.config)
	if err != nil {
		return err
	}

	// Create a temporary file for the creds
	tmpFile, err := os.CreateTemp("", "cuno-creds-*.txt")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.Write(creds.Bytes()); err != nil {
		tmpFile.Close()
		return err
	}
	tmpFile.Close()

	importCmd := exec.Command("sh", "-c", fmt.Sprintf("cuno creds import %s", tmpFile.Name()))
	_, err = importCmd.CombinedOutput()
	if err != nil {
		return err
	}

	return nil
}

func (s *CunoFsStorage) Unmount(localPath string) error {
	cunoFsUmount := func() error {
		cmd := exec.Command("cuno", "umount", localPath)

		output, err := cmd.CombinedOutput()
		if err != nil {
			slog.Error("error executing cuno umount", "error", err, "output", string(output))
			return err
		}

		slog.Info("cunofs filesystem unmounted", "local_path", localPath)
		return nil
	}

	err := backoff.Retry(cunoFsUmount, backoff.WithMaxRetries(backoff.NewConstantBackOff(1*time.Second), 10))
	if err == nil {
		return nil
	}

	// Forcefully kill the fuse mount devices
	err = exec.Command("fuser", "-k", "/dev/fuse").Run()
	if err != nil {
		return fmt.Errorf("error executing fuser -k /dev/fuse: %v", err)
	}

	return nil
}
