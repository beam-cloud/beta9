package storage

import (
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

type GeeseStorage struct {
	config types.GeeseConfig
	pid    int
}

func NewGeeseStorage(config types.GeeseConfig) (Storage, error) {
	return &GeeseStorage{
		config: config,
	}, nil
}

func (s *GeeseStorage) Mount(localPath string) error {
	log.Info().Str("local_path", localPath).Msg("geese filesystem mounting")

	args := []string{}
	if s.config.Debug {
		args = append(args, "--debug")
	}
	if s.config.Force {
		args = append(args, "-f")
	}
	if s.config.FsyncOnClose {
		args = append(args, "--fsync-on-close")
	}

	if s.config.MemoryLimit > 0 {
		args = append(args, fmt.Sprintf("--memory-limit=%d", s.config.MemoryLimit))
	}
	if s.config.MaxFlushers > 0 {
		args = append(args, fmt.Sprintf("--max-flushers=%d", s.config.MaxFlushers))
	}
	if s.config.MaxParallelParts > 0 {
		args = append(args, fmt.Sprintf("--max-parallel-parts=%d", s.config.MaxParallelParts))
	}
	if s.config.PartSizes > 0 {
		args = append(args, fmt.Sprintf("--part-sizes=%d", s.config.PartSizes))
	}
	if s.config.DirMode != "" {
		args = append(args, fmt.Sprintf("--dir-mode=%s", s.config.DirMode))
	}
	if s.config.FileMode != "" {
		args = append(args, fmt.Sprintf("--file-mode=%s", s.config.FileMode))
	}
	if s.config.ListType > 0 {
		args = append(args, fmt.Sprintf("--list-type=%d", s.config.ListType))
	}
	if s.config.EndpointUrl != "" {
		args = append(args, fmt.Sprintf("--endpoint=%s", s.config.EndpointUrl))
	}

	// Add bucket and mount path
	args = append(args, s.config.BucketName, localPath)

	cmd := exec.Command("geesefs", args...)

	// Set bucket credentials as env vars
	if s.config.AccessKey != "" || s.config.SecretKey != "" {
		cmd.Env = append(cmd.Env,
			fmt.Sprintf("AWS_ACCESS_KEY_ID=%s", s.config.AccessKey),
			fmt.Sprintf("AWS_SECRET_ACCESS_KEY=%s", s.config.SecretKey),
		)
	}

	// Start the geesefs process so we can capture the PID
	if err := cmd.Start(); err != nil {
		log.Error().Err(err).Msg("failed to start geesefs process")
		return err
	}

	s.pid = cmd.Process.Pid

	// Wait asynchronously
	go func() {
		if err := cmd.Wait(); err != nil {
			log.Error().Err(err).Msgf("geesefs mount process (%d) exited with error", s.pid)
		}
	}()

	// Poll until the filesystem is mounted or we timeout
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(30 * time.Second)

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
		return fmt.Errorf("failed to mount GeeseFS filesystem to: '%s'", localPath)
	}

	log.Info().Str("local_path", localPath).Msg("geesefs filesystem mounted")
	return nil
}

func (s *GeeseStorage) Unmount(localPath string) error {
	waitTimeSeconds := 5

	// Try to terminate the geesefs process w/ a SIGINT
	if s.pid > 0 {
		if p, err := os.FindProcess(s.pid); err == nil {
			log.Info().Str("local_path", localPath).Int("pid", s.pid).Msg("geesefs: unmounting filesystem")

			p.Signal(syscall.SIGINT)

			// Wait up to 3 seconds for graceful shutdown
			for i := 0; i < waitTimeSeconds; i++ {
				if p.Signal(syscall.Signal(0)) != nil {
					break // Process exited
				}

				time.Sleep(1 * time.Second)
			}

			// Force kill the process if still running
			p.Kill()
		}
	}

	log.Info().Str("local_path", localPath).Msg("geesefs filesystem unmounted")
	s.pid = 0
	return nil
}

func (s *GeeseStorage) Format(fsName string) error {
	return nil
}
