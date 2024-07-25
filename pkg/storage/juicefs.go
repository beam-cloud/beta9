package storage

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"time"

	"github.com/beam-cloud/beta9/pkg/types"
)

const juiceFsMountTimeout time.Duration = 30 * time.Second

type JuiceFsStorage struct {
	mountCmd *exec.Cmd
	config   types.JuiceFSConfig
}

func NewJuiceFsStorage(config types.JuiceFSConfig) (Storage, error) {
	return &JuiceFsStorage{
		config: config,
	}, nil
}

func (s *JuiceFsStorage) Mount(localPath string) error {
	log.Printf("JuiceFS filesystem mounting to: '%s'\n", localPath)

	cacheSize := strconv.FormatInt(s.config.CacheSize, 10)

	prefetch := strconv.FormatInt(s.config.Prefetch, 10)
	if s.config.Prefetch <= 0 {
		prefetch = "1"
	}

	bufferSize := strconv.FormatInt(s.config.BufferSize, 10)
	if s.config.BufferSize <= 0 {
		bufferSize = "300"
	}

	s.mountCmd = exec.Command(
		"juicefs",
		"mount",
		s.config.RedisURI,
		localPath,
		"-d",
		"--bucket", s.config.AWSS3Bucket,
		"--cache-size", cacheSize,
		"--prefetch", prefetch,
		"--buffer-size", bufferSize,
		"--no-bgjob",
		"--no-usage-report",
	)

	// Start the mount command in the background
	go func() {
		output, err := s.mountCmd.CombinedOutput()
		if err != nil {
			log.Fatalf("error executing juicefs mount: %v, output: %s", err, string(output))
		}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	timeout := time.After(juiceFsMountTimeout)

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
		return fmt.Errorf("failed to mount JuiceFS filesystem to: '%s'", localPath)
	}

	log.Printf("JuiceFS filesystem mounted to: '%s'", localPath)
	return nil
}

func (s *JuiceFsStorage) Format(fsName string) error {
	blockSize := strconv.FormatInt(s.config.BlockSize, 10)
	if s.config.BlockSize <= 0 {
		blockSize = "4096"
	}

	cmd := exec.Command(
		"juicefs",
		"format",
		"--storage", "s3",
		"--bucket", s.config.AWSS3Bucket,
		"--block-size", blockSize,
		s.config.RedisURI,
		fsName,
		"--no-update",
	)

	if s.config.AWSAccessKey != "" || s.config.AWSSecretKey != "" {
		cmd.Args = append(
			cmd.Args,
			"--access-key", s.config.AWSAccessKey,
			"--secret-key", s.config.AWSSecretKey,
		)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("error executing juicefs format: %v, output: %s", err, string(output))
	}

	return nil
}

func (s *JuiceFsStorage) Unmount(localPath string) error {
	cmd := exec.Command("juicefs", "umount", localPath)

	_, err := cmd.CombinedOutput()
	if err != nil {
		log.Println("error executing juicefs umount: %v", err)

		time.Sleep(5 * time.Minute) // TEST: keep alive so we can debug

		// log.Printf("error executing juicefs umount: %v\n", err)
		// log.Printf("attempting to kill fuser processes\n")

		// cmd := exec.Command("fuser", "-k", "/dev/fuse")
		// output, err := cmd.CombinedOutput()
		// if err != nil {
		// 	return fmt.Errorf("error executing juicefs umount: %v, output: %s", err, string(output))
		// }
		return err
	}

	log.Printf("JuiceFS filesystem unmounted from: '%s'\n", localPath)
	return nil
}
