package storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

const defaultAlluxioCoordinatorPort = 19999

type AlluxioStorage struct {
	config types.AlluxioConfig
}

func NewAlluxioStorage(config types.AlluxioConfig) (Storage, error) {
	return &AlluxioStorage{
		config: config,
	}, nil
}

func (s *AlluxioStorage) Mount(localPath string) error {
	log.Info().Str("local_path", localPath).Msg("alluxio filesystem mounting")

	localPath = s.stripLocalPath(localPath)

	mountRequest := map[string]interface{}{
		"path": localPath,
		"ufs":  fmt.Sprintf("s3://%s", s.config.BucketName),
		"options": map[string]string{
			"s3a.accessKeyId":                        s.config.AccessKey,
			"s3a.secretKey":                          s.config.SecretKey,
			"alluxio.underfs.s3.endpoint":            s.config.EndpointURL,
			"alluxio.underfs.s3.endpoint.region":     s.config.Region,
			"alluxio.underfs.s3.inherit.acl":         "false",
			"alluxio.underfs.s3.disable.dns.buckets": "true",
		},
	}

	requestBody, err := json.Marshal(mountRequest)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s:%d/api/v1/mount", s.config.CoordinatorHostname, defaultAlluxioCoordinatorPort)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(requestBody))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("mount request failed with status %d", resp.StatusCode)
	}

	log.Info().Str("local_path", localPath).Msg("alluxio filesystem mounted successfully")
	return nil
}

func (s *AlluxioStorage) Unmount(localPath string) error {
	log.Info().Str("local_path", localPath).Msg("alluxio filesystem unmounting")

	localPath = s.stripLocalPath(localPath)

	unmountRequest := map[string]string{
		"path": localPath,
	}

	requestBody, err := json.Marshal(unmountRequest)
	if err != nil {
		return err
	}

	url := fmt.Sprintf("http://%s:%d/api/v1/mount", s.config.CoordinatorHostname, defaultAlluxioCoordinatorPort)
	req, err := http.NewRequest(http.MethodDelete, url, bytes.NewBuffer(requestBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unmount request failed with status %d", resp.StatusCode)
	}

	log.Info().Str("local_path", localPath).Msg("alluxio filesystem unmounted successfully")
	return nil
}

func (s *AlluxioStorage) stripLocalPath(localPath string) string {
	base := filepath.Base(localPath)
	return filepath.Join("/", base)
}

func (s *AlluxioStorage) Format(fsName string) error {
	return nil
}
