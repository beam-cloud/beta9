package storage

import (
	"fmt"
	"os/exec"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/rs/zerolog/log"
)

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

	alluxioJavaOpts := []string{
		"-Xmx32g -Xms8g -XX:MaxDirectMemorySize=32g",
		fmt.Sprintf("-Dalluxio.coordinator.hostname=%v", s.config.CoordinatorHostname),
		fmt.Sprintf("-Dalluxio.etcd.endpoints=%v", s.config.EtcdEndpoint),
		fmt.Sprintf("-Dalluxio.etcd.username=%v", s.config.EtcdUsername),
		fmt.Sprintf("-Dalluxio.etcd.password=%v", s.config.EtcdPassword),
		fmt.Sprintf("-Dalluxio.etcd.tls.enabled=%t", s.config.EtcdTlsEnabled),
		"-Dalluxio.etcd.tls.ca.cert=/etc/ssl/certs/ca-certificates.crt",
		fmt.Sprintf("-Dalluxio.license=%v", s.config.License),
		"-Dalluxio.worker.membership.manager.type=ETCD",
		"-Dalluxio.mount.table.source=ETCD",
		"-Dalluxio.user.metadata.cache.max.size=0",
		"-Dalluxio.security.authorization.permission.enabled=false",
	}

	args := []string{
		"run",
		"-d",
		"--privileged",
		"--net=host",
		"--name=alluxio-fuse",
		fmt.Sprintf("-v %v:%v:shared", localPath, localPath),
		fmt.Sprintf(`-e ALLUXIO_JAVA_OPTS="%v"`, strings.Join(alluxioJavaOpts, " ")),
		s.config.ImageUrl,
		"fuse",
		"-o allow_other",
		fmt.Sprintf("%v/fuse", localPath),
	}

	if err := exec.Command("docker", args...).Run(); err != nil {
		log.Error().Err(err).Str("local_path", localPath).Msg("alluxio: mount process exited with error")
		return err
	}

	return nil
}

func (s *AlluxioStorage) Unmount(localPath string) error {
	if err := exec.Command("docker", "rm", "-f", "alluxio-fuse").Run(); err != nil {
		log.Error().Err(err).Str("local_path", localPath).Msg("alluxio: unmount process exited with error")
		return err
	}
	return nil
}

func (s *AlluxioStorage) Format(fsName string) error {
	return nil
}
