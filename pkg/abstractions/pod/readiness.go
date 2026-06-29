package pod

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/beam-cloud/beta9/pkg/network"
)

const (
	postgresProtocolVersion uint32 = 196608
	postgresReadinessUser          = "postgres"
)

func (pb *PodProxyBuffer) checkContainerReady(address string, timeout time.Duration) bool {
	if llmEnabled(pb.stubConfig) {
		return pb.checkLLMContainerReady(address, timeout)
	}
	if pb.stubConfig != nil {
		if database := pb.stubConfig.EffectiveDatabaseConfig(); database != nil && database.IsPostgres() {
			return pb.checkPostgresContainerReady(address, timeout)
		}
	}
	return pb.checkContainerAvailableWithTimeout(address, timeout)
}

func (pb *PodProxyBuffer) checkPostgresContainerReady(address string, timeout time.Duration) bool {
	if timeout <= 0 {
		timeout = containerAvailableTimeout
	}
	conn, err := network.ConnectToBackend(pb.baseContext(), address, timeout, pb.tailscale, pb.tsConfig, pb.containerRepo)
	if err != nil {
		return false
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	payload := []byte("user\x00" + postgresReadinessUser + "\x00database\x00" + postgresReadinessUser + "\x00application_name\x00beta9_readiness\x00\x00")
	startup := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(startup[0:4], uint32(len(startup)))
	binary.BigEndian.PutUint32(startup[4:8], postgresProtocolVersion)
	copy(startup[8:], payload)
	if _, err := conn.Write(startup); err != nil {
		return false
	}

	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return false
	}
	size := int(binary.BigEndian.Uint32(header[1:5])) - 4
	if size < 0 || size > 1<<20 {
		return false
	}
	body := make([]byte, size)
	if _, err := io.ReadFull(conn, body); err != nil {
		return false
	}
	return header[0] != 'E' || !bytes.Contains(bytes.ToLower(body), []byte("starting up"))
}
