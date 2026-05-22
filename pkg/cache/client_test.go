package cache

import (
	"bytes"
	"context"
	"errors"
	"testing"

	proto "github.com/beam-cloud/beta9/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type orderedTestHasher struct {
	hosts []*Host
}

func (h *orderedTestHasher) Add(hosts ...*Host) {
	h.hosts = append(h.hosts, hosts...)
}

func (h *orderedTestHasher) Remove(host *Host) {
	filtered := h.hosts[:0]
	for _, existing := range h.hosts {
		if existing == nil || host == nil || existing.HostId != host.HostId {
			filtered = append(filtered, existing)
		}
	}
	h.hosts = filtered
}

func (h *orderedTestHasher) GetN(n int, key string) []*Host {
	if n > len(h.hosts) {
		n = len(h.hosts)
	}
	return h.hosts[:n]
}

func TestReadContentIntoUsesAttachedLocalServerOnlyForSelectedHost(t *testing.T) {
	store := newTestStore(t, 5)
	content := []byte("local-cache-content")
	hash, _, err := store.AddReader(context.Background(), bytes.NewReader(content))
	require.NoError(t, err)

	localHost := &Host{HostId: "local-host"}
	store.currentHost = localHost
	client := &Client{
		ctx:                   context.Background(),
		clientConfig:          ClientConfig{NTopHosts: 1},
		grpcClients:           make(map[string]proto.CacheClient),
		grpcConns:             make(map[string]*grpc.ClientConn),
		localServers:          make(map[string]*Server),
		rawReadPools:          make(map[string]*rawReadConnPool),
		localHostCache:        make(map[string]*localClientCache),
		hasher:                &orderedTestHasher{hosts: []*Host{{HostId: "remote-host"}}},
		maxGetContentAttempts: 1,
	}
	client.AttachLocalServer(&Server{cas: store})

	dst := make([]byte, len(content))
	_, err = client.ReadContentInto(context.Background(), hash, 0, dst, ClientOptions{})
	require.ErrorIs(t, err, ErrContentNotFound)

	client.removeLocalHostCache(hash)
	client.hasher = &orderedTestHasher{hosts: []*Host{localHost}}
	n, err := client.ReadContentInto(context.Background(), hash, 0, dst, ClientOptions{})
	require.NoError(t, err)
	require.Equal(t, int64(len(content)), n)
	require.Equal(t, content, dst)
}

func TestWithStoreFromContentLockReturnsUnlockErrorAndRetriesDeferredRelease(t *testing.T) {
	registry := &failFirstUnlockRegistry{MockRegistry: NewMockRegistry()}
	client := &Client{
		ctx:         context.Background(),
		locality:    "test",
		coordinator: registry,
	}

	hash, err := client.withStoreFromContentLock(context.Background(), "/source", true, func() (string, error) {
		return "hash", nil
	})

	require.Equal(t, "hash", hash)
	require.ErrorContains(t, err, "unlock failed")
	require.Equal(t, 2, registry.removeCalls)
	require.False(t, registry.locks["store-lock:test:/source"])
}

type failFirstUnlockRegistry struct {
	*MockRegistry
	removeCalls int
}

func (r *failFirstUnlockRegistry) RemoveStoreFromContentLock(ctx context.Context, locality string, sourcePath string) error {
	r.removeCalls++
	if r.removeCalls == 1 {
		return errors.New("unlock failed")
	}
	return r.MockRegistry.RemoveStoreFromContentLock(ctx, locality, sourcePath)
}
