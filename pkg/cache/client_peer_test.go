package cache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/beam-cloud/beta9/proto"
	"github.com/beam-cloud/rendezvous"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type peerDirectoryTest struct {
	hosts  []*Host
	client pb.CacheClient
	err    error
	origin bool
	calls  atomic.Int32
}

func (d *peerDirectoryTest) GetAvailableHosts(context.Context, string) ([]*Host, error) {
	return d.hosts, nil
}
func (d *peerDirectoryTest) NewCachePeerClient(context.Context, *Host) (pb.CacheClient, error) {
	d.calls.Add(1)
	return d.client, d.err
}
func (d *peerDirectoryTest) LocalOriginFillRequired() bool { return d.origin }

func peerTestServer(t *testing.T, id string) (*Server, pb.CacheClient) {
	t.Helper()
	store := newTestStore(t, 64<<10)
	store.currentHost.HostId = id
	server := &Server{ctx: context.Background(), hostId: id, locality: "own/pool", cas: store, globalConfig: GlobalConfig{GRPCMessageSizeBytes: 8 << 20}, metadataStore: NewMockCacheMetadataStore()}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	pb.RegisterCacheServer(grpcServer, server)
	go grpcServer.Serve(listener)
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(); grpcServer.Stop(); listener.Close() })
	return server, pb.NewCacheClient(conn)
}

func peerTestClient(t *testing.T, directory HostDirectory) *Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	c := &Client{ctx: ctx, locality: "own/pool", localNodeID: "node-local", hostDirectory: directory,
		globalConfig: GlobalConfig{GRPCMessageSizeBytes: 8 << 20, GRPCDialTimeoutS: 1, HostMonitorIntervalS: 3600},
		clientConfig: ClientConfig{NTopHosts: 1, ReadTransport: ClientReadTransportConfig{Enabled: true}},
		grpcClients:  map[string]pb.CacheClient{}, grpcConns: map[string]*grpc.ClientConn{}, rawReadPools: map[string]*rawReadConnPool{},
		localServers: map[string]*Server{}, localHostCache: map[localHostCacheKey]*localClientCache{}, hasher: rendezvous.New[*Host](), metadataStore: NewMockCacheMetadataStore(), maxGetContentAttempts: 1}
	c.hostMap = NewHostMap(c.globalConfig, c.addHost)
	c.discoveryClient = NewDiscoveryClient(c.globalConfig, c.hostMap, directory, c.locality)
	c.discoveryClient.localNodeID = c.localNodeID
	t.Cleanup(func() { cancel(); c.Cleanup() })
	return c
}

func TestRoutedCachePeerUsesFactoryWithoutPhysicalOrRawDial(t *testing.T) {
	server, peer := peerTestServer(t, "remote")
	body := bytes.Repeat([]byte("deterministic-peer-pages"), 10000)
	hash, _, err := server.StoreReader(context.Background(), bytes.NewReader(body), "")
	require.NoError(t, err)
	physical, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer physical.Close()
	var physicalDials atomic.Int32
	go func() {
		for {
			conn, err := physical.Accept()
			if err != nil {
				return
			}
			physicalDials.Add(1)
			conn.Close()
		}
	}()
	host := &Host{HostId: "remote", NodeID: "node-remote", RegistrationID: "lease", PoolName: "pool", Locality: "own/pool", CacheRouteID: "cache-route", PrivateAddr: physical.Addr().String()}
	directory := &peerDirectoryTest{hosts: []*Host{host}, client: peer}
	client := peerTestClient(t, directory)
	state, err := client.discoveryClient.GetHostState(context.Background(), host)
	require.NoError(t, err)
	require.Equal(t, host, state, "real GetState intentionally leaves Ok false")
	client.hostMap.Set(host)
	dst := make([]byte, 70001)
	n, err := client.readContentIntoFromHost(context.Background(), host, 0, hash, 65531, dst, &OperationTrace{})
	require.NoError(t, err)
	require.Equal(t, int64(len(dst)), n)
	require.Equal(t, body[65531:65531+len(dst)], dst)
	require.Zero(t, physicalDials.Load())
	require.Empty(t, client.rawReadPools)
	require.GreaterOrEqual(t, directory.calls.Load(), int32(2))
	require.NoError(t, client.Cleanup())
	require.Empty(t, client.grpcClients)
}

func TestRoutedCachePeerFailureNeverFallsBackToAdvertisedEndpoint(t *testing.T) {
	host := &Host{HostId: "remote", NodeID: "node-remote", CacheRouteID: "route", PrivateAddr: "127.0.0.1:1"}
	for name, directory := range map[string]HostDirectory{
		"denied":      &peerDirectoryTest{err: status.Error(codes.PermissionDenied, "denied")},
		"unsupported": testHostDirectoryFunc(func(context.Context, string) ([]*Host, error) { return nil, nil }),
	} {
		t.Run(name, func(t *testing.T) {
			client := peerTestClient(t, directory)
			_, err := client.discoveryClient.GetHostState(context.Background(), host)
			require.Error(t, err)
			require.Error(t, client.addHost(host))
			require.Empty(t, client.grpcClients)
			require.Empty(t, client.grpcConns)
		})
	}
}

func TestSameNodeCacheRouteRetainsLocalFastPath(t *testing.T) {
	server, _ := peerTestServer(t, "local")
	body := []byte("same-node-cache")
	hash, _, err := server.StoreReader(context.Background(), bytes.NewReader(body), "")
	require.NoError(t, err)
	directory := &peerDirectoryTest{err: status.Error(codes.PermissionDenied, "must not relay local bytes")}
	client := peerTestClient(t, directory)
	client.AttachLocalServer(server)
	host := &Host{HostId: "local", NodeID: "node-local", CacheRouteID: "local-route"}
	require.False(t, requiresCachePeerRoute(host, client.localNodeID))
	dst := make([]byte, len(body))
	_, err = client.readContentIntoFromHost(context.Background(), host, 0, hash, 0, dst, &OperationTrace{})
	require.NoError(t, err)
	require.Equal(t, body, dst)
	require.Zero(t, directory.calls.Load())
}

type sourceForbiddenPeer struct {
	pb.CacheClient
	origins atomic.Int32
}

func (c *sourceForbiddenPeer) StoreContentFromSource(context.Context, *pb.CacheStoreContentFromSourceRequest, ...grpc.CallOption) (*pb.CacheStoreContentFromSourceResponse, error) {
	c.origins.Add(1)
	return nil, status.Error(codes.PermissionDenied, "origin crossed peer boundary")
}
func (c *sourceForbiddenPeer) StoreContentFromSourceWithLock(context.Context, *pb.CacheStoreContentFromSourceRequest, ...grpc.CallOption) (*pb.CacheStoreContentFromSourceWithLockResponse, error) {
	c.origins.Add(1)
	return nil, status.Error(codes.PermissionDenied, "origin crossed peer boundary")
}

func TestAgentOriginFillStaysLocalAndUploadsVerifiedBytes(t *testing.T) {
	local, _ := peerTestServer(t, "local")
	remote, rpc := peerTestServer(t, "remote")
	peer := &sourceForbiddenPeer{CacheClient: rpc}
	body := bytes.Repeat([]byte("scoped-origin-content"), 10000)
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	var heads, gets atomic.Int32
	var invalidOrigin atomic.Bool
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Authorization"), "scoped-access") {
			invalidOrigin.Store(true)
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		if r.Method == http.MethodHead {
			heads.Add(1)
			w.WriteHeader(http.StatusOK)
			return
		}
		gets.Add(1)
		start, end := 0, len(body)-1
		if value := r.Header.Get("Range"); value != "" {
			if _, err := fmt.Sscanf(value, "bytes=%d-%d", &start, &end); err != nil {
				invalidOrigin.Store(true)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			if end > len(body)-1 {
				end = len(body) - 1
			}
			w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(body)))
			w.WriteHeader(http.StatusPartialContent)
		}
		w.Write(body[start : end+1])
	}))
	defer origin.Close()
	host := &Host{HostId: "remote", NodeID: "node-remote", RegistrationID: "lease", PoolName: "pool", Locality: "own/pool", CacheRouteID: "route"}
	directory := &peerDirectoryTest{hosts: []*Host{host}, client: peer, origin: true}
	client := peerTestClient(t, directory)
	client.AttachLocalServer(local)
	client.hostMap.Set(host)
	request := &pb.CacheStoreContentFromSourceRequest{Source: &pb.CacheSource{Path: "objects/model", BucketName: "bucket", Region: "us-east-1", EndpointUrl: origin.URL, AccessKey: "scoped-access", SecretKey: "scoped-secret", ForcePathStyle: true, ExpectedHash: hash}}
	for range 2 {
		got, err := client.storeContentFromSource(context.Background(), request, hash, hash, false)
		require.NoError(t, err)
		require.Equal(t, hash, got)
	}
	require.True(t, local.HasCompleteContent(hash, int64(len(body))))
	require.True(t, remote.HasCompleteContent(hash, int64(len(body))))
	require.Equal(t, int32(1), heads.Load())
	require.Positive(t, gets.Load())
	require.Zero(t, peer.origins.Load())
	require.False(t, invalidOrigin.Load())
	dst := make([]byte, len(body))
	_, err := remote.ReadContentInto(context.Background(), hash, 0, dst)
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(string(dst), "scoped-origin"))
	require.Equal(t, body, dst)
}
