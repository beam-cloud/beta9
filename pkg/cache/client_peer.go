package cache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"

	pb "github.com/beam-cloud/beta9/proto"
)

// Agent workers resolve their own brokered origins. Only verified bytes and an
// explicit logical cache path may leave this node; CacheSource credentials and
// host-local origin paths never reach a peer or the gateway relay.
func (c *Client) storeContentFromLocalOrigin(ctx context.Context, req *pb.CacheStoreContentFromSourceRequest, hash, routingKey string, lock bool) (string, error) {
	if req == nil || req.Source == nil {
		return "", errors.New("cache source is required")
	}
	servers := c.localServersSnapshot()
	if len(servers) == 0 {
		return "", errors.New("agent origin fill requires an attached local cache server")
	}
	sort.Slice(servers, func(i, j int) bool { return servers[i].Host().HostId < servers[j].Host().HostId })
	server := servers[0]
	if lock {
		response, err := server.StoreContentFromSourceWithLock(ctx, req)
		if err != nil {
			return "", err
		}
		if response != nil && response.FailedToAcquireLock {
			return "", ErrUnableToAcquireLock
		}
		if response == nil || !response.Ok {
			return "", ErrUnableToPopulateContent
		}
		hash = response.Hash
	} else {
		response, err := server.StoreContentFromSource(ctx, req)
		if err != nil {
			return "", err
		}
		if response == nil || !response.Ok {
			return "", ErrUnableToPopulateContent
		}
		hash = response.Hash
	}
	if !isContentHash(hash) || !server.HasCompleteContent(hash, 0) {
		return "", ErrUnableToPopulateContent
	}
	defer server.PinContent(hash)()
	if routingKey == "" {
		routingKey = hash
	}

	_, host, err := c.getSelectedGRPCClient(ctx, ClientRequestTypeStorage, hash, routingKey)
	if err != nil {
		// The origin is already complete on this node. An unavailable owner
		// does not invalidate those bytes or require another origin request.
		if isStoreHostUnavailable(err) {
			return hash, nil
		}
		return "", err
	}
	if local := c.localServerForHost(host); local == server {
		return hash, nil
	}
	size := server.ContentSizeBytes(hash)
	if size < 0 {
		return "", ErrContentNotFound
	}
	stored, err := c.storeContentFromReaderToHostWithContext(ctx, host, &localOriginCacheReader{ctx: ctx, server: server, hash: hash, size: size}, hash, req.Source.CachePath, nil)
	if err != nil {
		if isStoreHostUnavailable(err) {
			Logger.Debugf("Retained verified local origin content while its cache owner is unavailable")
			return hash, nil
		}
		return "", err
	}
	if stored != hash {
		return "", fmt.Errorf("cache peer stored content hash mismatch: expected %s, got %s", hash, stored)
	}
	return hash, nil
}

type localOriginCacheReader struct {
	ctx    context.Context
	server *Server
	hash   string
	offset int64
	size   int64
}

func (r *localOriginCacheReader) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if r.offset >= r.size {
		return 0, io.EOF
	}
	if int64(len(data)) > r.size-r.offset {
		data = data[:r.size-r.offset]
	}
	n, err := r.server.ReadContentInto(r.ctx, r.hash, r.offset, data)
	r.offset += n
	if n == 0 && err == nil {
		err = io.EOF
	}
	return int(n), err
}
