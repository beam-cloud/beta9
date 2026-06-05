package cache

import (
	"errors"
	"time"
)

type OperationTraceAttempt struct {
	HostIndex      int    `json:"host_index"`
	HostID         string `json:"host_id,omitempty"`
	RegistrationID string `json:"registration_id,omitempty"`
	PoolName       string `json:"pool_name,omitempty"`
	Locality       string `json:"locality,omitempty"`
	NodeID         string `json:"node_id,omitempty"`
	CachePathID    string `json:"cache_path_id,omitempty"`
	Addr           string `json:"addr,omitempty"`
	PrivateAddr    string `json:"private_addr,omitempty"`
	HasEndpoint    bool   `json:"has_endpoint"`
	Source         string `json:"source"`
	Result         string `json:"result"`
	Read           int64  `json:"read,omitempty"`
	Bytes          int64  `json:"bytes,omitempty"`
	ExpectedSize   int64  `json:"expected_size,omitempty"`
	ContentStatus  string `json:"content_status,omitempty"`
	ElapsedUs      int64  `json:"elapsed_us,omitempty"`
	Error          string `json:"error,omitempty"`
}

type OperationTrace struct {
	Operation     string                  `json:"operation,omitempty"`
	Result        string                  `json:"result,omitempty"`
	Hash          string                  `json:"hash,omitempty"`
	RoutingKey    string                  `json:"routing_key,omitempty"`
	Offset        int64                   `json:"offset,omitempty"`
	Length        int64                   `json:"length,omitempty"`
	Read          int64                   `json:"read,omitempty"`
	Bytes         int64                   `json:"bytes,omitempty"`
	ExpectedSize  int64                   `json:"expected_size,omitempty"`
	Views         int                     `json:"views,omitempty"`
	DurationUs    int64                   `json:"duration_us,omitempty"`
	HostRefreshes int                     `json:"host_refreshes,omitempty"`
	Attempts      []OperationTraceAttempt `json:"attempts,omitempty"`
}

func (t *OperationTrace) addAttempt(hostIndex int, host *Host, source string, result string, read int64, elapsed time.Duration, err error) {
	t.addAttemptWithDetails(hostIndex, host, source, result, 0, 0, "", read, elapsed, err)
}

func (t *OperationTrace) addStoreAttempt(hostIndex int, host *Host, source string, result string, bytes int64, expectedSize int64, contentStatus string, elapsed time.Duration, err error) {
	t.addAttemptWithDetails(hostIndex, host, source, result, bytes, expectedSize, contentStatus, 0, elapsed, err)
}

func (t *OperationTrace) addAttemptWithDetails(hostIndex int, host *Host, source string, result string, bytes int64, expectedSize int64, contentStatus string, read int64, elapsed time.Duration, err error) {
	if t == nil {
		return
	}

	attempt := OperationTraceAttempt{
		HostIndex:     hostIndex,
		Source:        source,
		Result:        result,
		Read:          read,
		Bytes:         bytes,
		ExpectedSize:  expectedSize,
		ContentStatus: contentStatus,
		ElapsedUs:     elapsed.Microseconds(),
		HasEndpoint:   host.HasEndpoint(),
	}
	if err != nil {
		attempt.Error = err.Error()
	}
	if host != nil {
		attempt.HostID = host.HostId
		attempt.RegistrationID = host.RegistrationID
		attempt.PoolName = host.PoolName
		attempt.Locality = host.Locality
		attempt.NodeID = host.NodeID
		attempt.CachePathID = host.CachePathID
		attempt.Addr = host.Addr
		attempt.PrivateAddr = host.PrivateAddr
	}

	t.Attempts = append(t.Attempts, attempt)
}

func operationTraceStoreResult(err error) string {
	switch {
	case err == nil:
		return "stored"
	case errors.Is(err, ErrUnableToAcquireLock):
		return "lock_unavailable"
	case errors.Is(err, ErrSelectedHostUnavailable), errors.Is(err, ErrUnableToReachHost), errors.Is(err, ErrHostNotFound), errors.Is(err, ErrClientNotFound):
		return "unavailable"
	default:
		return "error"
	}
}

func operationTraceReadResult(err error, read int64, length int64) string {
	switch {
	case err == nil && read == length:
		return "hit"
	case errors.Is(err, ErrContentNotFound):
		return "miss"
	case errors.Is(err, ErrSelectedHostUnavailable), errors.Is(err, ErrUnableToReachHost), errors.Is(err, ErrHostNotFound), errors.Is(err, ErrClientNotFound):
		return "unavailable"
	case err == nil && read != length:
		return "short_read"
	default:
		return "error"
	}
}

func operationTracePageViewResult(err error, viewCount int) string {
	switch {
	case err == nil && viewCount > 0:
		return "hit"
	case err == nil:
		return "miss"
	case errors.Is(err, ErrContentNotFound):
		return "miss"
	case errors.Is(err, ErrSelectedHostUnavailable), errors.Is(err, ErrUnableToReachHost), errors.Is(err, ErrHostNotFound), errors.Is(err, ErrClientNotFound):
		return "unavailable"
	default:
		return "error"
	}
}
