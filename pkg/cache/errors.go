package cache

import (
	"errors"
	"fmt"
)

var (
	ErrHostNotFound            = errors.New("host not found")
	ErrUnableToReachHost       = errors.New("unable to reach host")
	ErrInvalidHostVersion      = errors.New("invalid host version")
	ErrContentNotFound         = errors.New("content not found")
	ErrRawReadBusy             = errors.New("raw cache read server is busy")
	ErrSelectedHostUnavailable = errors.New("selected cache host unavailable")
	ErrClientNotFound          = errors.New("client not found")
	ErrCacheLockHeld           = errors.New("cache lock held")
	ErrUnableToPopulateContent = errors.New("unable to populate content from original source")
	ErrFSMountFailure          = errors.New("failed to mount cachefs")
	ErrUnableToAcquireLock     = errors.New("unable to acquire lock")
)

type ErrNodeNotFound struct {
	Id string
}

func (e *ErrNodeNotFound) Error() string {
	return fmt.Sprintf("cachefs node not found: %s", e.Id)
}
