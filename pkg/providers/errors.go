package providers

import (
	"errors"
	"fmt"
)

var (
	// ErrOutOfStock: the provider has none of the offer left.
	ErrOutOfStock = errors.New("out of stock")
	// ErrQuota: the account has reached a limit and may not buy more.
	ErrQuota = errors.New("quota exceeded")
	// ErrNotFound: the provider no longer has the instance.
	ErrNotFound = errors.New("instance not found")
)

// because wraps one of the errors above with the provider's own explanation.
func because(kind error, cause error) error {
	return fmt.Errorf("%w: %w", kind, cause)
}
