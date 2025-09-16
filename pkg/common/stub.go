package common

import (
	"errors"
	"strings"
)

func ExtractStubIdFromContainerId(containerId string) (string, error) {
	parts := strings.Split(containerId, "-")
	if len(parts) < 7 {
		return "", errors.New("invalid container id")
	}

	return strings.Join(parts[1:6], "-"), nil
}
