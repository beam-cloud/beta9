package common

import (
	"errors"
	"strings"
)

func ExtractStubIdFromContainerId(containerId string) (string, error) {
	parts := strings.Split(containerId, "-")
	if len(parts) < 8 {
		return "", errors.New("invalid container id")
	}

	return strings.Join(parts[2:7], "-"), nil
}
