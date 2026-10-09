package common

import (
	"errors"
	"slices"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

func ExtractStubIdFromContainerId(containerId string) (string, error) {
	parts := strings.Split(containerId, "-")
	if len(parts) < 7 {
		return "", errors.New("invalid container id")
	}

	return strings.Join(parts[1:6], "-"), nil
}

func ExtractStubIdFromStubScopedContainerId(containerId string) (string, bool) {
	prefix, _, ok := strings.Cut(containerId, "-")
	if !ok {
		return "", false
	}
	if !slices.Contains(types.StubScopedContainerPrefixes(), prefix) {
		return "", false
	}

	stubId, err := ExtractStubIdFromContainerId(containerId)
	if err != nil {
		return "", false
	}
	return stubId, true
}
