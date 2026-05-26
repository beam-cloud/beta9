package common

import (
	"errors"
	"strings"
)

var stubScopedContainerPrefixes = map[string]struct{}{
	"sandbox":   {},
	"pod":       {},
	"endpoint":  {},
	"taskqueue": {},
}

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
	if _, ok := stubScopedContainerPrefixes[prefix]; !ok {
		return "", false
	}

	stubId, err := ExtractStubIdFromContainerId(containerId)
	if err != nil {
		return "", false
	}
	return stubId, true
}
