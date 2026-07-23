package gatewayservices

import (
	"testing"
	"time"
)

func TestContainerTimestamp(t *testing.T) {
	if timestamp := containerTimestamp(0); timestamp != nil {
		t.Fatalf("expected zero start time to be omitted, got %s", timestamp)
	}

	const unixSeconds = int64(1_700_000_000)
	timestamp := containerTimestamp(unixSeconds)
	if timestamp == nil || !timestamp.AsTime().Equal(time.Unix(unixSeconds, 0)) {
		t.Fatalf("unexpected timestamp: %v", timestamp)
	}
}
