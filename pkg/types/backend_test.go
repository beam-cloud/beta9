package types

import (
	"testing"
	"time"
)

// TestIsServe checks the IsServe method for various stub types
func TestIsServe(t *testing.T) {
	tests := []struct {
		stubType StubType
		want     bool
	}{
		{StubType(StubTypeFunctionServe), true},
		{StubType(StubTypeTaskQueueServe), true},
		{StubType(StubTypeEndpointServe), true},
		{StubType(StubTypeASGIServe), true},
		{StubType(StubTypeFunctionDeployment), false},
		{StubType(StubTypeTaskQueueDeployment), false},
		{StubType(StubTypeEndpointDeployment), false},
		{StubType(StubTypeASGIDeployment), false},
		{StubType(StubTypeFunction), false},
		{StubType(StubTypeTaskQueue), false},
		{StubType(StubTypeEndpoint), false},
		{StubType(StubTypeASGI), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.stubType), func(t *testing.T) {
			if got := tt.stubType.IsServe(); got != tt.want {
				t.Errorf("StubType.IsServe() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestIsDeployment checks the IsDeployment method for various stub types
func TestIsDeployment(t *testing.T) {
	tests := []struct {
		stubType StubType
		want     bool
	}{
		{StubType(StubTypeFunctionDeployment), true},
		{StubType(StubTypeTaskQueueDeployment), true},
		{StubType(StubTypeEndpointDeployment), true},
		{StubType(StubTypeASGIDeployment), true},
		{StubType(StubTypeFunctionServe), false},
		{StubType(StubTypeTaskQueueServe), false},
		{StubType(StubTypeEndpointServe), false},
		{StubType(StubTypeASGIServe), false},
		{StubType(StubTypeFunction), false},
		{StubType(StubTypeTaskQueue), false},
		{StubType(StubTypeEndpoint), false},
		{StubType(StubTypeASGI), false},
	}

	for _, tt := range tests {
		t.Run(string(tt.stubType), func(t *testing.T) {
			if got := tt.stubType.IsDeployment(); got != tt.want {
				t.Errorf("StubType.IsDeployment() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNullTimeSQLAndSerialization(t *testing.T) {
	now := time.Date(2026, 5, 19, 12, 30, 0, 123, time.UTC)

	var nullTime NullTime
	if err := nullTime.Scan(now); err != nil {
		t.Fatalf("scan valid time: %v", err)
	}

	if !nullTime.Valid {
		t.Fatal("expected scanned time to be valid")
	}

	if !nullTime.Time.Equal(now) {
		t.Fatalf("scanned time = %v, want %v", nullTime.Time, now)
	}

	if got := nullTime.Serialize(); got != now.Format(time.RFC3339Nano) {
		t.Fatalf("serialized time = %v, want %v", got, now.Format(time.RFC3339Nano))
	}

	value, err := nullTime.Value()
	if err != nil {
		t.Fatalf("value valid time: %v", err)
	}

	if got, ok := value.(time.Time); !ok || !got.Equal(now) {
		t.Fatalf("value = %v, want %v", value, now)
	}

	if err := nullTime.Scan(nil); err != nil {
		t.Fatalf("scan nil time: %v", err)
	}

	if nullTime.Valid {
		t.Fatal("expected nil scan to be invalid")
	}

	if got := nullTime.Serialize(); got != nil {
		t.Fatalf("serialized invalid time = %v, want nil", got)
	}

	value, err = nullTime.Value()
	if err != nil {
		t.Fatalf("value invalid time: %v", err)
	}

	if value != nil {
		t.Fatalf("invalid value = %v, want nil", value)
	}
}
