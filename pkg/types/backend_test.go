package types

import (
	"testing"
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
