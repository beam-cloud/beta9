package auth

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGRPCValidateToken(t *testing.T) {
	mockDetails := mockBackendWithValidToken()

	tests := []struct {
		name     string
		tokenKey string
		prepares func()
		success  bool
	}{
		{
			name:     "valid token",
			tokenKey: mockDetails.tokenForTest.Key,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				addTokenRow(mockDetails.mock, *mockDetails.tokenForTest.Workspace, mockDetails.tokenForTest)
			},
			success: true,
		},
		{
			name:     "invalid token",
			tokenKey: mockDetails.tokenForTest.Key,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
			},
			success: false,
		},
		{
			name:     "no token",
			tokenKey: "",
			prepares: func() {},
			success:  false,
		},
		{
			name:     "workspace public token should fail",
			tokenKey: mockDetails.publicTokenForTest.Key,
			prepares: func() {
				mockDetails.mockRedis.FlushAll(context.Background())
				addTokenRow(mockDetails.mock, *mockDetails.publicTokenForTest.Workspace, mockDetails.publicTokenForTest)
			},
			success: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.prepares()
			ai := NewAuthInterceptor(mockDetails.backendRepo, mockDetails.workspaceRepo)

			md := map[string][]string{
				"authorization": {tt.tokenKey},
			}
			authInfo, ok := ai.validateToken(md)

			assert.Equal(t, tt.success, ok)
			if tt.success {
				assert.Equal(t, mockDetails.tokenForTest.Key, authInfo.Token.Key)
				assert.Equal(t, mockDetails.tokenForTest.Workspace.ExternalId, authInfo.Workspace.ExternalId)
			}
		})
	}
}
