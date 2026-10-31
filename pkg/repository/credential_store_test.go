package repository

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/stretchr/testify/require"
	"testing"
)

type memoryCredentialKMS struct {
	context map[string]string
	deny    bool
}

func (m *memoryCredentialKMS) Encrypt(_ context.Context, in *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	if m.deny {
		return nil, fmt.Errorf("denied")
	}
	m.context = in.EncryptionContext
	return &kms.EncryptOutput{CiphertextBlob: append([]byte("wrapped:"), in.Plaintext...)}, nil
}
func (m *memoryCredentialKMS) Decrypt(_ context.Context, in *kms.DecryptInput, _ ...func(*kms.Options)) (*kms.DecryptOutput, error) {
	if m.deny || m.context["purpose"] != in.EncryptionContext["purpose"] || m.context["record"] != in.EncryptionContext["record"] || m.context["workspace"] != in.EncryptionContext["workspace"] {
		return nil, fmt.Errorf("denied")
	}
	return &kms.DecryptOutput{Plaintext: in.CiphertextBlob[len("wrapped:"):]}, nil
}
func TestCredentialWrappingContext(t *testing.T) {
	ctx := context.Background()
	client := &memoryCredentialKMS{}
	repo := &PostgresBackendRepository{config: types.PostgresConfig{CredentialKMSKeyID: "test-key"}, credentialKMS: client}
	value, err := repo.wrapCredential(ctx, "workspace", "one", "original-signing-key")
	require.NoError(t, err)
	plain, err := repo.unwrapCredential(ctx, "workspace", "one", value)
	require.NoError(t, err)
	require.Equal(t, "original-signing-key", plain)
	_, err = repo.unwrapCredential(ctx, "workspace", "other", value)
	require.Error(t, err)
	_, err = repo.unwrapCredential(ctx, "token", "one", value)
	require.Error(t, err)
	client.deny = true
	_, err = repo.unwrapCredential(ctx, "workspace", "one", value)
	require.Error(t, err)
}
func TestTokenRecoveryPolicy(t *testing.T) {
	repo := &PostgresBackendRepository{}
	ctx := context.Background()
	for _, kind := range []string{types.TokenTypeClusterAdmin, types.TokenTypeWorkspace, types.TokenTypePlatformDeployer} {
		value, err := repo.storedTokenKey(ctx, types.Token{ExternalId: "id", Key: "original", TokenType: kind})
		require.NoError(t, err)
		require.NotContains(t, value, "original")
		plain, err := repo.unwrapCredential(ctx, "token", "id", value)
		require.NoError(t, err)
		require.Empty(t, plain)
	}
}
