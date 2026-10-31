package repository

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/beam-cloud/beta9/pkg/common"
	"github.com/beam-cloud/beta9/pkg/types"
)

type credentialKMS interface {
	Encrypt(context.Context, *kms.EncryptInput, ...func(*kms.Options)) (*kms.EncryptOutput, error)
	Decrypt(context.Context, *kms.DecryptInput, ...func(*kms.Options)) (*kms.DecryptOutput, error)
}

func tokenDigest(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func (r *PostgresBackendRepository) credentialClient(ctx context.Context) (credentialKMS, error) {
	r.credentialMu.Lock()
	defer r.credentialMu.Unlock()
	if r.credentialKMS != nil {
		return r.credentialKMS, nil
	}
	config, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	r.credentialKMS = kms.NewFromConfig(config)
	return r.credentialKMS, nil
}

func credentialContext(purpose, id string) map[string]string {
	workspace, record := id, id
	if purpose == "token" {
		workspace, record, _ = strings.Cut(id, "/")
	}
	return map[string]string{"service": "beta9", "purpose": purpose, "workspace": workspace, "record": record}
}

func (r *PostgresBackendRepository) wrapCredential(ctx context.Context, purpose, id, value string) (string, error) {
	if r.config.CredentialKMSKeyID == "" {
		if !strings.HasPrefix(r.config.EncryptionKey, "sk_") {
			return "", fmt.Errorf("credential encryption is not configured")
		}
		key, err := common.ParseSecretKey(r.config.EncryptionKey)
		if err != nil {
			return "", err
		}
		wrapped, err := common.Encrypt(key, purpose+"\x00"+id+"\x00"+value)
		return "enc:v1:" + wrapped, err
	}
	client, err := r.credentialClient(ctx)
	if err != nil {
		return "", err
	}
	resp, err := client.Encrypt(ctx, &kms.EncryptInput{KeyId: aws.String(r.config.CredentialKMSKeyID), Plaintext: []byte(value), EncryptionContext: credentialContext(purpose, id)})
	if err != nil {
		return "", fmt.Errorf("wrap credential: %w", err)
	}
	return "kms:v1:" + base64.StdEncoding.EncodeToString(resp.CiphertextBlob), nil
}

func (r *PostgresBackendRepository) unwrapCredential(ctx context.Context, purpose, id, value string) (string, error) {
	if strings.HasPrefix(value, "hash:") {
		return "", nil
	}
	if strings.HasPrefix(value, "enc:v1:") {
		if !strings.HasPrefix(r.config.EncryptionKey, "sk_") {
			return "", fmt.Errorf("credential encryption is not configured")
		}
		key, err := common.ParseSecretKey(r.config.EncryptionKey)
		if err != nil {
			return "", err
		}
		plain, err := common.Decrypt(key, strings.TrimPrefix(value, "enc:v1:"))
		if err != nil {
			return "", err
		}
		prefix := purpose + "\x00" + id + "\x00"
		if !strings.HasPrefix(plain, prefix) {
			return "", fmt.Errorf("credential context does not match")
		}
		return strings.TrimPrefix(plain, prefix), nil
	}
	if !strings.HasPrefix(value, "kms:v1:") {
		return value, nil
	}
	if r.config.CredentialKMSKeyID == "" {
		return "", fmt.Errorf("credential KMS key is required")
	}
	blob, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(value, "kms:v1:"))
	if err != nil {
		return "", err
	}
	client, err := r.credentialClient(ctx)
	if err != nil {
		return "", err
	}
	resp, err := client.Decrypt(ctx, &kms.DecryptInput{KeyId: aws.String(r.config.CredentialKMSKeyID), CiphertextBlob: blob, EncryptionContext: credentialContext(purpose, id)})
	if err != nil {
		return "", fmt.Errorf("unwrap credential: %w", err)
	}
	return string(resp.Plaintext), nil
}

func (r *PostgresBackendRepository) unwrapWorkspace(ctx context.Context, workspace *types.Workspace) error {
	if workspace == nil || workspace.SigningKey == nil {
		return nil
	}
	value, err := r.unwrapCredential(ctx, "workspace", workspace.ExternalId, *workspace.SigningKey)
	if err == nil {
		workspace.SigningKey = &value
	}
	return err
}

func recoverableToken(kind string) bool {
	return kind == types.TokenTypeWorkspacePrimary || kind == types.TokenTypeWorkspaceRestricted || types.IsWorkerTokenType(kind)
}

func (r *PostgresBackendRepository) storedTokenKey(ctx context.Context, token types.Token) (string, error) {
	if !recoverableToken(token.TokenType) {
		return "hash:" + tokenDigest(token.Key), nil
	}
	return r.wrapCredential(ctx, "token", tokenCredentialRecord(token), token.Key)
}

func tokenCredentialRecord(token types.Token) string {
	var workspaceID uint
	if token.WorkspaceId != nil {
		workspaceID = *token.WorkspaceId
	}
	return fmt.Sprintf("%d/%s", workspaceID, token.ExternalId)
}

// ProtectCredentials is a resumable maintenance operation. Each row is replaced
// conditionally so multiple processes and concurrent writes are safe.
func (r *PostgresBackendRepository) ProtectCredentials(ctx context.Context) error {
	for {
		var rows []types.Token
		if err := r.client.SelectContext(ctx, &rows, `SELECT id,external_id,key,token_type,workspace_id FROM token WHERE key_hash IS NULL ORDER BY id LIMIT 100`); err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			stored, err := r.storedTokenKey(ctx, row)
			if err != nil {
				return err
			}
			if _, err = r.client.ExecContext(ctx, `UPDATE token SET key=$2,key_hash=$3 WHERE id=$1 AND key_hash IS NULL AND encode(sha256(convert_to(key,'UTF8')),'hex')=$4`, row.Id, stored, tokenDigest(row.Key), tokenDigest(row.Key)); err != nil {
				return err
			}
		}
	}
	for {
		var rows []types.Workspace
		if err := r.client.SelectContext(ctx, &rows, `SELECT id,external_id,signing_key FROM workspace WHERE signing_key LIKE 'sk\_%' ESCAPE '\' ORDER BY id LIMIT 100`); err != nil {
			return err
		}
		if len(rows) == 0 {
			return nil
		}
		for _, row := range rows {
			if row.SigningKey == nil {
				continue
			}
			stored, err := r.wrapCredential(ctx, "workspace", row.ExternalId, *row.SigningKey)
			if err != nil {
				return err
			}
			if _, err = r.client.ExecContext(ctx, `UPDATE workspace SET signing_key=$2 WHERE id=$1 AND encode(sha256(convert_to(signing_key,'UTF8')),'hex')=$3`, row.Id, stored, tokenDigest(*row.SigningKey)); err != nil {
				return err
			}
		}
	}
}
func (r *PostgresBackendRepository) TokenIsActive(ctx context.Context, tokenID, key string) (bool, error) {
	var active bool
	err := r.client.GetContext(ctx, &active, `SELECT EXISTS(SELECT 1 FROM token WHERE external_id=$1 AND (key_hash=$2 OR (key_hash IS NULL AND encode(sha256(convert_to(key,'UTF8')),'hex')=$2)) AND active=TRUE AND disabled_by_cluster_admin=FALSE AND (expires_at IS NULL OR expires_at>NOW()))`, tokenID, tokenDigest(key))
	return active, err
}
