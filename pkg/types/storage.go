package types

import "fmt"

func WorkspaceBucketName(prefix, workspaceExternalId string) string {
	return fmt.Sprintf("%s-%s", prefix, workspaceExternalId)
}
