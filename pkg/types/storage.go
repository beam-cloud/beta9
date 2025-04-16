package types

import "fmt"

func WorkspaceBucketName(workspaceExternalId string) string {
	return fmt.Sprintf("workspace-%s", workspaceExternalId)
}
