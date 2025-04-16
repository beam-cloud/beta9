package types

import "fmt"

func WorkspaceBucketName(workspaceId uint) string {
	return fmt.Sprintf("workspace-%d", workspaceId)
}
