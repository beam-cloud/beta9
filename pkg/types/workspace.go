package types

import "strings"

func WorkspaceBucketName(prefix, workspaceID string) string {
	prefix = strings.Trim(strings.TrimSpace(prefix), "-")
	workspaceID = strings.Trim(strings.TrimSpace(workspaceID), "-")
	if prefix == "" {
		return workspaceID
	}
	if workspaceID == "" {
		return prefix
	}
	return prefix + "-" + workspaceID
}
