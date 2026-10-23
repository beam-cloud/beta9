package common

import (
	"slices"
	"testing"
)

func TestInspectSkipsTheRepositoryTagList(t *testing.T) {
	for _, creds := range []string{"", "user:token"} {
		args := (&skopeoClient{enableTLS: true}).inspectArgs(creds)
		if !slices.Contains(args, "--no-tags") {
			t.Fatalf("inspect args %v list every tag in the repository", args)
		}
	}
}
