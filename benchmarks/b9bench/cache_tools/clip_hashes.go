package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/beam-cloud/clip/pkg/clip"
	clipCommon "github.com/beam-cloud/clip/pkg/common"
)

func main() {
	if len(os.Args) < 2 || len(os.Args) > 3 {
		fmt.Fprintln(os.Stderr, "usage: clip_hashes <archive.clip> [target-path]")
		os.Exit(2)
	}
	targetPath := ""
	if len(os.Args) == 3 {
		targetPath = os.Args[2]
	}

	meta, err := clip.NewClipArchiver().ExtractMetadata(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var hashes []string
	switch info := meta.StorageInfo.(type) {
	case clipCommon.OCIStorageInfo:
		for _, hash := range info.DecompressedHashByLayer {
			hashes = append(hashes, hash)
		}
	case *clipCommon.OCIStorageInfo:
		for _, hash := range info.DecompressedHashByLayer {
			hashes = append(hashes, hash)
		}
	default:
		if meta.Index == nil {
			fmt.Fprintf(os.Stderr, "archive storage is %T and has no index\n", meta.StorageInfo)
			os.Exit(1)
		}
		seen := map[string]struct{}{}
		meta.Index.Ascend(meta.Index.Min(), func(value interface{}) bool {
			node, ok := value.(*clipCommon.ClipNode)
			if !ok || node == nil || node.ContentHash == "" {
				return true
			}
			if targetPath != "" && node.Path != targetPath && "/"+node.Path != targetPath {
				return true
			}
			if _, ok := seen[node.ContentHash]; ok {
				return true
			}
			seen[node.ContentHash] = struct{}{}
			hashes = append(hashes, node.ContentHash)
			return true
		})
	}
	if len(hashes) == 0 {
		fmt.Fprintln(os.Stderr, "no content hashes found")
		os.Exit(1)
	}
	sort.Strings(hashes)
	_ = json.NewEncoder(os.Stdout).Encode(hashes)
}
