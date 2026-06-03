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
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: clip_hashes <archive.clip>")
		os.Exit(2)
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
		fmt.Fprintf(os.Stderr, "archive storage is %T, not OCI\n", meta.StorageInfo)
		os.Exit(1)
	}
	sort.Strings(hashes)
	_ = json.NewEncoder(os.Stdout).Encode(hashes)
}
