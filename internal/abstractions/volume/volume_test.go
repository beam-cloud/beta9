package volume

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInputArgument(t *testing.T) {
	tests := []struct {
		name        string
		have        string
		wantVolName string
		wantVolPath string
	}{
		{
			name:        "volume without path",
			have:        "myvol",
			wantVolName: "myvol",
			wantVolPath: ".",
		},
		{
			name:        "volume with subdir",
			have:        "myvol/subdir",
			wantVolName: "myvol",
			wantVolPath: "subdir",
		},
		{
			name:        "volume with file path",
			have:        "myvol/file.txt",
			wantVolName: "myvol",
			wantVolPath: "file.txt",
		},
		{
			name:        "volume with subdir file path",
			have:        "myvol/path/to/dir/file.txt",
			wantVolName: "myvol",
			wantVolPath: "path/to/dir/file.txt",
		},
		{
			name:        "volume with subdir file path up a level",
			have:        "myvol/path/../to/dir/file.txt",
			wantVolName: "myvol",
			wantVolPath: "to/dir/file.txt",
		},
		{
			name:        "volume with subdir file path up a level",
			have:        "myvol/path/../../to/dir/file.txt",
			wantVolName: "myvol",
			wantVolPath: "../to/dir/file.txt",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			volName, volPath := parseVolumeInput(test.have)
			assert.Equal(t, test.wantVolName, volName)
			assert.Equal(t, test.wantVolPath, volPath)
		})
	}
}
