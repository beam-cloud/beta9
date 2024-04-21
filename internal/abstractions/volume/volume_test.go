package volume

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetVolumePaths(t *testing.T) {
	tests := []struct {
		name        string
		have        string
		wantVolName string
		wantVolPath string
		wantErr     bool
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
			name:        "volume with subdir file path up one level past parent dir",
			have:        "myvol/path/../../to/dir/file.txt",
			wantVolName: "myvol",
			wantVolPath: "../to/dir/file.txt",
			wantErr:     true,
		},
		{
			name:        "volume with subdir path up three levels past parent dir",
			have:        "myvol/../../../path/to/dir",
			wantVolName: "myvol",
			wantVolPath: "../../../path/to/dir",
			wantErr:     true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			volName, volPath := parseVolumeInput(test.have)
			assert.Equal(t, test.wantVolName, volName)
			assert.Equal(t, test.wantVolPath, volPath)

			_, _, err := GetVolumePaths("work-name", "vol-id", volPath)
			if test.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
