package gatewayservices

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func TestWorkspaceObjectHasHashMetadata(t *testing.T) {
	expected := "abc123"
	tests := []struct {
		name     string
		metadata map[string]string
		want     bool
	}{
		{
			name:     "exact geesefs hash key",
			metadata: map[string]string{workspaceObjectHashMetadataKey: expected},
			want:     true,
		},
		{
			name:     "s3 header style key",
			metadata: map[string]string{"x-amz-meta---content-sha256": expected},
			want:     true,
		},
		{
			name:     "normalized hash key",
			metadata: map[string]string{"content-sha256": expected},
			want:     true,
		},
		{
			name:     "wrong hash",
			metadata: map[string]string{workspaceObjectHashMetadataKey: "other"},
			want:     false,
		},
		{
			name:     "missing hash",
			metadata: map[string]string{"content-type": "application/zip"},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := workspaceObjectHasHashMetadata(&s3.HeadObjectOutput{Metadata: tt.metadata}, expected); got != tt.want {
				t.Fatalf("workspaceObjectHasHashMetadata() = %v, want %v", got, tt.want)
			}
		})
	}
}
