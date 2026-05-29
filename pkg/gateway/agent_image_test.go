package gateway

import "testing"

func TestValidAgentImageArchiveName(t *testing.T) {
	for _, tt := range []struct {
		name    string
		imageID string
		file    string
		want    bool
	}{
		{name: "local archive", imageID: "image-a", file: "image-a.clip", want: true},
		{name: "remote archive", imageID: "image-a", file: "image-a.rclip", want: true},
		{name: "wrong image", imageID: "image-a", file: "image-b.clip", want: false},
		{name: "path traversal", imageID: "../image-a", file: "image-a.clip", want: false},
		{name: "wrong extension", imageID: "image-a", file: "image-a.tar", want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := validAgentImageArchiveName(tt.imageID, tt.file); got != tt.want {
				t.Fatalf("validAgentImageArchiveName() = %v, want %v", got, tt.want)
			}
		})
	}
}
