package volume

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v4"
)

func TestSDKUploadSigningRouteParameters(t *testing.T) {
	cases := []struct {
		name, encoded, volume, relative string
		invalid                         bool
	}{
		{"plain", "uploads%2Fclip_fixed-id.wav", "uploads", "clip_fixed-id.wav", false},
		{"percent", "uploads%2Fclip%25_fixed-id.wav", "uploads", "clip%_fixed-id.wav", false},
		{"literal slash escape", "uploads%2Fclip%252F_fixed-id.wav", "uploads", "clip%2F_fixed-id.wav", false},
		{"literal percent escape", "uploads%2Fclip%2525_fixed-id.wav", "uploads", "clip%25_fixed-id.wav", false},
		{"plus", "uploads%2Fclip%2Bone_fixed-id.wav", "uploads", "clip+one_fixed-id.wav", false},
		{"unicode", "uploads%2F%E4%BD%A0%E5%A5%BD_fixed-id.wav", "uploads", "你好_fixed-id.wav", false},
		{"parent normalized before encoding", "media%2Fclip_fixed-id.wav", "media", "clip_fixed-id.wav", false},
		{"parent with literal escape", "media%2Fclip%252F%2Bone_fixed-id.wav", "media", "clip%2F+one_fixed-id.wav", false},
		{"absolute", "uploads%2F%2Ftmp%2Fclip_fixed-id.wav", "uploads", "/tmp/clip_fixed-id.wav", false},
		{"unsafe parent remains rejected", "uploads%2F..%2Fmedia%2Fclip_fixed-id.wav", "", "", true},
	}
	for _, operation := range []string{"upload", "download"} {
		for _, tc := range cases {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				e := echo.New()
				registerVolumeRoutes(e.Group("/volume"), nil)
				var routePath string
				for _, route := range e.Routes() {
					if strings.Contains(route.Path, "/generate-"+operation+"-url/") {
						routePath = route.Path
						break
					}
				}
				if routePath == "" {
					t.Fatal("signing route not registered")
				}
				// Exercise the registered route shape and its parameter contract without auth or storage.
				e.GET(routePath, func(c echo.Context) error {
					decoded, err := url.QueryUnescape(c.Param("volumePath*"))
					if err != nil {
						return c.NoContent(http.StatusBadRequest)
					}
					volume, relative := parseVolumeInput(decoded)
					key, err := volumeObjectKey("vol-id", relative)
					if err != nil {
						return c.NoContent(http.StatusBadRequest)
					}
					if volume != tc.volume || relative != tc.relative {
						t.Errorf("got volume=%q relative=%q, want %q %q", volume, relative, tc.volume, tc.relative)
					}
					if !strings.HasPrefix(key, "volumes/vol-id/") {
						t.Errorf("key outside volume: %q", key)
					}
					return c.NoContent(http.StatusOK)
				})
				recorder := httptest.NewRecorder()
				target := "/volume/workspace/generate-" + operation + "-url/" + tc.encoded
				e.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, target, nil))
				wantStatus := http.StatusOK
				if tc.invalid {
					wantStatus = http.StatusBadRequest
				}
				if recorder.Code != wantStatus {
					t.Errorf("got status=%d want %d", recorder.Code, wantStatus)
				}
			})
		}
	}
}
