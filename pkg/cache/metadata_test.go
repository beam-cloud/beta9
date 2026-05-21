package cache

import "testing"

func TestMetadataHostKeepAliveKeyIncludesLocalityAndHost(t *testing.T) {
	got := MetadataKeys.MetadataHostKeepAlive("locality-a", "host-b")
	want := "cache:host:keepalive:locality-a:host-b"

	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}
