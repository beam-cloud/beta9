package shell

import "testing"

func TestStandaloneContainerOwnership(t *testing.T) {
	t.Parallel()

	if !IsStandaloneContainer("shell-stub-12345678") {
		t.Fatal("standalone shell container should own a shell TTL")
	}
	for _, containerId := range []string{
		"pod-stub-12345678",
		"function-stub-12345678",
		"endpoint-stub-12345678",
	} {
		if IsStandaloneContainer(containerId) {
			t.Fatalf("attached workload %q must not receive a shell TTL", containerId)
		}
	}
}
