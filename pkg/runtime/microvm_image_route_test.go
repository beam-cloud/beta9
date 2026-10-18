package runtime

import "testing"

func TestSnapshotImageRouteRequiresExactSavedRoot(t *testing.T) {
	for _, v := range []struct {
		args       string
		direct, ok bool
	}{
		{"console=ttyS0 root=beamfs ro", false, true},
		{"console=ttyS0 root=beambinds rw", true, true},
		{"root=beamfs", true, false}, {"root=beambinds", false, false},
		{"", false, false}, {"root=/dev/vda", false, false},
		{"root=beambinds root=beambinds", true, false}, {"root=beamfs root=beambinds", true, false},
	} {
		if e := validateMicroVMSnapshotRoot(v.args, v.direct); (e == nil) != v.ok {
			t.Fatalf("%q direct=%v error=%v", v.args, v.direct, e)
		}
	}
}
