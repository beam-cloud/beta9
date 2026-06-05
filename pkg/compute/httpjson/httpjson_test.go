package httpjson

import "testing"

func TestNumericLookupSkipsInvalidFallbacks(t *testing.T) {
	data := map[string]any{
		"bad_int":   "not-an-int",
		"good_int":  int64(42),
		"bad_float": "not-a-float",
		"float":     "3.5",
	}

	if got := Int64(data, "bad_int", "good_int"); got != 42 {
		t.Fatalf("Int64 fallback = %d, want 42", got)
	}
	if got := Float64(data, "bad_float", "float"); got != 3.5 {
		t.Fatalf("Float64 fallback = %f, want 3.5", got)
	}
}
