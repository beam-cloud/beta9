package providers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPriceInDollarsIsReadFromANumberOrAString(t *testing.T) {
	for text, micros := range map[string]int64{`"44"`: 44_000_000, `"1.50"`: 1_500_000, `0.375`: 375_000, `"0.01920"`: 19_200, `""`: 0, `null`: 0} {
		price := dollars(1)
		require.NoError(t, json.Unmarshal([]byte(text), &price), text)
		require.Equal(t, micros, int64(price), text)
	}
	var price dollars
	require.Error(t, json.Unmarshal([]byte(`"a lot"`), &price))
}

func TestACountIsReadFromANumberOrAString(t *testing.T) {
	for text, want := range map[string]int64{`8`: 8, `"8"`: 8, `"1600"`: 1600, `0`: 0, `""`: 0, `null`: 0} {
		n := count(1)
		require.NoError(t, json.Unmarshal([]byte(text), &n), text)
		require.Equal(t, want, int64(n), text)
	}
	var n count
	require.Error(t, json.Unmarshal([]byte(`"many"`), &n))
	require.Error(t, json.Unmarshal([]byte(`1.5`), &n))
}
