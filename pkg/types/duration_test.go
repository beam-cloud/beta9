package types

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDurationIsWrittenAsText(t *testing.T) {
	encoded, err := json.Marshal(struct {
		Idle Duration `json:"idle"`
		Boot Duration `json:"boot,omitempty"`
	}{Idle: Duration(10 * time.Minute)})
	require.NoError(t, err)
	require.JSONEq(t, `{"idle":"10m0s"}`, string(encoded), "an unset duration is left out like any other")
}

func TestDurationIsReadFromTextOrNanoseconds(t *testing.T) {
	for _, test := range []struct {
		json    string
		want    time.Duration
		wantErr string
	}{
		{json: `"10m"`, want: 10 * time.Minute},
		{json: `"1h30m"`, want: 90 * time.Minute},
		{json: `"0s"`, want: 0},
		{json: `null`, want: 0},
		// What encoding/json writes for a time.Duration, and what a bare
		// number is in the config file. Validation refuses one under a second.
		{json: `600000000000`, want: 10 * time.Minute},
		{json: `600`, want: 600},
		{json: `"ten minutes"`, wantErr: `"ten minutes" is not a duration`},
		{json: `"600"`, wantErr: `"600" is not a duration`},
		{json: `true`, wantErr: "a duration is written as text"},
		{json: `1.5`, wantErr: "a duration is written as text"},
	} {
		t.Run(test.json, func(t *testing.T) {
			var decoded Duration
			err := json.Unmarshal([]byte(test.json), &decoded)
			if test.wantErr != "" {
				require.ErrorContains(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, Duration(test.want), decoded)
		})
	}
}
