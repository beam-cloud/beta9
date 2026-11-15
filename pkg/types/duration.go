package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// Duration is a time.Duration that is written as text, such as "10m", in
// JSON as it is in the config file. encoding/json writes a time.Duration as
// a number of nanoseconds, which nobody can type into an API call.
type Duration time.Duration

func (d Duration) String() string { return time.Duration(d).String() }

func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

func (d *Duration) UnmarshalText(text []byte) error {
	parsed, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("%q is not a duration: write one like 90s, 10m or 1h30m", text)
	}
	*d = Duration(parsed)
	return nil
}

// UnmarshalJSON reads text, or a number of nanoseconds, which is also what a
// bare number means in the config file.
func (d *Duration) UnmarshalJSON(data []byte) error {
	var nanoseconds int64
	if json.Unmarshal(data, &nanoseconds) == nil {
		*d = Duration(nanoseconds)
		return nil
	}
	var text string
	if json.Unmarshal(data, &text) != nil {
		return fmt.Errorf("%s: a duration is written as text, such as \"10m\"", data)
	}
	return d.UnmarshalText([]byte(text))
}
