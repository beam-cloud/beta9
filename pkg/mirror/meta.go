package mirror

import (
	"encoding/json"
	"path"
	"regexp"
)

// homeName is a conservative account name. It cannot begin with a dot, so
// it is never . or ..
var homeName = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,31}$`)

// ValidHome reports whether home is a home directory a tree may be presented
// at: /Users/<name> or /home/<name>, clean and absolute. A manifest's home is
// written by a client, so nothing else is taken.
func ValidHome(home string) bool {
	parent, name := path.Split(home)
	return (parent == "/Users/" || parent == "/home/") && homeName.MatchString(name) && path.Clean(home) == home
}

// SourceHome is the home directory meta records the files came from, or
// empty when meta records none that ValidHome accepts. Meta is the client's
// to fill: malformed meta has no home rather than an error.
func SourceHome(meta []byte) string {
	var decoded struct {
		Source struct {
			Home string `json:"home"`
		} `json:"source"`
	}
	if json.Unmarshal(meta, &decoded) != nil || !ValidHome(decoded.Source.Home) {
		return ""
	}
	return decoded.Source.Home
}
