package types

import "time"

// The workspace-bucket layout for mirrors: every object of a mirror, its
// manifests included, lives at mirrors/<mirror id>/objects/<sha256>.
const (
	DefaultMirrorsPrefix = "mirrors"
	MirrorObjectsPrefix  = "objects"
)

// MirrorObjectKey is the bucket key of a mirror object. digest must be a
// validated lowercase SHA-256.
func MirrorObjectKey(mirrorID, digest string) string {
	return DefaultMirrorsPrefix + "/" + mirrorID + "/" + MirrorObjectsPrefix + "/" + digest
}

// MirrorPrefix is the bucket prefix holding everything a mirror stores.
func MirrorPrefix(mirrorID string) string {
	return DefaultMirrorsPrefix + "/" + mirrorID + "/"
}

// Mirror is a named collection of local files kept in the workspace bucket.
type Mirror struct {
	ID          string            `json:"id"`
	WorkspaceID uint              `json:"-"`
	Name        string            `json:"name"`
	CreatedAt   time.Time         `json:"created_at"`
	Latest      *MirrorGeneration `json:"latest,omitempty"`
}

// MirrorGeneration is one committed version of a mirror.
type MirrorGeneration struct {
	MirrorID       string    `json:"-"`
	Number         int64     `json:"number"`
	Parent         int64     `json:"parent"`
	ManifestDigest string    `json:"manifest_digest"`
	ManifestSize   int64     `json:"manifest_size"`
	Entries        int64     `json:"entries"`
	Objects        int64     `json:"objects"`
	TotalSize      int64     `json:"total_size"`
	TokenID        string    `json:"-"`
	CreatedAt      time.Time `json:"created_at"`
	// Set once nothing keeps the generation. Its objects are deleted a grace
	// period later, unless something pinned to it turns up first.
	ExpiredAt *time.Time `json:"-"`
	// Home is where the files were on the machine they came from, when the
	// manifest says so and it is a home the tree can be presented at.
	Home string `json:"home,omitempty"`
}

// VMMirror pins a VM to one generation of a mirror. A request names the
// mirror; the gateway fills in the rest at create, and the pin then holds
// for the VM's life and for everything forked or templated from it.
type VMMirror struct {
	Name           string `json:"name"`
	ID             string `json:"id,omitempty"`
	Generation     int64  `json:"generation,omitempty"`
	ManifestDigest string `json:"manifest_digest,omitempty"`
	ManifestSize   int64  `json:"manifest_size,omitempty"`
	// Home is where the tree is presented, as the desktop user's home; when
	// empty, it is presented as root's home and the desktop user's own. The
	// VM's disk holds its changes at those paths, so a pin keeps the home it
	// was made with.
	Home string `json:"home,omitempty"`
}

// Pinned reports whether the gateway has resolved the mirror to a generation.
func (m *VMMirror) Pinned() bool {
	return m != nil && m.ID != "" && m.Generation > 0 && m.ManifestDigest != ""
}

// MirrorReference is something that keeps a mirror generation alive.
type MirrorReference struct {
	Kind       string `json:"kind"` // vm, snapshot or template
	Name       string `json:"name"`
	Generation int64  `json:"generation"`
}
