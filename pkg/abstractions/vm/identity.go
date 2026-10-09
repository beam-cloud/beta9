package vm

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/google/uuid"
)

func randomHexID() string {
	id := uuid.New()
	return hex.EncodeToString(id[:])
}

// VM IDs are compact, opaque 64-bit identifiers. A request ID keeps retries
// deterministic within the workspace; credentials use the full random ID.
func vmID(workspaceID uint, requestID string) string {
	if requestID == "" {
		return randomHexID()[:16]
	}
	id := sha256.Sum256([]byte(fmt.Sprintf("beam-vm:%d:%s", workspaceID, requestID)))
	return hex.EncodeToString(id[:8])
}

func defaultVMName(id string) string {
	adjectives := [...]string{"bright", "calm", "clear", "cool", "eager", "gentle", "happy", "keen", "lively", "lucky", "mellow", "nimble", "quiet", "steady", "swift", "warm"}
	nouns := [...]string{"badger", "cedar", "dolphin", "falcon", "finch", "fox", "heron", "maple", "otter", "owl", "panda", "pine", "robin", "seal", "tiger", "willow"}
	seed, _ := hex.DecodeString(id[:2])
	return adjectives[seed[0]>>4] + "-" + nouns[seed[0]&15] + "-" + id[len(id)-6:]
}
