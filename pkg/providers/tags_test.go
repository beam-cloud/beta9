package providers

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnedTagsAddTheOwnerToTheCallersTags(t *testing.T) {
	callers := map[string]string{"beam-machine": "a1b2c3d4"}
	owned, err := ownedTags("beam-prod-1a2b3c", callers)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"beam-machine": "a1b2c3d4", OwnerTag: "beam-prod-1a2b3c"}, owned)
	require.Equal(t, map[string]string{"beam-machine": "a1b2c3d4"}, callers, "the caller's map is left alone")

	require.Equal(t, callers, callerTags(owned))
	require.Nil(t, callerTags(map[string]string{OwnerTag: "beam-prod-1a2b3c"}))
}

func TestOwnedTagsRefuseWhatSomeProviderCouldNotStore(t *testing.T) {
	for name, tags := range map[string]map[string]string{
		"key with underscore":   {"beam_pool": "a"},
		"key begins on a digit": {"1st-pool": "a"},
		"value with a dot":      {"pool": "a.b"},
		"empty value":           {"pool": ""},
		"value too long":        {"pool": strings.Repeat("a", 64)},
		"the owner tag itself":  {OwnerTag: "beam"},
	} {
		_, err := ownedTags("beam", tags)
		require.Error(t, err, name)
	}

	// Only a key has to begin on a letter, which Google asks of a label's.
	_, err := ownedTags("beam", map[string]string{"k": "v", strings.Repeat("b", 63): "0", "k8s": "1st"})
	require.NoError(t, err)

	// The owner is not looked at here: the registry checks it once, before
	// it makes a client.
	require.Error(t, checkOwner("Beam"))
	require.NoError(t, checkOwner("0"+strings.Repeat("a", 62)))
}

// Shadeform and Vast keep words with an instance, and no pairs.
func TestTagsAreWrittenAsWordsInTheOrderOfTheirKeysAndReadBack(t *testing.T) {
	tags := map[string]string{"beam-pool-machine": "machine-a1b2c3d4", "beam-pool": "rtx5090", OwnerTag: testOwner}
	words := tagWords(tags)
	require.Equal(t, []string{"beam-owner=" + testOwner, "beam-pool=rtx5090", "beam-pool-machine=machine-a1b2c3d4"}, words)
	require.Equal(t, tags, wordTags(words))

	// What somebody else wrote next to them is no tag.
	require.Equal(t, tags, wordTags(append([]string{"my-training-job", ""}, words...)))
	require.Empty(t, tagWords(nil))
	require.Empty(t, wordTags(nil))
}
