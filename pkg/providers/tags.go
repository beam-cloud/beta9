package providers

import (
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"
)

// OwnerTag marks an instance with the control plane that bought it. List
// returns only instances that carry this control plane's value, so two
// deployments that share a provider account do not see each other's machines.
const OwnerTag = "beam-owner"

// Tag keys and values stay inside the strictest provider's label rules, so
// every provider can store every tag: lowercase letters, digits and dashes,
// at most 63 of them, with a letter or a digit first and last. A key begins
// on a letter, which Google asks of a label's key and of no value.
var (
	tagKeyPattern   = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$`)
	tagValuePattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
)

// checkOwner refuses an owner that some provider could not store as a tag's
// value. An owner does not change, so it is asked once: by the registry,
// before it makes a client.
func checkOwner(owner string) error {
	if !tagValuePattern.MatchString(owner) {
		return fmt.Errorf("provider owner %q must be lowercase letters, digits and dashes, at most 63 characters", owner)
	}
	return nil
}

// ownedTags returns the tags to store with a new instance: the caller's, plus
// the owner.
func ownedTags(owner string, tags map[string]string) (map[string]string, error) {
	if _, reserved := tags[OwnerTag]; reserved {
		return nil, fmt.Errorf("tag %q is set by the provider client", OwnerTag)
	}
	for key, value := range tags {
		if !tagKeyPattern.MatchString(key) || !tagValuePattern.MatchString(value) {
			return nil, fmt.Errorf("tag %q=%q must be lowercase letters, digits and dashes, at most 63 characters, and its key must begin with a letter", key, value)
		}
	}
	owned := maps.Clone(tags)
	if owned == nil {
		owned = map[string]string{}
	}
	owned[OwnerTag] = owner
	return owned, nil
}

// callerTags drops the owner from an instance's stored tags, leaving what
// the caller passed to Create.
func callerTags(stored map[string]string) map[string]string {
	tags := maps.Clone(stored)
	delete(tags, OwnerTag)
	if len(tags) == 0 {
		return nil
	}
	return tags
}

// tagWords writes tags for a provider that keeps words with an instance,
// and no pairs: each tag as the word "key=value", in the order of the keys.
// Keys and values have neither spaces nor equals signs.
func tagWords(tags map[string]string) []string {
	words := make([]string, 0, len(tags))
	for _, key := range slices.Sorted(maps.Keys(tags)) {
		words = append(words, key+"="+tags[key])
	}
	return words
}

// wordTags reads tags back from such words. A word that is not a tag, as
// one somebody else wrote, is left out.
func wordTags(words []string) map[string]string {
	tags := map[string]string{}
	for _, word := range words {
		if key, value, ok := strings.Cut(word, "="); ok {
			tags[key] = value
		}
	}
	return tags
}
