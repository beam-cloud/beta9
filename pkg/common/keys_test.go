package common

import "testing"

func TestThunderClientEnrollmentKeysShareHashTag(t *testing.T) {
	stateKey := RedisKeys.ThunderClientEnrollment("container-1")
	indexKey := RedisKeys.ThunderClientEnrollmentIndex()

	stateTag, ok := redisHashTag(stateKey)
	if !ok {
		t.Fatalf("state key %q has no hash tag", stateKey)
	}
	indexTag, ok := redisHashTag(indexKey)
	if !ok {
		t.Fatalf("index key %q has no hash tag", indexKey)
	}
	if stateTag != indexTag {
		t.Fatalf("hash tags differ: state %q index %q", stateTag, indexTag)
	}
}

func redisHashTag(key string) (string, bool) {
	start := -1
	for i, ch := range key {
		if ch == '{' {
			start = i
			break
		}
	}
	if start < 0 {
		return "", false
	}
	for i := start + 1; i < len(key); i++ {
		if key[i] == '}' {
			if i == start+1 {
				return "", false
			}
			return key[start+1 : i], true
		}
	}
	return "", false
}
