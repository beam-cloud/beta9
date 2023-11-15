package common

import (
	"context"
	"errors"
	"sync"
	"time"
)

type StateStore struct {
	rdb *RedisClient
}

var store *StateStore
var onceStore sync.Once

func Store(rdb *RedisClient) *StateStore {
	onceStore.Do(func() {
		store = &StateStore{rdb: rdb}
	})

	return store
}

// Sets a key in the redis cache
func (ss *StateStore) Set(key string, value string) error {
	err := ss.rdb.Set(context.TODO(), key, value, 0).Err()
	if err != nil {
		return err
	}

	return nil
}

// Sets a key in the redis cache with a TTL
func (ss *StateStore) SetExp(key string, value string, ttl int64) error {
	err := ss.rdb.SetEx(context.TODO(), key, value, time.Duration(ttl)*time.Second).Err()
	if err != nil {
		return err
	}

	return nil
}

// Checks if a key exists
func (ss *StateStore) Exists(key string) (bool, error) {
	res, err := ss.rdb.Exists(context.TODO(), key).Result()
	if err != nil {
		return false, err
	}

	return res > 0, nil
}

// Gets a key from the redis cache
func (ss *StateStore) Get(key string) (string, error) {
	val, err := ss.rdb.Get(context.TODO(), key).Result()
	if err != nil {
		return "", errors.New("key not found")
	}

	return val, nil
}

// Convenience methods for setting/getting various common things in state
// We may want to move these out of this package and into the activator at some point
func (ss *StateStore) GetDefaultDeploymentBucket(appId string) (string, error) {
	bucketName, err := ss.Get(RedisKeys.ActivatorDefaultDeployment(appId))
	return bucketName, err
}

func (ss *StateStore) SetDefaultDeploymentBucket(appId string, bucketName string) {
	ss.Set(RedisKeys.ActivatorDefaultDeployment(appId), bucketName)
}

func (ss *StateStore) SetAverageTaskDuration(appId string, bucketName string) {
	ss.Set(RedisKeys.ActivatorDefaultDeployment(appId), bucketName)
}

func (ss *StateStore) MinContainerCount(appId string) (int, error) {
	containerCount := 0

	containerCountKey := RedisKeys.ActivatorDeploymentMinContainerCount(appId)
	containerCountKeyExists, err := ss.Exists(containerCountKey)
	if err != nil || !containerCountKeyExists {
		return -1, errors.New("min container key not set")
	}

	if containerCountKeyExists {
		err = ss.rdb.Get(context.TODO(), containerCountKey).Scan(&containerCount)
	}

	return containerCount, err
}

// Check if user is authorized to access an app
func (ss *StateStore) IsAuthorized(appId string, encodedAuthString string) bool {
	_, err := ss.Get(RedisKeys.ActivatorAuthKey(appId, encodedAuthString))
	return err == nil
}

// Authorize user to access an app
func (ss *StateStore) Authorize(appId string, encodedAuthString string) error {
	return ss.SetExp(RedisKeys.ActivatorAuthKey(appId, encodedAuthString), "true", 24*60*60) // Authorize user for one day
}
