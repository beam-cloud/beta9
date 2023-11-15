package common

import "fmt"

type names struct{}

var Names names

func (n *names) RequestBucketName(appId string, version uint) string {
	return fmt.Sprintf("%s-%04d", appId, version)
}

func (n *names) RequestBucketNameId(appId string, bucketId string) string {
	return fmt.Sprintf("%s-%s", appId, bucketId)
}
