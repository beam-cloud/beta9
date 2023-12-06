package bucket

type Bucket interface {
	PutObject(key string, data []byte)
}
