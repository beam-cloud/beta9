package dmap

type Map interface {
	Set(string, []byte) error
	Get(string) ([]byte, error)
}
