package dmap

type Map interface {
	Set(string, string) error
	Get(string) (string, error)
}
