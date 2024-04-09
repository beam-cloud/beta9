package common

type AutoScaler interface {
	Sample()
	Start()
}
