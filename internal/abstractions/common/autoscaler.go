package abstractions

type AutoScaler interface {
	Sample()
	Start()
}
