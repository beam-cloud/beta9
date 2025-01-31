package types

type PythonVersion string

const (
	Python3   PythonVersion = "python3"
	Python38  PythonVersion = "python3.8"
	Python39  PythonVersion = "python3.9"
	Python310 PythonVersion = "python3.10"
	Python311 PythonVersion = "python3.11"
	Python312 PythonVersion = "python3.12"
)

func (p PythonVersion) String() string {
	return string(p)
}
