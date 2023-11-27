package main

import "C"
import (
	"github.com/traefik/yaegi/interp"
)

type FunctionRunner struct{}

const src = `package somecode
func Bar(s string) string { return s + "-else" }`

func (p *FunctionRunner) Run(input []byte) ([]byte, error) {
	i := interp.New(interp.Options{})

	_, err := i.Eval(src)
	if err != nil {
		panic(err)
	}

	v, err := i.Eval("somecode.Bar")
	if err != nil {
		panic(err)
	}

	bar := v.Interface().(func(string) string)

	r := bar("something")
	println(r)
	return []byte{}, nil
}

var PluginRunner FunctionRunner
