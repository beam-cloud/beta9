package main

import "C"

type ImageBuilder struct{}

func (p *ImageBuilder) Run(input []byte) ([]byte, error) {
	return []byte{}, nil
}

var PluginRunner ImageBuilder
