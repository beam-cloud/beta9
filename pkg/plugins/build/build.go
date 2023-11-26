package main

import "C"
import "log"

type MyPlugin struct{}

func (p *MyPlugin) Run(input []byte) ([]byte, error) {
	// Plugin logic goes here
	// For instance, process the input and return the result
	return processInput(input), nil
}

var PluginRunner MyPlugin

// Helper function (optional)
func processInput(input []byte) []byte {
	log.Println("running it")
	// Process the input and return the result
	return []byte("Processed data: " + string(input))
}
