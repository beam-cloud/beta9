package main

import (
	"log"
	"plugin"
)

type Runner interface {
	Run(input []byte) ([]byte, error)
}

func loadAndExecutePlugin(pluginPath string, input []byte) ([]byte, error) {
	p, err := plugin.Open(pluginPath)
	if err != nil {
		log.Fatalf("unable to load plugin: %+v\n", err)
	}

	// Lookup the runner symbol
	symbol, err := p.Lookup("PluginRunner")
	if err != nil {
		log.Fatalf("unable to find PluginRunner symbol: %+v\n", err)
	}

	runner, ok := symbol.(Runner)
	if !ok {
		log.Fatal("PluginRunner does not implement Runner interface")
	}

	// Execute the plugin's Run method
	return runner.Run(input)
}

func main() {
	loadAndExecutePlugin("function.so", []byte{})
}
