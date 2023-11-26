package main

import (
	"log"
	"plugin"
)

type Runner interface {
	Run(input []byte) ([]byte, error)
}

func loadAndExecutePlugin(pluginPath string, input []byte) ([]byte, error) {
	// Load the plugin
	p, err := plugin.Open(pluginPath)
	if err != nil {
		log.Fatal("Error loading plugin:", err)
	}

	// Lookup the symbol (an exported variable in this case)
	symbol, err := p.Lookup("PluginRunner")
	if err != nil {
		log.Fatal("Error finding PluginRunner symbol:", err)
	}

	runner, ok := symbol.(Runner)
	if !ok {
		log.Fatal("PluginRunner does not implement Runner interface")
	}

	// Execute the plugin's Run method
	return runner.Run(input)
}

func main() {
	loadAndExecutePlugin("build.so", []byte{})
}
