package common

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
	"github.com/rs/zerolog/log"
)

//go:embed config.default.yaml
var defaultConfig []byte

// ConfigLoaderFunc is a function type used to load configuration into a Koanf
// instance. It takes a Koanf pointer 'k' as a parameter and returns an error
// if the loading process encounters any issues.
type ConfigLoaderFunc func(k *koanf.Koanf) error

// ConfigManager is a generic configuration manager that allows handling and
// manipulation of configuration data for various types. It includes a Koanf
// instance ('kf') for managing configuration settings.
type ConfigManager[T any] struct {
	kf  *koanf.Koanf
	tag string
}

// NewConfigManager creates a new instance of the ConfigManager[T].
// It initializes a Koanf instance and loads the default configuration.
// It then loads the configuration from the /etc/beta9.d/ directory and
// the user-specified configuration file from CONFIG_PATH. If the
// CONFIG_JSON environment variable is set, it loads the configuration
// from the JSON string. If debug mode is enabled, it prints the current
// configuration.
func NewConfigManager[T any]() (*ConfigManager[T], error) {
	// Initialize a ConfigManager[T] with the specified 'T' type.
	cm := &ConfigManager[T]{
		kf:  koanf.New("."),
		tag: "key",
	}

	// Load default configuration from embedded variable
	err := cm.LoadConfig(YAMLConfigFormat, rawbytes.Provider(defaultConfig))
	if err != nil {
		return nil, err
	}

	// Attempt to load configuration from path in CONFIG_PATH
	cp := os.Getenv("CONFIG_PATH")
	ce := filepath.Ext(cp)
	if cp != "" && ce != "" {
		if err := cm.LoadConfig(ConfigFormat(ce), file.Provider(cp)); err != nil {
			return nil, err
		}
	}

	// Attempt to load configs from /etc/beta9.d/
	for ext := range parserMap {
		if matches, err := filepath.Glob(fmt.Sprintf("/etc/beta9.d/*%s", ext)); err == nil {
			sort.Strings(matches)
			for _, path := range matches {
				if err := cm.LoadConfig(ext, file.Provider(path)); err != nil {
					log.Error().Str("path", path).Err(err).Msg("failed to load config")
				}
			}
		}
	}

	// Attempt to load configuration from string in CONFIG_JSON
	configJson := os.Getenv("CONFIG_JSON")
	if configJson != "" {
		if err := cm.LoadConfig(JSONConfigFormat, rawbytes.Provider([]byte(configJson))); err != nil {
			log.Error().Err(err).Msg("failed to load config from CONFIG_JSON")
		} else {
			cm.tag = "json"
		}
	}

	// If debug mode is enabled, print the current configuration.
	if cm.kf.Bool("debugMode") {
		log.Info().Str("config", cm.Print()).Msg("debug mode enabled. current configuration")
	}

	return cm, nil
}

// Print returns a string representation of the current configuration state.
func (cm *ConfigManager[T]) Print() string {
	return cm.kf.Sprint()
}

// GetConfig retrieves the current configuration of type 'T' from the ConfigManager.
// It unmarshals the configuration data and returns it. If any errors occur during
// unmarshaling, it logs a fatal error and exits the application.
func (cm *ConfigManager[T]) GetConfig() T {
	var c T

	err := cm.kf.UnmarshalWithConf("", &c, koanf.UnmarshalConf{Tag: cm.tag, FlatPaths: false})
	if err != nil {
		log.Error().Err(err).Msg("failed to unmarshal config")
		os.Exit(1)
	}

	return c
}

// LoadConfig loads configuration data from a given provider in the specified format
// into the ConfigManager. It obtains a parser for the format, and then loads the
// configuration data. If any errors occur during the loading process, they are
// returned as an error.
func (cm *ConfigManager[T]) LoadConfig(format ConfigFormat, provider koanf.Provider) error {
	parser, err := GetConfigParser(format)
	if err != nil {
		return err
	}

	return cm.kf.Load(provider, parser)
}

var (
	JSONConfigFormat ConfigFormat = ".json"
	YAMLConfigFormat ConfigFormat = ".yaml"
	YMLConfigFormat  ConfigFormat = ".yml"

	parserMap map[ConfigFormat]ParserFunc = map[ConfigFormat]ParserFunc{
		JSONConfigFormat: jsonParserFunc,
		YAMLConfigFormat: yamlParserFunc,
		YMLConfigFormat:  yamlParserFunc,
	}
)

type ConfigFormat string

type ParserFunc func() (koanf.Parser, error)

func GetConfigParser(format ConfigFormat) (koanf.Parser, error) {
	if parserFunc, ok := parserMap[format]; ok {
		return parserFunc()
	}
	return nil, errors.New("parser not found for format" + string(format))
}

func jsonParserFunc() (koanf.Parser, error) {
	return json.Parser(), nil
}

func yamlParserFunc() (koanf.Parser, error) {
	return yaml.Parser(), nil
}
