package common

import (
	_ "embed"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/rawbytes"
	"github.com/knadh/koanf/v2"
	"github.com/mitchellh/mapstructure"
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
	// unknown lists the keys that were loaded but match no field of T.
	unknown []UnknownConfigKey
}

// UnknownConfigKey is a key in a config source that no field of the config
// type reads, so setting it has no effect. It is usually a typo or a setting
// that has since been removed.
type UnknownConfigKey struct {
	// Source is where the key was set: a file path, "CONFIG_JSON", or
	// "defaults" for the built-in config.
	Source string
	// Key is the key's path, such as "worker.pools[default].gpuTyp".
	Key string
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
	err := cm.load(YAMLConfigFormat, rawbytes.Provider(defaultConfig), "defaults")
	if err != nil {
		return nil, err
	}

	// Attempt to load configuration from path in CONFIG_PATH
	cp := os.Getenv("CONFIG_PATH")
	ce := filepath.Ext(cp)
	if cp != "" && ce != "" {
		if err := cm.load(ConfigFormat(ce), file.Provider(cp), cp); err != nil {
			return nil, err
		}
	}

	if minimalConfigEnabled() {
		return cm, nil
	}

	// Attempt to load configs from /etc/beta9.d/
	for ext := range parserMap {
		if matches, err := filepath.Glob(fmt.Sprintf("/etc/beta9.d/*%s", ext)); err == nil {
			sort.Strings(matches)
			for _, path := range matches {
				if err := cm.load(ext, file.Provider(path), path); err != nil {
					log.Error().Str("path", path).Err(err).Msg("failed to load config")
				}
			}
		}
	}

	// Attempt to load configuration from string in CONFIG_JSON
	configJson := os.Getenv("CONFIG_JSON")
	if configJson != "" {
		// CONFIG_JSON uses JSON field names, while the defaults and file
		// overlays use key tags. Preserve those values before switching tags.
		var merged T
		if err := cm.kf.UnmarshalWithConf("", &merged, koanf.UnmarshalConf{Tag: cm.tag}); err != nil {
			return nil, err
		}
		baseline, err := stdjson.Marshal(merged)
		if err != nil {
			return nil, err
		}
		// The baseline is written from the config type, so it has no keys
		// of its own to check.
		if err := cm.load(JSONConfigFormat, rawbytes.Provider(baseline), ""); err != nil {
			return nil, err
		}
		cm.tag = "json"
		if err := cm.load(JSONConfigFormat, rawbytes.Provider([]byte(configJson)), "CONFIG_JSON"); err != nil {
			log.Error().Err(err).Msg("failed to load config from CONFIG_JSON")
			cm.tag = "key"
		}
	}

	// If debug mode is enabled, print the current configuration.
	if cm.kf.Bool("debugMode") {
		log.Info().Str("config", cm.Print()).Msg("debug mode enabled. current configuration")
	}

	return cm, nil
}

func minimalConfigEnabled() bool {
	switch strings.TrimSpace(strings.ToLower(os.Getenv(types.WorkerMinimalConfigEnv))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
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
	return cm.load(format, provider, "config")
}

// UnknownKeys returns the keys that were loaded but that no field of the
// config type reads, in the order their sources were loaded.
func (cm *ConfigManager[T]) UnknownKeys() []UnknownConfigKey {
	return append([]UnknownConfigKey(nil), cm.unknown...)
}

// WarnUnknownKeys logs each key that was loaded but has no effect.
func (cm *ConfigManager[T]) WarnUnknownKeys() {
	for _, unknown := range cm.unknown {
		warning := log.Warn().Str("source", unknown.Source).Str("key", unknown.Key)
		if hint := removedKeyHint(unknown.Key); hint != "" {
			warning.Msg("config key was removed and has no effect: " + hint)
			continue
		}
		warning.Msg("config key is not recognised and has no effect; check its spelling or remove it")
	}
}

// removedKeys are keys that the previous release read, with what to do about
// each now. Map keys and list indexes are left out of the paths. An entry
// goes once no deployed config can still hold its key.
var removedKeys = map[string]string{
	"scheduling.failover.onDemand":        onDemandRemoved,
	"scheduling.failover.chains.onDemand": onDemandRemoved,
	"scheduling.failover.health":          "the pool health thresholds were removed, with the pool.schedulable and pool.unschedulable events they produced",
}

const onDemandRemoved = "on-demand failover was removed; a pool now buys its own machines with worker.pools.<name>.provisioning"

var keyIndex = regexp.MustCompile(`\[[^\]]*\]`)

// removedKeyHint says what became of a key that the previous release read.
// It is empty for a key that no release knew.
func removedKeyHint(key string) string {
	return removedKeys[keyIndex.ReplaceAllString(key, "")]
}

// load merges one source over the current config. A source with a name also
// has its keys checked against the config type.
func (cm *ConfigManager[T]) load(format ConfigFormat, provider koanf.Provider, source string) error {
	parser, err := GetConfigParser(format)
	if err != nil {
		return err
	}

	layer := koanf.New(".")
	if err := layer.Load(provider, parser); err != nil {
		return err
	}
	config := cm.kf.Copy()
	if err := config.Merge(layer); err != nil {
		return err
	}

	// Preserve numeric YAML/JSON permissions before weak decoding turns them
	// into decimal strings that the filesystem would interpret as octal.
	for _, path := range config.Keys() {
		_, field, _ := strings.Cut(path, ".geese.")
		switch field {
		case "dirMode", "fileMode", "dir_mode", "file_mode":
		default:
			continue
		}

		var mode int
		switch value := config.Get(path).(type) {
		case int:
			mode = value
		case float64:
			mode = int(value)
			if value != float64(mode) {
				return fmt.Errorf("%s must be an integer file mode", path)
			}
		default:
			continue
		}
		if mode < 0 || mode > 07777 {
			return fmt.Errorf("%s file mode is out of range: %d", path, mode)
		}
		if err := config.Set(path, fmt.Sprintf("%#o", mode)); err != nil {
			return err
		}
	}

	cm.kf = config
	if source != "" {
		for _, key := range unknownKeys[T](layer.Raw(), cm.tag) {
			cm.unknown = append(cm.unknown, UnknownConfigKey{Source: source, Key: key})
		}
	}
	return nil
}

// unknownKeys decodes one source's keys into the config type the way
// GetConfig does and returns the ones the decoder had no field for. A source
// that does not decode reports nothing here: GetConfig reports that error.
func unknownKeys[T any](raw map[string]interface{}, tag string) []string {
	var target T
	var metadata mapstructure.Metadata
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.TextUnmarshallerHookFunc()),
		Metadata:         &metadata,
		Result:           &target,
		WeaklyTypedInput: true,
		TagName:          tag,
	})
	if err != nil {
		return nil
	}
	if err := decoder.Decode(raw); err != nil {
		return nil
	}
	sort.Strings(metadata.Unused)
	return metadata.Unused
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
