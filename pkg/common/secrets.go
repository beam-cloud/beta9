package common

import (
	"errors"
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/aws/karpenter/pkg/utils/env"
	"github.com/joho/godotenv"
)

const (
	EnvLocal      string = "local"
	EnvStaging    string = "staging"
	EnvProduction string = "production"
)

// Secrets manager
var secretsFileName string = env.WithDefaultString("CONFIG_PATH", ".secrets")
var secretsStore *SecretStore
var onceSecrets sync.Once

type SecretStore struct {
	loaded bool
}

func Secrets() *SecretStore {
	onceSecrets.Do(func() {
		secretsStore = &SecretStore{}
		err := secretsStore.load(secretsFileName)
		if err != nil {
			log.Fatalf("Failed to load secrets: %v\n", err)
		}
	})

	return secretsStore
}

func (s *SecretStore) load(filename string) error {
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("secrets file not found: %v", err)
	}

	godotenv.Load(filename)
	s.loaded = true
	return nil
}

// Retrieve a secret as a raw string
func (s *SecretStore) Get(key string) string {
	if !s.loaded {
		s.load(secretsFileName)
	}

	return os.Getenv(key)
}

// Retrieve a secret as a raw string, or return a default value
func (s *SecretStore) GetWithDefault(key string, defaultValue string) string {
	val := s.Get(key)
	if val == "" {
		return defaultValue
	}
	return val
}

// Retrieve a secret and convert to a signed integer
func (s *SecretStore) GetInt(key string) int {
	val := s.Get(key)
	intVal, err := strconv.Atoi(val)

	if err != nil {
		log.Printf("Invalid value: %s", val)
		return -1
	}
	return intVal
}

// Retrieve a secret and convert to a signed integer
func (s *SecretStore) GetIntWithDefault(key string, defaultValue int) int {
	val := s.Get(key)

	intVal, err := strconv.Atoi(val)
	if err != nil {
		return defaultValue
	}

	return intVal
}

// Load and map env vars to a struct using struct tags.
func (s *SecretStore) EnvUnmarshal(st interface{}) error {
	val := reflect.ValueOf(st).Elem()
	typ := val.Type()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if !field.CanSet() {
			continue
		}

		// Get env var name or continue
		envTag := typ.Field(i).Tag.Get("env")
		if envTag == "" || envTag == "-" {
			continue
		}

		// Get env var, use the default, or continue
		envVal := os.Getenv(envTag)
		if envVal == "" {
			defTag := typ.Field(i).Tag.Get("default")
			if defTag == "" {
				continue
			}
			envVal = defTag
		}

		switch field.Kind() {

		case reflect.String:
			field.SetString(envVal)

		case reflect.Int32:
			val, err := strconv.ParseInt(envVal, 10, 32)
			if err != nil {
				return fmt.Errorf("could not convert <%s> to int32: %v", envTag, err)
			}
			field.SetInt(val)

		case reflect.Int, reflect.Int64:
			// Duration is recognized as an int64
			if field.Type() == reflect.ValueOf(time.Duration(0)).Type() {
				val, err := time.ParseDuration(envVal)
				if err != nil {
					return fmt.Errorf("could not convert <%s> to duration: %v", envTag, err)
				}
				field.Set(reflect.ValueOf(val))
			} else {
				val, err := strconv.ParseInt(envVal, 10, 64)
				if err != nil {
					return fmt.Errorf("could not convert <%s> to int64: %v", envTag, err)
				}
				field.SetInt(val)
			}

		case reflect.Uint32:
			val, err := strconv.ParseUint(envVal, 10, 32)
			if err != nil {
				return fmt.Errorf("could not convert <%s> to uint32: %v", envTag, err)
			}
			field.SetUint(val)

		case reflect.Uint64:
			val, err := strconv.ParseUint(envVal, 10, 64)
			if err != nil {
				return fmt.Errorf("could not convert <%s> to uint64: %v", envTag, err)
			}
			field.SetUint(val)

		case reflect.Float32:
			val, err := strconv.ParseFloat(envVal, 32)
			if err != nil {
				return fmt.Errorf("could not convert <%s> to float32: %v", envTag, err)
			}
			field.SetFloat(val)

		case reflect.Float64:
			val, err := strconv.ParseFloat(envVal, 64)
			if err != nil {
				return fmt.Errorf("could not convert <%s> to float64: %v", envTag, err)
			}
			field.SetFloat(val)

		case reflect.Bool:
			val, err := strconv.ParseBool(envVal)
			if err != nil {
				return fmt.Errorf("could not convert <%s> to bool: %v", envTag, err)
			}
			field.SetBool(val)

		default:
			log.Printf("parsing tag <%s> of type <%s> is not supported", envTag, field.Kind())
		}

	}
	return nil
}
