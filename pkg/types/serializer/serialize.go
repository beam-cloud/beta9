package serializer

import (
	"fmt"
	"reflect"
	"strings"
)

const TagName = "serializer"
const SourceTag = "source:"

type Serializer interface {
	Serialize() interface{}
}

func checkSerializeMethod(val reflect.Value) bool {
	// Handle invalid values
	if !val.IsValid() {
		return false
	}

	method := val.MethodByName("Serialize")

	// Also check method signature (0 inputs, 1 output)
	// Note: This checks the signature but doesn't validate if the *returned type*
	// is inherently serializable by standard means (e.g., JSON). It relies on the
	// custom Serialize method returning a sensible value (primitive, map, slice, etc.).
	if method.IsValid() && method.Type().NumIn() == 0 && method.Type().NumOut() == 1 {
		return true
	}

	return false
}

func parseStruct(val reflect.Value, typ reflect.Type) map[string]interface{} {
	embeddedFields := make(map[string]interface{})
	result := make(map[string]interface{})
	fromMap := make(map[string]string)

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)
		tagValue := fieldType.Tag.Get(TagName)
		metaTags := strings.Split(tagValue, ",")
		tagName := ""

		if field.Kind() == reflect.Ptr && field.IsNil() {
			tagName = ""
		}

		// If the field is an anonymous struct, recursively serialize it
		if fieldType.Anonymous {
			embeddedFields = parseStruct(field, fieldType.Type)
			continue
		}

		for index, meta := range metaTags {
			if index == 0 {
				if meta == "-" {
					continue
				}

				tagName = meta
				continue
			}

			if strings.HasPrefix(meta, SourceTag) {
				fromMap[tagName] = strings.TrimPrefix(meta, SourceTag)
			}

			if meta == "omitempty" {
				if field.IsZero() {
					tagName = ""
					break
				}
			}
		}

		if tagName == "" {
			continue
		}

		result[tagName] = serialize(field.Interface())
	}

	// Process 'source:' tags after initial field processing
	for key, value := range fromMap {
		keys := strings.Split(value, ".")
		var currentVal interface{} = result // Start traversal from the top-level result map relative to the field
		validPath := true

		for _, k := range keys {
			mapVal, ok := currentVal.(map[string]interface{})
			if !ok {
				// Current value is not a map, cannot traverse deeper
				validPath = false
				break
			}

			nestedVal, exists := mapVal[k]
			if !exists {
				// Key does not exist at this level
				validPath = false
				break
			}
			currentVal = nestedVal // Move to the nested value for the next iteration
		}

		if validPath {
			result[key] = serialize(currentVal)
		} else {
			delete(result, key)
		}
	}

	for k, v := range embeddedFields {
		if _, ok := result[k]; !ok {
			// If the field is not already in the result, add it
			// This is to ensure that child fields take precedence over parent fields
			result[k] = v
		}
	}

	return result
}

func serialize(v interface{}) interface{} {
	val := reflect.ValueOf(v)
	typ := reflect.TypeOf(v)

	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}

		val = val.Elem()
		return serialize(val.Interface())
	}

	switch val.Kind() {
	case reflect.Struct:
		if checkSerializeMethod(val) {
			return val.MethodByName("Serialize").Call(nil)[0].Interface()
		}

		return parseStruct(val, typ)
	case reflect.Slice, reflect.Array:
		result := make([]interface{}, val.Len())
		for i := 0; i < val.Len(); i++ {
			result[i] = serialize(val.Index(i).Interface())
		}
		return result
	case reflect.Map:
		result := make(map[string]interface{})
		for _, key := range val.MapKeys() {
			result[key.String()] = serialize(val.MapIndex(key).Interface())
		}
		return result
	default:
		if !val.IsValid() {
			return nil
		}

		return val.Interface()
	}
}

func Serialize(v interface{}) (res interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = e
			} else {
				err = fmt.Errorf("failed to serialize: %v", r)
			}
		}
	}()

	return serialize(v), nil
}
