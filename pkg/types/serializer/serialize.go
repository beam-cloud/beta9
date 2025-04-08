package serializer

import (
	"reflect"
	"strings"
)

const TagName = "serializer"
const SourceTag = "source:"

func checkSerializeMethod(val reflect.Value) bool {
	// Handle invalid values
	if !val.IsValid() {
		return false
	}

	// Convert to pointer if needed and addressable
	if val.Kind() != reflect.Ptr && val.CanAddr() {
		val = val.Addr()
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

func loopFields(val reflect.Value, typ reflect.Type) map[string]interface{} {
	result := make(map[string]interface{})
	fromMap := make(map[string]string)

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		fieldType := typ.Field(i)
		tagValue := fieldType.Tag.Get(TagName)
		tagMeta := strings.Split(tagValue, ",")
		tagName := ""

		if field.Kind() == reflect.Ptr && field.IsNil() {
			tagName = ""
		}

		if fieldType.Anonymous {
			for k, v := range loopFields(field, fieldType.Type) {
				result[k] = v
			}
			continue
		}

		for index, meta := range tagMeta {
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

		result[tagName] = Serialize(field.Interface())
	}

	// Process 'source:' tags after initial field processing
	for key, value := range fromMap {
		keys := strings.Split(value, ".")
		var currentVal interface{} = result // Start traversal from the top-level result map
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
			// Only serialize and assign if the full path was valid
			result[key] = Serialize(currentVal)
		} else {
			delete(result, key)
		}
	}
	return result
}

func Serialize(v interface{}) interface{} {
	val := reflect.ValueOf(v)
	typ := reflect.TypeOf(v)

	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}

		val = val.Elem()
		typ = typ.Elem()
	}

	if val.Kind() == reflect.Struct {
		if checkSerializeMethod(val) {
			return val.MethodByName("Serialize").Call(nil)[0].Interface()
		}

		return loopFields(val, typ)
	} else if val.Kind() == reflect.Slice {
		result := make([]interface{}, val.Len())
		for i := 0; i < val.Len(); i++ {
			result[i] = Serialize(val.Index(i).Interface())
		}
		return result
	} else {
		if !val.IsValid() {
			return nil
		}

		return val.Interface()
	}
}
