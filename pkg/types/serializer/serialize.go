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

		result[tagName] = serialize(field.Interface())
	}

	for key, value := range fromMap {
		keys := strings.Split(value, ".")
		val := serialize(result)
		for _, k := range keys {
			_val, ok := val.(map[string]interface{})
			if !ok {
				break
			}

			val = _val[k]
		}

		result[key] = serialize(val)
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
			result[i] = serialize(val.Index(i).Interface())
		}
		return result
	} else {
		if !val.IsValid() {
			return nil
		}

		return val.Interface()
	}
}

func Serialize(v interface{}) interface{} {
	return serialize(v)
}
