package common

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/beam-cloud/beta9/pkg/types"
)

func FindField(v reflect.Value, fieldName string) (reflect.Value, bool) {
	if v.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}

	for i := 0; i < v.NumField(); i++ {
		fieldType := v.Type().Field(i)
		jsonTag := fieldType.Tag.Get("json")

		if commaIndex := strings.Index(jsonTag, ","); commaIndex != -1 {
			jsonTag = jsonTag[:commaIndex]
		}

		if jsonTag == fieldName {
			return v.Field(i), true
		}
	}

	return reflect.Value{}, false
}

func ConvertValue(targetType reflect.Type, value interface{}) (reflect.Value, error) {
	valueType := reflect.TypeOf(value)

	if valueType == targetType {
		return reflect.ValueOf(value), nil
	}

	if value == nil {
		return reflect.Zero(targetType), nil
	}

	if targetType == reflect.TypeOf(types.GpuType("")) {
		switch v := value.(type) {
		case string:
			return reflect.ValueOf(types.GpuType(v)), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to GpuType", value, value)
		}
	}

	switch targetType.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := value.(type) {
		case float64:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		case int:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		case int64:
			return reflect.ValueOf(v).Convert(targetType), nil
		case uint:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		case uint64:
			return reflect.ValueOf(int64(v)).Convert(targetType), nil
		case string:
			var i int64
			if _, err := fmt.Sscanf(v, "%d", &i); err != nil {
				return reflect.Value{}, fmt.Errorf("cannot convert string '%s' to %v", v, targetType)
			}
			return reflect.ValueOf(i).Convert(targetType), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to %v", value, value, targetType)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := value.(type) {
		case float64:
			if v < 0 {
				return reflect.Value{}, fmt.Errorf("cannot convert negative float %v to %v", v, targetType)
			}
			return reflect.ValueOf(uint64(v)).Convert(targetType), nil
		case int:
			if v < 0 {
				return reflect.Value{}, fmt.Errorf("cannot convert negative int %v to %v", v, targetType)
			}
			return reflect.ValueOf(uint64(v)).Convert(targetType), nil
		case int64:
			if v < 0 {
				return reflect.Value{}, fmt.Errorf("cannot convert negative int64 %v to %v", v, targetType)
			}
			return reflect.ValueOf(uint64(v)).Convert(targetType), nil
		case uint:
			return reflect.ValueOf(uint64(v)).Convert(targetType), nil
		case uint64:
			return reflect.ValueOf(v).Convert(targetType), nil
		case string:
			var u uint64
			if _, err := fmt.Sscanf(v, "%d", &u); err != nil {
				return reflect.Value{}, fmt.Errorf("cannot convert string '%s' to %v", v, targetType)
			}
			return reflect.ValueOf(u).Convert(targetType), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to %v", value, value, targetType)
		}
	case reflect.Float32, reflect.Float64:
		switch v := value.(type) {
		case int:
			return reflect.ValueOf(float64(v)).Convert(targetType), nil
		case int64:
			return reflect.ValueOf(float64(v)).Convert(targetType), nil
		case uint:
			return reflect.ValueOf(float64(v)).Convert(targetType), nil
		case uint64:
			return reflect.ValueOf(float64(v)).Convert(targetType), nil
		case float64:
			return reflect.ValueOf(v).Convert(targetType), nil
		case string:
			var f float64
			if _, err := fmt.Sscanf(v, "%f", &f); err != nil {
				return reflect.Value{}, fmt.Errorf("cannot convert string '%s' to %v", v, targetType)
			}
			return reflect.ValueOf(f).Convert(targetType), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to %v", value, value, targetType)
		}
	case reflect.String:
		switch v := value.(type) {
		case string:
			return reflect.ValueOf(v), nil
		case int, int64, uint, uint64, float64, bool:
			return reflect.ValueOf(fmt.Sprintf("%v", v)), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to string", value, value)
		}
	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			return reflect.ValueOf(v), nil
		case string:
			switch strings.ToLower(v) {
			case "true", "1", "yes", "on":
				return reflect.ValueOf(true), nil
			case "false", "0", "no", "off":
				return reflect.ValueOf(false), nil
			default:
				return reflect.Value{}, fmt.Errorf("cannot convert string '%s' to bool", v)
			}
		case int, int64:
			return reflect.ValueOf(v != 0), nil
		case uint, uint64:
			return reflect.ValueOf(v != 0), nil
		case float64:
			return reflect.ValueOf(v != 0), nil
		default:
			return reflect.Value{}, fmt.Errorf("cannot convert %v (%T) to bool", value, value)
		}
	case reflect.Slice:
		if valueType.Kind() == reflect.Slice {
			srcSlice := reflect.ValueOf(value)
			dstSlice := reflect.MakeSlice(targetType, srcSlice.Len(), srcSlice.Len())

			for i := 0; i < srcSlice.Len(); i++ {
				converted, err := ConvertValue(targetType.Elem(), srcSlice.Index(i).Interface())
				if err != nil {
					return reflect.Value{}, fmt.Errorf("failed to convert slice element %d: %v", i, err)
				}
				dstSlice.Index(i).Set(converted)
			}
			return dstSlice, nil
		}
		return reflect.Value{}, fmt.Errorf("cannot convert %v to slice", valueType)
	case reflect.Ptr:
		if value == nil {
			return reflect.Zero(targetType), nil
		}
		elemValue, err := ConvertValue(targetType.Elem(), value)
		if err != nil {
			return reflect.Value{}, err
		}
		ptrValue := reflect.New(targetType.Elem())
		ptrValue.Elem().Set(elemValue)
		return ptrValue, nil
	default:
		return reflect.Value{}, fmt.Errorf("unsupported type conversion to %v", targetType)
	}
}
