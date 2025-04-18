package serializer

import (
	"encoding/json"
	"log"
	"testing"
	"time"
)

func TestBasicSerialize(t *testing.T) {
	type Test struct {
		ID   int    `serializer:"id"`
		Name string `serializer:"name"`
	}

	test := Test{
		ID:   1,
		Name: "test",
	}

	result, err := Serialize(test)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if parsedResult["id"] != float64(1) || parsedResult["name"] != "test" {
		// JSON unmarshal converts ints to floats in the map
		t.Error("Values are not correct")
	}
}

func TestNestedSerializeSource(t *testing.T) {
	type DeepNested struct {
		ID         int    `serializer:"id"`
		Name       string `serializer:"name"`
		ExternalID string `serializer:"external_id"`
	}

	type Nested struct {
		ID         int        `serializer:"id"`
		Name       string     `serializer:"name"`
		DeepNested DeepNested `serializer:"deep_nested"`
	}

	type Test struct {
		Nested       Nested `serializer:"nested"`
		ID           int    `serializer:"id,source:nested.id"`
		Name         string `serializer:"name,source:nested.name"`
		Name2        string `serializer:"name2,source:nested.deep_nested.name"`
		DeepNestedID string `serializer:"deep_nested_id,source:nested.deep_nested.external_id"`
	}

	test := Test{
		ID: 1,
		Nested: Nested{
			ID:   5,
			Name: "test",
			DeepNested: DeepNested{
				ID:         2,
				Name:       "test2",
				ExternalID: "abcdefg",
			},
		},
	}

	result, err := Serialize(test)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if int64(parsedResult["id"].(float64)) != int64(test.Nested.ID) || parsedResult["name"] != test.Nested.Name || parsedResult["name2"] != test.Nested.DeepNested.Name || parsedResult["deep_nested_id"] != test.Nested.DeepNested.ExternalID {
		t.Error("Values are not correct")
	}
}

func TestSerializeSourceAtSameLevel(t *testing.T) {
	type Test struct {
		ID    int    `serializer:"id"`
		Name  string `serializer:"name"`
		Name2 string `serializer:"name2,source:name"`
	}

	test := Test{
		ID:   1,
		Name: "test",
	}

	result, err := Serialize(test)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if parsedResult["name2"] != "test" {
		t.Error("Values are not correct")
	}
}

func TestSerializeArray(t *testing.T) {
	type Nested struct {
		ID         int    `serializer:"id"`
		Name       string `serializer:"name"`
		ExternalID string `serializer:"external_id"`
	}

	type Test struct {
		ID          int      `serializer:"-"`
		ExternalID  string   `serializer:"id"`
		Name        string   `serializer:"name"`
		Nested      Nested   `serializer:"nested"`
		NestedID    string   `serializer:"nested_id,source:nested.external_id"`
		NestedArray []Nested `serializer:"nested_array"`
	}

	test := []Test{
		{
			ID:         1,
			Name:       "test",
			ExternalID: "123",
			Nested: Nested{
				ID:         2,
				Name:       "test2",
				ExternalID: "abcdefg",
			},
		},
		{
			ID:   2,
			Name: "test2",
		},
		{
			ID:   3,
			Name: "test3",
			NestedArray: []Nested{
				{
					ID:         4,
					Name:       "test4",
					ExternalID: "ffff",
				},
				{
					ID:         5,
					Name:       "test5",
					ExternalID: "gggg",
				},
			},
		},
	}

	result, err := Serialize(test)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult []map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if len(parsedResult) != 3 {
		t.Error("Expected 3 items in the array")
	}

	if parsedResult[0]["id"] != "123" || parsedResult[0]["name"] != "test" || parsedResult[0]["nested_id"] != "abcdefg" {
		t.Error("Values are not correct")
	}

	bytes, err = json.Marshal(parsedResult[2]["nested_array"])
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	var parsedNestedArray []map[string]interface{}
	err = json.Unmarshal(bytes, &parsedNestedArray)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if parsedNestedArray[0]["external_id"] != "ffff" || parsedNestedArray[1]["external_id"] != "gggg" {
		t.Error("Values are not correct")
	}
}

func TestOmitempty(t *testing.T) {
	type Nested struct {
		ID       int    `serializer:"id"`
		Name     string `serializer:"name"`
		Omitting string `serializer:"omitting,omitempty"`
	}

	type Test struct {
		ID     int    `serializer:"id"`
		Name   string `serializer:"name,omitempty"`
		Nested Nested `serializer:"nested,omitempty"`
	}

	test := Test{
		ID: 1,
		Nested: Nested{
			ID:   2,
			Name: "test2",
		},
	}

	result, err := Serialize(test)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if _, ok := parsedResult["name"]; ok {
		t.Error("Expected nested to be omitted")
	}

	bytes, err = json.Marshal(parsedResult["nested"])
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	var parsedNested map[string]interface{}
	err = json.Unmarshal(bytes, &parsedNested)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if _, ok := parsedNested["omitting"]; ok {
		t.Error("Expected omitting to be nil")
	}

	type Test2 struct {
		ID     int    `serializer:"id"`
		Name   string `serializer:"name,omitempty"`
		Nested Nested `serializer:"nested,omitempty"`
	}

	test2 := Test2{
		ID:   1,
		Name: "test",
	}

	result, err = Serialize(test2)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err = json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult2 map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult2)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if _, ok := parsedResult2["nested"]; ok {
		t.Error("Expected nested to be omitted")
	}
}

func TestPointers(t *testing.T) {
	type Nested struct {
		ID              int     `serializer:"id"`
		Name            string  `serializer:"name"`
		NilVal          *string `serializer:"nil_val"`
		NilValOmitempty *string `serializer:"nil_val_omitempty,omitempty"`
	}

	type Test struct {
		ID     int     `serializer:"id"`
		Name   *string `serializer:"name"`
		Nested *Nested `serializer:"nested"`
	}

	test := Test{
		ID:   1,
		Name: nil,
	}

	result, err := Serialize(test)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if val, ok := parsedResult["name"]; ok && val != nil {
		t.Error("Expected name to be nil")
	}

	if val, ok := parsedResult["nested"]; ok && val != nil {
		t.Error("Expected nested to be omitted")
	}

	test2 := Test{
		ID:   1,
		Name: nil,
		Nested: &Nested{
			ID:              2,
			Name:            "test2",
			NilVal:          nil,
			NilValOmitempty: nil,
		},
	}

	result, err = Serialize(test2)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err = json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult2 map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult2)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if _, ok := parsedResult2["nested"]; !ok {
		t.Error("Expected nested to be present")
		return
	}

	var parsedNested map[string]interface{}
	err = json.Unmarshal(bytes, &parsedNested)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if val, ok := parsedNested["nil_val"]; ok && val != nil {
		t.Error("Expected nil_val to be nil")
	}

	if _, ok := parsedNested["nil_val_omitempty"]; ok {
		t.Error("Expected nil_val_omitempty to not exist")
	}
}

func TestEmbeddedStruct(t *testing.T) {
	type DeepNested struct {
		DeepNestedID   int    `serializer:"deep_nested_id"`
		DeepNestedName string `serializer:"deep_nested_name"`
	}

	type Nested struct {
		DeepNested
		ID   int    `serializer:"id"`
		Name string `serializer:"name"`
	}

	type Test struct {
		Nested
	}

	test := Test{
		Nested: Nested{
			ID:   1,
			Name: "test",
			DeepNested: DeepNested{
				DeepNestedID:   2,
				DeepNestedName: "test2",
			},
		},
	}

	result, err := Serialize(test)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if parsedResult["id"] != float64(1) || parsedResult["name"] != "test" {
		t.Error("Values are not correct in the top level")
	}

	var parsedNested map[string]interface{}
	err = json.Unmarshal(bytes, &parsedNested)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if parsedNested["deep_nested_id"] != float64(2) || parsedNested["deep_nested_name"] != "test2" {
		t.Error("Values are not correct in the nested level")
	}
}

type SerializeAbleTime time.Time

func (t SerializeAbleTime) Serialize() string {
	return time.Time(t).Format(time.RFC3339)
}

func TestSerializeMethod(t *testing.T) {

	type Test struct {
		Time SerializeAbleTime `serializer:"time"`
	}

	test := Test{
		Time: SerializeAbleTime(time.Now()),
	}

	result, err := Serialize(test)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	log.Println(parsedResult["time"])

	if parsedResult["time"] != test.Time.Serialize() {
		t.Error("Time is not correct")
	}
}

func TestSerializeMaps(t *testing.T) {
	type Test struct {
		ID  int                    `serializer:"id"`
		Map map[string]interface{} `serializer:"map"`
	}

	test := Test{
		ID:  1,
		Map: map[string]interface{}{"key": "value"},
	}

	result, err := Serialize(test)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	mapVal, ok := parsedResult["map"].(map[string]interface{})
	if !ok {
		t.Error("Map is not correct")
	}

	if mapVal["key"] != "value" {
		t.Error("Map is not correct")
	}
}

func TestSerializePrimitive(t *testing.T) {
	tests := []struct {
		input    interface{}
		expected interface{}
	}{
		{1, 1},
		{"test", "test"},
		{true, true},
		{false, false},
	}

	for _, test := range tests {
		result, err := Serialize(test.input)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
			return
		}

		if result != test.expected {
			t.Errorf("Expected %v, got %v", test.expected, result)
		}
	}

	testMap := map[string]interface{}{
		"key": "value",
	}

	result, err := Serialize(testMap)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	mapVal, ok := result.(map[string]interface{})
	if !ok {
		t.Error("Result is not a map")
	}

	if mapVal["key"] != "value" {
		t.Error("Map is not correct")
	}

	testSlice := []interface{}{1, "test", true, false}
	result, err = Serialize(testSlice)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	sliceVal, ok := result.([]interface{})
	if !ok {
		t.Error("Result is not a slice")
	}

	for i, val := range sliceVal {
		if val != testSlice[i] {
			t.Errorf("Expected %v, got %v", testSlice[i], val)
		}
	}
}

func TestSerializeJsonStringAsString(t *testing.T) {
	type Test struct {
		ID   int    `serializer:"id"`
		Data string `serializer:"data"`
	}

	test := Test{
		ID:   1,
		Data: "{\"key\": \"value\"}",
	}

	result, err := Serialize(test)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	bytes, err := json.Marshal(result)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
		return
	}

	var parsedResult map[string]interface{}
	err = json.Unmarshal(bytes, &parsedResult)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	if parsedResult["data"] != "{\"key\": \"value\"}" {
		t.Error("Data is not correct")
	}
}
