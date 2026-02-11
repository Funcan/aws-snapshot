package awsclient

import (
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// tagsToMap converts a slice of AWS SDK tags to a map of string key-value pairs.
// It works with any AWS SDK tag type that has Key and Value fields of type *string.
// Returns nil if the tags slice is empty.
// This function uses reflection to support tags from different AWS service packages.
func tagsToMap(tags interface{}) map[string]string {
	v := reflect.ValueOf(tags)
	if v.Kind() != reflect.Slice || v.Len() == 0 {
		return nil
	}

	m := make(map[string]string)
	for i := 0; i < v.Len(); i++ {
		tag := v.Index(i)

		// Ensure the element is a struct
		if tag.Kind() != reflect.Struct {
			continue
		}

		// Get Key and Value fields
		keyField := tag.FieldByName("Key")
		valueField := tag.FieldByName("Value")

		if keyField.IsValid() && valueField.IsValid() {
			// Convert *string to string using aws.ToString
			key := keyField.Interface().(*string)
			value := valueField.Interface().(*string)
			m[aws.ToString(key)] = aws.ToString(value)
		}
	}

	// Return nil if no valid tags were found
	if len(m) == 0 {
		return nil
	}
	return m
}
