package awsclient

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
)

func TestTagsToMap(t *testing.T) {
	tests := []struct {
		name     string
		tags     interface{}
		expected map[string]string
	}{
		{
			name:     "nil slice",
			tags:     ([]ec2types.Tag)(nil),
			expected: nil,
		},
		{
			name:     "empty slice",
			tags:     []ec2types.Tag{},
			expected: nil,
		},
		{
			name: "single tag - EC2",
			tags: []ec2types.Tag{
				{Key: aws.String("Name"), Value: aws.String("test-vpc")},
			},
			expected: map[string]string{
				"Name": "test-vpc",
			},
		},
		{
			name: "multiple tags - EC2",
			tags: []ec2types.Tag{
				{Key: aws.String("Name"), Value: aws.String("test-vpc")},
				{Key: aws.String("Environment"), Value: aws.String("production")},
				{Key: aws.String("Owner"), Value: aws.String("team-a")},
			},
			expected: map[string]string{
				"Name":        "test-vpc",
				"Environment": "production",
				"Owner":       "team-a",
			},
		},
		{
			name: "tags with nil key",
			tags: []ec2types.Tag{
				{Key: aws.String("Name"), Value: aws.String("test")},
				{Key: nil, Value: aws.String("value")},
			},
			expected: map[string]string{
				"Name": "test",
				"":     "value",
			},
		},
		{
			name: "tags with nil value",
			tags: []ec2types.Tag{
				{Key: aws.String("Name"), Value: aws.String("test")},
				{Key: aws.String("Empty"), Value: nil},
			},
			expected: map[string]string{
				"Name":  "test",
				"Empty": "",
			},
		},
		{
			name: "ECR tags (different type)",
			tags: []ecrtypes.Tag{
				{Key: aws.String("Repository"), Value: aws.String("my-app")},
				{Key: aws.String("Version"), Value: aws.String("1.0.0")},
			},
			expected: map[string]string{
				"Repository": "my-app",
				"Version":    "1.0.0",
			},
		},
		{
			name: "DynamoDB tags (different type)",
			tags: []dynamodbtypes.Tag{
				{Key: aws.String("Table"), Value: aws.String("users")},
				{Key: aws.String("Region"), Value: aws.String("us-east-1")},
			},
			expected: map[string]string{
				"Table":  "users",
				"Region": "us-east-1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tagsToMap(tt.tags)

			// Check nil cases
			if tt.expected == nil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}

			// Check length
			if len(result) != len(tt.expected) {
				t.Errorf("expected %d tags, got %d", len(tt.expected), len(result))
			}

			// Check each key-value pair
			for key, expectedValue := range tt.expected {
				actualValue, exists := result[key]
				if !exists {
					t.Errorf("expected key %q not found in result", key)
					continue
				}
				if actualValue != expectedValue {
					t.Errorf("for key %q: expected %q, got %q", key, expectedValue, actualValue)
				}
			}
		})
	}
}

func TestTagsToMap_InvalidInput(t *testing.T) {
	tests := []struct {
		name  string
		input interface{}
	}{
		{
			name:  "nil interface",
			input: nil,
		},
		{
			name:  "not a slice",
			input: "not a slice",
		},
		{
			name:  "slice of non-structs",
			input: []string{"foo", "bar"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tagsToMap(tt.input)
			if result != nil {
				t.Errorf("expected nil for invalid input, got %v", result)
			}
		})
	}
}

func TestTagsToMap_PreservesAllTags(t *testing.T) {
	// Test with a larger number of tags to ensure none are lost
	tags := make([]ec2types.Tag, 100)
	expected := make(map[string]string)

	for i := 0; i < 100; i++ {
		key := aws.String("Key" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
		value := aws.String("Value" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
		tags[i] = ec2types.Tag{Key: key, Value: value}
		expected[*key] = *value
	}

	result := tagsToMap(tags)

	if len(result) != len(expected) {
		t.Errorf("expected %d tags, got %d", len(expected), len(result))
	}

	for key, expectedValue := range expected {
		if actualValue, exists := result[key]; !exists {
			t.Errorf("key %q not found in result", key)
		} else if actualValue != expectedValue {
			t.Errorf("for key %q: expected %q, got %q", key, expectedValue, actualValue)
		}
	}
}
