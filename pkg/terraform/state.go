package terraform

import (
	"encoding/json"
	"fmt"
)

// State represents a Terraform state file.
type State struct {
	Version   int        `json:"version"`
	Resources []Resource `json:"resources"`
}

// Resource represents a single resource block in the state file.
type Resource struct {
	Mode      string     `json:"mode"`
	Type      string     `json:"type"`
	Name      string     `json:"name"`
	Instances []Instance `json:"instances"`
}

// Instance represents a single instance of a resource.
type Instance struct {
	Attributes map[string]interface{} `json:"attributes"`
}

// ParseState parses a Terraform state file from JSON bytes.
func ParseState(data []byte) (*State, error) {
	var state State
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("parsing state file: %w", err)
	}
	return &state, nil
}

// ResourcesByType returns all resources matching the given Terraform resource type.
func (s *State) ResourcesByType(resourceType string) []Resource {
	var result []Resource
	for _, r := range s.Resources {
		if r.Type == resourceType {
			result = append(result, r)
		}
	}
	return result
}

// InstanceAttribute extracts a string attribute from an instance.
func InstanceAttribute(inst Instance, key string) string {
	val, ok := inst.Attributes[key]
	if !ok {
		return ""
	}
	s, _ := val.(string)
	return s
}
