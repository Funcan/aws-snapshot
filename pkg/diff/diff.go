// Package diff computes differences between two JSON snapshot objects.
package diff

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// identityFields is the priority-ordered list of fields used to identify
// elements within JSON arrays. The first matching field wins.
var identityFields = []string{
	"name", "identifier", "table_name", "function_name",
	"repository_name", "cluster_name", "domain_name",
	"cache_cluster_id", "replication_group_id",
	"load_balancer_name", "target_group_name",
	"stage_name", "rule_name",
	"vpc_id", "id", "arn",
}

// skipFields are top-level fields excluded from comparison.
var skipFields = map[string]bool{
	"timestamp": true,
}

// ChangeType indicates whether a field was added, removed, or modified.
type ChangeType string

const (
	Added    ChangeType = "added"
	Removed  ChangeType = "removed"
	Modified ChangeType = "modified"
)

// Change represents a single difference between two snapshots.
type Change struct {
	Path     []string
	OldValue interface{}
	NewValue interface{}
	Type     ChangeType
}

// String formats a Change for display.
func (c Change) String() string {
	path := strings.Join(c.Path, "->")
	switch c.Type {
	case Added:
		return fmt.Sprintf("%s: (added)", path)
	case Removed:
		return fmt.Sprintf("%s: (removed)", path)
	default:
		return fmt.Sprintf("%s: %s -> %s", path, FormatValue(c.OldValue), FormatValue(c.NewValue))
	}
}

// Compare computes differences between two JSON objects.
// The "timestamp" field is excluded from comparison.
func Compare(old, new map[string]interface{}) []Change {
	changes := diffMaps(nil, old, new)
	sort.Slice(changes, func(i, j int) bool {
		return pathString(changes[i].Path) < pathString(changes[j].Path)
	})
	return changes
}

func pathString(p []string) string {
	return strings.Join(p, "->")
}

func diffMaps(path []string, old, new map[string]interface{}) []Change {
	var changes []Change

	keys := make(map[string]bool)
	for k := range old {
		keys[k] = true
	}
	for k := range new {
		keys[k] = true
	}

	for k := range keys {
		if len(path) == 0 && skipFields[k] {
			continue
		}

		oldVal, oldOK := old[k]
		newVal, newOK := new[k]
		p := appendPath(path, k)

		if !oldOK {
			changes = append(changes, Change{Path: p, NewValue: newVal, Type: Added})
			continue
		}
		if !newOK {
			changes = append(changes, Change{Path: p, OldValue: oldVal, Type: Removed})
			continue
		}

		changes = append(changes, diffValues(p, oldVal, newVal)...)
	}

	return changes
}

func diffValues(path []string, old, new interface{}) []Change {
	if reflect.DeepEqual(old, new) {
		return nil
	}

	switch oldVal := old.(type) {
	case map[string]interface{}:
		if newVal, ok := new.(map[string]interface{}); ok {
			return diffMaps(path, oldVal, newVal)
		}
	case []interface{}:
		if newVal, ok := new.([]interface{}); ok {
			return diffArrays(path, oldVal, newVal)
		}
	}

	// Primitive change or type mismatch
	return []Change{{Path: path, OldValue: old, NewValue: new, Type: Modified}}
}

func diffArrays(path []string, old, new []interface{}) []Change {
	idKey := findIdentityKey(old, new)
	if idKey != "" {
		return diffArrayByKey(path, old, new, idKey)
	}

	// No identity key found — compare as opaque values
	if reflect.DeepEqual(old, new) {
		return nil
	}
	return []Change{{Path: path, OldValue: old, NewValue: new, Type: Modified}}
}

func diffArrayByKey(path []string, old, new []interface{}, idKey string) []Change {
	oldMap := indexByKey(old, idKey)
	newMap := indexByKey(new, idKey)

	var changes []Change

	for id, oldObj := range oldMap {
		newObj, ok := newMap[id]
		if !ok {
			changes = append(changes, Change{Path: appendPath(path, id), Type: Removed})
			continue
		}
		changes = append(changes, diffMaps(appendPath(path, id), oldObj, newObj)...)
	}

	for id := range newMap {
		if _, ok := oldMap[id]; !ok {
			changes = append(changes, Change{Path: appendPath(path, id), Type: Added})
		}
	}

	return changes
}

func indexByKey(arr []interface{}, key string) map[string]map[string]interface{} {
	result := make(map[string]map[string]interface{})
	for _, item := range arr {
		obj, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		id, ok := obj[key]
		if !ok {
			continue
		}
		result[fmt.Sprintf("%v", id)] = obj
	}
	return result
}

func findIdentityKey(old, new []interface{}) string {
	all := make([]interface{}, 0, len(old)+len(new))
	all = append(all, old...)
	all = append(all, new...)

	if len(all) == 0 {
		return ""
	}

	first, ok := all[0].(map[string]interface{})
	if !ok {
		return ""
	}

	for _, field := range identityFields {
		if _, ok := first[field]; ok {
			return field
		}
	}
	return ""
}

func appendPath(path []string, elem string) []string {
	result := make([]string, len(path)+1)
	copy(result, path)
	result[len(path)] = elem
	return result
}

// FormatValue formats a value for human-readable diff output.
func FormatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("%q", val)
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%g", val)
	case bool:
		return fmt.Sprintf("%t", val)
	case nil:
		return "null"
	case []interface{}:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = FormatValue(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]interface{}:
		return fmt.Sprintf("{...%d keys}", len(val))
	default:
		return fmt.Sprintf("%v", val)
	}
}
