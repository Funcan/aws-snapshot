package diff

import (
	"testing"
)

func TestCompareModifiedPrimitive(t *testing.T) {
	old := map[string]interface{}{
		"EKS": []interface{}{
			map[string]interface{}{"name": "cluster1", "kubernetes_version": "1.32"},
		},
	}
	new := map[string]interface{}{
		"EKS": []interface{}{
			map[string]interface{}{"name": "cluster1", "kubernetes_version": "1.33"},
		},
	}

	changes := Compare(old, new)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %v", len(changes), changes)
	}

	c := changes[0]
	if c.Type != Modified {
		t.Errorf("expected Modified, got %s", c.Type)
	}
	want := `EKS->cluster1->kubernetes_version: "1.32" -> "1.33"`
	if c.String() != want {
		t.Errorf("got %q, want %q", c.String(), want)
	}
}

func TestCompareAddedElement(t *testing.T) {
	old := map[string]interface{}{
		"S3": []interface{}{
			map[string]interface{}{"name": "bucket1"},
		},
	}
	new := map[string]interface{}{
		"S3": []interface{}{
			map[string]interface{}{"name": "bucket1"},
			map[string]interface{}{"name": "bucket2"},
		},
	}

	changes := Compare(old, new)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %v", len(changes), changes)
	}

	c := changes[0]
	if c.Type != Added {
		t.Errorf("expected Added, got %s", c.Type)
	}
	if c.String() != "S3->bucket2: (added)" {
		t.Errorf("got %q", c.String())
	}
}

func TestCompareRemovedElement(t *testing.T) {
	old := map[string]interface{}{
		"S3": []interface{}{
			map[string]interface{}{"name": "bucket1"},
			map[string]interface{}{"name": "bucket2"},
		},
	}
	new := map[string]interface{}{
		"S3": []interface{}{
			map[string]interface{}{"name": "bucket1"},
		},
	}

	changes := Compare(old, new)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %v", len(changes), changes)
	}

	c := changes[0]
	if c.Type != Removed {
		t.Errorf("expected Removed, got %s", c.Type)
	}
	if c.String() != "S3->bucket2: (removed)" {
		t.Errorf("got %q", c.String())
	}
}

func TestCompareNoChanges(t *testing.T) {
	data := map[string]interface{}{
		"EKS": []interface{}{
			map[string]interface{}{"name": "c1", "version": "1.32"},
		},
	}

	changes := Compare(data, data)
	if len(changes) != 0 {
		t.Errorf("expected 0 changes, got %d: %v", len(changes), changes)
	}
}

func TestCompareTimestampSkipped(t *testing.T) {
	old := map[string]interface{}{
		"timestamp": "2025-01-01T00:00:00Z",
		"S3":        []interface{}{},
	}
	new := map[string]interface{}{
		"timestamp": "2025-01-02T00:00:00Z",
		"S3":        []interface{}{},
	}

	changes := Compare(old, new)
	if len(changes) != 0 {
		t.Errorf("expected timestamp to be skipped, got %d changes: %v", len(changes), changes)
	}
}

func TestCompareNestedMapChange(t *testing.T) {
	old := map[string]interface{}{
		"RDS": map[string]interface{}{
			"instances": []interface{}{
				map[string]interface{}{
					"identifier": "db1",
					"storage_gb": float64(100),
				},
			},
		},
	}
	new := map[string]interface{}{
		"RDS": map[string]interface{}{
			"instances": []interface{}{
				map[string]interface{}{
					"identifier": "db1",
					"storage_gb": float64(200),
				},
			},
		},
	}

	changes := Compare(old, new)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %v", len(changes), changes)
	}

	want := "RDS->instances->db1->storage_gb: 100 -> 200"
	if changes[0].String() != want {
		t.Errorf("got %q, want %q", changes[0].String(), want)
	}
}

func TestCompareAddedTopLevelKey(t *testing.T) {
	old := map[string]interface{}{}
	new := map[string]interface{}{
		"Lambda": []interface{}{
			map[string]interface{}{"function_name": "fn1"},
		},
	}

	changes := Compare(old, new)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %v", len(changes), changes)
	}
	if changes[0].Type != Added {
		t.Errorf("expected Added, got %s", changes[0].Type)
	}
}

func TestCompareRemovedTopLevelKey(t *testing.T) {
	old := map[string]interface{}{
		"Lambda": []interface{}{
			map[string]interface{}{"function_name": "fn1"},
		},
	}
	new := map[string]interface{}{}

	changes := Compare(old, new)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d: %v", len(changes), changes)
	}
	if changes[0].Type != Removed {
		t.Errorf("expected Removed, got %s", changes[0].Type)
	}
}

func TestCompareMultipleChanges(t *testing.T) {
	old := map[string]interface{}{
		"EKS": []interface{}{
			map[string]interface{}{"name": "c1", "version": "1.32", "nodes": float64(3)},
		},
	}
	new := map[string]interface{}{
		"EKS": []interface{}{
			map[string]interface{}{"name": "c1", "version": "1.33", "nodes": float64(5)},
		},
	}

	changes := Compare(old, new)
	if len(changes) != 2 {
		t.Fatalf("expected 2 changes, got %d: %v", len(changes), changes)
	}
}

func TestFormatValueTypes(t *testing.T) {
	tests := []struct {
		input interface{}
		want  string
	}{
		{"hello", `"hello"`},
		{float64(42), "42"},
		{float64(3.14), "3.14"},
		{true, "true"},
		{false, "false"},
		{nil, "null"},
		{[]interface{}{"a", "b"}, `["a", "b"]`},
	}

	for _, tt := range tests {
		got := FormatValue(tt.input)
		if got != tt.want {
			t.Errorf("FormatValue(%v) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestCompareBoolChange(t *testing.T) {
	old := map[string]interface{}{
		"S3": []interface{}{
			map[string]interface{}{"name": "b1", "versioning": false},
		},
	}
	new := map[string]interface{}{
		"S3": []interface{}{
			map[string]interface{}{"name": "b1", "versioning": true},
		},
	}

	changes := Compare(old, new)
	if len(changes) != 1 {
		t.Fatalf("expected 1 change, got %d", len(changes))
	}
	want := "S3->b1->versioning: false -> true"
	if changes[0].String() != want {
		t.Errorf("got %q, want %q", changes[0].String(), want)
	}
}
