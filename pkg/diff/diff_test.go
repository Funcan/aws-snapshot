package diff

import (
	"strings"
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

func changesString(changes []Change) string {
	lines := make([]string, len(changes))
	for i, c := range changes {
		lines[i] = c.String()
	}
	return strings.Join(lines, "\n")
}

// snapshot1 is a baseline with EKS, RDS, S3, and Lambda resources.
var snapshot1 = map[string]interface{}{
	"timestamp": "2025-02-20T12:00:00Z",
	"EKS": []interface{}{
		map[string]interface{}{
			"name":               "production",
			"kubernetes_version": "1.32",
			"node_count":         float64(5),
			"endpoint_private":   true,
		},
		map[string]interface{}{
			"name":               "staging",
			"kubernetes_version": "1.31",
			"node_count":         float64(2),
			"endpoint_private":   false,
		},
	},
	"RDS": map[string]interface{}{
		"instances": []interface{}{
			map[string]interface{}{
				"identifier":     "app-db",
				"engine":         "postgres",
				"engine_version": "15.4",
				"instance_class": "db.r6g.xlarge",
				"storage_gb":     float64(100),
			},
			map[string]interface{}{
				"identifier":     "analytics-db",
				"engine":         "postgres",
				"engine_version": "15.4",
				"instance_class": "db.r6g.large",
				"storage_gb":     float64(500),
			},
		},
	},
	"S3": []interface{}{
		map[string]interface{}{
			"name":       "app-assets",
			"versioning": true,
		},
		map[string]interface{}{
			"name":       "logs-archive",
			"versioning": false,
		},
	},
	"Lambda": []interface{}{
		map[string]interface{}{
			"function_name": "api-handler",
			"runtime":       "nodejs18.x",
			"memory_mb":     float64(256),
			"timeout":       float64(30),
		},
	},
}

// snapshot2: EKS production upgraded, RDS scaled up, new S3 bucket, Lambda runtime bumped.
var snapshot2 = map[string]interface{}{
	"timestamp": "2025-02-21T13:00:00Z",
	"EKS": []interface{}{
		map[string]interface{}{
			"name":               "production",
			"kubernetes_version": "1.33",
			"node_count":         float64(7),
			"endpoint_private":   true,
		},
		map[string]interface{}{
			"name":               "staging",
			"kubernetes_version": "1.31",
			"node_count":         float64(2),
			"endpoint_private":   false,
		},
	},
	"RDS": map[string]interface{}{
		"instances": []interface{}{
			map[string]interface{}{
				"identifier":     "app-db",
				"engine":         "postgres",
				"engine_version": "15.4",
				"instance_class": "db.r6g.2xlarge",
				"storage_gb":     float64(200),
			},
			map[string]interface{}{
				"identifier":     "analytics-db",
				"engine":         "postgres",
				"engine_version": "15.4",
				"instance_class": "db.r6g.large",
				"storage_gb":     float64(500),
			},
		},
	},
	"S3": []interface{}{
		map[string]interface{}{
			"name":       "app-assets",
			"versioning": true,
		},
		map[string]interface{}{
			"name":       "logs-archive",
			"versioning": false,
		},
		map[string]interface{}{
			"name":       "data-exports",
			"versioning": true,
		},
	},
	"Lambda": []interface{}{
		map[string]interface{}{
			"function_name": "api-handler",
			"runtime":       "nodejs20.x",
			"memory_mb":     float64(256),
			"timeout":       float64(30),
		},
	},
}

// snapshot3: staging EKS removed, analytics-db upgraded, Lambda memory bumped, data-exports versioning off.
var snapshot3 = map[string]interface{}{
	"timestamp": "2025-02-22T09:30:00Z",
	"EKS": []interface{}{
		map[string]interface{}{
			"name":               "production",
			"kubernetes_version": "1.33",
			"node_count":         float64(7),
			"endpoint_private":   true,
		},
	},
	"RDS": map[string]interface{}{
		"instances": []interface{}{
			map[string]interface{}{
				"identifier":     "app-db",
				"engine":         "postgres",
				"engine_version": "15.4",
				"instance_class": "db.r6g.2xlarge",
				"storage_gb":     float64(200),
			},
			map[string]interface{}{
				"identifier":     "analytics-db",
				"engine":         "postgres",
				"engine_version": "16.1",
				"instance_class": "db.r6g.xlarge",
				"storage_gb":     float64(500),
			},
		},
	},
	"S3": []interface{}{
		map[string]interface{}{
			"name":       "app-assets",
			"versioning": true,
		},
		map[string]interface{}{
			"name":       "logs-archive",
			"versioning": false,
		},
		map[string]interface{}{
			"name":       "data-exports",
			"versioning": false,
		},
	},
	"Lambda": []interface{}{
		map[string]interface{}{
			"function_name": "api-handler",
			"runtime":       "nodejs20.x",
			"memory_mb":     float64(512),
			"timeout":       float64(60),
		},
	},
}

func TestRealisticDiffTwoSnapshots(t *testing.T) {
	changes := Compare(snapshot1, snapshot2)
	got := changesString(changes)

	want := strings.Join([]string{
		`EKS->production->kubernetes_version: "1.32" -> "1.33"`,
		`EKS->production->node_count: 5 -> 7`,
		`Lambda->api-handler->runtime: "nodejs18.x" -> "nodejs20.x"`,
		`RDS->instances->app-db->instance_class: "db.r6g.xlarge" -> "db.r6g.2xlarge"`,
		`RDS->instances->app-db->storage_gb: 100 -> 200`,
		`S3->data-exports: (added)`,
	}, "\n")

	if got != want {
		t.Errorf("diff snapshot1→2:\n got:\n%s\n\nwant:\n%s", got, want)
	}
}

func TestRealisticDiffThreeSnapshots(t *testing.T) {
	// First window: snapshot1 → snapshot2
	changes1 := Compare(snapshot1, snapshot2)
	got1 := changesString(changes1)

	want1 := strings.Join([]string{
		`EKS->production->kubernetes_version: "1.32" -> "1.33"`,
		`EKS->production->node_count: 5 -> 7`,
		`Lambda->api-handler->runtime: "nodejs18.x" -> "nodejs20.x"`,
		`RDS->instances->app-db->instance_class: "db.r6g.xlarge" -> "db.r6g.2xlarge"`,
		`RDS->instances->app-db->storage_gb: 100 -> 200`,
		`S3->data-exports: (added)`,
	}, "\n")

	if got1 != want1 {
		t.Errorf("window 1 (snapshot1→2):\n got:\n%s\n\nwant:\n%s", got1, want1)
	}

	// Second window: snapshot2 → snapshot3
	changes2 := Compare(snapshot2, snapshot3)
	got2 := changesString(changes2)

	want2 := strings.Join([]string{
		`EKS->staging: (removed)`,
		`Lambda->api-handler->memory_mb: 256 -> 512`,
		`Lambda->api-handler->timeout: 30 -> 60`,
		`RDS->instances->analytics-db->engine_version: "15.4" -> "16.1"`,
		`RDS->instances->analytics-db->instance_class: "db.r6g.large" -> "db.r6g.xlarge"`,
		`S3->data-exports->versioning: true -> false`,
	}, "\n")

	if got2 != want2 {
		t.Errorf("window 2 (snapshot2→3):\n got:\n%s\n\nwant:\n%s", got2, want2)
	}
}
