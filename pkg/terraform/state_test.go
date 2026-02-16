package terraform

import (
	"testing"
)

var testState = []byte(`{
  "version": 4,
  "resources": [
    {
      "mode": "managed",
      "type": "aws_s3_bucket",
      "name": "logs",
      "instances": [
        {
          "attributes": {
            "bucket": "my-logs-bucket",
            "arn": "arn:aws:s3:::my-logs-bucket"
          }
        }
      ]
    },
    {
      "mode": "managed",
      "type": "aws_s3_bucket",
      "name": "data",
      "instances": [
        {
          "attributes": {
            "bucket": "my-data-bucket",
            "arn": "arn:aws:s3:::my-data-bucket"
          }
        }
      ]
    },
    {
      "mode": "managed",
      "type": "aws_s3_directory_bucket",
      "name": "fast",
      "instances": [
        {
          "attributes": {
            "bucket": "my-fast-bucket--usw2-az1--x-s3"
          }
        }
      ]
    },
    {
      "mode": "managed",
      "type": "aws_lambda_function",
      "name": "handler",
      "instances": [
        {
          "attributes": {
            "function_name": "my-handler"
          }
        }
      ]
    }
  ]
}`)

func TestParseState(t *testing.T) {
	state, err := ParseState(testState)
	if err != nil {
		t.Fatalf("ParseState: %v", err)
	}
	if state.Version != 4 {
		t.Errorf("version = %d, want 4", state.Version)
	}
	if len(state.Resources) != 4 {
		t.Errorf("got %d resources, want 4", len(state.Resources))
	}
}

func TestResourcesByType(t *testing.T) {
	state, err := ParseState(testState)
	if err != nil {
		t.Fatalf("ParseState: %v", err)
	}

	buckets := state.ResourcesByType("aws_s3_bucket")
	if len(buckets) != 2 {
		t.Errorf("got %d aws_s3_bucket resources, want 2", len(buckets))
	}

	dirBuckets := state.ResourcesByType("aws_s3_directory_bucket")
	if len(dirBuckets) != 1 {
		t.Errorf("got %d aws_s3_directory_bucket resources, want 1", len(dirBuckets))
	}

	lambdas := state.ResourcesByType("aws_lambda_function")
	if len(lambdas) != 1 {
		t.Errorf("got %d aws_lambda_function resources, want 1", len(lambdas))
	}

	none := state.ResourcesByType("aws_rds_cluster")
	if len(none) != 0 {
		t.Errorf("got %d aws_rds_cluster resources, want 0", len(none))
	}
}

func TestInstanceAttribute(t *testing.T) {
	state, err := ParseState(testState)
	if err != nil {
		t.Fatalf("ParseState: %v", err)
	}

	buckets := state.ResourcesByType("aws_s3_bucket")
	name := InstanceAttribute(buckets[0].Instances[0], "bucket")
	if name != "my-logs-bucket" {
		t.Errorf("bucket = %q, want %q", name, "my-logs-bucket")
	}

	missing := InstanceAttribute(buckets[0].Instances[0], "nonexistent")
	if missing != "" {
		t.Errorf("nonexistent = %q, want empty", missing)
	}
}
