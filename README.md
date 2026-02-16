# aws-snapshot

Capture diffable JSON snapshots of AWS resources. Compare snapshots to detect drift, track changes over time, and cross-check against Terraform state.

## Commands

### snapshot

Take a snapshot of AWS resources. Each resource type is a subcommand.

```bash
aws-snapshot snapshot s3
aws-snapshot snapshot eks rds lambda
aws-snapshot snapshot all -o snapshot.json
aws-snapshot snapshot all -o s3://my-bucket/snapshot.json
```

Supported resource types: `apigateway`, `cloudfront`, `dynamodb`, `ecr`, `ecs`, `eks`, `elasticache`, `elb`, `eventbridge`, `iam`, `lambda`, `msk`, `opensearch`, `rds`, `route53`, `s3`, `sns`, `sqs`, `vpc`, `all`.

### diff

Compare two or more snapshots. Files are sorted by their `timestamp` field and compared pairwise.

```bash
aws-snapshot diff before.json after.json
aws-snapshot diff s3://bucket/snap1.json s3://bucket/snap2.json
```

Output:

```
EKS->production->kubernetes_version: "1.32" -> "1.33"
S3->data-exports: (added)
Lambda->old-function: (removed)
```

### version

Show version information for AWS resources as a table.

```bash
aws-snapshot version eks
aws-snapshot version rds
```

Supported types: `eks`, `rds`, `msk`, `elasticache`, `opensearch`.

### terraform-check

Compare Terraform state with actual AWS resources to find drift.

```bash
aws-snapshot terraform-check --statefile terraform.tfstate s3 rds
aws-snapshot terraform-check --statefile s3://bucket/state.json --statefile other.tfstate all
```

Shows resources that exist in AWS but not in Terraform state, and vice versa.

### terraform-dump

List resources found in Terraform state files.

```bash
aws-snapshot terraform-dump --statefile terraform.tfstate s3 lambda
```

Output: `resource-type: resource-name: state-file-path`

## Global flags

```
--profile string    AWS profile to use
--region string     AWS region to use
-v, --verbose       Print progress messages to stderr
-c, --concurrency   Max parallel resource fetches (default 10)
-o, --outfile       Output file path or s3://bucket/key (default: stdout)
```

## Output format

Snapshots are JSON with arrays sorted for consistent diffs:

```json
{
  "timestamp": "2025-02-20T12:00:00Z",
  "S3": [
    {
      "name": "my-bucket",
      "region": "us-east-1",
      "versioning_state": "enabled"
    }
  ],
  "EKS": [
    {
      "name": "production",
      "version": "1.32"
    }
  ],
  "RDS": {
    "instances": [],
    "clusters": []
  }
}
```

## Installation

```bash
git clone https://github.com/funcan/aws-snapshot.git
cd aws-snapshot
make build
```

## Requirements

- Go 1.23+
- AWS credentials configured (environment, profile, or IAM role)

## License

GPLv2
