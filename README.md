# aws-snapshot

A tool that captures diffable snapshots of AWS resources and their key attributes across an account.

## Purpose

Track changes to your AWS infrastructure over time by generating consistent, diff-friendly snapshots of all resources. Compare snapshots to identify:
- Unintended configuration drift
- Changes made outside of IaC
- Resource additions and deletions

## Usage

```bash
aws-snapshot --profile my-account --output snapshot.json
```

Compare two snapshots:
```bash
diff snapshot-before.json snapshot-after.json
```

## Configuration

Create a `config.yaml` to customize which attributes are captured for each resource type:

```yaml
# config.yaml
resources:
  ec2:
    instance:
      - InstanceId
      - InstanceType
      - State
      - Tags
      - VpcId
      - SubnetId
    security_group:
      - GroupId
      - GroupName
      - Description
      - VpcId
      - IpPermissions
      - IpPermissionsEgress

  s3:
    bucket:
      - Name
      - CreationDate
      - Tags

  lambda:
    function:
      - FunctionName
      - Runtime
      - Handler
      - MemorySize
      - Timeout
      - Environment
      - Tags

  rds:
    instance:
      - DBInstanceIdentifier
      - DBInstanceClass
      - Engine
      - EngineVersion
      - MultiAZ
      - StorageType
      - AllocatedStorage
```

## Output Format

Snapshots are JSON files with resources sorted alphabetically for consistent diffs:

```json
{
  "account_id": "123456789012",
  "region": "us-east-1",
  "timestamp": "2026-02-09T12:00:00Z",
  "resources": {
    "ec2:instance": [
      {
        "InstanceId": "i-0123456789abcdef0",
        "InstanceType": "t3.micro",
        "State": "running"
      }
    ],
    "s3:bucket": [
      {
        "Name": "my-bucket",
        "CreationDate": "2025-01-15T08:30:00Z"
      }
    ]
  }
}
```

## Requirements

- Go 1.21+
- AWS credentials configured (via environment, profile, or IAM role)
- Read permissions for resources you want to snapshot

## Installation

```bash
go install github.com/funcan/aws-snapshot@latest
```

Or build from source:

```bash
git clone https://github.com/funcan/aws-snapshot.git
cd aws-snapshot
go build -o aws-snapshot .
```

## License

GPLv2
