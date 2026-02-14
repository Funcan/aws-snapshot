// Package cmd contains the CLI command definitions for aws-snapshot.
package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"aws-snapshot/pkg/awsclient"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
)

var (
	Profile     string
	Region      string
	Verbose     bool
	Concurrency int
	Outfile     string
)

// RootCmd is the top-level command.
var RootCmd = &cobra.Command{
	Use:   "aws-snapshot",
	Short: "Capture diffable snapshots of AWS resources",
}

// SetVersion sets the version string on the root command.
func SetVersion(v string) {
	RootCmd.Version = v
}

func init() {
	RootCmd.PersistentFlags().StringVar(&Profile, "profile", "", "AWS profile to use")
	RootCmd.PersistentFlags().StringVar(&Region, "region", "", "AWS region to use")
	RootCmd.PersistentFlags().BoolVarP(&Verbose, "verbose", "v", false, "Print progress messages to stderr")
	RootCmd.PersistentFlags().IntVarP(&Concurrency, "concurrency", "c", 10, "Maximum number of resources to process in parallel")
	RootCmd.PersistentFlags().StringVarP(&Outfile, "outfile", "o", "", "Output file path or s3://bucket/key URL (default: stdout)")

	RootCmd.AddCommand(snapshotCmd)
	RootCmd.AddCommand(diffCmd)
	RootCmd.AddCommand(versionCmd)
}

// BuildClient creates an AWS client using the global flags.
func BuildClient(ctx context.Context) (*awsclient.Client, error) {
	var opts []awsclient.Option
	if Profile != "" {
		opts = append(opts, awsclient.WithProfile(Profile))
	}
	if Region != "" {
		opts = append(opts, awsclient.WithRegion(Region))
	}
	return awsclient.New(ctx, opts...)
}

// Statusf prints a status message to stderr when verbose mode is enabled.
func Statusf(format string, args ...any) {
	if Verbose {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

// Snapshot represents the top-level output structure.
type Snapshot struct {
	Timestamp   string                          `json:"timestamp"`
	S3          []awsclient.BucketSummary       `json:"S3,omitempty"`
	EKS         []awsclient.ClusterSummary      `json:"EKS,omitempty"`
	RDS         *awsclient.RDSSummary           `json:"RDS,omitempty"`
	OpenSearch  []awsclient.DomainSummary       `json:"OpenSearch,omitempty"`
	ElastiCache *awsclient.ElastiCacheSummary   `json:"ElastiCache,omitempty"`
	DynamoDB    []awsclient.TableSummary        `json:"DynamoDB,omitempty"`
	Lambda      []awsclient.FunctionSummary     `json:"Lambda,omitempty"`
	ECR         []awsclient.RepositorySummary   `json:"ECR,omitempty"`
	ECS         *awsclient.ECSSummary           `json:"ECS,omitempty"`
	ELB         *awsclient.ELBSummary           `json:"ELB,omitempty"`
	Route53     []awsclient.HostedZoneSummary   `json:"Route53,omitempty"`
	MSK         []awsclient.MSKClusterSummary   `json:"MSK,omitempty"`
	VPC         []awsclient.VPCSummary          `json:"VPC,omitempty"`
	CloudFront  []awsclient.DistributionSummary `json:"CloudFront,omitempty"`
	APIGateway  *awsclient.APIGatewaySummary    `json:"APIGateway,omitempty"`
	SQS         []awsclient.QueueSummary        `json:"SQS,omitempty"`
	SNS         []awsclient.TopicSummary        `json:"SNS,omitempty"`
	EventBridge *awsclient.EventBridgeSummary   `json:"EventBridge,omitempty"`
	IAM         *awsclient.IAMSummary           `json:"IAM,omitempty"`
}

// OutputSnapshot encodes and writes a Snapshot as sorted JSON.
func OutputSnapshot(snap Snapshot) error {
	snap.Timestamp = time.Now().UTC().Format(time.RFC3339)

	// Encode to JSON
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(snap); err != nil {
		return err
	}

	// Sort all arrays in the JSON for diffability
	sortedJSON, err := sortJSONArrays(buf.Bytes())
	if err != nil {
		return err
	}

	// Default to stdout
	if Outfile == "" {
		_, err := os.Stdout.Write(sortedJSON)
		return err
	}

	// Check for S3 URL
	if strings.HasPrefix(Outfile, "s3://") {
		return writeToS3(sortedJSON)
	}

	// Write to local file
	return os.WriteFile(Outfile, sortedJSON, 0644)
}

// sortJSONArrays recursively sorts all arrays in the JSON for consistent diffable output.
func sortJSONArrays(data []byte) ([]byte, error) {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, err
	}

	sortValue(v)

	// Re-encode with indentation
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// sortValue recursively sorts all arrays in an interface{} value.
func sortValue(v interface{}) {
	switch val := v.(type) {
	case map[string]interface{}:
		for _, value := range val {
			sortValue(value)
		}
	case []interface{}:
		// First, recursively sort nested structures
		for _, item := range val {
			sortValue(item)
		}
		// Then sort this array by JSON representation
		sort.Slice(val, func(i, j int) bool {
			return jsonString(val[i]) < jsonString(val[j])
		})
	}
}

// jsonString returns the JSON representation of a value for sorting.
func jsonString(v interface{}) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func writeToS3(data []byte) error {
	ctx := context.Background()

	// Parse s3://bucket/key
	path := strings.TrimPrefix(Outfile, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("invalid S3 URL: %s (expected s3://bucket/key)", Outfile)
	}
	bucket := parts[0]
	key := parts[1]

	Statusf("Uploading to s3://%s/%s...", bucket, key)

	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	s3Client := s3.NewFromConfig(client.Config())
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return fmt.Errorf("uploading to S3: %w", err)
	}

	Statusf("Successfully uploaded to s3://%s/%s", bucket, key)
	return nil
}
