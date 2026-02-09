package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"aws-snapshot/pkg/awsclient"

	"github.com/spf13/cobra"
)

var (
	profile     string
	region      string
	verbose     bool
	concurrency int
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "aws-snapshot",
	Short: "Capture diffable snapshots of AWS resources",
}

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Take a snapshot of AWS resources",
}

var snapshotS3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "Snapshot S3 buckets",
	RunE:  runSnapshotS3,
}

var snapshotEKSCmd = &cobra.Command{
	Use:   "eks",
	Short: "Snapshot EKS clusters",
	RunE:  runSnapshotEKS,
}

var snapshotRDSCmd = &cobra.Command{
	Use:   "rds",
	Short: "Snapshot RDS instances and clusters",
	RunE:  runSnapshotRDS,
}

var snapshotOpenSearchCmd = &cobra.Command{
	Use:   "opensearch",
	Short: "Snapshot OpenSearch domains",
	RunE:  runSnapshotOpenSearch,
}

var snapshotElastiCacheCmd = &cobra.Command{
	Use:   "elasticache",
	Short: "Snapshot ElastiCache clusters and replication groups",
	RunE:  runSnapshotElastiCache,
}

var snapshotDynamoDBCmd = &cobra.Command{
	Use:   "dynamodb",
	Short: "Snapshot DynamoDB tables",
	RunE:  runSnapshotDynamoDB,
}

var snapshotAllCmd = &cobra.Command{
	Use:   "all",
	Short: "Snapshot all supported resources",
	RunE:  runSnapshotAll,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&profile, "profile", "", "AWS profile to use")
	rootCmd.PersistentFlags().StringVar(&region, "region", "", "AWS region to use")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Print progress messages to stderr")
	rootCmd.PersistentFlags().IntVarP(&concurrency, "concurrency", "c", 50, "Maximum number of resources to process in parallel")

	rootCmd.AddCommand(snapshotCmd)
	snapshotCmd.AddCommand(snapshotS3Cmd)
	snapshotCmd.AddCommand(snapshotEKSCmd)
	snapshotCmd.AddCommand(snapshotRDSCmd)
	snapshotCmd.AddCommand(snapshotOpenSearchCmd)
	snapshotCmd.AddCommand(snapshotElastiCacheCmd)
	snapshotCmd.AddCommand(snapshotDynamoDBCmd)
	snapshotCmd.AddCommand(snapshotAllCmd)
}

func buildClient(ctx context.Context) (*awsclient.Client, error) {
	var opts []awsclient.Option
	if profile != "" {
		opts = append(opts, awsclient.WithProfile(profile))
	}
	if region != "" {
		opts = append(opts, awsclient.WithRegion(region))
	}
	return awsclient.New(ctx, opts...)
}

func statusf(format string, args ...any) {
	if verbose {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

// Snapshot represents the top-level output structure.
type Snapshot struct {
	S3          []awsclient.BucketSummary     `json:"S3,omitempty"`
	EKS         []awsclient.ClusterSummary    `json:"EKS,omitempty"`
	RDS         *awsclient.RDSSummary         `json:"RDS,omitempty"`
	OpenSearch  []awsclient.DomainSummary     `json:"OpenSearch,omitempty"`
	ElastiCache *awsclient.ElastiCacheSummary `json:"ElastiCache,omitempty"`
	DynamoDB    []awsclient.TableSummary      `json:"DynamoDB,omitempty"`
}

func outputSnapshot(snap Snapshot) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(snap)
}

func runSnapshotS3(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var s3Opts []awsclient.S3Option
	if verbose {
		s3Opts = append(s3Opts, awsclient.WithStatusFunc(statusf))
	}
	s3Opts = append(s3Opts, awsclient.WithConcurrency(concurrency))
	s3client := client.S3Client(s3Opts...)

	statusf("Fetching S3 buckets...")
	buckets, err := s3client.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing S3 buckets: %w", err)
	}

	// Sort by bucket name for consistent diffs
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	if err := outputSnapshot(Snapshot{S3: buckets}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotEKS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var eksOpts []awsclient.EKSOption
	if verbose {
		eksOpts = append(eksOpts, awsclient.WithEKSStatusFunc(statusf))
	}
	eksOpts = append(eksOpts, awsclient.WithEKSConcurrency(concurrency))
	eksClient := client.EKSClient(eksOpts...)

	statusf("Fetching EKS clusters...")
	clusters, err := eksClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing EKS clusters: %w", err)
	}

	// Sort by cluster name for consistent diffs
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Name < clusters[j].Name
	})

	if err := outputSnapshot(Snapshot{EKS: clusters}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotRDS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var rdsOpts []awsclient.RDSOption
	if verbose {
		rdsOpts = append(rdsOpts, awsclient.WithRDSStatusFunc(statusf))
	}
	rdsOpts = append(rdsOpts, awsclient.WithRDSConcurrency(concurrency))
	rdsClient := client.RDSClient(rdsOpts...)

	statusf("Fetching RDS instances and clusters...")
	rdsSummary, err := rdsClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing RDS resources: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(rdsSummary.Instances, func(i, j int) bool {
		return rdsSummary.Instances[i].Identifier < rdsSummary.Instances[j].Identifier
	})
	sort.Slice(rdsSummary.Clusters, func(i, j int) bool {
		return rdsSummary.Clusters[i].Identifier < rdsSummary.Clusters[j].Identifier
	})

	if err := outputSnapshot(Snapshot{RDS: rdsSummary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotOpenSearch(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var osOpts []awsclient.OpenSearchOption
	if verbose {
		osOpts = append(osOpts, awsclient.WithOpenSearchStatusFunc(statusf))
	}
	osClient := client.OpenSearchClient(osOpts...)

	statusf("Fetching OpenSearch domains...")
	domains, err := osClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing OpenSearch domains: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(domains, func(i, j int) bool {
		return domains[i].DomainName < domains[j].DomainName
	})

	if err := outputSnapshot(Snapshot{OpenSearch: domains}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotElastiCache(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ecOpts []awsclient.ElastiCacheOption
	if verbose {
		ecOpts = append(ecOpts, awsclient.WithElastiCacheStatusFunc(statusf))
	}
	ecClient := client.ElastiCacheClient(ecOpts...)

	statusf("Fetching ElastiCache resources...")
	ecSummary, err := ecClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing ElastiCache resources: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(ecSummary.CacheClusters, func(i, j int) bool {
		return ecSummary.CacheClusters[i].CacheClusterId < ecSummary.CacheClusters[j].CacheClusterId
	})
	sort.Slice(ecSummary.ReplicationGroups, func(i, j int) bool {
		return ecSummary.ReplicationGroups[i].ReplicationGroupId < ecSummary.ReplicationGroups[j].ReplicationGroupId
	})

	if err := outputSnapshot(Snapshot{ElastiCache: ecSummary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotDynamoDB(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ddbOpts []awsclient.DynamoDBOption
	if verbose {
		ddbOpts = append(ddbOpts, awsclient.WithDynamoDBStatusFunc(statusf))
	}
	ddbOpts = append(ddbOpts, awsclient.WithDynamoDBConcurrency(concurrency))
	ddbClient := client.DynamoDBClient(ddbOpts...)

	statusf("Fetching DynamoDB tables...")
	tables, err := ddbClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing DynamoDB tables: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TableName < tables[j].TableName
	})

	if err := outputSnapshot(Snapshot{DynamoDB: tables}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotAll(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var snap Snapshot

	// Fetch S3 buckets
	var s3Opts []awsclient.S3Option
	if verbose {
		s3Opts = append(s3Opts, awsclient.WithStatusFunc(statusf))
	}
	s3Opts = append(s3Opts, awsclient.WithConcurrency(concurrency))
	s3client := client.S3Client(s3Opts...)

	statusf("Fetching S3 buckets...")
	buckets, err := s3client.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing S3 buckets: %w", err)
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})
	snap.S3 = buckets

	// Fetch EKS clusters
	var eksOpts []awsclient.EKSOption
	if verbose {
		eksOpts = append(eksOpts, awsclient.WithEKSStatusFunc(statusf))
	}
	eksOpts = append(eksOpts, awsclient.WithEKSConcurrency(concurrency))
	eksClient := client.EKSClient(eksOpts...)

	statusf("Fetching EKS clusters...")
	clusters, err := eksClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing EKS clusters: %w", err)
	}

	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Name < clusters[j].Name
	})
	snap.EKS = clusters

	// Fetch RDS instances and clusters
	var rdsOpts []awsclient.RDSOption
	if verbose {
		rdsOpts = append(rdsOpts, awsclient.WithRDSStatusFunc(statusf))
	}
	rdsOpts = append(rdsOpts, awsclient.WithRDSConcurrency(concurrency))
	rdsClient := client.RDSClient(rdsOpts...)

	statusf("Fetching RDS instances and clusters...")
	rdsSummary, err := rdsClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing RDS resources: %w", err)
	}

	sort.Slice(rdsSummary.Instances, func(i, j int) bool {
		return rdsSummary.Instances[i].Identifier < rdsSummary.Instances[j].Identifier
	})
	sort.Slice(rdsSummary.Clusters, func(i, j int) bool {
		return rdsSummary.Clusters[i].Identifier < rdsSummary.Clusters[j].Identifier
	})
	snap.RDS = rdsSummary

	// Fetch OpenSearch domains
	var osOpts []awsclient.OpenSearchOption
	if verbose {
		osOpts = append(osOpts, awsclient.WithOpenSearchStatusFunc(statusf))
	}
	osClient := client.OpenSearchClient(osOpts...)

	statusf("Fetching OpenSearch domains...")
	domains, err := osClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing OpenSearch domains: %w", err)
	}

	sort.Slice(domains, func(i, j int) bool {
		return domains[i].DomainName < domains[j].DomainName
	})
	snap.OpenSearch = domains

	// Fetch ElastiCache clusters and replication groups
	var ecOpts []awsclient.ElastiCacheOption
	if verbose {
		ecOpts = append(ecOpts, awsclient.WithElastiCacheStatusFunc(statusf))
	}
	ecClient := client.ElastiCacheClient(ecOpts...)

	statusf("Fetching ElastiCache resources...")
	ecSummary, err := ecClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing ElastiCache resources: %w", err)
	}

	sort.Slice(ecSummary.CacheClusters, func(i, j int) bool {
		return ecSummary.CacheClusters[i].CacheClusterId < ecSummary.CacheClusters[j].CacheClusterId
	})
	sort.Slice(ecSummary.ReplicationGroups, func(i, j int) bool {
		return ecSummary.ReplicationGroups[i].ReplicationGroupId < ecSummary.ReplicationGroups[j].ReplicationGroupId
	})
	snap.ElastiCache = ecSummary

	// Fetch DynamoDB tables
	var ddbOpts []awsclient.DynamoDBOption
	if verbose {
		ddbOpts = append(ddbOpts, awsclient.WithDynamoDBStatusFunc(statusf))
	}
	ddbOpts = append(ddbOpts, awsclient.WithDynamoDBConcurrency(concurrency))
	ddbClient := client.DynamoDBClient(ddbOpts...)

	statusf("Fetching DynamoDB tables...")
	tables, err := ddbClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing DynamoDB tables: %w", err)
	}

	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TableName < tables[j].TableName
	})
	snap.DynamoDB = tables

	if err := outputSnapshot(snap); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}
