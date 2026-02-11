package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"text/tabwriter"

	"aws-snapshot/pkg/awsclient"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/spf13/cobra"
)

var (
	version = "dev"

	profile     string
	region      string
	verbose     bool
	concurrency int
	outfile     string
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:     "aws-snapshot",
	Short:   "Capture diffable snapshots of AWS resources",
	Version: version,
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

var snapshotLambdaCmd = &cobra.Command{
	Use:   "lambda",
	Short: "Snapshot Lambda functions",
	RunE:  runSnapshotLambda,
}

var snapshotECRCmd = &cobra.Command{
	Use:   "ecr",
	Short: "Snapshot ECR repositories",
	RunE:  runSnapshotECR,
}

var snapshotECSCmd = &cobra.Command{
	Use:   "ecs",
	Short: "Snapshot ECS clusters and services",
	RunE:  runSnapshotECS,
}

var snapshotELBCmd = &cobra.Command{
	Use:   "elb",
	Short: "Snapshot load balancers and target groups",
	RunE:  runSnapshotELB,
}

var snapshotRoute53Cmd = &cobra.Command{
	Use:   "route53",
	Short: "Snapshot Route53 hosted zones and records",
	RunE:  runSnapshotRoute53,
}

var snapshotMSKCmd = &cobra.Command{
	Use:   "msk",
	Short: "Snapshot MSK clusters",
	RunE:  runSnapshotMSK,
}

var snapshotVPCCmd = &cobra.Command{
	Use:   "vpc",
	Short: "Snapshot VPCs and networking resources",
	RunE:  runSnapshotVPC,
}

var snapshotCloudFrontCmd = &cobra.Command{
	Use:   "cloudfront",
	Short: "Snapshot CloudFront distributions",
	RunE:  runSnapshotCloudFront,
}

var snapshotAPIGatewayCmd = &cobra.Command{
	Use:   "apigateway",
	Short: "Snapshot API Gateway REST and HTTP APIs",
	RunE:  runSnapshotAPIGateway,
}

var snapshotSQSCmd = &cobra.Command{
	Use:   "sqs",
	Short: "Snapshot SQS queues",
	RunE:  runSnapshotSQS,
}

var snapshotSNSCmd = &cobra.Command{
	Use:   "sns",
	Short: "Snapshot SNS topics",
	RunE:  runSnapshotSNS,
}

var snapshotEventBridgeCmd = &cobra.Command{
	Use:   "eventbridge",
	Short: "Snapshot EventBridge event buses and rules",
	RunE:  runSnapshotEventBridge,
}

var snapshotIAMCmd = &cobra.Command{
	Use:   "iam",
	Short: "Snapshot IAM users, groups, roles, and policies",
	RunE:  runSnapshotIAM,
}

var snapshotAllCmd = &cobra.Command{
	Use:   "all",
	Short: "Snapshot all supported resources",
	RunE:  runSnapshotAll,
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version information for AWS resources",
}

var versionEKSCmd = &cobra.Command{
	Use:   "eks",
	Short: "Show EKS cluster and node versions",
	RunE:  runVersionEKS,
}

var versionRDSCmd = &cobra.Command{
	Use:   "rds",
	Short: "Show RDS instance and cluster engine versions",
	RunE:  runVersionRDS,
}

var versionMSKCmd = &cobra.Command{
	Use:   "msk",
	Short: "Show MSK cluster Kafka versions",
	RunE:  runVersionMSK,
}

var versionElastiCacheCmd = &cobra.Command{
	Use:   "elasticache",
	Short: "Show ElastiCache cluster and replication group versions",
	RunE:  runVersionElastiCache,
}

var versionOpenSearchCmd = &cobra.Command{
	Use:   "opensearch",
	Short: "Show OpenSearch domain versions",
	RunE:  runVersionOpenSearch,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&profile, "profile", "", "AWS profile to use")
	rootCmd.PersistentFlags().StringVar(&region, "region", "", "AWS region to use")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Print progress messages to stderr")
	rootCmd.PersistentFlags().IntVarP(&concurrency, "concurrency", "c", 10, "Maximum number of resources to process in parallel")
	rootCmd.PersistentFlags().StringVarP(&outfile, "outfile", "o", "", "Output file path or s3://bucket/key URL (default: stdout)")

	rootCmd.AddCommand(snapshotCmd)
	rootCmd.AddCommand(versionCmd)
	snapshotCmd.AddCommand(snapshotS3Cmd)
	snapshotCmd.AddCommand(snapshotEKSCmd)
	snapshotCmd.AddCommand(snapshotRDSCmd)
	snapshotCmd.AddCommand(snapshotOpenSearchCmd)
	snapshotCmd.AddCommand(snapshotElastiCacheCmd)
	snapshotCmd.AddCommand(snapshotDynamoDBCmd)
	snapshotCmd.AddCommand(snapshotLambdaCmd)
	snapshotCmd.AddCommand(snapshotECRCmd)
	snapshotCmd.AddCommand(snapshotECSCmd)
	snapshotCmd.AddCommand(snapshotELBCmd)
	snapshotCmd.AddCommand(snapshotRoute53Cmd)
	snapshotCmd.AddCommand(snapshotMSKCmd)
	snapshotCmd.AddCommand(snapshotVPCCmd)
	snapshotCmd.AddCommand(snapshotCloudFrontCmd)
	snapshotCmd.AddCommand(snapshotAPIGatewayCmd)
	snapshotCmd.AddCommand(snapshotSQSCmd)
	snapshotCmd.AddCommand(snapshotSNSCmd)
	snapshotCmd.AddCommand(snapshotEventBridgeCmd)
	snapshotCmd.AddCommand(snapshotIAMCmd)
	snapshotCmd.AddCommand(snapshotAllCmd)
	versionCmd.AddCommand(versionEKSCmd)
	versionCmd.AddCommand(versionRDSCmd)
	versionCmd.AddCommand(versionMSKCmd)
	versionCmd.AddCommand(versionElastiCacheCmd)
	versionCmd.AddCommand(versionOpenSearchCmd)
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

func outputSnapshot(snap Snapshot) error {
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
	if outfile == "" {
		_, err := os.Stdout.Write(sortedJSON)
		return err
	}

	// Check for S3 URL
	if strings.HasPrefix(outfile, "s3://") {
		return writeToS3(sortedJSON)
	}

	// Write to local file
	return os.WriteFile(outfile, sortedJSON, 0644)
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
	path := strings.TrimPrefix(outfile, "s3://")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("invalid S3 URL: %s (expected s3://bucket/key)", outfile)
	}
	bucket := parts[0]
	key := parts[1]

	statusf("Uploading to s3://%s/%s...", bucket, key)

	client, err := buildClient(ctx)
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

	statusf("Successfully uploaded to s3://%s/%s", bucket, key)
	return nil
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

func runSnapshotLambda(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var lambdaOpts []awsclient.LambdaOption
	if verbose {
		lambdaOpts = append(lambdaOpts, awsclient.WithLambdaStatusFunc(statusf))
	}
	lambdaOpts = append(lambdaOpts, awsclient.WithLambdaConcurrency(concurrency))
	lambdaClient := client.LambdaClient(lambdaOpts...)

	statusf("Fetching Lambda functions...")
	functions, err := lambdaClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing Lambda functions: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(functions, func(i, j int) bool {
		return functions[i].FunctionName < functions[j].FunctionName
	})

	if err := outputSnapshot(Snapshot{Lambda: functions}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotECR(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ecrOpts []awsclient.ECROption
	if verbose {
		ecrOpts = append(ecrOpts, awsclient.WithECRStatusFunc(statusf))
	}
	ecrOpts = append(ecrOpts, awsclient.WithECRConcurrency(concurrency))
	ecrClient := client.ECRClient(ecrOpts...)

	statusf("Fetching ECR repositories...")
	repos, err := ecrClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing ECR repositories: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(repos, func(i, j int) bool {
		return repos[i].RepositoryName < repos[j].RepositoryName
	})

	if err := outputSnapshot(Snapshot{ECR: repos}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotECS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ecsOpts []awsclient.ECSOption
	if verbose {
		ecsOpts = append(ecsOpts, awsclient.WithECSStatusFunc(statusf))
	}
	ecsOpts = append(ecsOpts, awsclient.WithECSConcurrency(concurrency))
	ecsClient := client.ECSClient(ecsOpts...)

	statusf("Fetching ECS clusters and services...")
	ecsSummary, err := ecsClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing ECS resources: %w", err)
	}

	// Sort clusters for consistent diffs
	sort.Slice(ecsSummary.Clusters, func(i, j int) bool {
		return ecsSummary.Clusters[i].ClusterName < ecsSummary.Clusters[j].ClusterName
	})
	// Sort services within each cluster
	for i := range ecsSummary.Clusters {
		sort.Slice(ecsSummary.Clusters[i].Services, func(a, b int) bool {
			return ecsSummary.Clusters[i].Services[a].ServiceName < ecsSummary.Clusters[i].Services[b].ServiceName
		})
	}

	if err := outputSnapshot(Snapshot{ECS: ecsSummary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotELB(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var elbOpts []awsclient.ELBOption
	if verbose {
		elbOpts = append(elbOpts, awsclient.WithELBStatusFunc(statusf))
	}
	elbOpts = append(elbOpts, awsclient.WithELBConcurrency(concurrency))
	elbClient := client.ELBClient(elbOpts...)

	statusf("Fetching load balancers and target groups...")
	elbSummary, err := elbClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing ELB resources: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(elbSummary.LoadBalancers, func(i, j int) bool {
		return elbSummary.LoadBalancers[i].LoadBalancerName < elbSummary.LoadBalancers[j].LoadBalancerName
	})
	sort.Slice(elbSummary.TargetGroups, func(i, j int) bool {
		return elbSummary.TargetGroups[i].TargetGroupName < elbSummary.TargetGroups[j].TargetGroupName
	})

	if err := outputSnapshot(Snapshot{ELB: elbSummary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotRoute53(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var r53Opts []awsclient.Route53Option
	if verbose {
		r53Opts = append(r53Opts, awsclient.WithRoute53StatusFunc(statusf))
	}
	r53Opts = append(r53Opts, awsclient.WithRoute53Concurrency(concurrency))
	r53Client := client.Route53Client(r53Opts...)

	statusf("Fetching Route53 hosted zones...")
	zones, err := r53Client.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing Route53 zones: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(zones, func(i, j int) bool {
		return zones[i].Name < zones[j].Name
	})
	// Sort record sets within each zone
	for i := range zones {
		sort.Slice(zones[i].RecordSets, func(a, b int) bool {
			if zones[i].RecordSets[a].Name != zones[i].RecordSets[b].Name {
				return zones[i].RecordSets[a].Name < zones[i].RecordSets[b].Name
			}
			return zones[i].RecordSets[a].Type < zones[i].RecordSets[b].Type
		})
	}

	if err := outputSnapshot(Snapshot{Route53: zones}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotMSK(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var mskOpts []awsclient.MSKOption
	if verbose {
		mskOpts = append(mskOpts, awsclient.WithMSKStatusFunc(statusf))
	}
	mskOpts = append(mskOpts, awsclient.WithMSKConcurrency(concurrency))
	mskClient := client.MSKClient(mskOpts...)

	statusf("Fetching MSK clusters...")
	clusters, err := mskClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing MSK clusters: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].ClusterName < clusters[j].ClusterName
	})

	if err := outputSnapshot(Snapshot{MSK: clusters}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotVPC(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var vpcOpts []awsclient.VPCOption
	if verbose {
		vpcOpts = append(vpcOpts, awsclient.WithVPCStatusFunc(statusf))
	}
	vpcOpts = append(vpcOpts, awsclient.WithVPCConcurrency(concurrency))
	vpcClient := client.VPCClient(vpcOpts...)

	statusf("Fetching VPCs...")
	vpcs, err := vpcClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing VPCs: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(vpcs, func(i, j int) bool {
		return vpcs[i].VpcId < vpcs[j].VpcId
	})
	// Sort subnets, security groups, etc. within each VPC
	for i := range vpcs {
		sort.Slice(vpcs[i].Subnets, func(a, b int) bool {
			return vpcs[i].Subnets[a].SubnetId < vpcs[i].Subnets[b].SubnetId
		})
		sort.Slice(vpcs[i].SecurityGroups, func(a, b int) bool {
			return vpcs[i].SecurityGroups[a].GroupId < vpcs[i].SecurityGroups[b].GroupId
		})
		sort.Slice(vpcs[i].RouteTables, func(a, b int) bool {
			return vpcs[i].RouteTables[a].RouteTableId < vpcs[i].RouteTables[b].RouteTableId
		})
	}

	if err := outputSnapshot(Snapshot{VPC: vpcs}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotCloudFront(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var cfOpts []awsclient.CloudFrontOption
	if verbose {
		cfOpts = append(cfOpts, awsclient.WithCloudFrontStatusFunc(statusf))
	}
	cfOpts = append(cfOpts, awsclient.WithCloudFrontConcurrency(concurrency))
	cfClient := client.CloudFrontClient(cfOpts...)

	statusf("Fetching CloudFront distributions...")
	distributions, err := cfClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing CloudFront distributions: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(distributions, func(i, j int) bool {
		return distributions[i].Id < distributions[j].Id
	})

	if err := outputSnapshot(Snapshot{CloudFront: distributions}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotAPIGateway(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var apigwOpts []awsclient.APIGatewayOption
	if verbose {
		apigwOpts = append(apigwOpts, awsclient.WithAPIGatewayStatusFunc(statusf))
	}
	apigwOpts = append(apigwOpts, awsclient.WithAPIGatewayConcurrency(concurrency))
	apigwClient := client.APIGatewayClient(apigwOpts...)

	statusf("Fetching API Gateway APIs...")
	summary, err := apigwClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing API Gateway APIs: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(summary.RestAPIs, func(i, j int) bool {
		return summary.RestAPIs[i].Name < summary.RestAPIs[j].Name
	})
	sort.Slice(summary.HttpAPIs, func(i, j int) bool {
		return summary.HttpAPIs[i].Name < summary.HttpAPIs[j].Name
	})

	if err := outputSnapshot(Snapshot{APIGateway: summary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotSQS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var sqsOpts []awsclient.SQSOption
	if verbose {
		sqsOpts = append(sqsOpts, awsclient.WithSQSStatusFunc(statusf))
	}
	sqsOpts = append(sqsOpts, awsclient.WithSQSConcurrency(concurrency))
	sqsClient := client.SQSClient(sqsOpts...)

	statusf("Fetching SQS queues...")
	queues, err := sqsClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing SQS queues: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(queues, func(i, j int) bool {
		return queues[i].Name < queues[j].Name
	})

	if err := outputSnapshot(Snapshot{SQS: queues}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotSNS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var snsOpts []awsclient.SNSOption
	if verbose {
		snsOpts = append(snsOpts, awsclient.WithSNSStatusFunc(statusf))
	}
	snsOpts = append(snsOpts, awsclient.WithSNSConcurrency(concurrency))
	snsClient := client.SNSClient(snsOpts...)

	statusf("Fetching SNS topics...")
	topics, err := snsClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing SNS topics: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})

	if err := outputSnapshot(Snapshot{SNS: topics}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotEventBridge(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ebOpts []awsclient.EventBridgeOption
	if verbose {
		ebOpts = append(ebOpts, awsclient.WithEventBridgeStatusFunc(statusf))
	}
	ebOpts = append(ebOpts, awsclient.WithEventBridgeConcurrency(concurrency))
	ebClient := client.EventBridgeClient(ebOpts...)

	statusf("Fetching EventBridge event buses...")
	summary, err := ebClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing EventBridge event buses: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(summary.EventBuses, func(i, j int) bool {
		return summary.EventBuses[i].Name < summary.EventBuses[j].Name
	})
	for idx := range summary.EventBuses {
		sort.Slice(summary.EventBuses[idx].Rules, func(i, j int) bool {
			return summary.EventBuses[idx].Rules[i].Name < summary.EventBuses[idx].Rules[j].Name
		})
	}

	if err := outputSnapshot(Snapshot{EventBridge: summary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotIAM(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var iamOpts []awsclient.IAMOption
	if verbose {
		iamOpts = append(iamOpts, awsclient.WithIAMStatusFunc(statusf))
	}
	iamOpts = append(iamOpts, awsclient.WithIAMConcurrency(concurrency))
	iamClient := client.IAMClient(iamOpts...)

	statusf("Fetching IAM resources...")
	summary, err := iamClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing IAM resources: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(summary.Users, func(i, j int) bool {
		return summary.Users[i].Name < summary.Users[j].Name
	})
	sort.Slice(summary.Groups, func(i, j int) bool {
		return summary.Groups[i].Name < summary.Groups[j].Name
	})
	sort.Slice(summary.Roles, func(i, j int) bool {
		return summary.Roles[i].Name < summary.Roles[j].Name
	})
	sort.Slice(summary.Policies, func(i, j int) bool {
		return summary.Policies[i].Name < summary.Policies[j].Name
	})

	if err := outputSnapshot(Snapshot{IAM: summary}); err != nil {
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
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, 20)

	// S3
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.S3Option
		if verbose {
			opts = append(opts, awsclient.WithStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithConcurrency(concurrency))
		c := client.S3Client(opts...)

		statusf("Fetching S3 buckets...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing S3 buckets: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].Name < result[j].Name
		})
		mu.Lock()
		snap.S3 = result
		mu.Unlock()
	}()

	// EKS
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.EKSOption
		if verbose {
			opts = append(opts, awsclient.WithEKSStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithEKSConcurrency(concurrency))
		c := client.EKSClient(opts...)

		statusf("Fetching EKS clusters...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing EKS clusters: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].Name < result[j].Name
		})
		mu.Lock()
		snap.EKS = result
		mu.Unlock()
	}()

	// RDS
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.RDSOption
		if verbose {
			opts = append(opts, awsclient.WithRDSStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithRDSConcurrency(concurrency))
		c := client.RDSClient(opts...)

		statusf("Fetching RDS instances and clusters...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing RDS resources: %w", err)
			return
		}
		sort.Slice(result.Instances, func(i, j int) bool {
			return result.Instances[i].Identifier < result.Instances[j].Identifier
		})
		sort.Slice(result.Clusters, func(i, j int) bool {
			return result.Clusters[i].Identifier < result.Clusters[j].Identifier
		})
		mu.Lock()
		snap.RDS = result
		mu.Unlock()
	}()

	// OpenSearch
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.OpenSearchOption
		if verbose {
			opts = append(opts, awsclient.WithOpenSearchStatusFunc(statusf))
		}
		c := client.OpenSearchClient(opts...)

		statusf("Fetching OpenSearch domains...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing OpenSearch domains: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].DomainName < result[j].DomainName
		})
		mu.Lock()
		snap.OpenSearch = result
		mu.Unlock()
	}()

	// ElastiCache
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.ElastiCacheOption
		if verbose {
			opts = append(opts, awsclient.WithElastiCacheStatusFunc(statusf))
		}
		c := client.ElastiCacheClient(opts...)

		statusf("Fetching ElastiCache resources...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing ElastiCache resources: %w", err)
			return
		}
		sort.Slice(result.CacheClusters, func(i, j int) bool {
			return result.CacheClusters[i].CacheClusterId < result.CacheClusters[j].CacheClusterId
		})
		sort.Slice(result.ReplicationGroups, func(i, j int) bool {
			return result.ReplicationGroups[i].ReplicationGroupId < result.ReplicationGroups[j].ReplicationGroupId
		})
		mu.Lock()
		snap.ElastiCache = result
		mu.Unlock()
	}()

	// DynamoDB
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.DynamoDBOption
		if verbose {
			opts = append(opts, awsclient.WithDynamoDBStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithDynamoDBConcurrency(concurrency))
		c := client.DynamoDBClient(opts...)

		statusf("Fetching DynamoDB tables...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing DynamoDB tables: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].TableName < result[j].TableName
		})
		mu.Lock()
		snap.DynamoDB = result
		mu.Unlock()
	}()

	// Lambda
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.LambdaOption
		if verbose {
			opts = append(opts, awsclient.WithLambdaStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithLambdaConcurrency(concurrency))
		c := client.LambdaClient(opts...)

		statusf("Fetching Lambda functions...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing Lambda functions: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].FunctionName < result[j].FunctionName
		})
		mu.Lock()
		snap.Lambda = result
		mu.Unlock()
	}()

	// ECR
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.ECROption
		if verbose {
			opts = append(opts, awsclient.WithECRStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithECRConcurrency(concurrency))
		c := client.ECRClient(opts...)

		statusf("Fetching ECR repositories...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing ECR repositories: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].RepositoryName < result[j].RepositoryName
		})
		mu.Lock()
		snap.ECR = result
		mu.Unlock()
	}()

	// ECS
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.ECSOption
		if verbose {
			opts = append(opts, awsclient.WithECSStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithECSConcurrency(concurrency))
		c := client.ECSClient(opts...)

		statusf("Fetching ECS clusters and services...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing ECS resources: %w", err)
			return
		}
		sort.Slice(result.Clusters, func(i, j int) bool {
			return result.Clusters[i].ClusterName < result.Clusters[j].ClusterName
		})
		for i := range result.Clusters {
			sort.Slice(result.Clusters[i].Services, func(a, b int) bool {
				return result.Clusters[i].Services[a].ServiceName < result.Clusters[i].Services[b].ServiceName
			})
		}
		mu.Lock()
		snap.ECS = result
		mu.Unlock()
	}()

	// ELB
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.ELBOption
		if verbose {
			opts = append(opts, awsclient.WithELBStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithELBConcurrency(concurrency))
		c := client.ELBClient(opts...)

		statusf("Fetching load balancers and target groups...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing ELB resources: %w", err)
			return
		}
		sort.Slice(result.LoadBalancers, func(i, j int) bool {
			return result.LoadBalancers[i].LoadBalancerName < result.LoadBalancers[j].LoadBalancerName
		})
		sort.Slice(result.TargetGroups, func(i, j int) bool {
			return result.TargetGroups[i].TargetGroupName < result.TargetGroups[j].TargetGroupName
		})
		mu.Lock()
		snap.ELB = result
		mu.Unlock()
	}()

	// Route53
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.Route53Option
		if verbose {
			opts = append(opts, awsclient.WithRoute53StatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithRoute53Concurrency(concurrency))
		c := client.Route53Client(opts...)

		statusf("Fetching Route53 hosted zones...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing Route53 zones: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].Name < result[j].Name
		})
		for i := range result {
			sort.Slice(result[i].RecordSets, func(a, b int) bool {
				if result[i].RecordSets[a].Name != result[i].RecordSets[b].Name {
					return result[i].RecordSets[a].Name < result[i].RecordSets[b].Name
				}
				return result[i].RecordSets[a].Type < result[i].RecordSets[b].Type
			})
		}
		mu.Lock()
		snap.Route53 = result
		mu.Unlock()
	}()

	// MSK
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.MSKOption
		if verbose {
			opts = append(opts, awsclient.WithMSKStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithMSKConcurrency(concurrency))
		c := client.MSKClient(opts...)

		statusf("Fetching MSK clusters...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing MSK clusters: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].ClusterName < result[j].ClusterName
		})
		mu.Lock()
		snap.MSK = result
		mu.Unlock()
	}()

	// VPC
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.VPCOption
		if verbose {
			opts = append(opts, awsclient.WithVPCStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithVPCConcurrency(concurrency))
		c := client.VPCClient(opts...)

		statusf("Fetching VPCs...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing VPCs: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].VpcId < result[j].VpcId
		})
		for i := range result {
			sort.Slice(result[i].Subnets, func(a, b int) bool {
				return result[i].Subnets[a].SubnetId < result[i].Subnets[b].SubnetId
			})
			sort.Slice(result[i].SecurityGroups, func(a, b int) bool {
				return result[i].SecurityGroups[a].GroupId < result[i].SecurityGroups[b].GroupId
			})
			sort.Slice(result[i].RouteTables, func(a, b int) bool {
				return result[i].RouteTables[a].RouteTableId < result[i].RouteTables[b].RouteTableId
			})
		}
		mu.Lock()
		snap.VPC = result
		mu.Unlock()
	}()

	// CloudFront
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.CloudFrontOption
		if verbose {
			opts = append(opts, awsclient.WithCloudFrontStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithCloudFrontConcurrency(concurrency))
		c := client.CloudFrontClient(opts...)

		statusf("Fetching CloudFront distributions...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing CloudFront distributions: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].Id < result[j].Id
		})
		mu.Lock()
		snap.CloudFront = result
		mu.Unlock()
	}()

	// API Gateway
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.APIGatewayOption
		if verbose {
			opts = append(opts, awsclient.WithAPIGatewayStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithAPIGatewayConcurrency(concurrency))
		c := client.APIGatewayClient(opts...)

		statusf("Fetching API Gateway APIs...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing API Gateway APIs: %w", err)
			return
		}
		sort.Slice(result.RestAPIs, func(i, j int) bool {
			return result.RestAPIs[i].Name < result.RestAPIs[j].Name
		})
		sort.Slice(result.HttpAPIs, func(i, j int) bool {
			return result.HttpAPIs[i].Name < result.HttpAPIs[j].Name
		})
		mu.Lock()
		snap.APIGateway = result
		mu.Unlock()
	}()

	// SQS
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.SQSOption
		if verbose {
			opts = append(opts, awsclient.WithSQSStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithSQSConcurrency(concurrency))
		c := client.SQSClient(opts...)

		statusf("Fetching SQS queues...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing SQS queues: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].Name < result[j].Name
		})
		mu.Lock()
		snap.SQS = result
		mu.Unlock()
	}()

	// SNS
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.SNSOption
		if verbose {
			opts = append(opts, awsclient.WithSNSStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithSNSConcurrency(concurrency))
		c := client.SNSClient(opts...)

		statusf("Fetching SNS topics...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing SNS topics: %w", err)
			return
		}
		sort.Slice(result, func(i, j int) bool {
			return result[i].Name < result[j].Name
		})
		mu.Lock()
		snap.SNS = result
		mu.Unlock()
	}()

	// EventBridge
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.EventBridgeOption
		if verbose {
			opts = append(opts, awsclient.WithEventBridgeStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithEventBridgeConcurrency(concurrency))
		c := client.EventBridgeClient(opts...)

		statusf("Fetching EventBridge event buses...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing EventBridge event buses: %w", err)
			return
		}
		sort.Slice(result.EventBuses, func(i, j int) bool {
			return result.EventBuses[i].Name < result.EventBuses[j].Name
		})
		for idx := range result.EventBuses {
			sort.Slice(result.EventBuses[idx].Rules, func(i, j int) bool {
				return result.EventBuses[idx].Rules[i].Name < result.EventBuses[idx].Rules[j].Name
			})
		}
		mu.Lock()
		snap.EventBridge = result
		mu.Unlock()
	}()

	// IAM
	wg.Add(1)
	go func() {
		defer wg.Done()
		var opts []awsclient.IAMOption
		if verbose {
			opts = append(opts, awsclient.WithIAMStatusFunc(statusf))
		}
		opts = append(opts, awsclient.WithIAMConcurrency(concurrency))
		c := client.IAMClient(opts...)

		statusf("Fetching IAM resources...")
		result, err := c.Summarise(ctx)
		if err != nil {
			errs <- fmt.Errorf("listing IAM resources: %w", err)
			return
		}
		sort.Slice(result.Users, func(i, j int) bool {
			return result.Users[i].Name < result.Users[j].Name
		})
		sort.Slice(result.Groups, func(i, j int) bool {
			return result.Groups[i].Name < result.Groups[j].Name
		})
		sort.Slice(result.Roles, func(i, j int) bool {
			return result.Roles[i].Name < result.Roles[j].Name
		})
		sort.Slice(result.Policies, func(i, j int) bool {
			return result.Policies[i].Name < result.Policies[j].Name
		})
		mu.Lock()
		snap.IAM = result
		mu.Unlock()
	}()

	wg.Wait()
	close(errs)

	// Collect any errors
	var errMsgs []string
	for err := range errs {
		errMsgs = append(errMsgs, err.Error())
	}
	if len(errMsgs) > 0 {
		return fmt.Errorf("snapshot errors:\n  %s", strings.Join(errMsgs, "\n  "))
	}

	if err := outputSnapshot(snap); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runVersionEKS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

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

	// Sort clusters by name
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Name < clusters[j].Name
	})

	// Print table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tVERSION\tNODEGROUP\tK8S VERSION\tAMI TYPE\tAMI VERSION")

	for _, cluster := range clusters {
		if len(cluster.NodeGroups) == 0 {
			fmt.Fprintf(w, "%s\t%s\t-\t-\t-\t-\n", cluster.Name, cluster.Version)
			continue
		}
		// Sort node groups by name
		sort.Slice(cluster.NodeGroups, func(i, j int) bool {
			return cluster.NodeGroups[i].Name < cluster.NodeGroups[j].Name
		})
		for i, ng := range cluster.NodeGroups {
			clusterName := cluster.Name
			clusterVersion := cluster.Version
			if i > 0 {
				clusterName = ""
				clusterVersion = ""
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", clusterName, clusterVersion, ng.Name, ng.KubernetesVersion, ng.AmiType, ng.ReleaseVersion)
		}
	}

	w.Flush()
	return nil
}

func runVersionRDS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

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

	statusf("Fetching RDS resources...")
	rdsSummary, err := rdsClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing RDS resources: %w", err)
	}

	// Sort instances and clusters by identifier
	sort.Slice(rdsSummary.Instances, func(i, j int) bool {
		return rdsSummary.Instances[i].Identifier < rdsSummary.Instances[j].Identifier
	})
	sort.Slice(rdsSummary.Clusters, func(i, j int) bool {
		return rdsSummary.Clusters[i].Identifier < rdsSummary.Clusters[j].Identifier
	})

	// Print table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TYPE\tIDENTIFIER\tENGINE\tVERSION\tCLASS/MODE\tSTATUS")

	for _, cluster := range rdsSummary.Clusters {
		mode := cluster.EngineMode
		if mode == "" {
			mode = "provisioned"
		}
		fmt.Fprintf(w, "cluster\t%s\t%s\t%s\t%s\t%s\n", cluster.Identifier, cluster.Engine, cluster.EngineVersion, mode, cluster.Status)
	}

	for _, inst := range rdsSummary.Instances {
		fmt.Fprintf(w, "instance\t%s\t%s\t%s\t%s\t%s\n", inst.Identifier, inst.Engine, inst.EngineVersion, inst.InstanceClass, inst.Status)
	}

	w.Flush()
	return nil
}

func runVersionMSK(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var mskOpts []awsclient.MSKOption
	if verbose {
		mskOpts = append(mskOpts, awsclient.WithMSKStatusFunc(statusf))
	}
	mskOpts = append(mskOpts, awsclient.WithMSKConcurrency(concurrency))
	mskClient := client.MSKClient(mskOpts...)

	statusf("Fetching MSK clusters...")
	clusters, err := mskClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing MSK clusters: %w", err)
	}

	// Sort clusters by name
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].ClusterName < clusters[j].ClusterName
	})

	// Print table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tKAFKA VERSION\tINSTANCE TYPE\tBROKERS\tSTATE")

	for _, cluster := range clusters {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n", cluster.ClusterName, cluster.KafkaVersion, cluster.InstanceType, cluster.NumberOfBrokerNodes, cluster.State)
	}

	w.Flush()
	return nil
}

func runVersionElastiCache(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

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

	// Sort replication groups and cache clusters
	sort.Slice(ecSummary.ReplicationGroups, func(i, j int) bool {
		return ecSummary.ReplicationGroups[i].ReplicationGroupId < ecSummary.ReplicationGroups[j].ReplicationGroupId
	})
	sort.Slice(ecSummary.CacheClusters, func(i, j int) bool {
		return ecSummary.CacheClusters[i].CacheClusterId < ecSummary.CacheClusters[j].CacheClusterId
	})

	// Print table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TYPE\tIDENTIFIER\tENGINE\tVERSION\tNODE TYPE\tSTATUS")

	for _, rg := range ecSummary.ReplicationGroups {
		fmt.Fprintf(w, "replication-group\t%s\t%s\t%s\t%s\t%s\n", rg.ReplicationGroupId, rg.Engine, rg.EngineVersion, rg.NodeType, rg.Status)
	}

	// Only show standalone cache clusters (not part of a replication group)
	for _, cc := range ecSummary.CacheClusters {
		if cc.ReplicationGroupId != "" {
			continue
		}
		fmt.Fprintf(w, "cache-cluster\t%s\t%s\t%s\t%s\t%s\n", cc.CacheClusterId, cc.Engine, cc.EngineVersion, cc.CacheNodeType, cc.Status)
	}

	w.Flush()
	return nil
}

func runVersionOpenSearch(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

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

	// Sort domains by name
	sort.Slice(domains, func(i, j int) bool {
		return domains[i].DomainName < domains[j].DomainName
	})

	// Print table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "DOMAIN\tVERSION\tINSTANCE TYPE\tINSTANCES")

	for _, domain := range domains {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", domain.DomainName, domain.EngineVersion, domain.ClusterInstanceType, domain.ClusterInstanceCount)
	}

	w.Flush()
	return nil
}
