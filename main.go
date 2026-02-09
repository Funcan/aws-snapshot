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

	// Fetch Lambda functions
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

	sort.Slice(functions, func(i, j int) bool {
		return functions[i].FunctionName < functions[j].FunctionName
	})
	snap.Lambda = functions

	// Fetch ECR repositories
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

	sort.Slice(repos, func(i, j int) bool {
		return repos[i].RepositoryName < repos[j].RepositoryName
	})
	snap.ECR = repos

	// Fetch ECS clusters and services
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

	sort.Slice(ecsSummary.Clusters, func(i, j int) bool {
		return ecsSummary.Clusters[i].ClusterName < ecsSummary.Clusters[j].ClusterName
	})
	for i := range ecsSummary.Clusters {
		sort.Slice(ecsSummary.Clusters[i].Services, func(a, b int) bool {
			return ecsSummary.Clusters[i].Services[a].ServiceName < ecsSummary.Clusters[i].Services[b].ServiceName
		})
	}
	snap.ECS = ecsSummary

	// Fetch ELB load balancers and target groups
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

	sort.Slice(elbSummary.LoadBalancers, func(i, j int) bool {
		return elbSummary.LoadBalancers[i].LoadBalancerName < elbSummary.LoadBalancers[j].LoadBalancerName
	})
	sort.Slice(elbSummary.TargetGroups, func(i, j int) bool {
		return elbSummary.TargetGroups[i].TargetGroupName < elbSummary.TargetGroups[j].TargetGroupName
	})
	snap.ELB = elbSummary

	// Fetch Route53 hosted zones
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

	sort.Slice(zones, func(i, j int) bool {
		return zones[i].Name < zones[j].Name
	})
	for i := range zones {
		sort.Slice(zones[i].RecordSets, func(a, b int) bool {
			if zones[i].RecordSets[a].Name != zones[i].RecordSets[b].Name {
				return zones[i].RecordSets[a].Name < zones[i].RecordSets[b].Name
			}
			return zones[i].RecordSets[a].Type < zones[i].RecordSets[b].Type
		})
	}
	snap.Route53 = zones

	// Fetch MSK clusters
	var mskOpts []awsclient.MSKOption
	if verbose {
		mskOpts = append(mskOpts, awsclient.WithMSKStatusFunc(statusf))
	}
	mskOpts = append(mskOpts, awsclient.WithMSKConcurrency(concurrency))
	mskClient := client.MSKClient(mskOpts...)

	statusf("Fetching MSK clusters...")
	mskClusters, err := mskClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing MSK clusters: %w", err)
	}

	sort.Slice(mskClusters, func(i, j int) bool {
		return mskClusters[i].ClusterName < mskClusters[j].ClusterName
	})
	snap.MSK = mskClusters

	// Fetch VPCs
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

	sort.Slice(vpcs, func(i, j int) bool {
		return vpcs[i].VpcId < vpcs[j].VpcId
	})
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
	snap.VPC = vpcs

	// Fetch CloudFront distributions
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

	sort.Slice(distributions, func(i, j int) bool {
		return distributions[i].Id < distributions[j].Id
	})
	snap.CloudFront = distributions

	// Fetch API Gateway
	var apigwOpts []awsclient.APIGatewayOption
	if verbose {
		apigwOpts = append(apigwOpts, awsclient.WithAPIGatewayStatusFunc(statusf))
	}
	apigwOpts = append(apigwOpts, awsclient.WithAPIGatewayConcurrency(concurrency))
	apigwClient := client.APIGatewayClient(apigwOpts...)

	statusf("Fetching API Gateway APIs...")
	apigwSummary, err := apigwClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing API Gateway APIs: %w", err)
	}

	sort.Slice(apigwSummary.RestAPIs, func(i, j int) bool {
		return apigwSummary.RestAPIs[i].Name < apigwSummary.RestAPIs[j].Name
	})
	sort.Slice(apigwSummary.HttpAPIs, func(i, j int) bool {
		return apigwSummary.HttpAPIs[i].Name < apigwSummary.HttpAPIs[j].Name
	})
	snap.APIGateway = apigwSummary

	// Fetch SQS queues
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

	sort.Slice(queues, func(i, j int) bool {
		return queues[i].Name < queues[j].Name
	})
	snap.SQS = queues

	// Fetch SNS topics
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

	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})
	snap.SNS = topics

	if err := outputSnapshot(snap); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}
