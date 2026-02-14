package cmd

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"aws-snapshot/pkg/awsclient"

	"github.com/spf13/cobra"
)

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

func init() {
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
}

func runSnapshotS3(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var s3Opts []awsclient.S3Option
	if Verbose {
		s3Opts = append(s3Opts, awsclient.WithStatusFunc(Statusf))
	}
	s3Opts = append(s3Opts, awsclient.WithConcurrency(Concurrency))
	s3client := client.S3Client(s3Opts...)

	Statusf("Fetching S3 buckets...")
	buckets, err := s3client.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing S3 buckets: %w", err)
	}

	// Sort by bucket name for consistent diffs
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	if err := OutputSnapshot(Snapshot{S3: buckets}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotEKS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var eksOpts []awsclient.EKSOption
	if Verbose {
		eksOpts = append(eksOpts, awsclient.WithEKSStatusFunc(Statusf))
	}
	eksOpts = append(eksOpts, awsclient.WithEKSConcurrency(Concurrency))
	eksClient := client.EKSClient(eksOpts...)

	Statusf("Fetching EKS clusters...")
	clusters, err := eksClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing EKS clusters: %w", err)
	}

	// Sort by cluster name for consistent diffs
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Name < clusters[j].Name
	})

	if err := OutputSnapshot(Snapshot{EKS: clusters}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotRDS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var rdsOpts []awsclient.RDSOption
	if Verbose {
		rdsOpts = append(rdsOpts, awsclient.WithRDSStatusFunc(Statusf))
	}
	rdsOpts = append(rdsOpts, awsclient.WithRDSConcurrency(Concurrency))
	rdsClient := client.RDSClient(rdsOpts...)

	Statusf("Fetching RDS instances and clusters...")
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

	if err := OutputSnapshot(Snapshot{RDS: rdsSummary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotOpenSearch(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var osOpts []awsclient.OpenSearchOption
	if Verbose {
		osOpts = append(osOpts, awsclient.WithOpenSearchStatusFunc(Statusf))
	}
	osClient := client.OpenSearchClient(osOpts...)

	Statusf("Fetching OpenSearch domains...")
	domains, err := osClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing OpenSearch domains: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(domains, func(i, j int) bool {
		return domains[i].DomainName < domains[j].DomainName
	})

	if err := OutputSnapshot(Snapshot{OpenSearch: domains}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotElastiCache(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ecOpts []awsclient.ElastiCacheOption
	if Verbose {
		ecOpts = append(ecOpts, awsclient.WithElastiCacheStatusFunc(Statusf))
	}
	ecClient := client.ElastiCacheClient(ecOpts...)

	Statusf("Fetching ElastiCache resources...")
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

	if err := OutputSnapshot(Snapshot{ElastiCache: ecSummary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotDynamoDB(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ddbOpts []awsclient.DynamoDBOption
	if Verbose {
		ddbOpts = append(ddbOpts, awsclient.WithDynamoDBStatusFunc(Statusf))
	}
	ddbOpts = append(ddbOpts, awsclient.WithDynamoDBConcurrency(Concurrency))
	ddbClient := client.DynamoDBClient(ddbOpts...)

	Statusf("Fetching DynamoDB tables...")
	tables, err := ddbClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing DynamoDB tables: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(tables, func(i, j int) bool {
		return tables[i].TableName < tables[j].TableName
	})

	if err := OutputSnapshot(Snapshot{DynamoDB: tables}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotLambda(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var lambdaOpts []awsclient.LambdaOption
	if Verbose {
		lambdaOpts = append(lambdaOpts, awsclient.WithLambdaStatusFunc(Statusf))
	}
	lambdaOpts = append(lambdaOpts, awsclient.WithLambdaConcurrency(Concurrency))
	lambdaClient := client.LambdaClient(lambdaOpts...)

	Statusf("Fetching Lambda functions...")
	functions, err := lambdaClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing Lambda functions: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(functions, func(i, j int) bool {
		return functions[i].FunctionName < functions[j].FunctionName
	})

	if err := OutputSnapshot(Snapshot{Lambda: functions}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotECR(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ecrOpts []awsclient.ECROption
	if Verbose {
		ecrOpts = append(ecrOpts, awsclient.WithECRStatusFunc(Statusf))
	}
	ecrOpts = append(ecrOpts, awsclient.WithECRConcurrency(Concurrency))
	ecrClient := client.ECRClient(ecrOpts...)

	Statusf("Fetching ECR repositories...")
	repos, err := ecrClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing ECR repositories: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(repos, func(i, j int) bool {
		return repos[i].RepositoryName < repos[j].RepositoryName
	})

	if err := OutputSnapshot(Snapshot{ECR: repos}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotECS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ecsOpts []awsclient.ECSOption
	if Verbose {
		ecsOpts = append(ecsOpts, awsclient.WithECSStatusFunc(Statusf))
	}
	ecsOpts = append(ecsOpts, awsclient.WithECSConcurrency(Concurrency))
	ecsClient := client.ECSClient(ecsOpts...)

	Statusf("Fetching ECS clusters and services...")
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

	if err := OutputSnapshot(Snapshot{ECS: ecsSummary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotELB(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var elbOpts []awsclient.ELBOption
	if Verbose {
		elbOpts = append(elbOpts, awsclient.WithELBStatusFunc(Statusf))
	}
	elbOpts = append(elbOpts, awsclient.WithELBConcurrency(Concurrency))
	elbClient := client.ELBClient(elbOpts...)

	Statusf("Fetching load balancers and target groups...")
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

	if err := OutputSnapshot(Snapshot{ELB: elbSummary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotRoute53(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var r53Opts []awsclient.Route53Option
	if Verbose {
		r53Opts = append(r53Opts, awsclient.WithRoute53StatusFunc(Statusf))
	}
	r53Opts = append(r53Opts, awsclient.WithRoute53Concurrency(Concurrency))
	r53Client := client.Route53Client(r53Opts...)

	Statusf("Fetching Route53 hosted zones...")
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

	if err := OutputSnapshot(Snapshot{Route53: zones}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotMSK(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var mskOpts []awsclient.MSKOption
	if Verbose {
		mskOpts = append(mskOpts, awsclient.WithMSKStatusFunc(Statusf))
	}
	mskOpts = append(mskOpts, awsclient.WithMSKConcurrency(Concurrency))
	mskClient := client.MSKClient(mskOpts...)

	Statusf("Fetching MSK clusters...")
	clusters, err := mskClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing MSK clusters: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].ClusterName < clusters[j].ClusterName
	})

	if err := OutputSnapshot(Snapshot{MSK: clusters}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotVPC(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var vpcOpts []awsclient.VPCOption
	if Verbose {
		vpcOpts = append(vpcOpts, awsclient.WithVPCStatusFunc(Statusf))
	}
	vpcOpts = append(vpcOpts, awsclient.WithVPCConcurrency(Concurrency))
	vpcClient := client.VPCClient(vpcOpts...)

	Statusf("Fetching VPCs...")
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

	if err := OutputSnapshot(Snapshot{VPC: vpcs}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotCloudFront(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var cfOpts []awsclient.CloudFrontOption
	if Verbose {
		cfOpts = append(cfOpts, awsclient.WithCloudFrontStatusFunc(Statusf))
	}
	cfOpts = append(cfOpts, awsclient.WithCloudFrontConcurrency(Concurrency))
	cfClient := client.CloudFrontClient(cfOpts...)

	Statusf("Fetching CloudFront distributions...")
	distributions, err := cfClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing CloudFront distributions: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(distributions, func(i, j int) bool {
		return distributions[i].Id < distributions[j].Id
	})

	if err := OutputSnapshot(Snapshot{CloudFront: distributions}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotAPIGateway(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var apigwOpts []awsclient.APIGatewayOption
	if Verbose {
		apigwOpts = append(apigwOpts, awsclient.WithAPIGatewayStatusFunc(Statusf))
	}
	apigwOpts = append(apigwOpts, awsclient.WithAPIGatewayConcurrency(Concurrency))
	apigwClient := client.APIGatewayClient(apigwOpts...)

	Statusf("Fetching API Gateway APIs...")
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

	if err := OutputSnapshot(Snapshot{APIGateway: summary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotSQS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var sqsOpts []awsclient.SQSOption
	if Verbose {
		sqsOpts = append(sqsOpts, awsclient.WithSQSStatusFunc(Statusf))
	}
	sqsOpts = append(sqsOpts, awsclient.WithSQSConcurrency(Concurrency))
	sqsClient := client.SQSClient(sqsOpts...)

	Statusf("Fetching SQS queues...")
	queues, err := sqsClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing SQS queues: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(queues, func(i, j int) bool {
		return queues[i].Name < queues[j].Name
	})

	if err := OutputSnapshot(Snapshot{SQS: queues}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotSNS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var snsOpts []awsclient.SNSOption
	if Verbose {
		snsOpts = append(snsOpts, awsclient.WithSNSStatusFunc(Statusf))
	}
	snsOpts = append(snsOpts, awsclient.WithSNSConcurrency(Concurrency))
	snsClient := client.SNSClient(snsOpts...)

	Statusf("Fetching SNS topics...")
	topics, err := snsClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing SNS topics: %w", err)
	}

	// Sort for consistent diffs
	sort.Slice(topics, func(i, j int) bool {
		return topics[i].Name < topics[j].Name
	})

	if err := OutputSnapshot(Snapshot{SNS: topics}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotEventBridge(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ebOpts []awsclient.EventBridgeOption
	if Verbose {
		ebOpts = append(ebOpts, awsclient.WithEventBridgeStatusFunc(Statusf))
	}
	ebOpts = append(ebOpts, awsclient.WithEventBridgeConcurrency(Concurrency))
	ebClient := client.EventBridgeClient(ebOpts...)

	Statusf("Fetching EventBridge event buses...")
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

	if err := OutputSnapshot(Snapshot{EventBridge: summary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotIAM(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var iamOpts []awsclient.IAMOption
	if Verbose {
		iamOpts = append(iamOpts, awsclient.WithIAMStatusFunc(Statusf))
	}
	iamOpts = append(iamOpts, awsclient.WithIAMConcurrency(Concurrency))
	iamClient := client.IAMClient(iamOpts...)

	Statusf("Fetching IAM resources...")
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

	if err := OutputSnapshot(Snapshot{IAM: summary}); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotAll(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
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
		if Verbose {
			opts = append(opts, awsclient.WithStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithConcurrency(Concurrency))
		c := client.S3Client(opts...)

		Statusf("Fetching S3 buckets...")
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
		if Verbose {
			opts = append(opts, awsclient.WithEKSStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithEKSConcurrency(Concurrency))
		c := client.EKSClient(opts...)

		Statusf("Fetching EKS clusters...")
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
		if Verbose {
			opts = append(opts, awsclient.WithRDSStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithRDSConcurrency(Concurrency))
		c := client.RDSClient(opts...)

		Statusf("Fetching RDS instances and clusters...")
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
		if Verbose {
			opts = append(opts, awsclient.WithOpenSearchStatusFunc(Statusf))
		}
		c := client.OpenSearchClient(opts...)

		Statusf("Fetching OpenSearch domains...")
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
		if Verbose {
			opts = append(opts, awsclient.WithElastiCacheStatusFunc(Statusf))
		}
		c := client.ElastiCacheClient(opts...)

		Statusf("Fetching ElastiCache resources...")
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
		if Verbose {
			opts = append(opts, awsclient.WithDynamoDBStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithDynamoDBConcurrency(Concurrency))
		c := client.DynamoDBClient(opts...)

		Statusf("Fetching DynamoDB tables...")
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
		if Verbose {
			opts = append(opts, awsclient.WithLambdaStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithLambdaConcurrency(Concurrency))
		c := client.LambdaClient(opts...)

		Statusf("Fetching Lambda functions...")
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
		if Verbose {
			opts = append(opts, awsclient.WithECRStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithECRConcurrency(Concurrency))
		c := client.ECRClient(opts...)

		Statusf("Fetching ECR repositories...")
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
		if Verbose {
			opts = append(opts, awsclient.WithECSStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithECSConcurrency(Concurrency))
		c := client.ECSClient(opts...)

		Statusf("Fetching ECS clusters and services...")
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
		if Verbose {
			opts = append(opts, awsclient.WithELBStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithELBConcurrency(Concurrency))
		c := client.ELBClient(opts...)

		Statusf("Fetching load balancers and target groups...")
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
		if Verbose {
			opts = append(opts, awsclient.WithRoute53StatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithRoute53Concurrency(Concurrency))
		c := client.Route53Client(opts...)

		Statusf("Fetching Route53 hosted zones...")
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
		if Verbose {
			opts = append(opts, awsclient.WithMSKStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithMSKConcurrency(Concurrency))
		c := client.MSKClient(opts...)

		Statusf("Fetching MSK clusters...")
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
		if Verbose {
			opts = append(opts, awsclient.WithVPCStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithVPCConcurrency(Concurrency))
		c := client.VPCClient(opts...)

		Statusf("Fetching VPCs...")
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
		if Verbose {
			opts = append(opts, awsclient.WithCloudFrontStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithCloudFrontConcurrency(Concurrency))
		c := client.CloudFrontClient(opts...)

		Statusf("Fetching CloudFront distributions...")
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
		if Verbose {
			opts = append(opts, awsclient.WithAPIGatewayStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithAPIGatewayConcurrency(Concurrency))
		c := client.APIGatewayClient(opts...)

		Statusf("Fetching API Gateway APIs...")
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
		if Verbose {
			opts = append(opts, awsclient.WithSQSStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithSQSConcurrency(Concurrency))
		c := client.SQSClient(opts...)

		Statusf("Fetching SQS queues...")
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
		if Verbose {
			opts = append(opts, awsclient.WithSNSStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithSNSConcurrency(Concurrency))
		c := client.SNSClient(opts...)

		Statusf("Fetching SNS topics...")
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
		if Verbose {
			opts = append(opts, awsclient.WithEventBridgeStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithEventBridgeConcurrency(Concurrency))
		c := client.EventBridgeClient(opts...)

		Statusf("Fetching EventBridge event buses...")
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
		if Verbose {
			opts = append(opts, awsclient.WithIAMStatusFunc(Statusf))
		}
		opts = append(opts, awsclient.WithIAMConcurrency(Concurrency))
		c := client.IAMClient(opts...)

		Statusf("Fetching IAM resources...")
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

	if err := OutputSnapshot(snap); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}
