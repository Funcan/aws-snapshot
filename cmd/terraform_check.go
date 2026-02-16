package cmd

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"aws-snapshot/pkg/awsclient"
	"aws-snapshot/pkg/diff"
	"aws-snapshot/pkg/loadfile"
	"aws-snapshot/pkg/terraform"

	"github.com/spf13/cobra"
)

var stateFiles []string

var terraformCheckCmd = &cobra.Command{
	Use:   "terraform-check [flags] resource-type [resource-type ...]",
	Short: "Compare Terraform state with actual AWS resources",
	Long:  "Check for resources that exist in AWS but not in the Terraform state file, or vice versa.",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runTerraformCheck,
}

func init() {
	terraformCheckCmd.Flags().StringArrayVar(&stateFiles, "statefile", nil, "Path to Terraform state file (local path or s3:// URL, can be specified multiple times)")
	terraformCheckCmd.MarkFlagRequired("statefile")
}

// resourceChecker defines how to compare a resource type between Terraform state and AWS.
type resourceChecker struct {
	// terraformTypes are the Terraform resource types to look for in the state file.
	terraformTypes []string
	// attrKey is the attribute key that holds the resource identifier.
	attrKey string
	// fetchAWS fetches the list of resource names from AWS.
	fetchAWS func(ctx context.Context, client *awsclient.Client) ([]string, error)
}

var checkers = map[string]resourceChecker{
	"s3": {
		terraformTypes: []string{"aws_s3_bucket", "aws_s3_directory_bucket"},
		attrKey:        "bucket",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.S3Option
			if Verbose {
				opts = append(opts, awsclient.WithStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithConcurrency(Concurrency))
			s3client := client.S3Client(opts...)

			buckets, err := s3client.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(buckets))
			for i, b := range buckets {
				names[i] = b.Name
			}
			return names, nil
		},
	},
	"opensearch": {
		terraformTypes: []string{"aws_opensearch_domain"},
		attrKey:        "domain_name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.OpenSearchOption
			if Verbose {
				opts = append(opts, awsclient.WithOpenSearchStatusFunc(Statusf))
			}
			osClient := client.OpenSearchClient(opts...)

			domains, err := osClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(domains))
			for i, d := range domains {
				names[i] = d.DomainName
			}
			return names, nil
		},
	},
	"apigateway": {
		terraformTypes: []string{"aws_api_gateway_rest_api", "aws_apigatewayv2_api"},
		attrKey:        "name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.APIGatewayOption
			if Verbose {
				opts = append(opts, awsclient.WithAPIGatewayStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithAPIGatewayConcurrency(Concurrency))
			apigwClient := client.APIGatewayClient(opts...)

			summary, err := apigwClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			var names []string
			for _, a := range summary.RestAPIs {
				names = append(names, a.Name)
			}
			for _, a := range summary.HttpAPIs {
				names = append(names, a.Name)
			}
			return names, nil
		},
	},
	"cloudfront": {
		terraformTypes: []string{"aws_cloudfront_distribution"},
		attrKey:        "id",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.CloudFrontOption
			if Verbose {
				opts = append(opts, awsclient.WithCloudFrontStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithCloudFrontConcurrency(Concurrency))
			cfClient := client.CloudFrontClient(opts...)

			distributions, err := cfClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(distributions))
			for i, d := range distributions {
				names[i] = d.Id
			}
			return names, nil
		},
	},
	"dynamodb": {
		terraformTypes: []string{"aws_dynamodb_table"},
		attrKey:        "name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.DynamoDBOption
			if Verbose {
				opts = append(opts, awsclient.WithDynamoDBStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithDynamoDBConcurrency(Concurrency))
			ddbClient := client.DynamoDBClient(opts...)

			tables, err := ddbClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(tables))
			for i, t := range tables {
				names[i] = t.TableName
			}
			return names, nil
		},
	},
	"eks": {
		terraformTypes: []string{"aws_eks_cluster"},
		attrKey:        "name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.EKSOption
			if Verbose {
				opts = append(opts, awsclient.WithEKSStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithEKSConcurrency(Concurrency))
			eksClient := client.EKSClient(opts...)

			clusters, err := eksClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(clusters))
			for i, c := range clusters {
				names[i] = c.Name
			}
			return names, nil
		},
	},
	"ecr": {
		terraformTypes: []string{"aws_ecr_repository"},
		attrKey:        "name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.ECROption
			if Verbose {
				opts = append(opts, awsclient.WithECRStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithECRConcurrency(Concurrency))
			ecrClient := client.ECRClient(opts...)

			repos, err := ecrClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(repos))
			for i, r := range repos {
				names[i] = r.RepositoryName
			}
			return names, nil
		},
	},
	"elasticache": {
		terraformTypes: []string{"aws_elasticache_cluster", "aws_elasticache_replication_group"},
		attrKey:        "replication_group_id",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.ElastiCacheOption
			if Verbose {
				opts = append(opts, awsclient.WithElastiCacheStatusFunc(Statusf))
			}
			ecClient := client.ElastiCacheClient(opts...)

			summary, err := ecClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(summary.ReplicationGroups))
			for i, rg := range summary.ReplicationGroups {
				names[i] = rg.ReplicationGroupId
			}
			return names, nil
		},
	},
	"elb": {
		terraformTypes: []string{"aws_lb", "aws_alb"},
		attrKey:        "name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.ELBOption
			if Verbose {
				opts = append(opts, awsclient.WithELBStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithELBConcurrency(Concurrency))
			elbClient := client.ELBClient(opts...)

			summary, err := elbClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(summary.LoadBalancers))
			for i, lb := range summary.LoadBalancers {
				names[i] = lb.LoadBalancerName
			}
			return names, nil
		},
	},
	"eventbridge": {
		terraformTypes: []string{"aws_cloudwatch_event_bus"},
		attrKey:        "name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.EventBridgeOption
			if Verbose {
				opts = append(opts, awsclient.WithEventBridgeStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithEventBridgeConcurrency(Concurrency))
			ebClient := client.EventBridgeClient(opts...)

			summary, err := ebClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(summary.EventBuses))
			for i, eb := range summary.EventBuses {
				names[i] = eb.Name
			}
			return names, nil
		},
	},
	"iam": {
		terraformTypes: []string{"aws_iam_role"},
		attrKey:        "name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.IAMOption
			if Verbose {
				opts = append(opts, awsclient.WithIAMStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithIAMConcurrency(Concurrency))
			iamClient := client.IAMClient(opts...)

			summary, err := iamClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(summary.Roles))
			for i, r := range summary.Roles {
				names[i] = r.Name
			}
			return names, nil
		},
	},
	"lambda": {
		terraformTypes: []string{"aws_lambda_function"},
		attrKey:        "function_name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.LambdaOption
			if Verbose {
				opts = append(opts, awsclient.WithLambdaStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithLambdaConcurrency(Concurrency))
			lambdaClient := client.LambdaClient(opts...)

			functions, err := lambdaClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(functions))
			for i, f := range functions {
				names[i] = f.FunctionName
			}
			return names, nil
		},
	},
	"msk": {
		terraformTypes: []string{"aws_msk_cluster"},
		attrKey:        "cluster_name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.MSKOption
			if Verbose {
				opts = append(opts, awsclient.WithMSKStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithMSKConcurrency(Concurrency))
			mskClient := client.MSKClient(opts...)

			clusters, err := mskClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(clusters))
			for i, c := range clusters {
				names[i] = c.ClusterName
			}
			return names, nil
		},
	},
	"rds": {
		terraformTypes: []string{"aws_db_instance", "aws_rds_cluster"},
		attrKey:        "identifier",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.RDSOption
			if Verbose {
				opts = append(opts, awsclient.WithRDSStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithRDSConcurrency(Concurrency))
			rdsClient := client.RDSClient(opts...)

			summary, err := rdsClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			var names []string
			for _, inst := range summary.Instances {
				names = append(names, inst.Identifier)
			}
			for _, cl := range summary.Clusters {
				names = append(names, cl.Identifier)
			}
			return names, nil
		},
	},
	"route53": {
		terraformTypes: []string{"aws_route53_zone"},
		attrKey:        "name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.Route53Option
			if Verbose {
				opts = append(opts, awsclient.WithRoute53StatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithRoute53Concurrency(Concurrency))
			r53Client := client.Route53Client(opts...)

			zones, err := r53Client.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(zones))
			for i, z := range zones {
				names[i] = z.Name
			}
			return names, nil
		},
	},
	"sns": {
		terraformTypes: []string{"aws_sns_topic"},
		attrKey:        "name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.SNSOption
			if Verbose {
				opts = append(opts, awsclient.WithSNSStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithSNSConcurrency(Concurrency))
			snsClient := client.SNSClient(opts...)

			topics, err := snsClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(topics))
			for i, t := range topics {
				names[i] = t.Name
			}
			return names, nil
		},
	},
	"vpc": {
		terraformTypes: []string{"aws_vpc"},
		attrKey:        "id",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.VPCOption
			if Verbose {
				opts = append(opts, awsclient.WithVPCStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithVPCConcurrency(Concurrency))
			vpcClient := client.VPCClient(opts...)

			vpcs, err := vpcClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(vpcs))
			for i, v := range vpcs {
				names[i] = v.VpcId
			}
			return names, nil
		},
	},
	"ecs": {
		terraformTypes: []string{"aws_ecs_cluster"},
		attrKey:        "name",
		fetchAWS: func(ctx context.Context, client *awsclient.Client) ([]string, error) {
			var opts []awsclient.ECSOption
			if Verbose {
				opts = append(opts, awsclient.WithECSStatusFunc(Statusf))
			}
			opts = append(opts, awsclient.WithECSConcurrency(Concurrency))
			ecsClient := client.ECSClient(opts...)

			summary, err := ecsClient.Summarise(ctx)
			if err != nil {
				return nil, err
			}

			names := make([]string, len(summary.Clusters))
			for i, c := range summary.Clusters {
				names[i] = c.ClusterName
			}
			return names, nil
		},
	},
}

// extractNames returns resource names from a parsed state for the given checker.
func extractNames(state *terraform.State, checker resourceChecker) []string {
	var names []string
	for _, tfType := range checker.terraformTypes {
		for _, res := range state.ResourcesByType(tfType) {
			for _, inst := range res.Instances {
				name := terraform.InstanceAttribute(inst, checker.attrKey)
				if name != "" {
					names = append(names, name)
				}
			}
		}
	}
	return names
}

// allCheckerTypes returns sorted checker keys.
func allCheckerTypes() []string {
	types := make([]string, 0, len(checkers))
	for k := range checkers {
		types = append(types, k)
	}
	sort.Strings(types)
	return types
}

// expandResourceTypes replaces "all" with every checker key.
func expandResourceTypes(args []string) []string {
	for _, a := range args {
		if a == "all" {
			return allCheckerTypes()
		}
	}
	return args
}

func runTerraformCheck(cmd *cobra.Command, args []string) error {
	args = expandResourceTypes(args)

	// Validate resource types
	for _, rt := range args {
		if _, ok := checkers[rt]; !ok {
			supported := make([]string, 0, len(checkers))
			for k := range checkers {
				supported = append(supported, k)
			}
			sort.Strings(supported)
			return fmt.Errorf("unsupported resource type %q (supported: %v)", rt, supported)
		}
	}

	ctx := context.Background()

	// Build AWS client
	Statusf("Creating AWS client...")
	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	// Fetch everything in parallel: state files + one goroutine per resource type
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	setError := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}

	// Load and parse all state files in parallel
	states := make([]*terraform.State, len(stateFiles))
	for i, sf := range stateFiles {
		wg.Add(1)
		go func(i int, sf string) {
			defer wg.Done()
			Statusf("Loading state file %s...", sf)
			data, err := loadfile.Load(ctx, sf, awsOptions()...)
			if err != nil {
				setError(fmt.Errorf("loading state file %s: %w", sf, err))
				return
			}
			state, err := terraform.ParseState(data)
			if err != nil {
				setError(fmt.Errorf("parsing state file %s: %w", sf, err))
				return
			}
			states[i] = state
		}(i, sf)
	}

	// Fetch AWS resources per resource type in parallel
	awsResults := make(map[string][]string)
	var awsMu sync.Mutex
	for _, rt := range args {
		wg.Add(1)
		go func(rt string) {
			defer wg.Done()
			checker := checkers[rt]
			Statusf("Fetching %s resources from AWS...", rt)
			names, err := checker.fetchAWS(ctx, client)
			if err != nil {
				setError(fmt.Errorf("fetching %s from AWS: %w", rt, err))
				return
			}
			awsMu.Lock()
			awsResults[rt] = names
			awsMu.Unlock()
		}(rt)
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	// For each resource type: union state names across all files, then diff
	for _, rt := range args {
		checker := checkers[rt]

		// Track which state file each resource came from
		stateSource := make(map[string]string) // resource name -> state file path
		for i, state := range states {
			for _, name := range extractNames(state, checker) {
				if _, seen := stateSource[name]; !seen {
					stateSource[name] = stateFiles[i]
				}
			}
		}
		var stateNames []string
		for name := range stateSource {
			stateNames = append(stateNames, name)
		}

		d := diff.CompareSets(awsResults[rt], stateNames)

		if Verbose {
			// Log matched resources
			matched := make(map[string]bool, len(d.OnlyInA)+len(d.OnlyInB))
			for _, n := range d.OnlyInA {
				matched[n] = true
			}
			for _, n := range d.OnlyInB {
				matched[n] = true
			}
			var found []string
			for _, n := range awsResults[rt] {
				if !matched[n] {
					found = append(found, n)
				}
			}
			sort.Strings(found)
			for _, n := range found {
				Statusf("resource found: %s: %s: %s", rt, n, stateSource[n])
			}
		}

		if len(d.OnlyInA) == 0 && len(d.OnlyInB) == 0 {
			fmt.Printf("%s: all resources match\n", rt)
			continue
		}

		fmt.Printf("%s:\n", rt)
		for _, n := range d.OnlyInA {
			fmt.Printf("  in AWS but not in state: %s\n", n)
		}
		for _, n := range d.OnlyInB {
			fmt.Printf("  in state but not in AWS: %s\n", n)
		}
	}

	return nil
}
