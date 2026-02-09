package awsclient

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eks"
)

// EKSClient wraps the AWS EKS client with snapshot capabilities.
type EKSClient struct {
	client      *eks.Client
	statusf     StatusFunc
	concurrency int
}

// EKSOption is a functional option for configuring the EKSClient.
type EKSOption func(*eksOptions)

type eksOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithEKSStatusFunc sets a callback for progress messages.
func WithEKSStatusFunc(f StatusFunc) EKSOption {
	return func(o *eksOptions) {
		o.statusf = f
	}
}

// WithEKSConcurrency sets the maximum number of clusters to process in parallel.
func WithEKSConcurrency(n int) EKSOption {
	return func(o *eksOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// EKSClient returns an EKSClient configured with the given options.
func (c *Client) EKSClient(opts ...EKSOption) *EKSClient {
	o := &eksOptions{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &EKSClient{
		client:      eks.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (e *EKSClient) status(format string, args ...any) {
	if e.statusf != nil {
		e.statusf(format, args...)
	}
}

// ClusterSummary represents key attributes of an EKS cluster.
type ClusterSummary struct {
	Name                   string             `json:"name"`
	Arn                    string             `json:"arn"`
	Version                string             `json:"version"`
	Status                 string             `json:"status"`
	Endpoint               string             `json:"endpoint,omitempty"`
	RoleArn                string             `json:"role_arn,omitempty"`
	VpcId                  string             `json:"vpc_id,omitempty"`
	SubnetIds              []string           `json:"subnet_ids,omitempty"`
	SecurityGroupIds       []string           `json:"security_group_ids,omitempty"`
	ClusterSecurityGroupId string             `json:"cluster_security_group_id,omitempty"`
	EndpointPublicAccess   bool               `json:"endpoint_public_access"`
	EndpointPrivateAccess  bool               `json:"endpoint_private_access"`
	PublicAccessCidrs      []string           `json:"public_access_cidrs,omitempty"`
	EncryptionConfigured   bool               `json:"encryption_configured"`
	LoggingEnabled         []string           `json:"logging_enabled,omitempty"`
	Tags                   map[string]string  `json:"tags,omitempty"`
	NodeGroups             []NodeGroupSummary `json:"node_groups,omitempty"`
}

// NodeGroupSummary represents key attributes of an EKS node group.
type NodeGroupSummary struct {
	Name           string   `json:"name"`
	Status         string   `json:"status"`
	InstanceTypes  []string `json:"instance_types,omitempty"`
	AmiType        string   `json:"ami_type,omitempty"`
	ReleaseVersion string   `json:"release_version,omitempty"`
	MinSize        int32    `json:"min_size"`
	MaxSize        int32    `json:"max_size"`
}

// Summarise returns a summary of all EKS clusters.
func (e *EKSClient) Summarise(ctx context.Context) ([]ClusterSummary, error) {
	e.status("Listing EKS clusters...")

	var clusterNames []string
	paginator := eks.NewListClustersPaginator(e.client, &eks.ListClustersInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		clusterNames = append(clusterNames, page.Clusters...)
	}

	total := len(clusterNames)
	e.status("Found %d EKS clusters, processing with concurrency %d", total, e.concurrency)

	if total == 0 {
		return []ClusterSummary{}, nil
	}

	summaries := make([]ClusterSummary, total)
	var processed atomic.Int64

	// Create work channel
	workCh := make(chan int, total)
	for i := range clusterNames {
		workCh <- i
	}
	close(workCh)

	// Process clusters concurrently
	var wg sync.WaitGroup
	for i := 0; i < e.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				name := clusterNames[idx]
				summary := e.describeCluster(ctx, name)
				summaries[idx] = summary

				n := processed.Add(1)
				e.status("[%d/%d] Processed cluster: %s", n, total, name)
			}
		}()
	}

	wg.Wait()

	return summaries, nil
}

func (e *EKSClient) describeCluster(ctx context.Context, name string) ClusterSummary {
	summary := ClusterSummary{Name: name}

	resp, err := e.client.DescribeCluster(ctx, &eks.DescribeClusterInput{
		Name: &name,
	})
	if err != nil {
		return summary
	}

	cluster := resp.Cluster
	summary.Arn = aws.ToString(cluster.Arn)
	summary.Version = aws.ToString(cluster.Version)
	summary.Status = string(cluster.Status)
	summary.Endpoint = aws.ToString(cluster.Endpoint)
	summary.RoleArn = aws.ToString(cluster.RoleArn)

	if cluster.ResourcesVpcConfig != nil {
		summary.VpcId = aws.ToString(cluster.ResourcesVpcConfig.VpcId)
		summary.SubnetIds = cluster.ResourcesVpcConfig.SubnetIds
		summary.SecurityGroupIds = cluster.ResourcesVpcConfig.SecurityGroupIds
		summary.ClusterSecurityGroupId = aws.ToString(cluster.ResourcesVpcConfig.ClusterSecurityGroupId)
		summary.EndpointPublicAccess = cluster.ResourcesVpcConfig.EndpointPublicAccess
		summary.EndpointPrivateAccess = cluster.ResourcesVpcConfig.EndpointPrivateAccess
		summary.PublicAccessCidrs = cluster.ResourcesVpcConfig.PublicAccessCidrs
	}

	if cluster.EncryptionConfig != nil && len(cluster.EncryptionConfig) > 0 {
		summary.EncryptionConfigured = true
	}

	if cluster.Logging != nil && cluster.Logging.ClusterLogging != nil {
		for _, logSetup := range cluster.Logging.ClusterLogging {
			if aws.ToBool(logSetup.Enabled) {
				for _, logType := range logSetup.Types {
					summary.LoggingEnabled = append(summary.LoggingEnabled, string(logType))
				}
			}
		}
	}

	summary.Tags = cluster.Tags

	// Get node groups
	summary.NodeGroups = e.listNodeGroups(ctx, name)

	return summary
}

func (e *EKSClient) listNodeGroups(ctx context.Context, clusterName string) []NodeGroupSummary {
	var nodeGroups []NodeGroupSummary

	// List node group names
	var nodeGroupNames []string
	paginator := eks.NewListNodegroupsPaginator(e.client, &eks.ListNodegroupsInput{
		ClusterName: &clusterName,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nodeGroups
		}
		nodeGroupNames = append(nodeGroupNames, page.Nodegroups...)
	}

	// Describe each node group
	for _, ngName := range nodeGroupNames {
		resp, err := e.client.DescribeNodegroup(ctx, &eks.DescribeNodegroupInput{
			ClusterName:   &clusterName,
			NodegroupName: &ngName,
		})
		if err != nil {
			continue
		}

		ng := resp.Nodegroup
		summary := NodeGroupSummary{
			Name:           aws.ToString(ng.NodegroupName),
			Status:         string(ng.Status),
			InstanceTypes:  ng.InstanceTypes,
			AmiType:        string(ng.AmiType),
			ReleaseVersion: aws.ToString(ng.ReleaseVersion),
		}

		if ng.ScalingConfig != nil {
			if ng.ScalingConfig.MinSize != nil {
				summary.MinSize = int32(*ng.ScalingConfig.MinSize)
			}
			if ng.ScalingConfig.MaxSize != nil {
				summary.MaxSize = int32(*ng.ScalingConfig.MaxSize)
			}
		}

		nodeGroups = append(nodeGroups, summary)
	}

	return nodeGroups
}
