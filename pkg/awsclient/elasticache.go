package awsclient

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/elasticache"
)

// ElastiCacheClient wraps the AWS ElastiCache client with snapshot capabilities.
type ElastiCacheClient struct {
	client  *elasticache.Client
	statusf StatusFunc
}

// ElastiCacheOption is a functional option for configuring the ElastiCacheClient.
type ElastiCacheOption func(*elastiCacheOptions)

type elastiCacheOptions struct {
	statusf StatusFunc
}

// WithElastiCacheStatusFunc sets a callback for progress messages.
func WithElastiCacheStatusFunc(f StatusFunc) ElastiCacheOption {
	return func(o *elastiCacheOptions) {
		o.statusf = f
	}
}

// ElastiCacheClient returns an ElastiCacheClient configured with the given options.
func (c *Client) ElastiCacheClient(opts ...ElastiCacheOption) *ElastiCacheClient {
	o := &elastiCacheOptions{}
	for _, opt := range opts {
		opt(o)
	}

	return &ElastiCacheClient{
		client:  elasticache.NewFromConfig(c.cfg),
		statusf: o.statusf,
	}
}

func (e *ElastiCacheClient) status(format string, args ...any) {
	if e.statusf != nil {
		e.statusf(format, args...)
	}
}

// CacheClusterSummary represents key attributes of an ElastiCache cluster (Memcached or Redis non-clustered).
type CacheClusterSummary struct {
	CacheClusterId           string   `json:"cache_cluster_id"`
	Engine                   string   `json:"engine"`
	EngineVersion            string   `json:"engine_version"`
	CacheNodeType            string   `json:"cache_node_type"`
	NumCacheNodes            int32    `json:"num_cache_nodes"`
	Status                   string   `json:"status"`
	PreferredAZ              string   `json:"preferred_az,omitempty"`
	CacheSubnetGroupName     string   `json:"cache_subnet_group_name,omitempty"`
	SecurityGroups           []string `json:"security_groups,omitempty"`
	ParameterGroupName       string   `json:"parameter_group_name,omitempty"`
	SnapshotRetentionLimit   int32    `json:"snapshot_retention_limit"`
	AutoMinorVersionUpgrade  bool     `json:"auto_minor_version_upgrade"`
	TransitEncryptionEnabled bool     `json:"transit_encryption_enabled"`
	AtRestEncryptionEnabled  bool     `json:"at_rest_encryption_enabled"`
	AuthTokenEnabled         bool     `json:"auth_token_enabled"`
	ReplicationGroupId       string   `json:"replication_group_id,omitempty"`
}

// ReplicationGroupSummary represents key attributes of a Redis replication group.
type ReplicationGroupSummary struct {
	ReplicationGroupId       string   `json:"replication_group_id"`
	Description              string   `json:"description"`
	Status                   string   `json:"status"`
	NodeType                 string   `json:"node_type,omitempty"`
	Engine                   string   `json:"engine"`
	EngineVersion            string   `json:"engine_version,omitempty"`
	ClusterMode              string   `json:"cluster_mode"`
	NumNodeGroups            int      `json:"num_node_groups"`
	NumCacheClusters         int      `json:"num_cache_clusters"`
	AutomaticFailover        string   `json:"automatic_failover"`
	MultiAZ                  string   `json:"multi_az"`
	SnapshotRetentionLimit   int32    `json:"snapshot_retention_limit"`
	TransitEncryptionEnabled bool     `json:"transit_encryption_enabled"`
	AtRestEncryptionEnabled  bool     `json:"at_rest_encryption_enabled"`
	AuthTokenEnabled         bool     `json:"auth_token_enabled"`
	MemberClusters           []string `json:"member_clusters,omitempty"`
}

// ElastiCacheSummary contains both cache clusters and replication groups.
type ElastiCacheSummary struct {
	CacheClusters     []CacheClusterSummary     `json:"cache_clusters,omitempty"`
	ReplicationGroups []ReplicationGroupSummary `json:"replication_groups,omitempty"`
}

// Summarise returns a summary of all ElastiCache resources.
func (e *ElastiCacheClient) Summarise(ctx context.Context) (*ElastiCacheSummary, error) {
	summary := &ElastiCacheSummary{}

	// Fetch cache clusters first (needed to get engine version for replication groups)
	clusters, err := e.listCacheClusters(ctx)
	if err != nil {
		return nil, err
	}
	summary.CacheClusters = clusters

	// Build a map of cluster ID to engine version for quick lookup
	clusterVersions := make(map[string]string)
	for _, c := range clusters {
		clusterVersions[c.CacheClusterId] = c.EngineVersion
	}

	// Fetch replication groups
	groups, err := e.listReplicationGroups(ctx, clusterVersions)
	if err != nil {
		return nil, err
	}
	summary.ReplicationGroups = groups

	return summary, nil
}

func (e *ElastiCacheClient) listCacheClusters(ctx context.Context) ([]CacheClusterSummary, error) {
	e.status("Listing ElastiCache clusters...")

	var summaries []CacheClusterSummary
	paginator := elasticache.NewDescribeCacheClustersPaginator(e.client, &elasticache.DescribeCacheClustersInput{
		ShowCacheNodeInfo: aws.Bool(true),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, cluster := range page.CacheClusters {
			summary := CacheClusterSummary{
				CacheClusterId:           aws.ToString(cluster.CacheClusterId),
				Engine:                   aws.ToString(cluster.Engine),
				EngineVersion:            aws.ToString(cluster.EngineVersion),
				CacheNodeType:            aws.ToString(cluster.CacheNodeType),
				NumCacheNodes:            aws.ToInt32(cluster.NumCacheNodes),
				Status:                   aws.ToString(cluster.CacheClusterStatus),
				PreferredAZ:              aws.ToString(cluster.PreferredAvailabilityZone),
				CacheSubnetGroupName:     aws.ToString(cluster.CacheSubnetGroupName),
				SnapshotRetentionLimit:   aws.ToInt32(cluster.SnapshotRetentionLimit),
				AutoMinorVersionUpgrade:  aws.ToBool(cluster.AutoMinorVersionUpgrade),
				TransitEncryptionEnabled: aws.ToBool(cluster.TransitEncryptionEnabled),
				AtRestEncryptionEnabled:  aws.ToBool(cluster.AtRestEncryptionEnabled),
				AuthTokenEnabled:         aws.ToBool(cluster.AuthTokenEnabled),
				ReplicationGroupId:       aws.ToString(cluster.ReplicationGroupId),
			}

			if cluster.CacheParameterGroup != nil {
				summary.ParameterGroupName = aws.ToString(cluster.CacheParameterGroup.CacheParameterGroupName)
			}

			for _, sg := range cluster.SecurityGroups {
				summary.SecurityGroups = append(summary.SecurityGroups, aws.ToString(sg.SecurityGroupId))
			}

			summaries = append(summaries, summary)
		}
	}

	e.status("Found %d ElastiCache clusters", len(summaries))
	return summaries, nil
}

func (e *ElastiCacheClient) listReplicationGroups(ctx context.Context, clusterVersions map[string]string) ([]ReplicationGroupSummary, error) {
	e.status("Listing ElastiCache replication groups...")

	var summaries []ReplicationGroupSummary
	paginator := elasticache.NewDescribeReplicationGroupsPaginator(e.client, &elasticache.DescribeReplicationGroupsInput{})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, group := range page.ReplicationGroups {
			summary := ReplicationGroupSummary{
				ReplicationGroupId:       aws.ToString(group.ReplicationGroupId),
				Description:              aws.ToString(group.Description),
				Status:                   aws.ToString(group.Status),
				Engine:                   "redis",
				AutomaticFailover:        string(group.AutomaticFailover),
				MultiAZ:                  string(group.MultiAZ),
				SnapshotRetentionLimit:   aws.ToInt32(group.SnapshotRetentionLimit),
				TransitEncryptionEnabled: aws.ToBool(group.TransitEncryptionEnabled),
				AtRestEncryptionEnabled:  aws.ToBool(group.AtRestEncryptionEnabled),
				AuthTokenEnabled:         aws.ToBool(group.AuthTokenEnabled),
				MemberClusters:           group.MemberClusters,
			}

			// Get engine version from first member cluster
			if len(group.MemberClusters) > 0 {
				if version, ok := clusterVersions[group.MemberClusters[0]]; ok {
					summary.EngineVersion = version
				}
			}

			if group.CacheNodeType != nil {
				summary.NodeType = aws.ToString(group.CacheNodeType)
			}

			if group.NodeGroups != nil {
				summary.NumNodeGroups = len(group.NodeGroups)
			}

			if group.MemberClusters != nil {
				summary.NumCacheClusters = len(group.MemberClusters)
			}

			if group.ClusterEnabled != nil && *group.ClusterEnabled {
				summary.ClusterMode = "enabled"
			} else {
				summary.ClusterMode = "disabled"
			}

			summaries = append(summaries, summary)
		}
	}

	e.status("Found %d ElastiCache replication groups", len(summaries))
	return summaries, nil
}
