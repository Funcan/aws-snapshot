package awsclient

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
)

// RDSClient wraps the AWS RDS client with snapshot capabilities.
type RDSClient struct {
	client      *rds.Client
	statusf     StatusFunc
	concurrency int
}

// RDSOption is a functional option for configuring the RDSClient.
type RDSOption func(*rdsOptions)

type rdsOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithRDSStatusFunc sets a callback for progress messages.
func WithRDSStatusFunc(f StatusFunc) RDSOption {
	return func(o *rdsOptions) {
		o.statusf = f
	}
}

// WithRDSConcurrency sets the maximum number of instances to process in parallel.
func WithRDSConcurrency(n int) RDSOption {
	return func(o *rdsOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// RDSClient returns an RDSClient configured with the given options.
func (c *Client) RDSClient(opts ...RDSOption) *RDSClient {
	o := &rdsOptions{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &RDSClient{
		client:      rds.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (r *RDSClient) status(format string, args ...any) {
	if r.statusf != nil {
		r.statusf(format, args...)
	}
}

// DBInstanceSummary represents key attributes of an RDS instance.
type DBInstanceSummary struct {
	Identifier                 string            `json:"identifier"`
	Engine                     string            `json:"engine"`
	EngineVersion              string            `json:"engine_version"`
	InstanceClass              string            `json:"instance_class"`
	Status                     string            `json:"status"`
	MultiAZ                    bool              `json:"multi_az"`
	StorageType                string            `json:"storage_type"`
	AllocatedStorageGB         int32             `json:"allocated_storage_gb"`
	StorageEncrypted           bool              `json:"storage_encrypted"`
	PubliclyAccessible         bool              `json:"publicly_accessible"`
	VpcId                      string            `json:"vpc_id,omitempty"`
	AvailabilityZone           string            `json:"availability_zone,omitempty"`
	SecondaryAvailabilityZone  string            `json:"secondary_availability_zone,omitempty"`
	DBSubnetGroupName          string            `json:"db_subnet_group_name,omitempty"`
	SecurityGroups             []string          `json:"security_groups,omitempty"`
	ParameterGroupName         string            `json:"parameter_group_name,omitempty"`
	OptionGroupName            string            `json:"option_group_name,omitempty"`
	BackupRetentionDays        int32             `json:"backup_retention_days"`
	DeletionProtection         bool              `json:"deletion_protection"`
	AutoMinorVersionUpgrade    bool              `json:"auto_minor_version_upgrade"`
	PerformanceInsightsEnabled bool              `json:"performance_insights_enabled"`
	IAMDatabaseAuthentication  bool              `json:"iam_database_authentication"`
	Tags                       map[string]string `json:"tags,omitempty"`
}

// DBClusterSummary represents key attributes of an RDS Aurora cluster.
type DBClusterSummary struct {
	Identifier                string            `json:"identifier"`
	Engine                    string            `json:"engine"`
	EngineVersion             string            `json:"engine_version"`
	EngineMode                string            `json:"engine_mode,omitempty"`
	Status                    string            `json:"status"`
	MultiAZ                   bool              `json:"multi_az"`
	StorageEncrypted          bool              `json:"storage_encrypted"`
	VpcId                     string            `json:"vpc_id,omitempty"`
	AvailabilityZones         []string          `json:"availability_zones,omitempty"`
	DBSubnetGroupName         string            `json:"db_subnet_group_name,omitempty"`
	SecurityGroups            []string          `json:"security_groups,omitempty"`
	ParameterGroupName        string            `json:"parameter_group_name,omitempty"`
	BackupRetentionDays       int32             `json:"backup_retention_days"`
	DeletionProtection        bool              `json:"deletion_protection"`
	IAMDatabaseAuthentication bool              `json:"iam_database_authentication"`
	Members                   []string          `json:"members,omitempty"`
	Tags                      map[string]string `json:"tags,omitempty"`
}

// RDSSummary contains both instances and clusters.
type RDSSummary struct {
	Instances []DBInstanceSummary `json:"instances,omitempty"`
	Clusters  []DBClusterSummary  `json:"clusters,omitempty"`
}

// Summarise returns a summary of all RDS instances and clusters.
func (r *RDSClient) Summarise(ctx context.Context) (*RDSSummary, error) {
	summary := &RDSSummary{}

	// Fetch instances
	instances, err := r.listInstances(ctx)
	if err != nil {
		return nil, err
	}
	summary.Instances = instances

	// Fetch clusters
	clusters, err := r.listClusters(ctx)
	if err != nil {
		return nil, err
	}
	summary.Clusters = clusters

	return summary, nil
}

func (r *RDSClient) listInstances(ctx context.Context) ([]DBInstanceSummary, error) {
	r.status("Listing RDS instances...")

	var summaries []DBInstanceSummary
	paginator := rds.NewDescribeDBInstancesPaginator(r.client, &rds.DescribeDBInstancesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, inst := range page.DBInstances {
			summary := DBInstanceSummary{
				Identifier:                 aws.ToString(inst.DBInstanceIdentifier),
				Engine:                     aws.ToString(inst.Engine),
				EngineVersion:              aws.ToString(inst.EngineVersion),
				InstanceClass:              aws.ToString(inst.DBInstanceClass),
				Status:                     aws.ToString(inst.DBInstanceStatus),
				MultiAZ:                    aws.ToBool(inst.MultiAZ),
				StorageType:                aws.ToString(inst.StorageType),
				AllocatedStorageGB:         aws.ToInt32(inst.AllocatedStorage),
				StorageEncrypted:           aws.ToBool(inst.StorageEncrypted),
				PubliclyAccessible:         aws.ToBool(inst.PubliclyAccessible),
				AvailabilityZone:           aws.ToString(inst.AvailabilityZone),
				SecondaryAvailabilityZone:  aws.ToString(inst.SecondaryAvailabilityZone),
				BackupRetentionDays:        aws.ToInt32(inst.BackupRetentionPeriod),
				DeletionProtection:         aws.ToBool(inst.DeletionProtection),
				AutoMinorVersionUpgrade:    aws.ToBool(inst.AutoMinorVersionUpgrade),
				PerformanceInsightsEnabled: aws.ToBool(inst.PerformanceInsightsEnabled),
				IAMDatabaseAuthentication:  aws.ToBool(inst.IAMDatabaseAuthenticationEnabled),
			}

			if inst.DBSubnetGroup != nil {
				summary.DBSubnetGroupName = aws.ToString(inst.DBSubnetGroup.DBSubnetGroupName)
				summary.VpcId = aws.ToString(inst.DBSubnetGroup.VpcId)
			}

			for _, sg := range inst.VpcSecurityGroups {
				summary.SecurityGroups = append(summary.SecurityGroups, aws.ToString(sg.VpcSecurityGroupId))
			}

			if len(inst.DBParameterGroups) > 0 {
				summary.ParameterGroupName = aws.ToString(inst.DBParameterGroups[0].DBParameterGroupName)
			}

			if len(inst.OptionGroupMemberships) > 0 {
				summary.OptionGroupName = aws.ToString(inst.OptionGroupMemberships[0].OptionGroupName)
			}

			// Convert tags
			if len(inst.TagList) > 0 {
				summary.Tags = make(map[string]string)
				for _, tag := range inst.TagList {
					summary.Tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
				}
			}

			summaries = append(summaries, summary)
		}
	}

	r.status("Found %d RDS instances", len(summaries))
	return summaries, nil
}

func (r *RDSClient) listClusters(ctx context.Context) ([]DBClusterSummary, error) {
	r.status("Listing RDS clusters...")

	var clusters []DBClusterSummary
	paginator := rds.NewDescribeDBClustersPaginator(r.client, &rds.DescribeDBClustersInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, cluster := range page.DBClusters {
			summary := DBClusterSummary{
				Identifier:                aws.ToString(cluster.DBClusterIdentifier),
				Engine:                    aws.ToString(cluster.Engine),
				EngineVersion:             aws.ToString(cluster.EngineVersion),
				EngineMode:                aws.ToString(cluster.EngineMode),
				Status:                    aws.ToString(cluster.Status),
				MultiAZ:                   aws.ToBool(cluster.MultiAZ),
				StorageEncrypted:          aws.ToBool(cluster.StorageEncrypted),
				AvailabilityZones:         cluster.AvailabilityZones,
				DBSubnetGroupName:         aws.ToString(cluster.DBSubnetGroup),
				BackupRetentionDays:       aws.ToInt32(cluster.BackupRetentionPeriod),
				DeletionProtection:        aws.ToBool(cluster.DeletionProtection),
				IAMDatabaseAuthentication: aws.ToBool(cluster.IAMDatabaseAuthenticationEnabled),
			}

			if cluster.DBClusterParameterGroup != nil {
				summary.ParameterGroupName = aws.ToString(cluster.DBClusterParameterGroup)
			}

			for _, sg := range cluster.VpcSecurityGroups {
				summary.SecurityGroups = append(summary.SecurityGroups, aws.ToString(sg.VpcSecurityGroupId))
			}

			for _, member := range cluster.DBClusterMembers {
				summary.Members = append(summary.Members, aws.ToString(member.DBInstanceIdentifier))
			}

			// Convert tags
			if len(cluster.TagList) > 0 {
				summary.Tags = make(map[string]string)
				for _, tag := range cluster.TagList {
					summary.Tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
				}
			}

			clusters = append(clusters, summary)
		}
	}

	r.status("Found %d RDS clusters", len(clusters))
	return clusters, nil
}
