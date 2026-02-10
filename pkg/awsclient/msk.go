package awsclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
)

// MSKClient wraps the AWS MSK client with snapshot capabilities.
type MSKClient struct {
	client      *kafka.Client
	statusf     StatusFunc
	concurrency int
}

// MSKOption is a functional option for configuring the MSKClient.
type MSKOption func(*mskOptions)

type mskOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithMSKStatusFunc sets a callback for progress messages.
func WithMSKStatusFunc(f StatusFunc) MSKOption {
	return func(o *mskOptions) {
		o.statusf = f
	}
}

// WithMSKConcurrency sets the maximum number of clusters to process in parallel.
func WithMSKConcurrency(n int) MSKOption {
	return func(o *mskOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// MSKClient returns an MSKClient configured with the given options.
func (c *Client) MSKClient(opts ...MSKOption) *MSKClient {
	o := &mskOptions{
		concurrency: 10,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &MSKClient{
		client:      kafka.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (m *MSKClient) status(format string, args ...any) {
	if m.statusf != nil {
		m.statusf(format, args...)
	}
}

// MSKClusterSummary represents key attributes of an MSK cluster.
type MSKClusterSummary struct {
	ClusterName         string            `json:"cluster_name"`
	ClusterArn          string            `json:"cluster_arn"`
	ClusterType         string            `json:"cluster_type"`
	State               string            `json:"state"`
	KafkaVersion        string            `json:"kafka_version,omitempty"`
	NumberOfBrokerNodes int32             `json:"number_of_broker_nodes,omitempty"`
	InstanceType        string            `json:"instance_type,omitempty"`
	EBSVolumeSize       int32             `json:"ebs_volume_size_gb,omitempty"`
	ClientSubnets       []string          `json:"client_subnets,omitempty"`
	SecurityGroups      []string          `json:"security_groups,omitempty"`
	EncryptionInTransit string            `json:"encryption_in_transit,omitempty"`
	EncryptionAtRest    bool              `json:"encryption_at_rest"`
	EnhancedMonitoring  string            `json:"enhanced_monitoring,omitempty"`
	OpenMonitoring      bool              `json:"open_monitoring"`
	LoggingCloudWatch   bool              `json:"logging_cloudwatch"`
	LoggingFirehose     bool              `json:"logging_firehose"`
	LoggingS3           bool              `json:"logging_s3"`
	Tags                map[string]string `json:"tags,omitempty"`
}

// Summarise returns a summary of all MSK clusters.
func (m *MSKClient) Summarise(ctx context.Context) ([]MSKClusterSummary, error) {
	m.status("Listing MSK clusters...")

	// List all clusters
	var clusters []clusterInfo
	paginator := kafka.NewListClustersV2Paginator(m.client, &kafka.ListClustersV2Input{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, cluster := range page.ClusterInfoList {
			clusters = append(clusters, clusterInfo{
				name: aws.ToString(cluster.ClusterName),
				arn:  aws.ToString(cluster.ClusterArn),
			})
		}
	}

	total := len(clusters)
	m.status("Found %d MSK clusters, processing with concurrency %d", total, m.concurrency)

	if total == 0 {
		return []MSKClusterSummary{}, nil
	}

	summaries := make([]MSKClusterSummary, total)
	var processed atomic.Int64
	var errMu sync.Mutex
	var firstErr error

	// Create work channel
	workCh := make(chan int, total)
	for i := range clusters {
		workCh <- i
	}
	close(workCh)

	// Process clusters concurrently
	var wg sync.WaitGroup
	for i := 0; i < m.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				cluster := clusters[idx]
				summary, err := m.describeCluster(ctx, cluster)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}
				summaries[idx] = summary

				n := processed.Add(1)
				m.status("[%d/%d] Processed cluster: %s", n, total, cluster.name)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return summaries, nil
}

type clusterInfo struct {
	name string
	arn  string
}

func (m *MSKClient) describeCluster(ctx context.Context, cluster clusterInfo) (MSKClusterSummary, error) {
	summary := MSKClusterSummary{
		ClusterName: cluster.name,
		ClusterArn:  cluster.arn,
	}

	// Describe the cluster
	descResp, err := m.client.DescribeClusterV2(ctx, &kafka.DescribeClusterV2Input{
		ClusterArn: &cluster.arn,
	})
	if err != nil {
		return MSKClusterSummary{}, fmt.Errorf("describe cluster %s: %w", cluster.name, err)
	}
	if descResp.ClusterInfo == nil {
		return summary, nil
	}

	info := descResp.ClusterInfo
	summary.ClusterType = string(info.ClusterType)
	summary.State = string(info.State)
	summary.Tags = info.Tags

	// Handle provisioned cluster info
	if info.Provisioned != nil {
		prov := info.Provisioned
		summary.KafkaVersion = aws.ToString(prov.CurrentBrokerSoftwareInfo.KafkaVersion)
		summary.NumberOfBrokerNodes = aws.ToInt32(prov.NumberOfBrokerNodes)

		if prov.BrokerNodeGroupInfo != nil {
			bng := prov.BrokerNodeGroupInfo
			summary.InstanceType = aws.ToString(bng.InstanceType)
			summary.ClientSubnets = bng.ClientSubnets
			summary.SecurityGroups = bng.SecurityGroups

			if bng.StorageInfo != nil && bng.StorageInfo.EbsStorageInfo != nil {
				summary.EBSVolumeSize = aws.ToInt32(bng.StorageInfo.EbsStorageInfo.VolumeSize)
			}
		}

		if prov.EncryptionInfo != nil {
			if prov.EncryptionInfo.EncryptionInTransit != nil {
				summary.EncryptionInTransit = string(prov.EncryptionInfo.EncryptionInTransit.ClientBroker)
			}
			if prov.EncryptionInfo.EncryptionAtRest != nil {
				summary.EncryptionAtRest = true
			}
		}

		summary.EnhancedMonitoring = string(prov.EnhancedMonitoring)

		if prov.OpenMonitoring != nil && prov.OpenMonitoring.Prometheus != nil {
			if prov.OpenMonitoring.Prometheus.JmxExporter != nil {
				summary.OpenMonitoring = aws.ToBool(prov.OpenMonitoring.Prometheus.JmxExporter.EnabledInBroker)
			}
		}

		if prov.LoggingInfo != nil && prov.LoggingInfo.BrokerLogs != nil {
			logs := prov.LoggingInfo.BrokerLogs
			if logs.CloudWatchLogs != nil {
				summary.LoggingCloudWatch = aws.ToBool(logs.CloudWatchLogs.Enabled)
			}
			if logs.Firehose != nil {
				summary.LoggingFirehose = aws.ToBool(logs.Firehose.Enabled)
			}
			if logs.S3 != nil {
				summary.LoggingS3 = aws.ToBool(logs.S3.Enabled)
			}
		}
	}

	// Handle serverless cluster info
	if info.Serverless != nil {
		if len(info.Serverless.VpcConfigs) > 0 {
			for _, vpc := range info.Serverless.VpcConfigs {
				summary.ClientSubnets = append(summary.ClientSubnets, vpc.SubnetIds...)
				summary.SecurityGroups = append(summary.SecurityGroups, vpc.SecurityGroupIds...)
			}
		}
	}

	return summary, nil
}
