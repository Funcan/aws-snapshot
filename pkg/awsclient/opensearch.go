package awsclient

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/opensearch"
)

// OpenSearchClient wraps the AWS OpenSearch client with snapshot capabilities.
type OpenSearchClient struct {
	client  *opensearch.Client
	statusf StatusFunc
}

// OpenSearchOption is a functional option for configuring the OpenSearchClient.
type OpenSearchOption func(*openSearchOptions)

type openSearchOptions struct {
	statusf StatusFunc
}

// WithOpenSearchStatusFunc sets a callback for progress messages.
func WithOpenSearchStatusFunc(f StatusFunc) OpenSearchOption {
	return func(o *openSearchOptions) {
		o.statusf = f
	}
}

// OpenSearchClient returns an OpenSearchClient configured with the given options.
func (c *Client) OpenSearchClient(opts ...OpenSearchOption) *OpenSearchClient {
	o := &openSearchOptions{}
	for _, opt := range opts {
		opt(o)
	}

	return &OpenSearchClient{
		client:  opensearch.NewFromConfig(c.cfg),
		statusf: o.statusf,
	}
}

func (o *OpenSearchClient) status(format string, args ...any) {
	if o.statusf != nil {
		o.statusf(format, args...)
	}
}

// DomainSummary represents key attributes of an OpenSearch domain.
type DomainSummary struct {
	DomainName             string            `json:"domain_name"`
	DomainId               string            `json:"domain_id"`
	Arn                    string            `json:"arn"`
	EngineVersion          string            `json:"engine_version"`
	ClusterInstanceType    string            `json:"cluster_instance_type,omitempty"`
	ClusterInstanceCount   int32             `json:"cluster_instance_count,omitempty"`
	DedicatedMasterEnabled bool              `json:"dedicated_master_enabled"`
	DedicatedMasterType    string            `json:"dedicated_master_type,omitempty"`
	DedicatedMasterCount   int32             `json:"dedicated_master_count,omitempty"`
	ZoneAwarenessEnabled   bool              `json:"zone_awareness_enabled"`
	WarmEnabled            bool              `json:"warm_enabled"`
	WarmType               string            `json:"warm_type,omitempty"`
	WarmCount              int32             `json:"warm_count,omitempty"`
	EBSEnabled             bool              `json:"ebs_enabled"`
	VolumeType             string            `json:"volume_type,omitempty"`
	VolumeSizeGB           int32             `json:"volume_size_gb,omitempty"`
	EncryptionAtRest       bool              `json:"encryption_at_rest"`
	NodeToNodeEncryption   bool              `json:"node_to_node_encryption"`
	EnforceHTTPS           bool              `json:"enforce_https"`
	VPCEnabled             bool              `json:"vpc_enabled"`
	VpcId                  string            `json:"vpc_id,omitempty"`
	SubnetIds              []string          `json:"subnet_ids,omitempty"`
	SecurityGroupIds       []string          `json:"security_group_ids,omitempty"`
	Endpoint               string            `json:"endpoint,omitempty"`
	AutoTuneState          string            `json:"auto_tune_state,omitempty"`
	Tags                   map[string]string `json:"tags,omitempty"`
}

// Summarise returns a summary of all OpenSearch domains.
func (o *OpenSearchClient) Summarise(ctx context.Context) ([]DomainSummary, error) {
	o.status("Listing OpenSearch domains...")

	// List all domain names
	listResp, err := o.client.ListDomainNames(ctx, &opensearch.ListDomainNamesInput{})
	if err != nil {
		return nil, err
	}

	if len(listResp.DomainNames) == 0 {
		o.status("No OpenSearch domains found")
		return []DomainSummary{}, nil
	}

	o.status("Found %d OpenSearch domains", len(listResp.DomainNames))

	// Get domain names for batch describe
	domainNames := make([]string, 0, len(listResp.DomainNames))
	for _, d := range listResp.DomainNames {
		domainNames = append(domainNames, aws.ToString(d.DomainName))
	}

	// Describe domains (can batch up to 5)
	var summaries []DomainSummary
	for i := 0; i < len(domainNames); i += 5 {
		end := i + 5
		if end > len(domainNames) {
			end = len(domainNames)
		}
		batch := domainNames[i:end]

		o.status("Describing domains %d-%d of %d", i+1, end, len(domainNames))
		descResp, err := o.client.DescribeDomains(ctx, &opensearch.DescribeDomainsInput{
			DomainNames: batch,
		})
		if err != nil {
			return nil, err
		}

		for _, domain := range descResp.DomainStatusList {
			summary := DomainSummary{
				DomainName:    aws.ToString(domain.DomainName),
				DomainId:      aws.ToString(domain.DomainId),
				Arn:           aws.ToString(domain.ARN),
				EngineVersion: aws.ToString(domain.EngineVersion),
				Endpoint:      aws.ToString(domain.Endpoint),
			}

			if domain.ClusterConfig != nil {
				summary.ClusterInstanceType = string(domain.ClusterConfig.InstanceType)
				if domain.ClusterConfig.InstanceCount != nil {
					summary.ClusterInstanceCount = int32(*domain.ClusterConfig.InstanceCount)
				}
				summary.DedicatedMasterEnabled = aws.ToBool(domain.ClusterConfig.DedicatedMasterEnabled)
				if summary.DedicatedMasterEnabled {
					summary.DedicatedMasterType = string(domain.ClusterConfig.DedicatedMasterType)
					if domain.ClusterConfig.DedicatedMasterCount != nil {
						summary.DedicatedMasterCount = int32(*domain.ClusterConfig.DedicatedMasterCount)
					}
				}
				summary.ZoneAwarenessEnabled = aws.ToBool(domain.ClusterConfig.ZoneAwarenessEnabled)
				summary.WarmEnabled = aws.ToBool(domain.ClusterConfig.WarmEnabled)
				if summary.WarmEnabled {
					summary.WarmType = string(domain.ClusterConfig.WarmType)
					if domain.ClusterConfig.WarmCount != nil {
						summary.WarmCount = int32(*domain.ClusterConfig.WarmCount)
					}
				}
			}

			if domain.EBSOptions != nil {
				summary.EBSEnabled = aws.ToBool(domain.EBSOptions.EBSEnabled)
				if summary.EBSEnabled {
					summary.VolumeType = string(domain.EBSOptions.VolumeType)
					if domain.EBSOptions.VolumeSize != nil {
						summary.VolumeSizeGB = int32(*domain.EBSOptions.VolumeSize)
					}
				}
			}

			if domain.EncryptionAtRestOptions != nil {
				summary.EncryptionAtRest = aws.ToBool(domain.EncryptionAtRestOptions.Enabled)
			}

			if domain.NodeToNodeEncryptionOptions != nil {
				summary.NodeToNodeEncryption = aws.ToBool(domain.NodeToNodeEncryptionOptions.Enabled)
			}

			if domain.DomainEndpointOptions != nil {
				summary.EnforceHTTPS = aws.ToBool(domain.DomainEndpointOptions.EnforceHTTPS)
			}

			if domain.VPCOptions != nil {
				summary.VPCEnabled = true
				summary.VpcId = aws.ToString(domain.VPCOptions.VPCId)
				summary.SubnetIds = domain.VPCOptions.SubnetIds
				summary.SecurityGroupIds = domain.VPCOptions.SecurityGroupIds
			}

			if domain.AutoTuneOptions != nil {
				summary.AutoTuneState = string(domain.AutoTuneOptions.State)
			}

			summaries = append(summaries, summary)
		}
	}

	// Fetch tags for each domain
	for i := range summaries {
		tagsResp, err := o.client.ListTags(ctx, &opensearch.ListTagsInput{
			ARN: aws.String(summaries[i].Arn),
		})
		if err != nil {
			return nil, fmt.Errorf("ListTags for %s: %w", summaries[i].DomainName, err)
		}
		summaries[i].Tags = tagsToMap(tagsResp.TagList)
	}

	return summaries, nil
}
