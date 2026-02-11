package awsclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront"
)

// CloudFrontClient wraps the AWS CloudFront client with snapshot capabilities.
type CloudFrontClient struct {
	client      *cloudfront.Client
	statusf     StatusFunc
	concurrency int
}

// CloudFrontOption is a functional option for configuring the CloudFrontClient.
type CloudFrontOption func(*cloudfrontOptions)

type cloudfrontOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithCloudFrontStatusFunc sets a callback for progress messages.
func WithCloudFrontStatusFunc(f StatusFunc) CloudFrontOption {
	return func(o *cloudfrontOptions) {
		o.statusf = f
	}
}

// WithCloudFrontConcurrency sets the maximum number of distributions to process in parallel.
func WithCloudFrontConcurrency(n int) CloudFrontOption {
	return func(o *cloudfrontOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// CloudFrontClient returns a CloudFrontClient configured with the given options.
func (c *Client) CloudFrontClient(opts ...CloudFrontOption) *CloudFrontClient {
	o := &cloudfrontOptions{
		concurrency: 10,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &CloudFrontClient{
		client:      cloudfront.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (cf *CloudFrontClient) status(format string, args ...any) {
	if cf.statusf != nil {
		cf.statusf(format, args...)
	}
}

// OriginSummary represents key attributes of a CloudFront origin.
type OriginSummary struct {
	Id                    string `json:"id"`
	DomainName            string `json:"domain_name"`
	OriginPath            string `json:"origin_path,omitempty"`
	S3OriginAccessId      string `json:"s3_origin_access_identity,omitempty"`
	OriginAccessControlId string `json:"origin_access_control_id,omitempty"`
	OriginProtocolPolicy  string `json:"origin_protocol_policy,omitempty"`
}

// CacheBehaviorSummary represents key attributes of a cache behavior.
type CacheBehaviorSummary struct {
	PathPattern           string   `json:"path_pattern,omitempty"`
	TargetOriginId        string   `json:"target_origin_id"`
	ViewerProtocolPolicy  string   `json:"viewer_protocol_policy"`
	AllowedMethods        []string `json:"allowed_methods,omitempty"`
	CachePolicyId         string   `json:"cache_policy_id,omitempty"`
	OriginRequestPolicyId string   `json:"origin_request_policy_id,omitempty"`
	Compress              bool     `json:"compress"`
	FunctionAssociations  []string `json:"function_associations,omitempty"`
}

// DistributionSummary represents key attributes of a CloudFront distribution.
type DistributionSummary struct {
	Id                     string                 `json:"id"`
	ARN                    string                 `json:"arn"`
	DomainName             string                 `json:"domain_name"`
	Aliases                []string               `json:"aliases,omitempty"`
	Status                 string                 `json:"status"`
	Enabled                bool                   `json:"enabled"`
	PriceClass             string                 `json:"price_class"`
	HttpVersion            string                 `json:"http_version"`
	IsIPV6Enabled          bool                   `json:"is_ipv6_enabled"`
	DefaultRootObject      string                 `json:"default_root_object,omitempty"`
	WebACLId               string                 `json:"web_acl_id,omitempty"`
	Origins                []OriginSummary        `json:"origins,omitempty"`
	DefaultCacheBehavior   *CacheBehaviorSummary  `json:"default_cache_behavior,omitempty"`
	CacheBehaviors         []CacheBehaviorSummary `json:"cache_behaviors,omitempty"`
	ViewerCertificate      string                 `json:"viewer_certificate,omitempty"`
	CertificateSource      string                 `json:"certificate_source,omitempty"`
	MinimumProtocolVersion string                 `json:"minimum_protocol_version,omitempty"`
	Logging                bool                   `json:"logging"`
	LoggingBucket          string                 `json:"logging_bucket,omitempty"`
	LoggingPrefix          string                 `json:"logging_prefix,omitempty"`
	Tags                   map[string]string      `json:"tags,omitempty"`
}

// Summarise returns a summary of all CloudFront distributions.
func (cf *CloudFrontClient) Summarise(ctx context.Context) ([]DistributionSummary, error) {
	cf.status("Listing CloudFront distributions...")

	// List all distributions
	var distributions []distInfo
	var marker *string
	for {
		resp, err := cf.client.ListDistributions(ctx, &cloudfront.ListDistributionsInput{
			Marker: marker,
		})
		if err != nil {
			return nil, err
		}

		if resp.DistributionList != nil {
			for _, dist := range resp.DistributionList.Items {
				distributions = append(distributions, distInfo{
					id:  aws.ToString(dist.Id),
					arn: aws.ToString(dist.ARN),
				})
			}

			if resp.DistributionList.IsTruncated != nil && *resp.DistributionList.IsTruncated {
				marker = resp.DistributionList.NextMarker
			} else {
				break
			}
		} else {
			break
		}
	}

	total := len(distributions)
	cf.status("Found %d CloudFront distributions, processing with concurrency %d", total, cf.concurrency)

	if total == 0 {
		return []DistributionSummary{}, nil
	}

	summaries := make([]DistributionSummary, total)
	var processed atomic.Int64
	var errMu sync.Mutex
	var firstErr error

	// Create work channel
	workCh := make(chan int, total)
	for i := range distributions {
		workCh <- i
	}
	close(workCh)

	// Process distributions concurrently
	var wg sync.WaitGroup
	for i := 0; i < cf.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				dist := distributions[idx]
				summary, err := cf.describeDistribution(ctx, dist)
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
				cf.status("[%d/%d] Processed distribution: %s", n, total, dist.id)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return summaries, nil
}

type distInfo struct {
	id  string
	arn string
}

func (cf *CloudFrontClient) describeDistribution(ctx context.Context, dist distInfo) (DistributionSummary, error) {
	summary := DistributionSummary{
		Id:  dist.id,
		ARN: dist.arn,
	}

	// Get distribution details
	resp, err := cf.client.GetDistribution(ctx, &cloudfront.GetDistributionInput{
		Id: &dist.id,
	})
	if err != nil {
		return DistributionSummary{}, fmt.Errorf("get distribution %s: %w", dist.id, err)
	}
	if resp.Distribution == nil {
		return summary, nil
	}

	d := resp.Distribution
	summary.DomainName = aws.ToString(d.DomainName)
	summary.Status = aws.ToString(d.Status)

	if d.DistributionConfig != nil {
		cfg := d.DistributionConfig
		summary.Enabled = aws.ToBool(cfg.Enabled)
		summary.PriceClass = string(cfg.PriceClass)
		summary.HttpVersion = string(cfg.HttpVersion)
		summary.IsIPV6Enabled = aws.ToBool(cfg.IsIPV6Enabled)
		summary.DefaultRootObject = aws.ToString(cfg.DefaultRootObject)
		summary.WebACLId = aws.ToString(cfg.WebACLId)

		// Aliases
		if cfg.Aliases != nil {
			summary.Aliases = cfg.Aliases.Items
		}

		// Origins
		if cfg.Origins != nil {
			for _, origin := range cfg.Origins.Items {
				o := OriginSummary{
					Id:         aws.ToString(origin.Id),
					DomainName: aws.ToString(origin.DomainName),
					OriginPath: aws.ToString(origin.OriginPath),
				}
				if origin.S3OriginConfig != nil {
					o.S3OriginAccessId = aws.ToString(origin.S3OriginConfig.OriginAccessIdentity)
				}
				if origin.OriginAccessControlId != nil {
					o.OriginAccessControlId = aws.ToString(origin.OriginAccessControlId)
				}
				if origin.CustomOriginConfig != nil {
					o.OriginProtocolPolicy = string(origin.CustomOriginConfig.OriginProtocolPolicy)
				}
				summary.Origins = append(summary.Origins, o)
			}
		}

		// Default cache behavior
		if cfg.DefaultCacheBehavior != nil {
			dcb := cfg.DefaultCacheBehavior
			summary.DefaultCacheBehavior = &CacheBehaviorSummary{
				TargetOriginId:        aws.ToString(dcb.TargetOriginId),
				ViewerProtocolPolicy:  string(dcb.ViewerProtocolPolicy),
				CachePolicyId:         aws.ToString(dcb.CachePolicyId),
				OriginRequestPolicyId: aws.ToString(dcb.OriginRequestPolicyId),
				Compress:              aws.ToBool(dcb.Compress),
			}
			if dcb.AllowedMethods != nil {
				for _, m := range dcb.AllowedMethods.Items {
					summary.DefaultCacheBehavior.AllowedMethods = append(summary.DefaultCacheBehavior.AllowedMethods, string(m))
				}
			}
			if dcb.FunctionAssociations != nil {
				for _, fa := range dcb.FunctionAssociations.Items {
					summary.DefaultCacheBehavior.FunctionAssociations = append(
						summary.DefaultCacheBehavior.FunctionAssociations,
						aws.ToString(fa.FunctionARN),
					)
				}
			}
		}

		// Cache behaviors
		if cfg.CacheBehaviors != nil {
			for _, cb := range cfg.CacheBehaviors.Items {
				behavior := CacheBehaviorSummary{
					PathPattern:           aws.ToString(cb.PathPattern),
					TargetOriginId:        aws.ToString(cb.TargetOriginId),
					ViewerProtocolPolicy:  string(cb.ViewerProtocolPolicy),
					CachePolicyId:         aws.ToString(cb.CachePolicyId),
					OriginRequestPolicyId: aws.ToString(cb.OriginRequestPolicyId),
					Compress:              aws.ToBool(cb.Compress),
				}
				if cb.AllowedMethods != nil {
					for _, m := range cb.AllowedMethods.Items {
						behavior.AllowedMethods = append(behavior.AllowedMethods, string(m))
					}
				}
				summary.CacheBehaviors = append(summary.CacheBehaviors, behavior)
			}
		}

		// Viewer certificate
		if cfg.ViewerCertificate != nil {
			vc := cfg.ViewerCertificate
			if vc.ACMCertificateArn != nil {
				summary.ViewerCertificate = aws.ToString(vc.ACMCertificateArn)
				summary.CertificateSource = "acm"
			} else if vc.IAMCertificateId != nil {
				summary.ViewerCertificate = aws.ToString(vc.IAMCertificateId)
				summary.CertificateSource = "iam"
			} else if vc.CloudFrontDefaultCertificate != nil && *vc.CloudFrontDefaultCertificate {
				summary.CertificateSource = "cloudfront"
			}
			summary.MinimumProtocolVersion = string(vc.MinimumProtocolVersion)
		}

		// Logging
		if cfg.Logging != nil && aws.ToBool(cfg.Logging.Enabled) {
			summary.Logging = true
			summary.LoggingBucket = aws.ToString(cfg.Logging.Bucket)
			summary.LoggingPrefix = aws.ToString(cfg.Logging.Prefix)
		}
	}

	// Get tags
	tagsResp, err := cf.client.ListTagsForResource(ctx, &cloudfront.ListTagsForResourceInput{
		Resource: &dist.arn,
	})
	if err != nil {
		return DistributionSummary{}, fmt.Errorf("list tags for distribution %s: %w", dist.id, err)
	}
	if tagsResp.Tags != nil {
		summary.Tags = tagsToMap(tagsResp.Tags.Items)
	}

	return summary, nil
}
