package awsclient

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// StatusFunc is a callback for reporting progress.
type StatusFunc func(format string, args ...any)

// S3Client wraps the AWS S3 client with snapshot capabilities.
type S3Client struct {
	cfg              aws.Config
	client           *s3.Client
	includeNormal    bool
	includeDirectory bool
	statusf          StatusFunc
	concurrency      int
}

// S3Option is a functional option for configuring the S3Client.
type S3Option func(*s3Options)

type s3Options struct {
	includeNormal    bool
	includeDirectory bool
	statusf          StatusFunc
	concurrency      int
}

// WithoutNormalBuckets disables listing of normal S3 buckets.
func WithoutNormalBuckets() S3Option {
	return func(o *s3Options) {
		o.includeNormal = false
	}
}

// WithoutDirectoryBuckets disables listing of directory buckets.
func WithoutDirectoryBuckets() S3Option {
	return func(o *s3Options) {
		o.includeDirectory = false
	}
}

// WithStatusFunc sets a callback for progress messages.
func WithStatusFunc(f StatusFunc) S3Option {
	return func(o *s3Options) {
		o.statusf = f
	}
}

// WithConcurrency sets the maximum number of buckets to process in parallel.
func WithConcurrency(n int) S3Option {
	return func(o *s3Options) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// S3Client returns an S3Client configured with the given options.
func (c *Client) S3Client(opts ...S3Option) *S3Client {
	o := &s3Options{
		includeNormal:    true,
		includeDirectory: true,
		concurrency:      50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &S3Client{
		cfg:              c.cfg,
		client:           s3.NewFromConfig(c.cfg),
		includeNormal:    o.includeNormal,
		includeDirectory: o.includeDirectory,
		statusf:          o.statusf,
		concurrency:      o.concurrency,
	}
}

func (s *S3Client) status(format string, args ...any) {
	if s.statusf != nil {
		s.statusf(format, args...)
	}
}

// BucketSummary represents key attributes of an S3 bucket.
type BucketSummary struct {
	Name            string          `json:"name"`
	CreationDate    *time.Time      `json:"creation_date,omitempty"`
	Region          string          `json:"region,omitempty"`
	BucketType      string          `json:"bucket_type"` // "general-purpose" or "directory"
	VersioningState string          `json:"versioning_state,omitempty"`
	LifecycleRules  []LifecycleRule `json:"lifecycle_rules,omitempty"`
}

// LifecycleRule represents an S3 lifecycle expiration rule.
type LifecycleRule struct {
	ID                              string `json:"id,omitempty"`
	Prefix                          string `json:"prefix,omitempty"`
	Status                          string `json:"status"`
	ExpirationDays                  *int32 `json:"expiration_days,omitempty"`
	NoncurrentVersionExpirationDays *int32 `json:"noncurrent_version_expiration_days,omitempty"`
}

// Summarise returns a summary of all S3 buckets based on configured options.
func (s *S3Client) Summarise(ctx context.Context) ([]BucketSummary, error) {
	var summaries []BucketSummary

	if s.includeNormal {
		buckets, err := s.listNormalBuckets(ctx)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, buckets...)
	}

	if s.includeDirectory {
		buckets, err := s.listDirectoryBuckets(ctx)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, buckets...)
	}

	return summaries, nil
}

func (s *S3Client) listNormalBuckets(ctx context.Context) ([]BucketSummary, error) {
	s.status("Listing general-purpose buckets...")
	resp, err := s.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	total := len(resp.Buckets)
	s.status("Found %d general-purpose buckets, processing with concurrency %d", total, s.concurrency)

	summaries := make([]BucketSummary, total)
	var processed atomic.Int64

	// Create work channel and semaphore
	type work struct {
		index  int
		bucket string
	}
	workCh := make(chan work, total)
	for i, bucket := range resp.Buckets {
		workCh <- work{index: i, bucket: aws.ToString(bucket.Name)}
	}
	close(workCh)

	// Process buckets concurrently
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for i := 0; i < s.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for w := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				summary, err := s.processBucket(ctx, w.bucket, resp.Buckets[w.index].CreationDate)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("processing bucket %s: %w", w.bucket, err)
					}
					errMu.Unlock()
					continue
				}
				summaries[w.index] = summary

				n := processed.Add(1)
				s.status("[%d/%d] Processed bucket: %s", n, total, w.bucket)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	// Filter out unprocessed summaries (context cancellation)
	result := make([]BucketSummary, 0, total)
	for _, sum := range summaries {
		if sum.Name != "" {
			result = append(result, sum)
		}
	}

	return result, nil
}

func (s *S3Client) processBucket(ctx context.Context, bucketName string, creationDate *time.Time) (BucketSummary, error) {
	summary := BucketSummary{
		Name:         bucketName,
		CreationDate: creationDate,
		BucketType:   "general-purpose",
	}

	// Get bucket location
	locResp, err := s.client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: &bucketName,
	})
	if err != nil {
		return summary, fmt.Errorf("GetBucketLocation: %w", err)
	}
	region := string(locResp.LocationConstraint)
	if region == "" {
		region = "us-east-1" // Default region returns empty
	}
	summary.Region = region

	// Create a region-specific client for this bucket
	regionCfg := s.cfg.Copy()
	regionCfg.Region = region
	regionClient := s3.NewFromConfig(regionCfg)

	// Get versioning status
	verResp, err := regionClient.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
		Bucket: &bucketName,
	})
	if err != nil {
		return summary, fmt.Errorf("GetBucketVersioning: %w", err)
	}
	summary.VersioningState = string(verResp.Status)
	if summary.VersioningState == "" {
		summary.VersioningState = "disabled"
	}

	// Get lifecycle rules (NoSuchLifecycleConfiguration is expected if not configured)
	lcResp, err := regionClient.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{
		Bucket: &bucketName,
	})
	if err != nil {
		// NoSuchLifecycleConfiguration is expected when no lifecycle is configured
		if !strings.Contains(err.Error(), "NoSuchLifecycleConfiguration") {
			return summary, fmt.Errorf("GetBucketLifecycleConfiguration: %w", err)
		}
	} else if lcResp.Rules != nil {
		for _, rule := range lcResp.Rules {
			lr := LifecycleRule{
				ID:     aws.ToString(rule.ID),
				Status: string(rule.Status),
			}
			if rule.Filter != nil && rule.Filter.Prefix != nil {
				lr.Prefix = *rule.Filter.Prefix
			}
			if rule.Expiration != nil && rule.Expiration.Days != nil {
				lr.ExpirationDays = rule.Expiration.Days
			}
			if rule.NoncurrentVersionExpiration != nil && rule.NoncurrentVersionExpiration.NoncurrentDays != nil {
				lr.NoncurrentVersionExpirationDays = rule.NoncurrentVersionExpiration.NoncurrentDays
			}
			summary.LifecycleRules = append(summary.LifecycleRules, lr)
		}
	}

	return summary, nil
}

func (s *S3Client) listDirectoryBuckets(ctx context.Context) ([]BucketSummary, error) {
	s.status("Listing directory buckets...")
	var summaries []BucketSummary
	var continuationToken *string

	for {
		resp, err := s.client.ListDirectoryBuckets(ctx, &s3.ListDirectoryBucketsInput{
			ContinuationToken: continuationToken,
		})
		if err != nil {
			// Directory buckets may not be available in all regions/accounts
			s.status("Directory buckets not available or no permission")
			return summaries, nil
		}

		for _, bucket := range resp.Buckets {
			bucketName := aws.ToString(bucket.Name)
			s.status("Processing directory bucket: %s", bucketName)
			summary := BucketSummary{
				Name:       bucketName,
				BucketType: "directory",
			}

			// Extract region from bucket name (directory bucket names contain AZ info)
			if bucket.Name != nil {
				summary.Region = extractRegionFromDirectoryBucket(*bucket.Name)
			}

			summaries = append(summaries, summary)
		}

		if resp.ContinuationToken == nil {
			break
		}
		continuationToken = resp.ContinuationToken
	}

	return summaries, nil
}

// extractRegionFromDirectoryBucket attempts to extract region from directory bucket name.
// Directory bucket names follow pattern: bucket-name--azid--x-s3
func extractRegionFromDirectoryBucket(name string) string {
	// Directory bucket names encode the AZ, but not directly the region
	// Return empty and let caller determine region if needed
	return ""
}

// BucketRegion is a helper type for bucket region responses.
type BucketRegion = types.BucketLocationConstraint
