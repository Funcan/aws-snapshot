package awsclient

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
)

// ECRClient wraps the AWS ECR client with snapshot capabilities.
type ECRClient struct {
	client      *ecr.Client
	statusf     StatusFunc
	concurrency int
}

// ECROption is a functional option for configuring the ECRClient.
type ECROption func(*ecrOptions)

type ecrOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithECRStatusFunc sets a callback for progress messages.
func WithECRStatusFunc(f StatusFunc) ECROption {
	return func(o *ecrOptions) {
		o.statusf = f
	}
}

// WithECRConcurrency sets the maximum number of repositories to process in parallel.
func WithECRConcurrency(n int) ECROption {
	return func(o *ecrOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// ECRClient returns an ECRClient configured with the given options.
func (c *Client) ECRClient(opts ...ECROption) *ECRClient {
	o := &ecrOptions{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &ECRClient{
		client:      ecr.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (e *ECRClient) status(format string, args ...any) {
	if e.statusf != nil {
		e.statusf(format, args...)
	}
}

// RepositorySummary represents key attributes of an ECR repository.
type RepositorySummary struct {
	RepositoryName     string            `json:"repository_name"`
	RepositoryArn      string            `json:"repository_arn"`
	RepositoryUri      string            `json:"repository_uri"`
	CreatedAt          string            `json:"created_at,omitempty"`
	ImageTagMutability string            `json:"image_tag_mutability"`
	ScanOnPush         bool              `json:"scan_on_push"`
	EncryptionType     string            `json:"encryption_type,omitempty"`
	KmsKeyId           string            `json:"kms_key_id,omitempty"`
	LifecyclePolicy    string            `json:"lifecycle_policy,omitempty"`
	Tags               map[string]string `json:"tags,omitempty"`
}

// Summarise returns a summary of all ECR repositories.
func (e *ECRClient) Summarise(ctx context.Context) ([]RepositorySummary, error) {
	e.status("Listing ECR repositories...")

	// List all repositories
	var repos []repoInfo
	paginator := ecr.NewDescribeRepositoriesPaginator(e.client, &ecr.DescribeRepositoriesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, repo := range page.Repositories {
			repos = append(repos, repoInfo{
				name: aws.ToString(repo.RepositoryName),
				arn:  aws.ToString(repo.RepositoryArn),
				uri:  aws.ToString(repo.RepositoryUri),
			})
		}
	}

	total := len(repos)
	e.status("Found %d ECR repositories, processing with concurrency %d", total, e.concurrency)

	if total == 0 {
		return []RepositorySummary{}, nil
	}

	summaries := make([]RepositorySummary, total)
	var processed atomic.Int64

	// Create work channel
	workCh := make(chan int, total)
	for i := range repos {
		workCh <- i
	}
	close(workCh)

	// Process repositories concurrently
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

				repo := repos[idx]
				summaries[idx] = e.describeRepository(ctx, repo)

				n := processed.Add(1)
				e.status("[%d/%d] Processed repository: %s", n, total, repo.name)
			}
		}()
	}

	wg.Wait()

	// Filter out empty summaries (errors)
	result := make([]RepositorySummary, 0, total)
	for _, s := range summaries {
		if s.RepositoryName != "" {
			result = append(result, s)
		}
	}

	return result, nil
}

type repoInfo struct {
	name string
	arn  string
	uri  string
}

func (e *ECRClient) describeRepository(ctx context.Context, repo repoInfo) RepositorySummary {
	summary := RepositorySummary{
		RepositoryName: repo.name,
		RepositoryArn:  repo.arn,
		RepositoryUri:  repo.uri,
	}

	// Get full repository details
	descResp, err := e.client.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{
		RepositoryNames: []string{repo.name},
	})
	if err == nil && len(descResp.Repositories) > 0 {
		r := descResp.Repositories[0]
		if r.CreatedAt != nil {
			summary.CreatedAt = r.CreatedAt.Format("2006-01-02T15:04:05Z")
		}
		summary.ImageTagMutability = string(r.ImageTagMutability)
		if r.ImageScanningConfiguration != nil {
			summary.ScanOnPush = r.ImageScanningConfiguration.ScanOnPush
		}
		if r.EncryptionConfiguration != nil {
			summary.EncryptionType = string(r.EncryptionConfiguration.EncryptionType)
			summary.KmsKeyId = aws.ToString(r.EncryptionConfiguration.KmsKey)
		}
	}

	// Get lifecycle policy
	lifecycleResp, err := e.client.GetLifecyclePolicy(ctx, &ecr.GetLifecyclePolicyInput{
		RepositoryName: &repo.name,
	})
	if err == nil {
		summary.LifecyclePolicy = aws.ToString(lifecycleResp.LifecyclePolicyText)
	}

	// Get tags
	tagsResp, err := e.client.ListTagsForResource(ctx, &ecr.ListTagsForResourceInput{
		ResourceArn: &repo.arn,
	})
	if err == nil && len(tagsResp.Tags) > 0 {
		summary.Tags = make(map[string]string)
		for _, tag := range tagsResp.Tags {
			summary.Tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
		}
	}

	return summary
}
