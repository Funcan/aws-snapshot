package awsclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

// LambdaClient wraps the AWS Lambda client with snapshot capabilities.
type LambdaClient struct {
	client      *lambda.Client
	statusf     StatusFunc
	concurrency int
}

// LambdaOption is a functional option for configuring the LambdaClient.
type LambdaOption func(*lambdaOptions)

type lambdaOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithLambdaStatusFunc sets a callback for progress messages.
func WithLambdaStatusFunc(f StatusFunc) LambdaOption {
	return func(o *lambdaOptions) {
		o.statusf = f
	}
}

// WithLambdaConcurrency sets the maximum number of functions to process in parallel.
func WithLambdaConcurrency(n int) LambdaOption {
	return func(o *lambdaOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// LambdaClient returns a LambdaClient configured with the given options.
func (c *Client) LambdaClient(opts ...LambdaOption) *LambdaClient {
	o := &lambdaOptions{
		concurrency: 10,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &LambdaClient{
		client:      lambda.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (l *LambdaClient) status(format string, args ...any) {
	if l.statusf != nil {
		l.statusf(format, args...)
	}
}

// FunctionSummary represents key attributes of a Lambda function.
type FunctionSummary struct {
	FunctionName     string            `json:"function_name"`
	FunctionArn      string            `json:"function_arn"`
	Runtime          string            `json:"runtime,omitempty"`
	Handler          string            `json:"handler,omitempty"`
	CodeSize         int64             `json:"code_size"`
	Description      string            `json:"description,omitempty"`
	Timeout          int32             `json:"timeout"`
	MemorySize       int32             `json:"memory_size"`
	EphemeralStorage int32             `json:"ephemeral_storage"`
	LastModified     string            `json:"last_modified"`
	Role             string            `json:"role"`
	VpcId            string            `json:"vpc_id,omitempty"`
	SubnetIds        []string          `json:"subnet_ids,omitempty"`
	SecurityGroupIds []string          `json:"security_group_ids,omitempty"`
	Environment      map[string]string `json:"environment,omitempty"`
	Architectures    []string          `json:"architectures,omitempty"`
	PackageType      string            `json:"package_type"`
	State            string            `json:"state,omitempty"`
	TracingMode      string            `json:"tracing_mode,omitempty"`
	Layers           []string          `json:"layers,omitempty"`
	LoggingFormat    string            `json:"logging_format,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
}

// Summarise returns a summary of all Lambda functions.
func (l *LambdaClient) Summarise(ctx context.Context) ([]FunctionSummary, error) {
	l.status("Listing Lambda functions...")

	// List all functions
	var functions []string
	paginator := lambda.NewListFunctionsPaginator(l.client, &lambda.ListFunctionsInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, fn := range page.Functions {
			functions = append(functions, aws.ToString(fn.FunctionName))
		}
	}

	total := len(functions)
	l.status("Found %d Lambda functions, processing with concurrency %d", total, l.concurrency)

	if total == 0 {
		return []FunctionSummary{}, nil
	}

	summaries := make([]FunctionSummary, total)
	var processed atomic.Int64
	var errMu sync.Mutex
	var firstErr error

	// Create work channel
	workCh := make(chan int, total)
	for i := range functions {
		workCh <- i
	}
	close(workCh)

	// Process functions concurrently
	var wg sync.WaitGroup
	for i := 0; i < l.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				name := functions[idx]
				summary, err := l.describeFunction(ctx, name)
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
				l.status("[%d/%d] Processed function: %s", n, total, name)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return summaries, nil
}

func (l *LambdaClient) describeFunction(ctx context.Context, functionName string) (FunctionSummary, error) {
	resp, err := l.client.GetFunction(ctx, &lambda.GetFunctionInput{
		FunctionName: &functionName,
	})
	if err != nil {
		return FunctionSummary{}, fmt.Errorf("get function %s: %w", functionName, err)
	}

	summary := FunctionSummary{FunctionName: functionName}

	cfg := resp.Configuration
	summary.FunctionArn = aws.ToString(cfg.FunctionArn)
	summary.Runtime = string(cfg.Runtime)
	summary.Handler = aws.ToString(cfg.Handler)
	summary.CodeSize = cfg.CodeSize
	summary.Description = aws.ToString(cfg.Description)
	summary.Timeout = aws.ToInt32(cfg.Timeout)
	summary.MemorySize = aws.ToInt32(cfg.MemorySize)
	summary.LastModified = aws.ToString(cfg.LastModified)
	summary.Role = aws.ToString(cfg.Role)
	summary.PackageType = string(cfg.PackageType)
	summary.State = string(cfg.State)

	if cfg.EphemeralStorage != nil {
		summary.EphemeralStorage = aws.ToInt32(cfg.EphemeralStorage.Size)
	}

	if cfg.VpcConfig != nil {
		summary.VpcId = aws.ToString(cfg.VpcConfig.VpcId)
		summary.SubnetIds = cfg.VpcConfig.SubnetIds
		summary.SecurityGroupIds = cfg.VpcConfig.SecurityGroupIds
	}

	if cfg.Environment != nil && cfg.Environment.Variables != nil {
		// Only include variable names, not values (security)
		summary.Environment = make(map[string]string)
		for k := range cfg.Environment.Variables {
			summary.Environment[k] = "[REDACTED]"
		}
	}

	for _, arch := range cfg.Architectures {
		summary.Architectures = append(summary.Architectures, string(arch))
	}

	if cfg.TracingConfig != nil {
		summary.TracingMode = string(cfg.TracingConfig.Mode)
	}

	for _, layer := range cfg.Layers {
		summary.Layers = append(summary.Layers, aws.ToString(layer.Arn))
	}

	if cfg.LoggingConfig != nil {
		summary.LoggingFormat = string(cfg.LoggingConfig.LogFormat)
	}

	// Get tags
	summary.Tags = resp.Tags

	return summary, nil
}
