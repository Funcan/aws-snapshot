package awsclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/apigateway"
	"github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
)

// APIGatewayClient wraps the AWS API Gateway clients with snapshot capabilities.
type APIGatewayClient struct {
	v1Client    *apigateway.Client
	v2Client    *apigatewayv2.Client
	statusf     StatusFunc
	concurrency int
}

// APIGatewayOption is a functional option for configuring the APIGatewayClient.
type APIGatewayOption func(*apigatewayOptions)

type apigatewayOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithAPIGatewayStatusFunc sets a callback for progress messages.
func WithAPIGatewayStatusFunc(f StatusFunc) APIGatewayOption {
	return func(o *apigatewayOptions) {
		o.statusf = f
	}
}

// WithAPIGatewayConcurrency sets the maximum number of APIs to process in parallel.
func WithAPIGatewayConcurrency(n int) APIGatewayOption {
	return func(o *apigatewayOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// APIGatewayClient returns an APIGatewayClient configured with the given options.
func (c *Client) APIGatewayClient(opts ...APIGatewayOption) *APIGatewayClient {
	o := &apigatewayOptions{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &APIGatewayClient{
		v1Client:    apigateway.NewFromConfig(c.cfg),
		v2Client:    apigatewayv2.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (a *APIGatewayClient) status(format string, args ...any) {
	if a.statusf != nil {
		a.statusf(format, args...)
	}
}

// RestAPIStageSummary represents key attributes of a REST API stage.
type RestAPIStageSummary struct {
	StageName           string            `json:"stage_name"`
	DeploymentId        string            `json:"deployment_id,omitempty"`
	Description         string            `json:"description,omitempty"`
	CacheClusterEnabled bool              `json:"cache_cluster_enabled"`
	CacheClusterSize    string            `json:"cache_cluster_size,omitempty"`
	TracingEnabled      bool              `json:"tracing_enabled"`
	Tags                map[string]string `json:"tags,omitempty"`
}

// RestAPISummary represents key attributes of a REST API.
type RestAPISummary struct {
	Id                string                `json:"id"`
	Name              string                `json:"name"`
	Description       string                `json:"description,omitempty"`
	ApiKeySource      string                `json:"api_key_source,omitempty"`
	EndpointConfig    []string              `json:"endpoint_types,omitempty"`
	DisableExecuteApi bool                  `json:"disable_execute_api_endpoint"`
	Stages            []RestAPIStageSummary `json:"stages,omitempty"`
	Tags              map[string]string     `json:"tags,omitempty"`
}

// HttpAPIStageSummary represents key attributes of an HTTP API stage.
type HttpAPIStageSummary struct {
	StageName   string            `json:"stage_name"`
	AutoDeploy  bool              `json:"auto_deploy"`
	Description string            `json:"description,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
}

// HttpAPISummary represents key attributes of an HTTP API.
type HttpAPISummary struct {
	ApiId                     string                `json:"api_id"`
	Name                      string                `json:"name"`
	Description               string                `json:"description,omitempty"`
	ProtocolType              string                `json:"protocol_type"`
	ApiEndpoint               string                `json:"api_endpoint,omitempty"`
	DisableExecuteApiEndpoint bool                  `json:"disable_execute_api_endpoint"`
	CorsConfiguration         bool                  `json:"cors_enabled"`
	Stages                    []HttpAPIStageSummary `json:"stages,omitempty"`
	Tags                      map[string]string     `json:"tags,omitempty"`
}

// APIGatewaySummary represents the complete API Gateway snapshot.
type APIGatewaySummary struct {
	RestAPIs []RestAPISummary `json:"rest_apis,omitempty"`
	HttpAPIs []HttpAPISummary `json:"http_apis,omitempty"`
}

// Summarise returns a summary of all API Gateway APIs.
func (a *APIGatewayClient) Summarise(ctx context.Context) (*APIGatewaySummary, error) {
	summary := &APIGatewaySummary{}

	// Get REST APIs
	restAPIs, err := a.listRestAPIs(ctx)
	if err != nil {
		return nil, err
	}
	summary.RestAPIs = restAPIs

	// Get HTTP APIs
	httpAPIs, err := a.listHttpAPIs(ctx)
	if err != nil {
		return nil, err
	}
	summary.HttpAPIs = httpAPIs

	return summary, nil
}

func (a *APIGatewayClient) listRestAPIs(ctx context.Context) ([]RestAPISummary, error) {
	a.status("Listing REST APIs...")

	var apis []restAPIInfo
	var position *string
	for {
		resp, err := a.v1Client.GetRestApis(ctx, &apigateway.GetRestApisInput{
			Position: position,
		})
		if err != nil {
			return nil, err
		}

		for _, api := range resp.Items {
			apis = append(apis, restAPIInfo{
				id:   aws.ToString(api.Id),
				name: aws.ToString(api.Name),
			})
		}

		if resp.Position == nil {
			break
		}
		position = resp.Position
	}

	total := len(apis)
	a.status("Found %d REST APIs, processing with concurrency %d", total, a.concurrency)

	if total == 0 {
		return []RestAPISummary{}, nil
	}

	summaries := make([]RestAPISummary, total)
	var processed atomic.Int64
	var errMu sync.Mutex
	var firstErr error

	// Create work channel
	workCh := make(chan int, total)
	for i := range apis {
		workCh <- i
	}
	close(workCh)

	// Process APIs concurrently
	var wg sync.WaitGroup
	for i := 0; i < a.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				api := apis[idx]
				summary, err := a.describeRestAPI(ctx, api)
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
				a.status("[%d/%d] Processed REST API: %s", n, total, api.name)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return summaries, nil
}

type restAPIInfo struct {
	id   string
	name string
}

func (a *APIGatewayClient) describeRestAPI(ctx context.Context, api restAPIInfo) (RestAPISummary, error) {
	summary := RestAPISummary{
		Id:   api.id,
		Name: api.name,
	}

	// Get API details
	resp, err := a.v1Client.GetRestApi(ctx, &apigateway.GetRestApiInput{
		RestApiId: &api.id,
	})
	if err != nil {
		return RestAPISummary{}, fmt.Errorf("get REST API %s: %w", api.name, err)
	}

	summary.Description = aws.ToString(resp.Description)
	summary.ApiKeySource = string(resp.ApiKeySource)
	summary.DisableExecuteApi = resp.DisableExecuteApiEndpoint
	summary.Tags = resp.Tags

	if resp.EndpointConfiguration != nil {
		for _, t := range resp.EndpointConfiguration.Types {
			summary.EndpointConfig = append(summary.EndpointConfig, string(t))
		}
	}

	// Get stages
	stagesResp, err := a.v1Client.GetStages(ctx, &apigateway.GetStagesInput{
		RestApiId: &api.id,
	})
	if err != nil {
		return RestAPISummary{}, fmt.Errorf("get stages for REST API %s: %w", api.name, err)
	}
	for _, stage := range stagesResp.Item {
		s := RestAPIStageSummary{
			StageName:           aws.ToString(stage.StageName),
			DeploymentId:        aws.ToString(stage.DeploymentId),
			Description:         aws.ToString(stage.Description),
			CacheClusterEnabled: stage.CacheClusterEnabled,
			CacheClusterSize:    string(stage.CacheClusterSize),
			TracingEnabled:      stage.TracingEnabled,
			Tags:                stage.Tags,
		}
		summary.Stages = append(summary.Stages, s)
	}

	return summary, nil
}

func (a *APIGatewayClient) listHttpAPIs(ctx context.Context) ([]HttpAPISummary, error) {
	a.status("Listing HTTP APIs...")

	var apis []httpAPIInfo
	var nextToken *string
	for {
		resp, err := a.v2Client.GetApis(ctx, &apigatewayv2.GetApisInput{
			NextToken: nextToken,
		})
		if err != nil {
			return nil, err
		}

		for _, api := range resp.Items {
			apis = append(apis, httpAPIInfo{
				id:   aws.ToString(api.ApiId),
				name: aws.ToString(api.Name),
			})
		}

		if resp.NextToken == nil {
			break
		}
		nextToken = resp.NextToken
	}

	total := len(apis)
	a.status("Found %d HTTP APIs, processing with concurrency %d", total, a.concurrency)

	if total == 0 {
		return []HttpAPISummary{}, nil
	}

	summaries := make([]HttpAPISummary, total)
	var processed atomic.Int64
	var errMu sync.Mutex
	var firstErr error

	// Create work channel
	workCh := make(chan int, total)
	for i := range apis {
		workCh <- i
	}
	close(workCh)

	// Process APIs concurrently
	var wg sync.WaitGroup
	for i := 0; i < a.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				api := apis[idx]
				summary, err := a.describeHttpAPI(ctx, api)
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
				a.status("[%d/%d] Processed HTTP API: %s", n, total, api.name)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return summaries, nil
}

type httpAPIInfo struct {
	id   string
	name string
}

func (a *APIGatewayClient) describeHttpAPI(ctx context.Context, api httpAPIInfo) (HttpAPISummary, error) {
	summary := HttpAPISummary{
		ApiId: api.id,
		Name:  api.name,
	}

	// Get API details
	resp, err := a.v2Client.GetApi(ctx, &apigatewayv2.GetApiInput{
		ApiId: &api.id,
	})
	if err != nil {
		return HttpAPISummary{}, fmt.Errorf("get HTTP API %s: %w", api.name, err)
	}

	summary.Description = aws.ToString(resp.Description)
	summary.ProtocolType = string(resp.ProtocolType)
	summary.ApiEndpoint = aws.ToString(resp.ApiEndpoint)
	summary.DisableExecuteApiEndpoint = aws.ToBool(resp.DisableExecuteApiEndpoint)
	summary.CorsConfiguration = resp.CorsConfiguration != nil
	summary.Tags = resp.Tags

	// Get stages
	stagesResp, err := a.v2Client.GetStages(ctx, &apigatewayv2.GetStagesInput{
		ApiId: &api.id,
	})
	if err != nil {
		return HttpAPISummary{}, fmt.Errorf("get stages for HTTP API %s: %w", api.name, err)
	}
	for _, stage := range stagesResp.Items {
		s := HttpAPIStageSummary{
			StageName:   aws.ToString(stage.StageName),
			AutoDeploy:  aws.ToBool(stage.AutoDeploy),
			Description: aws.ToString(stage.Description),
			Tags:        stage.Tags,
		}
		summary.Stages = append(summary.Stages, s)
	}

	return summary, nil
}
