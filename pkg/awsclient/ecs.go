package awsclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

// ECSClient wraps the AWS ECS client with snapshot capabilities.
type ECSClient struct {
	client      *ecs.Client
	statusf     StatusFunc
	concurrency int
}

// ECSOption is a functional option for configuring the ECSClient.
type ECSOption func(*ecsOptions)

type ecsOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithECSStatusFunc sets a callback for progress messages.
func WithECSStatusFunc(f StatusFunc) ECSOption {
	return func(o *ecsOptions) {
		o.statusf = f
	}
}

// WithECSConcurrency sets the maximum number of clusters to process in parallel.
func WithECSConcurrency(n int) ECSOption {
	return func(o *ecsOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// ECSClient returns an ECSClient configured with the given options.
func (c *Client) ECSClient(opts ...ECSOption) *ECSClient {
	o := &ecsOptions{
		concurrency: 10,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &ECSClient{
		client:      ecs.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (e *ECSClient) status(format string, args ...any) {
	if e.statusf != nil {
		e.statusf(format, args...)
	}
}

// ServiceSummary represents key attributes of an ECS service.
type ServiceSummary struct {
	ServiceName          string   `json:"service_name"`
	ServiceArn           string   `json:"service_arn"`
	Status               string   `json:"status"`
	TaskDefinition       string   `json:"task_definition"`
	DesiredCount         int32    `json:"desired_count"`
	LaunchType           string   `json:"launch_type,omitempty"`
	CapacityProviders    []string `json:"capacity_providers,omitempty"`
	PlatformVersion      string   `json:"platform_version,omitempty"`
	EnableExecuteCmd     bool     `json:"enable_execute_command"`
	SchedulingStrategy   string   `json:"scheduling_strategy"`
	DeploymentController string   `json:"deployment_controller"`
	LoadBalancers        []string `json:"load_balancers,omitempty"`
}

// ClusterSummary represents key attributes of an ECS cluster.
type ECSClusterSummary struct {
	ClusterName                     string            `json:"cluster_name"`
	ClusterArn                      string            `json:"cluster_arn"`
	Status                          string            `json:"status"`
	CapacityProviders               []string          `json:"capacity_providers,omitempty"`
	DefaultCapacityProviderStrategy []string          `json:"default_capacity_provider_strategy,omitempty"`
	Settings                        map[string]string `json:"settings,omitempty"`
	Services                        []ServiceSummary  `json:"services,omitempty"`
}

// ECSSummary represents the complete ECS snapshot.
type ECSSummary struct {
	Clusters []ECSClusterSummary `json:"clusters,omitempty"`
}

// Summarise returns a summary of all ECS clusters and their services.
func (e *ECSClient) Summarise(ctx context.Context) (*ECSSummary, error) {
	e.status("Listing ECS clusters...")

	// List all cluster ARNs
	var clusterArns []string
	paginator := ecs.NewListClustersPaginator(e.client, &ecs.ListClustersInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		clusterArns = append(clusterArns, page.ClusterArns...)
	}

	total := len(clusterArns)
	e.status("Found %d ECS clusters, processing with concurrency %d", total, e.concurrency)

	if total == 0 {
		return &ECSSummary{Clusters: []ECSClusterSummary{}}, nil
	}

	clusters := make([]ECSClusterSummary, total)
	var processed atomic.Int64
	var errMu sync.Mutex
	var firstErr error

	// Create work channel
	workCh := make(chan int, total)
	for i := range clusterArns {
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

				arn := clusterArns[idx]
				summary, err := e.describeCluster(ctx, arn)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					errMu.Unlock()
					continue
				}
				clusters[idx] = summary

				n := processed.Add(1)
				e.status("[%d/%d] Processed cluster: %s", n, total, summary.ClusterName)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return &ECSSummary{Clusters: clusters}, nil
}

func (e *ECSClient) describeCluster(ctx context.Context, clusterArn string) (ECSClusterSummary, error) {
	// Describe the cluster
	descResp, err := e.client.DescribeClusters(ctx, &ecs.DescribeClustersInput{
		Clusters: []string{clusterArn},
		Include:  []types.ClusterField{types.ClusterFieldSettings, types.ClusterFieldConfigurations},
	})
	if err != nil {
		return ECSClusterSummary{}, fmt.Errorf("describe cluster %s: %w", clusterArn, err)
	}
	if len(descResp.Clusters) == 0 {
		return ECSClusterSummary{}, nil
	}

	cluster := descResp.Clusters[0]
	summary := ECSClusterSummary{}
	summary.ClusterName = aws.ToString(cluster.ClusterName)
	summary.ClusterArn = aws.ToString(cluster.ClusterArn)
	summary.Status = aws.ToString(cluster.Status)
	summary.CapacityProviders = cluster.CapacityProviders

	for _, strategy := range cluster.DefaultCapacityProviderStrategy {
		summary.DefaultCapacityProviderStrategy = append(
			summary.DefaultCapacityProviderStrategy,
			aws.ToString(strategy.CapacityProvider),
		)
	}

	if len(cluster.Settings) > 0 {
		summary.Settings = make(map[string]string)
		for _, setting := range cluster.Settings {
			summary.Settings[string(setting.Name)] = aws.ToString(setting.Value)
		}
	}

	// Get services for this cluster
	services, err := e.listServices(ctx, clusterArn)
	if err != nil {
		return ECSClusterSummary{}, err
	}
	summary.Services = services

	return summary, nil
}

func (e *ECSClient) listServices(ctx context.Context, clusterArn string) ([]ServiceSummary, error) {
	var serviceArns []string
	paginator := ecs.NewListServicesPaginator(e.client, &ecs.ListServicesInput{
		Cluster: &clusterArn,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list services for cluster %s: %w", clusterArn, err)
		}
		serviceArns = append(serviceArns, page.ServiceArns...)
	}

	if len(serviceArns) == 0 {
		return nil, nil
	}

	// Describe services in batches of 10 (AWS limit)
	var services []ServiceSummary
	for i := 0; i < len(serviceArns); i += 10 {
		end := i + 10
		if end > len(serviceArns) {
			end = len(serviceArns)
		}
		batch := serviceArns[i:end]

		descResp, err := e.client.DescribeServices(ctx, &ecs.DescribeServicesInput{
			Cluster:  &clusterArn,
			Services: batch,
		})
		if err != nil {
			return nil, fmt.Errorf("describe services for cluster %s: %w", clusterArn, err)
		}

		for _, svc := range descResp.Services {
			summary := ServiceSummary{
				ServiceName:        aws.ToString(svc.ServiceName),
				ServiceArn:         aws.ToString(svc.ServiceArn),
				Status:             aws.ToString(svc.Status),
				TaskDefinition:     aws.ToString(svc.TaskDefinition),
				DesiredCount:       svc.DesiredCount,
				LaunchType:         string(svc.LaunchType),
				PlatformVersion:    aws.ToString(svc.PlatformVersion),
				EnableExecuteCmd:   svc.EnableExecuteCommand,
				SchedulingStrategy: string(svc.SchedulingStrategy),
			}

			if svc.DeploymentController != nil {
				summary.DeploymentController = string(svc.DeploymentController.Type)
			}

			for _, cp := range svc.CapacityProviderStrategy {
				summary.CapacityProviders = append(summary.CapacityProviders, aws.ToString(cp.CapacityProvider))
			}

			for _, lb := range svc.LoadBalancers {
				if lb.TargetGroupArn != nil {
					summary.LoadBalancers = append(summary.LoadBalancers, aws.ToString(lb.TargetGroupArn))
				}
			}

			services = append(services, summary)
		}
	}

	return services, nil
}
