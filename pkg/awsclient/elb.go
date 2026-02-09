package awsclient

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2 "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
)

// ELBClient wraps the AWS ELB client with snapshot capabilities.
type ELBClient struct {
	v2Client    *elbv2.Client
	statusf     StatusFunc
	concurrency int
}

// ELBOption is a functional option for configuring the ELBClient.
type ELBOption func(*elbOptions)

type elbOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithELBStatusFunc sets a callback for progress messages.
func WithELBStatusFunc(f StatusFunc) ELBOption {
	return func(o *elbOptions) {
		o.statusf = f
	}
}

// WithELBConcurrency sets the maximum number of load balancers to process in parallel.
func WithELBConcurrency(n int) ELBOption {
	return func(o *elbOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// ELBClient returns an ELBClient configured with the given options.
func (c *Client) ELBClient(opts ...ELBOption) *ELBClient {
	o := &elbOptions{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &ELBClient{
		v2Client:    elbv2.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (e *ELBClient) status(format string, args ...any) {
	if e.statusf != nil {
		e.statusf(format, args...)
	}
}

// ListenerSummary represents key attributes of a load balancer listener.
type ListenerSummary struct {
	ListenerArn     string   `json:"listener_arn"`
	Port            int32    `json:"port"`
	Protocol        string   `json:"protocol"`
	SslPolicy       string   `json:"ssl_policy,omitempty"`
	CertificateArns []string `json:"certificate_arns,omitempty"`
}

// TargetGroupSummary represents key attributes of a target group.
type TargetGroupSummary struct {
	TargetGroupName     string            `json:"target_group_name"`
	TargetGroupArn      string            `json:"target_group_arn"`
	Protocol            string            `json:"protocol,omitempty"`
	Port                int32             `json:"port,omitempty"`
	VpcId               string            `json:"vpc_id,omitempty"`
	TargetType          string            `json:"target_type"`
	HealthCheckProtocol string            `json:"health_check_protocol,omitempty"`
	HealthCheckPort     string            `json:"health_check_port,omitempty"`
	HealthCheckPath     string            `json:"health_check_path,omitempty"`
	Tags                map[string]string `json:"tags,omitempty"`
}

// LoadBalancerSummary represents key attributes of a load balancer (ALB/NLB).
type LoadBalancerSummary struct {
	LoadBalancerName  string            `json:"load_balancer_name"`
	LoadBalancerArn   string            `json:"load_balancer_arn"`
	DNSName           string            `json:"dns_name"`
	Type              string            `json:"type"`
	Scheme            string            `json:"scheme"`
	State             string            `json:"state"`
	VpcId             string            `json:"vpc_id,omitempty"`
	AvailabilityZones []string          `json:"availability_zones,omitempty"`
	SecurityGroups    []string          `json:"security_groups,omitempty"`
	IpAddressType     string            `json:"ip_address_type"`
	Listeners         []ListenerSummary `json:"listeners,omitempty"`
	Tags              map[string]string `json:"tags,omitempty"`
}

// ELBSummary represents the complete ELB snapshot.
type ELBSummary struct {
	LoadBalancers []LoadBalancerSummary `json:"load_balancers,omitempty"`
	TargetGroups  []TargetGroupSummary  `json:"target_groups,omitempty"`
}

// Summarise returns a summary of all load balancers and target groups.
func (e *ELBClient) Summarise(ctx context.Context) (*ELBSummary, error) {
	e.status("Listing load balancers...")

	// List all load balancers
	var lbs []lbInfo
	paginator := elbv2.NewDescribeLoadBalancersPaginator(e.v2Client, &elbv2.DescribeLoadBalancersInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, lb := range page.LoadBalancers {
			lbs = append(lbs, lbInfo{
				name: aws.ToString(lb.LoadBalancerName),
				arn:  aws.ToString(lb.LoadBalancerArn),
			})
		}
	}

	total := len(lbs)
	e.status("Found %d load balancers, processing with concurrency %d", total, e.concurrency)

	loadBalancers := make([]LoadBalancerSummary, total)
	var processed atomic.Int64

	if total > 0 {
		// Create work channel
		workCh := make(chan int, total)
		for i := range lbs {
			workCh <- i
		}
		close(workCh)

		// Process load balancers concurrently
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

					lb := lbs[idx]
					loadBalancers[idx] = e.describeLoadBalancer(ctx, lb)

					n := processed.Add(1)
					e.status("[%d/%d] Processed load balancer: %s", n, total, lb.name)
				}
			}()
		}

		wg.Wait()
	}

	// Filter out empty summaries (errors)
	resultLBs := make([]LoadBalancerSummary, 0, total)
	for _, lb := range loadBalancers {
		if lb.LoadBalancerName != "" {
			resultLBs = append(resultLBs, lb)
		}
	}

	// Get target groups
	e.status("Listing target groups...")
	targetGroups, err := e.listTargetGroups(ctx)
	if err != nil {
		return nil, err
	}

	return &ELBSummary{
		LoadBalancers: resultLBs,
		TargetGroups:  targetGroups,
	}, nil
}

type lbInfo struct {
	name string
	arn  string
}

func (e *ELBClient) describeLoadBalancer(ctx context.Context, lb lbInfo) LoadBalancerSummary {
	summary := LoadBalancerSummary{
		LoadBalancerName: lb.name,
		LoadBalancerArn:  lb.arn,
	}

	// Describe the load balancer
	descResp, err := e.v2Client.DescribeLoadBalancers(ctx, &elbv2.DescribeLoadBalancersInput{
		LoadBalancerArns: []string{lb.arn},
	})
	if err != nil || len(descResp.LoadBalancers) == 0 {
		return summary
	}

	lbDetail := descResp.LoadBalancers[0]
	summary.DNSName = aws.ToString(lbDetail.DNSName)
	summary.Type = string(lbDetail.Type)
	summary.Scheme = string(lbDetail.Scheme)
	if lbDetail.State != nil {
		summary.State = string(lbDetail.State.Code)
	}
	summary.VpcId = aws.ToString(lbDetail.VpcId)
	summary.IpAddressType = string(lbDetail.IpAddressType)
	summary.SecurityGroups = lbDetail.SecurityGroups

	for _, az := range lbDetail.AvailabilityZones {
		summary.AvailabilityZones = append(summary.AvailabilityZones, aws.ToString(az.ZoneName))
	}

	// Get listeners
	listenersResp, err := e.v2Client.DescribeListeners(ctx, &elbv2.DescribeListenersInput{
		LoadBalancerArn: &lb.arn,
	})
	if err == nil {
		for _, listener := range listenersResp.Listeners {
			ls := ListenerSummary{
				ListenerArn: aws.ToString(listener.ListenerArn),
				Port:        aws.ToInt32(listener.Port),
				Protocol:    string(listener.Protocol),
				SslPolicy:   aws.ToString(listener.SslPolicy),
			}
			for _, cert := range listener.Certificates {
				ls.CertificateArns = append(ls.CertificateArns, aws.ToString(cert.CertificateArn))
			}
			summary.Listeners = append(summary.Listeners, ls)
		}
	}

	// Get tags
	tagsResp, err := e.v2Client.DescribeTags(ctx, &elbv2.DescribeTagsInput{
		ResourceArns: []string{lb.arn},
	})
	if err == nil && len(tagsResp.TagDescriptions) > 0 {
		summary.Tags = make(map[string]string)
		for _, tag := range tagsResp.TagDescriptions[0].Tags {
			summary.Tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
		}
	}

	return summary
}

func (e *ELBClient) listTargetGroups(ctx context.Context) ([]TargetGroupSummary, error) {
	var targetGroups []TargetGroupSummary

	paginator := elbv2.NewDescribeTargetGroupsPaginator(e.v2Client, &elbv2.DescribeTargetGroupsInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, tg := range page.TargetGroups {
			summary := TargetGroupSummary{
				TargetGroupName:     aws.ToString(tg.TargetGroupName),
				TargetGroupArn:      aws.ToString(tg.TargetGroupArn),
				Protocol:            string(tg.Protocol),
				Port:                aws.ToInt32(tg.Port),
				VpcId:               aws.ToString(tg.VpcId),
				TargetType:          string(tg.TargetType),
				HealthCheckProtocol: string(tg.HealthCheckProtocol),
				HealthCheckPort:     aws.ToString(tg.HealthCheckPort),
				HealthCheckPath:     aws.ToString(tg.HealthCheckPath),
			}

			// Get tags
			tagsResp, err := e.v2Client.DescribeTags(ctx, &elbv2.DescribeTagsInput{
				ResourceArns: []string{aws.ToString(tg.TargetGroupArn)},
			})
			if err == nil && len(tagsResp.TagDescriptions) > 0 {
				summary.Tags = make(map[string]string)
				for _, tag := range tagsResp.TagDescriptions[0].Tags {
					summary.Tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
				}
			}

			targetGroups = append(targetGroups, summary)
		}
	}

	return targetGroups, nil
}
