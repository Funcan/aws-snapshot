package awsclient

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/route53"
)

// Route53Client wraps the AWS Route53 client with snapshot capabilities.
type Route53Client struct {
	client      *route53.Client
	statusf     StatusFunc
	concurrency int
}

// Route53Option is a functional option for configuring the Route53Client.
type Route53Option func(*route53Options)

type route53Options struct {
	statusf     StatusFunc
	concurrency int
}

// WithRoute53StatusFunc sets a callback for progress messages.
func WithRoute53StatusFunc(f StatusFunc) Route53Option {
	return func(o *route53Options) {
		o.statusf = f
	}
}

// WithRoute53Concurrency sets the maximum number of zones to process in parallel.
func WithRoute53Concurrency(n int) Route53Option {
	return func(o *route53Options) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// Route53Client returns a Route53Client configured with the given options.
func (c *Client) Route53Client(opts ...Route53Option) *Route53Client {
	o := &route53Options{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &Route53Client{
		client:      route53.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (r *Route53Client) status(format string, args ...any) {
	if r.statusf != nil {
		r.statusf(format, args...)
	}
}

// RecordSetSummary represents key attributes of a DNS record set.
type RecordSetSummary struct {
	Name            string   `json:"name"`
	Type            string   `json:"type"`
	TTL             int64    `json:"ttl,omitempty"`
	Values          []string `json:"values,omitempty"`
	AliasTarget     string   `json:"alias_target,omitempty"`
	AliasHostedZone string   `json:"alias_hosted_zone,omitempty"`
	SetIdentifier   string   `json:"set_identifier,omitempty"`
	Weight          int64    `json:"weight,omitempty"`
	Region          string   `json:"region,omitempty"`
	Failover        string   `json:"failover,omitempty"`
	HealthCheckId   string   `json:"health_check_id,omitempty"`
}

// HostedZoneSummary represents key attributes of a Route53 hosted zone.
type HostedZoneSummary struct {
	Id             string             `json:"id"`
	Name           string             `json:"name"`
	Comment        string             `json:"comment,omitempty"`
	Private        bool               `json:"private"`
	RecordSetCount int64              `json:"record_set_count"`
	VPCs           []string           `json:"vpcs,omitempty"`
	RecordSets     []RecordSetSummary `json:"record_sets,omitempty"`
}

// Summarise returns a summary of all Route53 hosted zones and their records.
func (r *Route53Client) Summarise(ctx context.Context) ([]HostedZoneSummary, error) {
	r.status("Listing Route53 hosted zones...")

	// List all hosted zones
	var zones []zoneInfo
	paginator := route53.NewListHostedZonesPaginator(r.client, &route53.ListHostedZonesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, zone := range page.HostedZones {
			zones = append(zones, zoneInfo{
				id:   aws.ToString(zone.Id),
				name: aws.ToString(zone.Name),
			})
		}
	}

	total := len(zones)
	r.status("Found %d hosted zones, processing with concurrency %d", total, r.concurrency)

	if total == 0 {
		return []HostedZoneSummary{}, nil
	}

	summaries := make([]HostedZoneSummary, total)
	var processed atomic.Int64

	// Create work channel
	workCh := make(chan int, total)
	for i := range zones {
		workCh <- i
	}
	close(workCh)

	// Process zones concurrently
	var wg sync.WaitGroup
	for i := 0; i < r.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				zone := zones[idx]
				summaries[idx] = r.describeZone(ctx, zone)

				n := processed.Add(1)
				r.status("[%d/%d] Processed zone: %s", n, total, zone.name)
			}
		}()
	}

	wg.Wait()

	// Filter out empty summaries (errors)
	result := make([]HostedZoneSummary, 0, total)
	for _, s := range summaries {
		if s.Name != "" {
			result = append(result, s)
		}
	}

	return result, nil
}

type zoneInfo struct {
	id   string
	name string
}

func (r *Route53Client) describeZone(ctx context.Context, zone zoneInfo) HostedZoneSummary {
	summary := HostedZoneSummary{
		Id:   strings.TrimPrefix(zone.id, "/hostedzone/"),
		Name: zone.name,
	}

	// Get zone details
	zoneResp, err := r.client.GetHostedZone(ctx, &route53.GetHostedZoneInput{
		Id: &zone.id,
	})
	if err != nil {
		return summary
	}

	if zoneResp.HostedZone != nil {
		if zoneResp.HostedZone.Config != nil {
			summary.Comment = aws.ToString(zoneResp.HostedZone.Config.Comment)
			summary.Private = zoneResp.HostedZone.Config.PrivateZone
		}
		summary.RecordSetCount = aws.ToInt64(zoneResp.HostedZone.ResourceRecordSetCount)
	}

	// Get VPCs for private zones
	for _, vpc := range zoneResp.VPCs {
		summary.VPCs = append(summary.VPCs, aws.ToString(vpc.VPCId))
	}

	// Get record sets
	summary.RecordSets = r.listRecordSets(ctx, zone.id)

	return summary
}

func (r *Route53Client) listRecordSets(ctx context.Context, zoneId string) []RecordSetSummary {
	var records []RecordSetSummary

	paginator := route53.NewListResourceRecordSetsPaginator(r.client, &route53.ListResourceRecordSetsInput{
		HostedZoneId: &zoneId,
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			break
		}

		for _, rs := range page.ResourceRecordSets {
			record := RecordSetSummary{
				Name: aws.ToString(rs.Name),
				Type: string(rs.Type),
			}

			if rs.TTL != nil {
				record.TTL = aws.ToInt64(rs.TTL)
			}

			for _, rr := range rs.ResourceRecords {
				record.Values = append(record.Values, aws.ToString(rr.Value))
			}

			if rs.AliasTarget != nil {
				record.AliasTarget = aws.ToString(rs.AliasTarget.DNSName)
				record.AliasHostedZone = aws.ToString(rs.AliasTarget.HostedZoneId)
			}

			if rs.SetIdentifier != nil {
				record.SetIdentifier = aws.ToString(rs.SetIdentifier)
			}

			if rs.Weight != nil {
				record.Weight = aws.ToInt64(rs.Weight)
			}

			if rs.Region != "" {
				record.Region = string(rs.Region)
			}

			if rs.Failover != "" {
				record.Failover = string(rs.Failover)
			}

			if rs.HealthCheckId != nil {
				record.HealthCheckId = aws.ToString(rs.HealthCheckId)
			}

			records = append(records, record)
		}
	}

	return records
}
