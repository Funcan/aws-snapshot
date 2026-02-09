package awsclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

// VPCClient wraps the AWS EC2 client with VPC snapshot capabilities.
type VPCClient struct {
	client      *ec2.Client
	statusf     StatusFunc
	concurrency int
}

// VPCOption is a functional option for configuring the VPCClient.
type VPCOption func(*vpcOptions)

type vpcOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithVPCStatusFunc sets a callback for progress messages.
func WithVPCStatusFunc(f StatusFunc) VPCOption {
	return func(o *vpcOptions) {
		o.statusf = f
	}
}

// WithVPCConcurrency sets the maximum number of VPCs to process in parallel.
func WithVPCConcurrency(n int) VPCOption {
	return func(o *vpcOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// VPCClient returns a VPCClient configured with the given options.
func (c *Client) VPCClient(opts ...VPCOption) *VPCClient {
	o := &vpcOptions{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &VPCClient{
		client:      ec2.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (v *VPCClient) status(format string, args ...any) {
	if v.statusf != nil {
		v.statusf(format, args...)
	}
}

// SubnetSummary represents key attributes of a subnet.
type SubnetSummary struct {
	SubnetId            string            `json:"subnet_id"`
	CidrBlock           string            `json:"cidr_block"`
	AvailabilityZone    string            `json:"availability_zone"`
	AvailabilityZoneId  string            `json:"availability_zone_id"`
	MapPublicIpOnLaunch bool              `json:"map_public_ip_on_launch"`
	DefaultForAz        bool              `json:"default_for_az"`
	Tags                map[string]string `json:"tags,omitempty"`
}

// RouteTableSummary represents key attributes of a route table.
type RouteTableSummary struct {
	RouteTableId      string            `json:"route_table_id"`
	Main              bool              `json:"main"`
	AssociatedSubnets []string          `json:"associated_subnets,omitempty"`
	Routes            []RouteSummary    `json:"routes,omitempty"`
	Tags              map[string]string `json:"tags,omitempty"`
}

// RouteSummary represents a route in a route table.
type RouteSummary struct {
	DestinationCidr    string `json:"destination_cidr,omitempty"`
	DestinationPrefix  string `json:"destination_prefix,omitempty"`
	GatewayId          string `json:"gateway_id,omitempty"`
	NatGatewayId       string `json:"nat_gateway_id,omitempty"`
	TransitGatewayId   string `json:"transit_gateway_id,omitempty"`
	VpcPeeringId       string `json:"vpc_peering_id,omitempty"`
	NetworkInterfaceId string `json:"network_interface_id,omitempty"`
	State              string `json:"state,omitempty"`
}

// SecurityGroupSummary represents key attributes of a security group.
type SecurityGroupSummary struct {
	GroupId      string              `json:"group_id"`
	GroupName    string              `json:"group_name"`
	Description  string              `json:"description,omitempty"`
	IngressRules []SecurityGroupRule `json:"ingress_rules,omitempty"`
	EgressRules  []SecurityGroupRule `json:"egress_rules,omitempty"`
	Tags         map[string]string   `json:"tags,omitempty"`
}

// SecurityGroupRule represents an ingress or egress rule.
type SecurityGroupRule struct {
	Protocol       string   `json:"protocol"`
	FromPort       int32    `json:"from_port,omitempty"`
	ToPort         int32    `json:"to_port,omitempty"`
	CidrBlocks     []string `json:"cidr_blocks,omitempty"`
	Ipv6CidrBlocks []string `json:"ipv6_cidr_blocks,omitempty"`
	SourceGroups   []string `json:"source_groups,omitempty"`
	PrefixListIds  []string `json:"prefix_list_ids,omitempty"`
	Description    string   `json:"description,omitempty"`
}

// InternetGatewaySummary represents key attributes of an internet gateway.
type InternetGatewaySummary struct {
	InternetGatewayId string            `json:"internet_gateway_id"`
	Tags              map[string]string `json:"tags,omitempty"`
}

// NatGatewaySummary represents key attributes of a NAT gateway.
type NatGatewaySummary struct {
	NatGatewayId     string            `json:"nat_gateway_id"`
	SubnetId         string            `json:"subnet_id"`
	ConnectivityType string            `json:"connectivity_type"`
	State            string            `json:"state"`
	PublicIp         string            `json:"public_ip,omitempty"`
	PrivateIp        string            `json:"private_ip,omitempty"`
	Tags             map[string]string `json:"tags,omitempty"`
}

// VPCEndpointSummary represents key attributes of a VPC endpoint.
type VPCEndpointSummary struct {
	VpcEndpointId     string            `json:"vpc_endpoint_id"`
	ServiceName       string            `json:"service_name"`
	EndpointType      string            `json:"endpoint_type"`
	State             string            `json:"state"`
	SubnetIds         []string          `json:"subnet_ids,omitempty"`
	SecurityGroups    []string          `json:"security_groups,omitempty"`
	PrivateDnsEnabled bool              `json:"private_dns_enabled"`
	Tags              map[string]string `json:"tags,omitempty"`
}

// VPCSummary represents key attributes of a VPC.
type VPCSummary struct {
	VpcId                 string                   `json:"vpc_id"`
	CidrBlock             string                   `json:"cidr_block"`
	CidrBlockAssociations []string                 `json:"cidr_block_associations,omitempty"`
	IsDefault             bool                     `json:"is_default"`
	State                 string                   `json:"state"`
	DhcpOptionsId         string                   `json:"dhcp_options_id,omitempty"`
	InstanceTenancy       string                   `json:"instance_tenancy"`
	EnableDnsHostnames    bool                     `json:"enable_dns_hostnames"`
	EnableDnsSupport      bool                     `json:"enable_dns_support"`
	Subnets               []SubnetSummary          `json:"subnets,omitempty"`
	RouteTables           []RouteTableSummary      `json:"route_tables,omitempty"`
	SecurityGroups        []SecurityGroupSummary   `json:"security_groups,omitempty"`
	InternetGateways      []InternetGatewaySummary `json:"internet_gateways,omitempty"`
	NatGateways           []NatGatewaySummary      `json:"nat_gateways,omitempty"`
	VpcEndpoints          []VPCEndpointSummary     `json:"vpc_endpoints,omitempty"`
	Tags                  map[string]string        `json:"tags,omitempty"`
}

// Summarise returns a summary of all VPCs and their resources.
func (v *VPCClient) Summarise(ctx context.Context) ([]VPCSummary, error) {
	v.status("Listing VPCs...")

	// List all VPCs
	var vpcs []vpcInfo
	paginator := ec2.NewDescribeVpcsPaginator(v.client, &ec2.DescribeVpcsInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, vpc := range page.Vpcs {
			vpcs = append(vpcs, vpcInfo{
				id: aws.ToString(vpc.VpcId),
			})
		}
	}

	total := len(vpcs)
	v.status("Found %d VPCs, processing with concurrency %d", total, v.concurrency)

	if total == 0 {
		return []VPCSummary{}, nil
	}

	summaries := make([]VPCSummary, total)
	var processed atomic.Int64

	// Create work channel
	workCh := make(chan int, total)
	for i := range vpcs {
		workCh <- i
	}
	close(workCh)

	// Process VPCs concurrently
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	for i := 0; i < v.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				vpc := vpcs[idx]
				summary, err := v.describeVPC(ctx, vpc)
				if err != nil {
					errMu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("describing VPC %s: %w", vpc.id, err)
					}
					errMu.Unlock()
					continue
				}
				summaries[idx] = summary

				n := processed.Add(1)
				v.status("[%d/%d] Processed VPC: %s", n, total, vpc.id)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	// Filter out unprocessed summaries (context cancellation)
	result := make([]VPCSummary, 0, total)
	for _, s := range summaries {
		if s.VpcId != "" {
			result = append(result, s)
		}
	}

	return result, nil
}

type vpcInfo struct {
	id string
}

func (v *VPCClient) describeVPC(ctx context.Context, vpc vpcInfo) (VPCSummary, error) {
	summary := VPCSummary{VpcId: vpc.id}

	// Get VPC details
	descResp, err := v.client.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{
		VpcIds: []string{vpc.id},
	})
	if err != nil {
		return summary, fmt.Errorf("DescribeVpcs: %w", err)
	}
	if len(descResp.Vpcs) == 0 {
		return summary, fmt.Errorf("VPC %s not found", vpc.id)
	}

	vpcDetail := descResp.Vpcs[0]
	summary.CidrBlock = aws.ToString(vpcDetail.CidrBlock)
	summary.IsDefault = aws.ToBool(vpcDetail.IsDefault)
	summary.State = string(vpcDetail.State)
	summary.DhcpOptionsId = aws.ToString(vpcDetail.DhcpOptionsId)
	summary.InstanceTenancy = string(vpcDetail.InstanceTenancy)

	for _, assoc := range vpcDetail.CidrBlockAssociationSet {
		summary.CidrBlockAssociations = append(summary.CidrBlockAssociations, aws.ToString(assoc.CidrBlock))
	}

	summary.Tags = tagsToMap(vpcDetail.Tags)

	// Get DNS attributes
	dnsHostnames, err := v.client.DescribeVpcAttribute(ctx, &ec2.DescribeVpcAttributeInput{
		VpcId:     &vpc.id,
		Attribute: "enableDnsHostnames",
	})
	if err != nil {
		return summary, fmt.Errorf("DescribeVpcAttribute (enableDnsHostnames): %w", err)
	}
	if dnsHostnames.EnableDnsHostnames != nil {
		summary.EnableDnsHostnames = aws.ToBool(dnsHostnames.EnableDnsHostnames.Value)
	}

	dnsSupport, err := v.client.DescribeVpcAttribute(ctx, &ec2.DescribeVpcAttributeInput{
		VpcId:     &vpc.id,
		Attribute: "enableDnsSupport",
	})
	if err != nil {
		return summary, fmt.Errorf("DescribeVpcAttribute (enableDnsSupport): %w", err)
	}
	if dnsSupport.EnableDnsSupport != nil {
		summary.EnableDnsSupport = aws.ToBool(dnsSupport.EnableDnsSupport.Value)
	}

	// Get subnets
	subnets, err := v.listSubnets(ctx, vpc.id)
	if err != nil {
		return summary, err
	}
	summary.Subnets = subnets

	// Get route tables
	routeTables, err := v.listRouteTables(ctx, vpc.id)
	if err != nil {
		return summary, err
	}
	summary.RouteTables = routeTables

	// Get security groups
	securityGroups, err := v.listSecurityGroups(ctx, vpc.id)
	if err != nil {
		return summary, err
	}
	summary.SecurityGroups = securityGroups

	// Get internet gateways
	internetGateways, err := v.listInternetGateways(ctx, vpc.id)
	if err != nil {
		return summary, err
	}
	summary.InternetGateways = internetGateways

	// Get NAT gateways
	natGateways, err := v.listNatGateways(ctx, vpc.id)
	if err != nil {
		return summary, err
	}
	summary.NatGateways = natGateways

	// Get VPC endpoints
	vpcEndpoints, err := v.listVpcEndpoints(ctx, vpc.id)
	if err != nil {
		return summary, err
	}
	summary.VpcEndpoints = vpcEndpoints

	return summary, nil
}

func tagsToMap(tags []types.Tag) map[string]string {
	if len(tags) == 0 {
		return nil
	}
	m := make(map[string]string)
	for _, tag := range tags {
		m[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	return m
}

func (v *VPCClient) listSubnets(ctx context.Context, vpcId string) ([]SubnetSummary, error) {
	var subnets []SubnetSummary

	resp, err := v.client.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		Filters: []types.Filter{
			{Name: aws.String("vpc-id"), Values: []string{vpcId}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("DescribeSubnets: %w", err)
	}

	for _, subnet := range resp.Subnets {
		subnets = append(subnets, SubnetSummary{
			SubnetId:            aws.ToString(subnet.SubnetId),
			CidrBlock:           aws.ToString(subnet.CidrBlock),
			AvailabilityZone:    aws.ToString(subnet.AvailabilityZone),
			AvailabilityZoneId:  aws.ToString(subnet.AvailabilityZoneId),
			MapPublicIpOnLaunch: aws.ToBool(subnet.MapPublicIpOnLaunch),
			DefaultForAz:        aws.ToBool(subnet.DefaultForAz),
			Tags:                tagsToMap(subnet.Tags),
		})
	}

	return subnets, nil
}

func (v *VPCClient) listRouteTables(ctx context.Context, vpcId string) ([]RouteTableSummary, error) {
	var tables []RouteTableSummary

	resp, err := v.client.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
		Filters: []types.Filter{
			{Name: aws.String("vpc-id"), Values: []string{vpcId}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("DescribeRouteTables: %w", err)
	}

	for _, rt := range resp.RouteTables {
		table := RouteTableSummary{
			RouteTableId: aws.ToString(rt.RouteTableId),
			Tags:         tagsToMap(rt.Tags),
		}

		for _, assoc := range rt.Associations {
			if aws.ToBool(assoc.Main) {
				table.Main = true
			}
			if assoc.SubnetId != nil {
				table.AssociatedSubnets = append(table.AssociatedSubnets, aws.ToString(assoc.SubnetId))
			}
		}

		for _, route := range rt.Routes {
			r := RouteSummary{
				DestinationCidr:    aws.ToString(route.DestinationCidrBlock),
				DestinationPrefix:  aws.ToString(route.DestinationPrefixListId),
				GatewayId:          aws.ToString(route.GatewayId),
				NatGatewayId:       aws.ToString(route.NatGatewayId),
				TransitGatewayId:   aws.ToString(route.TransitGatewayId),
				VpcPeeringId:       aws.ToString(route.VpcPeeringConnectionId),
				NetworkInterfaceId: aws.ToString(route.NetworkInterfaceId),
				State:              string(route.State),
			}
			table.Routes = append(table.Routes, r)
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func (v *VPCClient) listSecurityGroups(ctx context.Context, vpcId string) ([]SecurityGroupSummary, error) {
	var groups []SecurityGroupSummary

	resp, err := v.client.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
		Filters: []types.Filter{
			{Name: aws.String("vpc-id"), Values: []string{vpcId}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("DescribeSecurityGroups: %w", err)
	}

	for _, sg := range resp.SecurityGroups {
		group := SecurityGroupSummary{
			GroupId:     aws.ToString(sg.GroupId),
			GroupName:   aws.ToString(sg.GroupName),
			Description: aws.ToString(sg.Description),
			Tags:        tagsToMap(sg.Tags),
		}

		for _, rule := range sg.IpPermissions {
			group.IngressRules = append(group.IngressRules, convertRule(rule))
		}

		for _, rule := range sg.IpPermissionsEgress {
			group.EgressRules = append(group.EgressRules, convertRule(rule))
		}

		groups = append(groups, group)
	}

	return groups, nil
}

func convertRule(rule types.IpPermission) SecurityGroupRule {
	r := SecurityGroupRule{
		Protocol: aws.ToString(rule.IpProtocol),
		FromPort: aws.ToInt32(rule.FromPort),
		ToPort:   aws.ToInt32(rule.ToPort),
	}

	for _, ip := range rule.IpRanges {
		r.CidrBlocks = append(r.CidrBlocks, aws.ToString(ip.CidrIp))
		if ip.Description != nil {
			r.Description = aws.ToString(ip.Description)
		}
	}

	for _, ip := range rule.Ipv6Ranges {
		r.Ipv6CidrBlocks = append(r.Ipv6CidrBlocks, aws.ToString(ip.CidrIpv6))
	}

	for _, group := range rule.UserIdGroupPairs {
		r.SourceGroups = append(r.SourceGroups, aws.ToString(group.GroupId))
	}

	for _, prefix := range rule.PrefixListIds {
		r.PrefixListIds = append(r.PrefixListIds, aws.ToString(prefix.PrefixListId))
	}

	return r
}

func (v *VPCClient) listInternetGateways(ctx context.Context, vpcId string) ([]InternetGatewaySummary, error) {
	var gateways []InternetGatewaySummary

	resp, err := v.client.DescribeInternetGateways(ctx, &ec2.DescribeInternetGatewaysInput{
		Filters: []types.Filter{
			{Name: aws.String("attachment.vpc-id"), Values: []string{vpcId}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("DescribeInternetGateways: %w", err)
	}

	for _, igw := range resp.InternetGateways {
		gateways = append(gateways, InternetGatewaySummary{
			InternetGatewayId: aws.ToString(igw.InternetGatewayId),
			Tags:              tagsToMap(igw.Tags),
		})
	}

	return gateways, nil
}

func (v *VPCClient) listNatGateways(ctx context.Context, vpcId string) ([]NatGatewaySummary, error) {
	var gateways []NatGatewaySummary

	resp, err := v.client.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{
		Filter: []types.Filter{
			{Name: aws.String("vpc-id"), Values: []string{vpcId}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("DescribeNatGateways: %w", err)
	}

	for _, nat := range resp.NatGateways {
		gw := NatGatewaySummary{
			NatGatewayId:     aws.ToString(nat.NatGatewayId),
			SubnetId:         aws.ToString(nat.SubnetId),
			ConnectivityType: string(nat.ConnectivityType),
			State:            string(nat.State),
			Tags:             tagsToMap(nat.Tags),
		}

		for _, addr := range nat.NatGatewayAddresses {
			if addr.PublicIp != nil {
				gw.PublicIp = aws.ToString(addr.PublicIp)
			}
			if addr.PrivateIp != nil {
				gw.PrivateIp = aws.ToString(addr.PrivateIp)
			}
		}

		gateways = append(gateways, gw)
	}

	return gateways, nil
}

func (v *VPCClient) listVpcEndpoints(ctx context.Context, vpcId string) ([]VPCEndpointSummary, error) {
	var endpoints []VPCEndpointSummary

	resp, err := v.client.DescribeVpcEndpoints(ctx, &ec2.DescribeVpcEndpointsInput{
		Filters: []types.Filter{
			{Name: aws.String("vpc-id"), Values: []string{vpcId}},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("DescribeVpcEndpoints: %w", err)
	}

	for _, ep := range resp.VpcEndpoints {
		endpoint := VPCEndpointSummary{
			VpcEndpointId:     aws.ToString(ep.VpcEndpointId),
			ServiceName:       aws.ToString(ep.ServiceName),
			EndpointType:      string(ep.VpcEndpointType),
			State:             string(ep.State),
			SubnetIds:         ep.SubnetIds,
			PrivateDnsEnabled: aws.ToBool(ep.PrivateDnsEnabled),
			Tags:              tagsToMap(ep.Tags),
		}

		for _, sg := range ep.Groups {
			endpoint.SecurityGroups = append(endpoint.SecurityGroups, aws.ToString(sg.GroupId))
		}

		endpoints = append(endpoints, endpoint)
	}

	return endpoints, nil
}
