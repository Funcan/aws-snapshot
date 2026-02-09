// Package awsclient provides a configurable AWS client wrapper.
package awsclient

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/config"
)

// Client wraps AWS SDK configuration and provides access to AWS services.
type Client struct {
	cfg     aws.Config
	profile string
	region  string
}

// Option is a functional option for configuring the Client.
type Option func(*options)

type options struct {
	profile    string
	region     string
	configOpts []func(*config.LoadOptions) error
}

// WithProfile sets the AWS profile to use.
func WithProfile(profile string) Option {
	return func(o *options) {
		o.profile = profile
	}
}

// WithRegion sets the AWS region to use.
func WithRegion(region string) Option {
	return func(o *options) {
		o.region = region
	}
}

// WithEndpoint sets a custom endpoint URL (useful for testing with LocalStack).
func WithEndpoint(endpoint string) Option {
	return func(o *options) {
		o.configOpts = append(o.configOpts, config.WithEndpointResolverWithOptions(
			aws.EndpointResolverWithOptionsFunc(func(service, region string, opts ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{URL: endpoint}, nil
			}),
		))
	}
}

// WithConfigOption adds a custom AWS SDK config option.
func WithConfigOption(opt func(*config.LoadOptions) error) Option {
	return func(o *options) {
		o.configOpts = append(o.configOpts, opt)
	}
}

// New creates a new AWS Client with the given options.
func New(ctx context.Context, opts ...Option) (*Client, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	var configOpts []func(*config.LoadOptions) error

	if o.profile != "" {
		configOpts = append(configOpts, config.WithSharedConfigProfile(o.profile))
	}

	if o.region != "" {
		configOpts = append(configOpts, config.WithRegion(o.region))
	}

	// Configure retry with exponential backoff for throttling errors
	configOpts = append(configOpts, config.WithRetryer(func() aws.Retryer {
		return retry.NewStandard(func(o *retry.StandardOptions) {
			o.MaxAttempts = 10
			o.MaxBackoff = 30 * time.Second
		})
	}))

	configOpts = append(configOpts, o.configOpts...)

	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	return &Client{
		cfg:     cfg,
		profile: o.profile,
		region:  cfg.Region,
	}, nil
}

// Config returns the underlying AWS SDK config.
func (c *Client) Config() aws.Config {
	return c.cfg
}

// Region returns the configured region.
func (c *Client) Region() string {
	return c.region
}

// Profile returns the configured profile name.
func (c *Client) Profile() string {
	return c.profile
}

// AccountID retrieves the AWS account ID using STS GetCallerIdentity.
func (c *Client) AccountID(ctx context.Context) (string, error) {
	// This will be implemented when we add the STS dependency
	// For now, return empty - consumers should use sts.NewFromConfig(c.Config())
	return "", fmt.Errorf("not implemented: use sts.NewFromConfig(c.Config()).GetCallerIdentity()")
}
