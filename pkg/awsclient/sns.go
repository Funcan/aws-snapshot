package awsclient

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
)

// SNSClient wraps the AWS SNS client with snapshot capabilities.
type SNSClient struct {
	client      *sns.Client
	statusf     StatusFunc
	concurrency int
}

// SNSOption is a functional option for configuring the SNSClient.
type SNSOption func(*snsOptions)

type snsOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithSNSStatusFunc sets a callback for progress messages.
func WithSNSStatusFunc(f StatusFunc) SNSOption {
	return func(o *snsOptions) {
		o.statusf = f
	}
}

// WithSNSConcurrency sets the maximum number of topics to process in parallel.
func WithSNSConcurrency(n int) SNSOption {
	return func(o *snsOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// SNSClient returns an SNSClient configured with the given options.
func (c *Client) SNSClient(opts ...SNSOption) *SNSClient {
	o := &snsOptions{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &SNSClient{
		client:      sns.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (s *SNSClient) status(format string, args ...any) {
	if s.statusf != nil {
		s.statusf(format, args...)
	}
}

// SubscriptionSummary represents key attributes of an SNS subscription.
type SubscriptionSummary struct {
	Arn      string `json:"arn"`
	Protocol string `json:"protocol"`
	Endpoint string `json:"endpoint,omitempty"`
	Owner    string `json:"owner,omitempty"`
}

// TopicSummary represents key attributes of an SNS topic.
type TopicSummary struct {
	Arn                       string                `json:"arn"`
	Name                      string                `json:"name"`
	DisplayName               string                `json:"display_name,omitempty"`
	FifoTopic                 bool                  `json:"fifo_topic"`
	ContentBasedDeduplication bool                  `json:"content_based_deduplication,omitempty"`
	KmsMasterKeyId            string                `json:"kms_master_key_id,omitempty"`
	Policy                    bool                  `json:"has_policy"`
	DeliveryPolicy            bool                  `json:"has_delivery_policy"`
	Subscriptions             []SubscriptionSummary `json:"subscriptions,omitempty"`
	Tags                      map[string]string     `json:"tags,omitempty"`
}

// Summarise returns a summary of all SNS topics.
func (s *SNSClient) Summarise(ctx context.Context) ([]TopicSummary, error) {
	s.status("Listing SNS topics...")

	var topicArns []string
	var nextToken *string
	for {
		resp, err := s.client.ListTopics(ctx, &sns.ListTopicsInput{
			NextToken: nextToken,
		})
		if err != nil {
			return nil, err
		}

		for _, topic := range resp.Topics {
			topicArns = append(topicArns, aws.ToString(topic.TopicArn))
		}

		if resp.NextToken == nil {
			break
		}
		nextToken = resp.NextToken
	}

	total := len(topicArns)
	s.status("Found %d topics, processing with concurrency %d", total, s.concurrency)

	if total == 0 {
		return []TopicSummary{}, nil
	}

	summaries := make([]TopicSummary, total)
	var processed atomic.Int64

	// Create work channel
	workCh := make(chan int, total)
	for i := range topicArns {
		workCh <- i
	}
	close(workCh)

	// Process topics concurrently
	var wg sync.WaitGroup
	for i := 0; i < s.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				arn := topicArns[idx]
				summaries[idx] = s.describeTopic(ctx, arn)

				n := processed.Add(1)
				s.status("[%d/%d] Processed topic: %s", n, total, summaries[idx].Name)
			}
		}()
	}

	wg.Wait()

	// Filter out empty summaries (errors)
	result := make([]TopicSummary, 0, total)
	for _, sum := range summaries {
		if sum.Arn != "" {
			result = append(result, sum)
		}
	}

	return result, nil
}

func (s *SNSClient) describeTopic(ctx context.Context, topicArn string) TopicSummary {
	summary := TopicSummary{
		Arn:  topicArn,
		Name: extractTopicName(topicArn),
	}

	// Get topic attributes
	attrsResp, err := s.client.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: &topicArn,
	})
	if err != nil {
		return summary
	}

	attrs := attrsResp.Attributes
	summary.DisplayName = attrs["DisplayName"]
	summary.KmsMasterKeyId = attrs["KmsMasterKeyId"]

	if attrs["FifoTopic"] == "true" {
		summary.FifoTopic = true
	}
	if attrs["ContentBasedDeduplication"] == "true" {
		summary.ContentBasedDeduplication = true
	}
	if attrs["Policy"] != "" {
		summary.Policy = true
	}
	if attrs["DeliveryPolicy"] != "" {
		summary.DeliveryPolicy = true
	}

	// Get subscriptions
	var subscriptions []SubscriptionSummary
	var nextToken *string
	for {
		subsResp, err := s.client.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
			TopicArn:  &topicArn,
			NextToken: nextToken,
		})
		if err != nil {
			break
		}

		for _, sub := range subsResp.Subscriptions {
			subscriptions = append(subscriptions, SubscriptionSummary{
				Arn:      aws.ToString(sub.SubscriptionArn),
				Protocol: aws.ToString(sub.Protocol),
				Endpoint: aws.ToString(sub.Endpoint),
				Owner:    aws.ToString(sub.Owner),
			})
		}

		if subsResp.NextToken == nil {
			break
		}
		nextToken = subsResp.NextToken
	}
	summary.Subscriptions = subscriptions

	// Get tags
	tagsResp, err := s.client.ListTagsForResource(ctx, &sns.ListTagsForResourceInput{
		ResourceArn: &topicArn,
	})
	if err == nil && len(tagsResp.Tags) > 0 {
		summary.Tags = make(map[string]string)
		for _, tag := range tagsResp.Tags {
			summary.Tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
		}
	}

	return summary
}

func extractTopicName(arn string) string {
	// ARN format: arn:aws:sns:region:account-id:topic-name
	lastColon := -1
	for i := len(arn) - 1; i >= 0; i-- {
		if arn[i] == ':' {
			lastColon = i
			break
		}
	}
	if lastColon >= 0 && lastColon < len(arn)-1 {
		return arn[lastColon+1:]
	}
	return arn
}
