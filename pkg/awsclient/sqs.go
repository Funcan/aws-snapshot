package awsclient

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SQSClient wraps the AWS SQS client with snapshot capabilities.
type SQSClient struct {
	client      *sqs.Client
	statusf     StatusFunc
	concurrency int
}

// SQSOption is a functional option for configuring the SQSClient.
type SQSOption func(*sqsOptions)

type sqsOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithSQSStatusFunc sets a callback for progress messages.
func WithSQSStatusFunc(f StatusFunc) SQSOption {
	return func(o *sqsOptions) {
		o.statusf = f
	}
}

// WithSQSConcurrency sets the maximum number of queues to process in parallel.
func WithSQSConcurrency(n int) SQSOption {
	return func(o *sqsOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// SQSClient returns an SQSClient configured with the given options.
func (c *Client) SQSClient(opts ...SQSOption) *SQSClient {
	o := &sqsOptions{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &SQSClient{
		client:      sqs.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (s *SQSClient) status(format string, args ...any) {
	if s.statusf != nil {
		s.statusf(format, args...)
	}
}

// QueueSummary represents key attributes of an SQS queue.
type QueueSummary struct {
	Name                          string            `json:"name"`
	URL                           string            `json:"url"`
	Arn                           string            `json:"arn,omitempty"`
	FifoQueue                     bool              `json:"fifo_queue"`
	ContentBasedDeduplication     bool              `json:"content_based_deduplication,omitempty"`
	VisibilityTimeoutSeconds      string            `json:"visibility_timeout_seconds,omitempty"`
	MessageRetentionSeconds       string            `json:"message_retention_seconds,omitempty"`
	MaxMessageSizeBytes           string            `json:"max_message_size_bytes,omitempty"`
	DelaySeconds                  string            `json:"delay_seconds,omitempty"`
	ReceiveMessageWaitTimeSeconds string            `json:"receive_message_wait_time_seconds,omitempty"`
	DeadLetterTargetArn           string            `json:"dead_letter_target_arn,omitempty"`
	MaxReceiveCount               string            `json:"max_receive_count,omitempty"`
	KmsMasterKeyId                string            `json:"kms_master_key_id,omitempty"`
	SqsManagedSseEnabled          bool              `json:"sqs_managed_sse_enabled"`
	Tags                          map[string]string `json:"tags,omitempty"`
}

// Summarise returns a summary of all SQS queues.
func (s *SQSClient) Summarise(ctx context.Context) ([]QueueSummary, error) {
	s.status("Listing SQS queues...")

	var queueURLs []string
	var nextToken *string
	for {
		resp, err := s.client.ListQueues(ctx, &sqs.ListQueuesInput{
			NextToken: nextToken,
		})
		if err != nil {
			return nil, err
		}

		queueURLs = append(queueURLs, resp.QueueUrls...)

		if resp.NextToken == nil {
			break
		}
		nextToken = resp.NextToken
	}

	total := len(queueURLs)
	s.status("Found %d queues, processing with concurrency %d", total, s.concurrency)

	if total == 0 {
		return []QueueSummary{}, nil
	}

	summaries := make([]QueueSummary, total)
	var processed atomic.Int64

	// Create work channel
	workCh := make(chan int, total)
	for i := range queueURLs {
		workCh <- i
	}
	close(workCh)

	// Process queues concurrently
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

				url := queueURLs[idx]
				summaries[idx] = s.describeQueue(ctx, url)

				n := processed.Add(1)
				s.status("[%d/%d] Processed queue: %s", n, total, summaries[idx].Name)
			}
		}()
	}

	wg.Wait()

	// Filter out empty summaries (errors)
	result := make([]QueueSummary, 0, total)
	for _, sum := range summaries {
		if sum.URL != "" {
			result = append(result, sum)
		}
	}

	return result, nil
}

func (s *SQSClient) describeQueue(ctx context.Context, queueURL string) QueueSummary {
	summary := QueueSummary{
		URL: queueURL,
	}

	// Extract queue name from URL
	parts := splitQueueURL(queueURL)
	if len(parts) > 0 {
		summary.Name = parts[len(parts)-1]
	}

	// Get queue attributes
	attrsResp, err := s.client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: &queueURL,
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameAll,
		},
	})
	if err != nil {
		return summary
	}

	attrs := attrsResp.Attributes
	summary.Arn = attrs[string(types.QueueAttributeNameQueueArn)]
	summary.VisibilityTimeoutSeconds = attrs[string(types.QueueAttributeNameVisibilityTimeout)]
	summary.MessageRetentionSeconds = attrs[string(types.QueueAttributeNameMessageRetentionPeriod)]
	summary.MaxMessageSizeBytes = attrs[string(types.QueueAttributeNameMaximumMessageSize)]
	summary.DelaySeconds = attrs[string(types.QueueAttributeNameDelaySeconds)]
	summary.ReceiveMessageWaitTimeSeconds = attrs[string(types.QueueAttributeNameReceiveMessageWaitTimeSeconds)]
	summary.KmsMasterKeyId = attrs[string(types.QueueAttributeNameKmsMasterKeyId)]

	if attrs[string(types.QueueAttributeNameFifoQueue)] == "true" {
		summary.FifoQueue = true
	}
	if attrs[string(types.QueueAttributeNameContentBasedDeduplication)] == "true" {
		summary.ContentBasedDeduplication = true
	}
	if attrs[string(types.QueueAttributeNameSqsManagedSseEnabled)] == "true" {
		summary.SqsManagedSseEnabled = true
	}

	// Parse redrive policy for DLQ info
	if redrivePolicy := attrs[string(types.QueueAttributeNameRedrivePolicy)]; redrivePolicy != "" {
		summary.DeadLetterTargetArn, summary.MaxReceiveCount = parseRedrivePolicy(redrivePolicy)
	}

	// Get tags
	tagsResp, err := s.client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{
		QueueUrl: &queueURL,
	})
	if err == nil && len(tagsResp.Tags) > 0 {
		summary.Tags = tagsResp.Tags
	}

	return summary
}

func splitQueueURL(url string) []string {
	var parts []string
	current := ""
	for _, c := range url {
		if c == '/' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else {
			current += string(c)
		}
	}
	if current != "" {
		parts = append(parts, current)
	}
	return parts
}

func parseRedrivePolicy(policy string) (deadLetterArn, maxReceiveCount string) {
	// Simple JSON parsing for redrive policy
	// Format: {"deadLetterTargetArn":"arn:...","maxReceiveCount":5}
	inKey := false
	inValue := false
	inString := false
	currentKey := ""
	currentValue := ""

	for i := 0; i < len(policy); i++ {
		c := policy[i]
		switch {
		case c == '"':
			if inString {
				if inKey {
					inKey = false
				} else if inValue {
					if currentKey == "deadLetterTargetArn" {
						deadLetterArn = currentValue
					}
					inValue = false
					currentKey = ""
					currentValue = ""
				}
				inString = false
			} else {
				inString = true
				if !inValue {
					inKey = true
					currentKey = ""
				}
			}
		case c == ':' && !inString:
			inValue = true
			currentValue = ""
		case c == ',' && !inString:
			if currentKey == "maxReceiveCount" && currentValue != "" {
				maxReceiveCount = currentValue
			}
			currentKey = ""
			currentValue = ""
			inValue = false
		case c == '}' && !inString:
			if currentKey == "maxReceiveCount" && currentValue != "" {
				maxReceiveCount = currentValue
			}
		default:
			if inString {
				if inKey {
					currentKey += string(c)
				} else if inValue {
					currentValue += string(c)
				}
			} else if inValue && c >= '0' && c <= '9' {
				currentValue += string(c)
			}
		}
	}

	return
}
