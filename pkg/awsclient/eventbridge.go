package awsclient

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
)

// EventBridgeClient wraps the AWS EventBridge client with snapshot capabilities.
type EventBridgeClient struct {
	client      *eventbridge.Client
	statusf     StatusFunc
	concurrency int
}

// EventBridgeOption is a functional option for configuring the EventBridgeClient.
type EventBridgeOption func(*eventbridgeOptions)

type eventbridgeOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithEventBridgeStatusFunc sets a callback for progress messages.
func WithEventBridgeStatusFunc(f StatusFunc) EventBridgeOption {
	return func(o *eventbridgeOptions) {
		o.statusf = f
	}
}

// WithEventBridgeConcurrency sets the maximum number of rules to process in parallel.
func WithEventBridgeConcurrency(n int) EventBridgeOption {
	return func(o *eventbridgeOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// EventBridgeClient returns an EventBridgeClient configured with the given options.
func (c *Client) EventBridgeClient(opts ...EventBridgeOption) *EventBridgeClient {
	o := &eventbridgeOptions{
		concurrency: 10,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &EventBridgeClient{
		client:      eventbridge.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (e *EventBridgeClient) status(format string, args ...any) {
	if e.statusf != nil {
		e.statusf(format, args...)
	}
}

// RuleTargetSummary represents key attributes of an EventBridge rule target.
type RuleTargetSummary struct {
	Id      string `json:"id"`
	Arn     string `json:"arn"`
	RoleArn string `json:"role_arn,omitempty"`
}

// RuleSummary represents key attributes of an EventBridge rule.
type RuleSummary struct {
	Name               string              `json:"name"`
	Arn                string              `json:"arn,omitempty"`
	State              string              `json:"state"`
	Description        string              `json:"description,omitempty"`
	ScheduleExpression string              `json:"schedule_expression,omitempty"`
	EventPattern       bool                `json:"has_event_pattern"`
	Targets            []RuleTargetSummary `json:"targets,omitempty"`
}

// EventBusSummary represents key attributes of an EventBridge event bus.
type EventBusSummary struct {
	Name   string            `json:"name"`
	Arn    string            `json:"arn,omitempty"`
	Policy bool              `json:"has_policy"`
	Rules  []RuleSummary     `json:"rules,omitempty"`
	Tags   map[string]string `json:"tags,omitempty"`
}

// EventBridgeSummary represents the complete EventBridge snapshot.
type EventBridgeSummary struct {
	EventBuses []EventBusSummary `json:"event_buses,omitempty"`
}

// Summarise returns a summary of all EventBridge event buses and rules.
func (e *EventBridgeClient) Summarise(ctx context.Context) (*EventBridgeSummary, error) {
	e.status("Listing EventBridge event buses...")

	var buses []eventBusInfo
	var nextToken *string
	for {
		resp, err := e.client.ListEventBuses(ctx, &eventbridge.ListEventBusesInput{
			NextToken: nextToken,
		})
		if err != nil {
			return nil, err
		}

		for _, bus := range resp.EventBuses {
			buses = append(buses, eventBusInfo{
				name: aws.ToString(bus.Name),
				arn:  aws.ToString(bus.Arn),
			})
		}

		if resp.NextToken == nil {
			break
		}
		nextToken = resp.NextToken
	}

	total := len(buses)
	e.status("Found %d event buses, processing with concurrency %d", total, e.concurrency)

	if total == 0 {
		return &EventBridgeSummary{EventBuses: []EventBusSummary{}}, nil
	}

	summaries := make([]EventBusSummary, total)
	var processed atomic.Int64
	var errMu sync.Mutex
	var firstErr error

	// Create work channel
	workCh := make(chan int, total)
	for i := range buses {
		workCh <- i
	}
	close(workCh)

	// Process buses concurrently
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

				bus := buses[idx]
				summary, err := e.describeEventBus(ctx, bus)
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
				e.status("[%d/%d] Processed event bus: %s", n, total, bus.name)
			}
		}()
	}

	wg.Wait()

	if firstErr != nil {
		return nil, firstErr
	}

	return &EventBridgeSummary{EventBuses: summaries}, nil
}

type eventBusInfo struct {
	name string
	arn  string
}

func (e *EventBridgeClient) describeEventBus(ctx context.Context, bus eventBusInfo) (EventBusSummary, error) {
	summary := EventBusSummary{
		Name: bus.name,
		Arn:  bus.arn,
	}

	// Check if bus has a policy
	descResp, err := e.client.DescribeEventBus(ctx, &eventbridge.DescribeEventBusInput{
		Name: &bus.name,
	})
	if err != nil {
		return EventBusSummary{}, fmt.Errorf("describe event bus %s: %w", bus.name, err)
	}
	if descResp.Policy != nil && *descResp.Policy != "" {
		summary.Policy = true
	}

	// Get rules for this bus
	var rules []RuleSummary
	var nextToken *string
	for {
		rulesResp, err := e.client.ListRules(ctx, &eventbridge.ListRulesInput{
			EventBusName: &bus.name,
			NextToken:    nextToken,
		})
		if err != nil {
			return EventBusSummary{}, fmt.Errorf("list rules for event bus %s: %w", bus.name, err)
		}

		for _, rule := range rulesResp.Rules {
			r := RuleSummary{
				Name:               aws.ToString(rule.Name),
				Arn:                aws.ToString(rule.Arn),
				State:              string(rule.State),
				Description:        aws.ToString(rule.Description),
				ScheduleExpression: aws.ToString(rule.ScheduleExpression),
				EventPattern:       rule.EventPattern != nil && *rule.EventPattern != "",
			}

			// Get targets for this rule
			targetsResp, err := e.client.ListTargetsByRule(ctx, &eventbridge.ListTargetsByRuleInput{
				Rule:         rule.Name,
				EventBusName: &bus.name,
			})
			if err != nil {
				return EventBusSummary{}, fmt.Errorf("list targets for rule %s: %w", aws.ToString(rule.Name), err)
			}
			for _, target := range targetsResp.Targets {
				r.Targets = append(r.Targets, RuleTargetSummary{
					Id:      aws.ToString(target.Id),
					Arn:     aws.ToString(target.Arn),
					RoleArn: aws.ToString(target.RoleArn),
				})
			}

			rules = append(rules, r)
		}

		if rulesResp.NextToken == nil {
			break
		}
		nextToken = rulesResp.NextToken
	}
	summary.Rules = rules

	// Get tags
	if bus.arn != "" {
		tagsResp, err := e.client.ListTagsForResource(ctx, &eventbridge.ListTagsForResourceInput{
			ResourceARN: &bus.arn,
		})
		if err != nil {
			return EventBusSummary{}, fmt.Errorf("list tags for event bus %s: %w", bus.name, err)
		}
		summary.Tags = tagsToMap(tagsResp.Tags)
	}

	return summary, nil
}
