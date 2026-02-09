package awsclient

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoDBClient wraps the AWS DynamoDB client with snapshot capabilities.
type DynamoDBClient struct {
	client      *dynamodb.Client
	statusf     StatusFunc
	concurrency int
}

// DynamoDBOption is a functional option for configuring the DynamoDBClient.
type DynamoDBOption func(*dynamoDBOptions)

type dynamoDBOptions struct {
	statusf     StatusFunc
	concurrency int
}

// WithDynamoDBStatusFunc sets a callback for progress messages.
func WithDynamoDBStatusFunc(f StatusFunc) DynamoDBOption {
	return func(o *dynamoDBOptions) {
		o.statusf = f
	}
}

// WithDynamoDBConcurrency sets the maximum number of tables to process in parallel.
func WithDynamoDBConcurrency(n int) DynamoDBOption {
	return func(o *dynamoDBOptions) {
		if n > 0 {
			o.concurrency = n
		}
	}
}

// DynamoDBClient returns a DynamoDBClient configured with the given options.
func (c *Client) DynamoDBClient(opts ...DynamoDBOption) *DynamoDBClient {
	o := &dynamoDBOptions{
		concurrency: 50,
	}
	for _, opt := range opts {
		opt(o)
	}

	return &DynamoDBClient{
		client:      dynamodb.NewFromConfig(c.cfg),
		statusf:     o.statusf,
		concurrency: o.concurrency,
	}
}

func (d *DynamoDBClient) status(format string, args ...any) {
	if d.statusf != nil {
		d.statusf(format, args...)
	}
}

// TableSummary represents key attributes of a DynamoDB table.
type TableSummary struct {
	TableName              string             `json:"table_name"`
	TableArn               string             `json:"table_arn"`
	TableStatus            string             `json:"table_status"`
	BillingMode            string             `json:"billing_mode"`
	ReadCapacityUnits      int64              `json:"read_capacity_units,omitempty"`
	WriteCapacityUnits     int64              `json:"write_capacity_units,omitempty"`
	ItemCount              int64              `json:"item_count"`
	TableSizeBytes         int64              `json:"table_size_bytes"`
	KeySchema              []KeySchemaElement `json:"key_schema"`
	GlobalSecondaryIndexes []GSISummary       `json:"global_secondary_indexes,omitempty"`
	LocalSecondaryIndexes  []LSISummary       `json:"local_secondary_indexes,omitempty"`
	StreamEnabled          bool               `json:"stream_enabled"`
	StreamViewType         string             `json:"stream_view_type,omitempty"`
	TTLEnabled             bool               `json:"ttl_enabled"`
	TTLAttributeName       string             `json:"ttl_attribute_name,omitempty"`
	PointInTimeRecovery    bool               `json:"point_in_time_recovery"`
	SSEEnabled             bool               `json:"sse_enabled"`
	SSEType                string             `json:"sse_type,omitempty"`
	DeletionProtection     bool               `json:"deletion_protection"`
	TableClass             string             `json:"table_class,omitempty"`
	Tags                   map[string]string  `json:"tags,omitempty"`
}

// KeySchemaElement represents a key schema element.
type KeySchemaElement struct {
	AttributeName string `json:"attribute_name"`
	KeyType       string `json:"key_type"` // HASH or RANGE
}

// GSISummary represents a global secondary index.
type GSISummary struct {
	IndexName          string             `json:"index_name"`
	IndexStatus        string             `json:"index_status"`
	KeySchema          []KeySchemaElement `json:"key_schema"`
	Projection         string             `json:"projection"`
	ReadCapacityUnits  int64              `json:"read_capacity_units,omitempty"`
	WriteCapacityUnits int64              `json:"write_capacity_units,omitempty"`
}

// LSISummary represents a local secondary index.
type LSISummary struct {
	IndexName  string             `json:"index_name"`
	KeySchema  []KeySchemaElement `json:"key_schema"`
	Projection string             `json:"projection"`
}

// Summarise returns a summary of all DynamoDB tables.
func (d *DynamoDBClient) Summarise(ctx context.Context) ([]TableSummary, error) {
	d.status("Listing DynamoDB tables...")

	// List all table names
	var tableNames []string
	paginator := dynamodb.NewListTablesPaginator(d.client, &dynamodb.ListTablesInput{})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, page.TableNames...)
	}

	total := len(tableNames)
	d.status("Found %d DynamoDB tables, processing with concurrency %d", total, d.concurrency)

	if total == 0 {
		return []TableSummary{}, nil
	}

	summaries := make([]TableSummary, total)
	var processed atomic.Int64

	// Create work channel
	workCh := make(chan int, total)
	for i := range tableNames {
		workCh <- i
	}
	close(workCh)

	// Process tables concurrently
	var wg sync.WaitGroup
	for i := 0; i < d.concurrency && i < total; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range workCh {
				select {
				case <-ctx.Done():
					return
				default:
				}

				name := tableNames[idx]
				summaries[idx] = d.describeTable(ctx, name)

				n := processed.Add(1)
				d.status("[%d/%d] Processed table: %s", n, total, name)
			}
		}()
	}

	wg.Wait()

	// Filter out empty summaries (errors)
	result := make([]TableSummary, 0, total)
	for _, s := range summaries {
		if s.TableName != "" {
			result = append(result, s)
		}
	}

	return result, nil
}

func (d *DynamoDBClient) describeTable(ctx context.Context, tableName string) TableSummary {
	summary := TableSummary{TableName: tableName}

	// Describe table
	resp, err := d.client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: &tableName,
	})
	if err != nil {
		return summary
	}

	table := resp.Table
	summary.TableArn = aws.ToString(table.TableArn)
	summary.TableStatus = string(table.TableStatus)
	summary.ItemCount = aws.ToInt64(table.ItemCount)
	summary.TableSizeBytes = aws.ToInt64(table.TableSizeBytes)
	summary.DeletionProtection = aws.ToBool(table.DeletionProtectionEnabled)

	if table.TableClassSummary != nil {
		summary.TableClass = string(table.TableClassSummary.TableClass)
	}

	// Billing mode
	if table.BillingModeSummary != nil {
		summary.BillingMode = string(table.BillingModeSummary.BillingMode)
	} else {
		summary.BillingMode = "PROVISIONED"
	}

	// Provisioned throughput
	if table.ProvisionedThroughput != nil {
		if table.ProvisionedThroughput.ReadCapacityUnits != nil {
			summary.ReadCapacityUnits = *table.ProvisionedThroughput.ReadCapacityUnits
		}
		if table.ProvisionedThroughput.WriteCapacityUnits != nil {
			summary.WriteCapacityUnits = *table.ProvisionedThroughput.WriteCapacityUnits
		}
	}

	// Key schema
	for _, ks := range table.KeySchema {
		summary.KeySchema = append(summary.KeySchema, KeySchemaElement{
			AttributeName: aws.ToString(ks.AttributeName),
			KeyType:       string(ks.KeyType),
		})
	}

	// Global secondary indexes
	for _, gsi := range table.GlobalSecondaryIndexes {
		g := GSISummary{
			IndexName:   aws.ToString(gsi.IndexName),
			IndexStatus: string(gsi.IndexStatus),
		}
		if gsi.Projection != nil {
			g.Projection = string(gsi.Projection.ProjectionType)
		}
		for _, ks := range gsi.KeySchema {
			g.KeySchema = append(g.KeySchema, KeySchemaElement{
				AttributeName: aws.ToString(ks.AttributeName),
				KeyType:       string(ks.KeyType),
			})
		}
		if gsi.ProvisionedThroughput != nil {
			if gsi.ProvisionedThroughput.ReadCapacityUnits != nil {
				g.ReadCapacityUnits = *gsi.ProvisionedThroughput.ReadCapacityUnits
			}
			if gsi.ProvisionedThroughput.WriteCapacityUnits != nil {
				g.WriteCapacityUnits = *gsi.ProvisionedThroughput.WriteCapacityUnits
			}
		}
		summary.GlobalSecondaryIndexes = append(summary.GlobalSecondaryIndexes, g)
	}

	// Local secondary indexes
	for _, lsi := range table.LocalSecondaryIndexes {
		l := LSISummary{
			IndexName: aws.ToString(lsi.IndexName),
		}
		if lsi.Projection != nil {
			l.Projection = string(lsi.Projection.ProjectionType)
		}
		for _, ks := range lsi.KeySchema {
			l.KeySchema = append(l.KeySchema, KeySchemaElement{
				AttributeName: aws.ToString(ks.AttributeName),
				KeyType:       string(ks.KeyType),
			})
		}
		summary.LocalSecondaryIndexes = append(summary.LocalSecondaryIndexes, l)
	}

	// Stream specification
	if table.StreamSpecification != nil {
		summary.StreamEnabled = aws.ToBool(table.StreamSpecification.StreamEnabled)
		if summary.StreamEnabled {
			summary.StreamViewType = string(table.StreamSpecification.StreamViewType)
		}
	}

	// SSE
	if table.SSEDescription != nil {
		summary.SSEEnabled = table.SSEDescription.Status == types.SSEStatusEnabled
		if summary.SSEEnabled {
			summary.SSEType = string(table.SSEDescription.SSEType)
		}
	}

	// TTL
	ttlResp, err := d.client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: &tableName,
	})
	if err == nil && ttlResp.TimeToLiveDescription != nil {
		summary.TTLEnabled = ttlResp.TimeToLiveDescription.TimeToLiveStatus == types.TimeToLiveStatusEnabled
		if summary.TTLEnabled {
			summary.TTLAttributeName = aws.ToString(ttlResp.TimeToLiveDescription.AttributeName)
		}
	}

	// Point in time recovery
	pitrResp, err := d.client.DescribeContinuousBackups(ctx, &dynamodb.DescribeContinuousBackupsInput{
		TableName: &tableName,
	})
	if err == nil && pitrResp.ContinuousBackupsDescription != nil {
		if pitrResp.ContinuousBackupsDescription.PointInTimeRecoveryDescription != nil {
			summary.PointInTimeRecovery = pitrResp.ContinuousBackupsDescription.PointInTimeRecoveryDescription.PointInTimeRecoveryStatus == types.PointInTimeRecoveryStatusEnabled
		}
	}

	// Tags
	tagsResp, err := d.client.ListTagsOfResource(ctx, &dynamodb.ListTagsOfResourceInput{
		ResourceArn: table.TableArn,
	})
	if err == nil && len(tagsResp.Tags) > 0 {
		summary.Tags = make(map[string]string)
		for _, tag := range tagsResp.Tags {
			summary.Tags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
		}
	}

	return summary
}
