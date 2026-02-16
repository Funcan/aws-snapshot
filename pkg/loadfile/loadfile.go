// Package loadfile reads files from local paths or S3 URLs.
package loadfile

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"

	"aws-snapshot/pkg/awsclient"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Load reads a file from a local path or s3:// URL.
// For S3 URLs, an AWS client is built using the provided options.
func Load(ctx context.Context, path string, awsOpts ...awsclient.Option) ([]byte, error) {
	if !strings.HasPrefix(path, "s3://") {
		return os.ReadFile(path)
	}

	trimmed := strings.TrimPrefix(path, "s3://")
	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("invalid S3 URL: %s (expected s3://bucket/key)", path)
	}

	client, err := awsclient.New(ctx, awsOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating AWS client: %w", err)
	}

	s3Client := s3.NewFromConfig(client.Config())
	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(parts[0]),
		Key:    aws.String(parts[1]),
	})
	if err != nil {
		return nil, fmt.Errorf("reading from S3: %w", err)
	}
	defer result.Body.Close()

	return io.ReadAll(result.Body)
}
