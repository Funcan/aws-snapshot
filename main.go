package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"aws-snapshot/pkg/awsclient"

	"github.com/spf13/cobra"
)

var (
	profile     string
	region      string
	verbose     bool
	concurrency int
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

var rootCmd = &cobra.Command{
	Use:   "aws-snapshot",
	Short: "Capture diffable snapshots of AWS resources",
}

var snapshotCmd = &cobra.Command{
	Use:   "snapshot",
	Short: "Take a snapshot of AWS resources",
}

var snapshotS3Cmd = &cobra.Command{
	Use:   "s3",
	Short: "Snapshot S3 buckets",
	RunE:  runSnapshotS3,
}

var snapshotEKSCmd = &cobra.Command{
	Use:   "eks",
	Short: "Snapshot EKS clusters",
	RunE:  runSnapshotEKS,
}

func init() {
	rootCmd.PersistentFlags().StringVar(&profile, "profile", "", "AWS profile to use")
	rootCmd.PersistentFlags().StringVar(&region, "region", "", "AWS region to use")
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Print progress messages to stderr")
	rootCmd.PersistentFlags().IntVarP(&concurrency, "concurrency", "c", 50, "Maximum number of resources to process in parallel")

	rootCmd.AddCommand(snapshotCmd)
	snapshotCmd.AddCommand(snapshotS3Cmd)
	snapshotCmd.AddCommand(snapshotEKSCmd)
}

func buildClient(ctx context.Context) (*awsclient.Client, error) {
	var opts []awsclient.Option
	if profile != "" {
		opts = append(opts, awsclient.WithProfile(profile))
	}
	if region != "" {
		opts = append(opts, awsclient.WithRegion(region))
	}
	return awsclient.New(ctx, opts...)
}

func statusf(format string, args ...any) {
	if verbose {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
	}
}

func runSnapshotS3(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var s3Opts []awsclient.S3Option
	if verbose {
		s3Opts = append(s3Opts, awsclient.WithStatusFunc(statusf))
	}
	s3Opts = append(s3Opts, awsclient.WithConcurrency(concurrency))
	s3client := client.S3Client(s3Opts...)

	statusf("Fetching S3 buckets...")
	buckets, err := s3client.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing S3 buckets: %w", err)
	}

	// Sort by bucket name for consistent diffs
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(buckets); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}

func runSnapshotEKS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	statusf("Creating AWS client...")
	client, err := buildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var eksOpts []awsclient.EKSOption
	if verbose {
		eksOpts = append(eksOpts, awsclient.WithEKSStatusFunc(statusf))
	}
	eksOpts = append(eksOpts, awsclient.WithEKSConcurrency(concurrency))
	eksClient := client.EKSClient(eksOpts...)

	statusf("Fetching EKS clusters...")
	clusters, err := eksClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing EKS clusters: %w", err)
	}

	// Sort by cluster name for consistent diffs
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Name < clusters[j].Name
	})

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(clusters); err != nil {
		return fmt.Errorf("encoding JSON: %w", err)
	}

	return nil
}
