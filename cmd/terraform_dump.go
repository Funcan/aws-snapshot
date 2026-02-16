package cmd

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"aws-snapshot/pkg/loadfile"
	"aws-snapshot/pkg/terraform"

	"github.com/spf13/cobra"
)

var dumpStateFiles []string

var terraformDumpCmd = &cobra.Command{
	Use:   "terraform-dump [flags] resource-type [resource-type ...]",
	Short: "List resources found in Terraform state files",
	Long:  "Parse Terraform state files and list all resource names of the given types.",
	Args:  cobra.MinimumNArgs(1),
	RunE:  runTerraformDump,
}

func init() {
	terraformDumpCmd.Flags().StringArrayVar(&dumpStateFiles, "statefile", nil, "Path to Terraform state file (local path or s3:// URL, can be specified multiple times)")
	terraformDumpCmd.MarkFlagRequired("statefile")
}

func runTerraformDump(cmd *cobra.Command, args []string) error {
	// Validate resource types
	for _, rt := range args {
		if _, ok := checkers[rt]; !ok {
			supported := make([]string, 0, len(checkers))
			for k := range checkers {
				supported = append(supported, k)
			}
			sort.Strings(supported)
			return fmt.Errorf("unsupported resource type %q (supported: %v)", rt, supported)
		}
	}

	ctx := context.Background()

	// Load all state files in parallel
	states := make([]*terraform.State, len(dumpStateFiles))
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error

	for i, sf := range dumpStateFiles {
		wg.Add(1)
		go func(i int, sf string) {
			defer wg.Done()
			Statusf("Loading state file %s...", sf)
			data, err := loadfile.Load(ctx, sf, awsOptions()...)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("loading state file %s: %w", sf, err)
				}
				mu.Unlock()
				return
			}
			state, err := terraform.ParseState(data)
			if err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("parsing state file %s: %w", sf, err)
				}
				mu.Unlock()
				return
			}
			states[i] = state
		}(i, sf)
	}

	wg.Wait()
	if firstErr != nil {
		return firstErr
	}

	for _, rt := range args {
		checker := checkers[rt]

		for i, state := range states {
			names := extractNames(state, checker)
			sort.Strings(names)
			for _, name := range names {
				fmt.Printf("%s: %s: %s\n", rt, name, dumpStateFiles[i])
			}
		}
	}

	return nil
}
