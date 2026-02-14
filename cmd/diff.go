package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	"aws-snapshot/pkg/diff"

	"github.com/spf13/cobra"
)

var diffCmd = &cobra.Command{
	Use:   "diff [flags] file1.json file2.json [file3.json ...]",
	Short: "Show differences between snapshots ordered by time",
	Args:  cobra.MinimumNArgs(2),
	RunE:  runDiff,
}

// snapshotFile holds a parsed snapshot with its timestamp.
type snapshotFile struct {
	filename  string
	timestamp time.Time
	data      map[string]interface{}
}

func runDiff(cmd *cobra.Command, args []string) error {
	files := make([]snapshotFile, 0, len(args))
	for _, filename := range args {
		data, ts, err := loadSnapshotFile(filename)
		if err != nil {
			return fmt.Errorf("loading %s: %w", filename, err)
		}
		files = append(files, snapshotFile{
			filename:  filename,
			timestamp: ts,
			data:      data,
		})
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].timestamp.Before(files[j].timestamp)
	})

	for i := 0; i < len(files)-1; i++ {
		changes := diff.Compare(files[i].data, files[i+1].data)
		if len(changes) == 0 {
			continue
		}

		fmt.Printf("%s:\n", formatTimeWindow(files[i].timestamp, files[i+1].timestamp))
		for _, c := range changes {
			fmt.Println(c.String())
		}
		fmt.Println()
	}

	return nil
}

func loadSnapshotFile(filename string) (map[string]interface{}, time.Time, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, time.Time{}, err
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		return nil, time.Time{}, err
	}

	var ts time.Time
	if tsStr, ok := obj["timestamp"].(string); ok {
		parsed, err := time.Parse(time.RFC3339, tsStr)
		if err == nil {
			ts = parsed
		}
	}

	if ts.IsZero() {
		fi, err := os.Stat(filename)
		if err != nil {
			return nil, time.Time{}, err
		}
		ts = fi.ModTime()
	}

	return obj, ts, nil
}

func formatTimeWindow(t1, t2 time.Time) string {
	const dateFmt = "2/1/06 15:04"
	const timeFmt = "15:04"

	if t1.Year() == t2.Year() && t1.Month() == t2.Month() && t1.Day() == t2.Day() {
		return fmt.Sprintf("%s - %s", t1.Format(dateFmt), t2.Format(timeFmt))
	}
	return fmt.Sprintf("%s - %s", t1.Format(dateFmt), t2.Format(dateFmt))
}
