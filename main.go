package main

import (
	"os"

	"aws-snapshot/cmd"
)

var version = "dev"

func main() {
	cmd.SetVersion(version)
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
