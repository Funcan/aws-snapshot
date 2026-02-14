package cmd

import (
	"context"
	"fmt"
	"os"
	"sort"
	"text/tabwriter"

	"aws-snapshot/pkg/awsclient"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Show version information for AWS resources",
}

var versionEKSCmd = &cobra.Command{
	Use:   "eks",
	Short: "Show EKS cluster and node versions",
	RunE:  runVersionEKS,
}

var versionRDSCmd = &cobra.Command{
	Use:   "rds",
	Short: "Show RDS instance and cluster engine versions",
	RunE:  runVersionRDS,
}

var versionMSKCmd = &cobra.Command{
	Use:   "msk",
	Short: "Show MSK cluster Kafka versions",
	RunE:  runVersionMSK,
}

var versionElastiCacheCmd = &cobra.Command{
	Use:   "elasticache",
	Short: "Show ElastiCache cluster and replication group versions",
	RunE:  runVersionElastiCache,
}

var versionOpenSearchCmd = &cobra.Command{
	Use:   "opensearch",
	Short: "Show OpenSearch domain versions",
	RunE:  runVersionOpenSearch,
}

func init() {
	versionCmd.AddCommand(versionEKSCmd)
	versionCmd.AddCommand(versionRDSCmd)
	versionCmd.AddCommand(versionMSKCmd)
	versionCmd.AddCommand(versionElastiCacheCmd)
	versionCmd.AddCommand(versionOpenSearchCmd)
}

func runVersionEKS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var eksOpts []awsclient.EKSOption
	if Verbose {
		eksOpts = append(eksOpts, awsclient.WithEKSStatusFunc(Statusf))
	}
	eksOpts = append(eksOpts, awsclient.WithEKSConcurrency(Concurrency))
	eksClient := client.EKSClient(eksOpts...)

	Statusf("Fetching EKS clusters...")
	clusters, err := eksClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing EKS clusters: %w", err)
	}

	// Sort clusters by name
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].Name < clusters[j].Name
	})

	// Print table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tVERSION\tNODEGROUP\tK8S VERSION\tAMI TYPE\tAMI VERSION")

	for _, cluster := range clusters {
		if len(cluster.NodeGroups) == 0 {
			fmt.Fprintf(w, "%s\t%s\t-\t-\t-\t-\n", cluster.Name, cluster.Version)
			continue
		}
		// Sort node groups by name
		sort.Slice(cluster.NodeGroups, func(i, j int) bool {
			return cluster.NodeGroups[i].Name < cluster.NodeGroups[j].Name
		})
		for i, ng := range cluster.NodeGroups {
			clusterName := cluster.Name
			clusterVersion := cluster.Version
			if i > 0 {
				clusterName = ""
				clusterVersion = ""
			}
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", clusterName, clusterVersion, ng.Name, ng.KubernetesVersion, ng.AmiType, ng.ReleaseVersion)
		}
	}

	w.Flush()
	return nil
}

func runVersionRDS(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var rdsOpts []awsclient.RDSOption
	if Verbose {
		rdsOpts = append(rdsOpts, awsclient.WithRDSStatusFunc(Statusf))
	}
	rdsOpts = append(rdsOpts, awsclient.WithRDSConcurrency(Concurrency))
	rdsClient := client.RDSClient(rdsOpts...)

	Statusf("Fetching RDS resources...")
	rdsSummary, err := rdsClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing RDS resources: %w", err)
	}

	// Sort instances and clusters by identifier
	sort.Slice(rdsSummary.Instances, func(i, j int) bool {
		return rdsSummary.Instances[i].Identifier < rdsSummary.Instances[j].Identifier
	})
	sort.Slice(rdsSummary.Clusters, func(i, j int) bool {
		return rdsSummary.Clusters[i].Identifier < rdsSummary.Clusters[j].Identifier
	})

	// Print table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TYPE\tIDENTIFIER\tENGINE\tVERSION\tCLASS/MODE\tSTATUS")

	for _, cluster := range rdsSummary.Clusters {
		mode := cluster.EngineMode
		if mode == "" {
			mode = "provisioned"
		}
		fmt.Fprintf(w, "cluster\t%s\t%s\t%s\t%s\t%s\n", cluster.Identifier, cluster.Engine, cluster.EngineVersion, mode, cluster.Status)
	}

	for _, inst := range rdsSummary.Instances {
		fmt.Fprintf(w, "instance\t%s\t%s\t%s\t%s\t%s\n", inst.Identifier, inst.Engine, inst.EngineVersion, inst.InstanceClass, inst.Status)
	}

	w.Flush()
	return nil
}

func runVersionMSK(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var mskOpts []awsclient.MSKOption
	if Verbose {
		mskOpts = append(mskOpts, awsclient.WithMSKStatusFunc(Statusf))
	}
	mskOpts = append(mskOpts, awsclient.WithMSKConcurrency(Concurrency))
	mskClient := client.MSKClient(mskOpts...)

	Statusf("Fetching MSK clusters...")
	clusters, err := mskClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing MSK clusters: %w", err)
	}

	// Sort clusters by name
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].ClusterName < clusters[j].ClusterName
	})

	// Print table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "CLUSTER\tKAFKA VERSION\tINSTANCE TYPE\tBROKERS\tSTATE")

	for _, cluster := range clusters {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\n", cluster.ClusterName, cluster.KafkaVersion, cluster.InstanceType, cluster.NumberOfBrokerNodes, cluster.State)
	}

	w.Flush()
	return nil
}

func runVersionElastiCache(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var ecOpts []awsclient.ElastiCacheOption
	if Verbose {
		ecOpts = append(ecOpts, awsclient.WithElastiCacheStatusFunc(Statusf))
	}
	ecClient := client.ElastiCacheClient(ecOpts...)

	Statusf("Fetching ElastiCache resources...")
	ecSummary, err := ecClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing ElastiCache resources: %w", err)
	}

	// Sort replication groups and cache clusters
	sort.Slice(ecSummary.ReplicationGroups, func(i, j int) bool {
		return ecSummary.ReplicationGroups[i].ReplicationGroupId < ecSummary.ReplicationGroups[j].ReplicationGroupId
	})
	sort.Slice(ecSummary.CacheClusters, func(i, j int) bool {
		return ecSummary.CacheClusters[i].CacheClusterId < ecSummary.CacheClusters[j].CacheClusterId
	})

	// Print table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TYPE\tIDENTIFIER\tENGINE\tVERSION\tNODE TYPE\tSTATUS")

	for _, rg := range ecSummary.ReplicationGroups {
		fmt.Fprintf(w, "replication-group\t%s\t%s\t%s\t%s\t%s\n", rg.ReplicationGroupId, rg.Engine, rg.EngineVersion, rg.NodeType, rg.Status)
	}

	// Only show standalone cache clusters (not part of a replication group)
	for _, cc := range ecSummary.CacheClusters {
		if cc.ReplicationGroupId != "" {
			continue
		}
		fmt.Fprintf(w, "cache-cluster\t%s\t%s\t%s\t%s\t%s\n", cc.CacheClusterId, cc.Engine, cc.EngineVersion, cc.CacheNodeType, cc.Status)
	}

	w.Flush()
	return nil
}

func runVersionOpenSearch(cmd *cobra.Command, args []string) error {
	ctx := context.Background()

	client, err := BuildClient(ctx)
	if err != nil {
		return fmt.Errorf("creating AWS client: %w", err)
	}

	var osOpts []awsclient.OpenSearchOption
	if Verbose {
		osOpts = append(osOpts, awsclient.WithOpenSearchStatusFunc(Statusf))
	}
	osClient := client.OpenSearchClient(osOpts...)

	Statusf("Fetching OpenSearch domains...")
	domains, err := osClient.Summarise(ctx)
	if err != nil {
		return fmt.Errorf("listing OpenSearch domains: %w", err)
	}

	// Sort domains by name
	sort.Slice(domains, func(i, j int) bool {
		return domains[i].DomainName < domains[j].DomainName
	})

	// Print table output
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "DOMAIN\tVERSION\tINSTANCE TYPE\tINSTANCES")

	for _, domain := range domains {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\n", domain.DomainName, domain.EngineVersion, domain.ClusterInstanceType, domain.ClusterInstanceCount)
	}

	w.Flush()
	return nil
}
