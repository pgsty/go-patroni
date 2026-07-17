package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	internalsecret "github.com/pgsty/go-patroni/internal/secret"
	"github.com/pgsty/go-patroni/internal/version"
	"github.com/pgsty/go-patroni/model"
	"github.com/pgsty/go-patroni/postgres"
	"github.com/spf13/cobra"
	"go.yaml.in/yaml/v3"
)

func (application *adapter) runDSN(command *cobra.Command, args []string, options *dsnOptions) (returnedError error) {
	if strings.TrimSpace(options.role) != "" && strings.TrimSpace(options.member) != "" {
		return usageError("--role and --member are mutually exclusive options")
	}
	role, err := parseRole(options.role)
	if err != nil {
		return err
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationClusterRead, explicitScope: firstArgument(args), explicitGroup: optionalGroup(command, options.group),
	})
	if err != nil {
		return err
	}
	defer closeCommandRuntime(runtime, &returnedError)
	result := runtime.service.DSN(command.Context(), control.DSNRequest{
		Target: runtime.target, Role: role, Member: options.member, Citus: runtime.resolved.Citus, AllowUnsupportedRead: application.root.allowUnsupportedRead,
	})
	return finishResult(application, application.root.output, runtime, result, "DSN", func(writer io.Writer, data control.DSNData) error {
		_, writeError := fmt.Fprintln(writer, data.String())
		return writeError
	})
}

func (application *adapter) runQuery(command *cobra.Command, args []string, options *queryOptions) (returnedError error) {
	if !oneOf(options.format, "pretty", "tsv", "json", "yaml") {
		return usageError(fmt.Sprintf("invalid --format %q: choose pretty, tsv, json, or yaml", options.format))
	}
	if options.file != "" && options.command != "" {
		return usageError("--file and --command are mutually exclusive options")
	}
	if options.file == "" && options.command == "" {
		return usageError("you need to specify either --command or --file")
	}
	if options.delimiter == "" {
		return usageError("--delimiter must not be empty")
	}
	role, err := parseRole(options.role)
	if err != nil {
		return err
	}
	sql, err := application.querySQL(command.Context(), options)
	if err != nil {
		return err
	}
	// Match patronictl/libpq behavior: inherit sslmode and fallback semantics
	// from standard PostgreSQL sources unless the embedding API explicitly
	// chooses a stricter mode.
	connection := (postgres.ConnectionOptions{Database: options.database, Username: options.username}).WithTLSMode(postgres.TLSFromSource)
	if options.password {
		password, promptError := application.promptPassword("Password")
		if promptError != nil {
			return promptError
		}
		connection = connection.WithPassword(password)
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationQuery, explicitScope: firstArgument(args), explicitGroup: optionalGroup(command, options.group),
	})
	if err != nil {
		return err
	}
	defer closeCommandRuntime(runtime, &returnedError)
	interval, err := watchInterval(options.watchTwo, options.watch)
	if err != nil {
		return err
	}
	if application.root.output != "" && interval > 0 {
		return usageError("--output cannot be combined with query watch mode")
	}
	return runWatch(command.Context(), interval, func() error {
		result := runtime.service.Query(command.Context(), control.QueryRequest{
			Target: runtime.target, Role: role, Member: options.member, Citus: runtime.resolved.Citus, Connection: connection, SQL: sql,
		})
		return finishResult(application, application.root.output, runtime, result, "QueryResult", func(writer io.Writer, data control.QueryData) error {
			return renderQueryData(writer, data, options.format, options.delimiter, application.wallNow())
		})
	})
}

func (application *adapter) runInspectConfig(command *cobra.Command) (returnedError error) {
	runtime, err := application.openRuntime(command, runtimeRequest{operation: config.OperationInspect})
	if err != nil {
		return err
	}
	defer closeCommandRuntime(runtime, &returnedError)
	result := runtime.service.InspectConfiguration(command.Context(), control.InspectConfigurationRequest{Resolved: runtime.resolved})
	return finishResult(application, application.root.output, runtime, result, "EffectiveConfiguration", renderConfigurationInspection)
}

func renderConfigurationInspection(writer io.Writer, data control.ConfigurationInspection) error {
	if _, err := fmt.Fprintln(writer, "Target:"); err != nil {
		return err
	}
	target, err := yaml.Marshal(data.Target)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSuffix(string(target), "\n"), "\n") {
		if _, err := fmt.Fprintf(writer, "  %s\n", line); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(writer, "Effective configuration:"); err != nil {
		return err
	}
	effective, err := yaml.Marshal(data.Effective)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(strings.TrimSuffix(string(effective), "\n"), "\n") {
		if _, err := fmt.Fprintf(writer, "  %s\n", line); err != nil {
			return err
		}
	}
	if _, err := fmt.Fprintln(writer, "Network deadlines:"); err != nil {
		return err
	}
	deadlines := data.NetworkTimeouts
	deadlineRows := [][]any{
		{"DNS lookup", (time.Duration(deadlines.DNSLookupMilliseconds) * time.Millisecond).String()},
		{"etcd3 dial", (time.Duration(deadlines.DCSDialMilliseconds) * time.Millisecond).String()},
		{"etcd3 request/watch lease", (time.Duration(deadlines.DCSRequestMilliseconds) * time.Millisecond).String()},
		{"Patroni REST request", (time.Duration(deadlines.PatroniRequestMilliseconds) * time.Millisecond).String()},
		{"PostgreSQL query", (time.Duration(deadlines.PostgresQueryMilliseconds) * time.Millisecond).String()},
		{"PostgreSQL close", (time.Duration(deadlines.PostgresCloseMilliseconds) * time.Millisecond).String()},
	}
	if err := renderRows(writer, []string{"Network operation", "Deadline"}, deadlineRows, "pretty", "\t", ""); err != nil {
		return err
	}
	if _, err := fmt.Fprintln(writer, "Configuration sources:"); err != nil {
		return err
	}
	rows := make([][]any, 0, len(data.Sources))
	for _, source := range data.Sources {
		rows = append(rows, []any{source.Field, source.Layer, source.Name})
	}
	if len(rows) == 0 {
		_, err := fmt.Fprintln(writer, "  (none)")
		return err
	}
	return renderRows(writer, []string{"Field", "Layer", "Source"}, rows, "pretty", "\t", "")
}

func (application *adapter) runDiscover(command *cobra.Command, options *discoverOptions) (returnedError error) {
	if !oneOf(options.format, "pretty", "tsv", "json", "yaml", "yml") {
		return usageError(fmt.Sprintf("invalid --format %q", options.format))
	}
	runtime, err := application.openRuntime(command, runtimeRequest{operation: config.OperationDiscover, allowMissingScope: true})
	if err != nil {
		return err
	}
	defer closeCommandRuntime(runtime, &returnedError)
	result := runtime.service.Discover(command.Context(), control.DiscoverRequest{
		Context: runtime.target.Context, Namespace: runtime.target.Namespace,
		AllowUnsupportedRead: application.root.allowUnsupportedRead,
	})
	if application.root.output == "json" || application.root.output == "yaml" {
		return finishResult(application, application.root.output, runtime, machineDiscoveryResult(result), "ClusterDiscovery", nil)
	}
	return finishResult(application, application.root.output, runtime, result, "ClusterDiscovery", func(writer io.Writer, data control.DiscoverData) error {
		return renderDiscoveryData(writer, data, options.format)
	})
}

func (application *adapter) runList(command *cobra.Command, args []string, options *listOptions) (returnedError error) {
	if !oneOf(options.format, "pretty", "tsv", "json", "yaml", "yml") {
		return usageError(fmt.Sprintf("invalid --format %q", options.format))
	}
	if options.all && len(args) > 0 {
		return usageError("--all and explicit cluster names are mutually exclusive")
	}
	if options.all && command.Flags().Changed("group") {
		return usageError("--all and --group are mutually exclusive")
	}
	interval, err := watchInterval(options.watchTwo, options.watch)
	if err != nil {
		return err
	}
	if application.root.output != "" && interval > 0 {
		return usageError("--output cannot be combined with list watch mode")
	}
	requestScope := firstArgument(args)
	operation := config.OperationClusterRead
	explicitGroup := optionalGroup(command, options.group)
	if options.all {
		operation, requestScope, explicitGroup = config.OperationDiscover, "", nil
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: operation, explicitScope: requestScope, explicitGroup: explicitGroup, allowMissingScope: true,
	})
	if err != nil {
		return err
	}
	defer closeCommandRuntime(runtime, &returnedError)
	if options.all {
		return runWatch(command.Context(), interval, func() error {
			result := runtime.service.ListAll(command.Context(), control.ListAllRequest{
				Context: runtime.target.Context, Namespace: runtime.target.Namespace,
				AllowUnsupportedRead: application.root.allowUnsupportedRead,
			})
			if application.root.output == "json" || application.root.output == "yaml" {
				return finishResult(application, application.root.output, runtime, machineClusterListResult(result), "ClusterList", nil)
			}
			return finishResult(application, application.root.output, runtime, result, "ClusterList", func(writer io.Writer, data control.ListData) error {
				if options.timestamp {
					if _, err := fmt.Fprintln(writer, application.wallNow().Format("2006-01-02 15:04:05")); err != nil {
						return err
					}
				}
				return renderListData(writer, data, runtime.resolved.Citus || clusterListHasGroup(data), options.extended, options.format)
			})
		})
	}
	clusterNames := append([]string(nil), args...)
	if len(clusterNames) == 0 && runtime.target.Scope != "" {
		clusterNames = []string{runtime.target.Scope}
	}
	if len(clusterNames) == 0 {
		_, _ = fmt.Fprintln(application.stderr, "Listing members: No cluster names were provided")
		return nil
	}
	return runWatch(command.Context(), interval, func() error {
		targets := make([]model.Target, 0, len(clusterNames))
		for _, scope := range clusterNames {
			target := runtime.target
			target.Scope = scope
			target.Member = ""
			targets = append(targets, target.Normalize())
		}
		result := runtime.service.List(command.Context(), control.ListRequest{
			Targets: targets, Citus: runtime.resolved.Citus, AllowUnsupportedRead: application.root.allowUnsupportedRead,
		})
		if application.root.output == "json" || application.root.output == "yaml" {
			return finishResult(application, application.root.output, runtime, machineClusterListResult(result), "ClusterList", nil)
		}
		return finishResult(application, application.root.output, runtime, result, "ClusterList", func(writer io.Writer, data control.ListData) error {
			if options.timestamp {
				if _, err := fmt.Fprintln(writer, application.wallNow().Format("2006-01-02 15:04:05")); err != nil {
					return err
				}
			}
			return renderListData(writer, data, runtime.resolved.Citus || clusterListHasGroup(data), options.extended, options.format)
		})
	})
}

type machineClusterList struct {
	Clusters []machineCluster `json:"clusters" yaml:"clusters"`
}

// machineCluster is kept separate from the normalized model so future adapter
// contract changes cannot silently alter the public control DTO.
type machineCluster struct {
	Target              model.Target               `json:"target" yaml:"target"`
	DiscoveryState      model.DiscoveryState       `json:"discoveryState" yaml:"discoveryState"`
	ManagementState     model.ManagementState      `json:"managementState" yaml:"managementState"`
	ReachabilityState   model.ReachabilityState    `json:"reachabilityState" yaml:"reachabilityState"`
	HealthState         model.HealthState          `json:"healthState" yaml:"healthState"`
	Revision            int64                      `json:"revision" yaml:"revision"`
	Initialize          string                     `json:"initialize,omitempty" yaml:"initialize,omitempty"`
	Leader              string                     `json:"leader,omitempty" yaml:"leader,omitempty"`
	Paused              bool                       `json:"paused" yaml:"paused"`
	ScheduledSwitchover *model.ScheduledSwitchover `json:"scheduledSwitchover,omitempty" yaml:"scheduledSwitchover,omitempty"`
	Members             []model.Member             `json:"members" yaml:"members"`
}

func machineClusterListResult(result control.Result[control.ListData]) control.Result[machineClusterList] {
	data := machineClusterList{Clusters: make([]machineCluster, 0, len(result.Data.Clusters))}
	for _, cluster := range result.Data.Clusters {
		data.Clusters = append(data.Clusters, projectMachineCluster(cluster))
	}
	return control.Result[machineClusterList]{
		OperationID: result.OperationID, Outcome: result.Outcome, Target: result.Target, Path: result.Path,
		Data: data, Evidence: append([]control.Evidence{}, result.Evidence...), Error: result.Error,
	}
}

func projectMachineCluster(cluster model.Cluster) machineCluster {
	return machineCluster{
		Target: cluster.Target, DiscoveryState: cluster.DiscoveryState, ManagementState: cluster.ManagementState,
		ReachabilityState: cluster.ReachabilityState, HealthState: cluster.HealthState,
		Revision: cluster.Revision, Initialize: cluster.Initialize, Leader: cluster.Leader, Paused: cluster.Paused,
		ScheduledSwitchover: cluster.ScheduledSwitchover, Members: append([]model.Member{}, cluster.Members...),
	}
}

func (application *adapter) runTopology(command *cobra.Command, args []string, options *topologyOptions) (returnedError error) {
	if options.all && len(args) > 0 {
		return usageError("--all and explicit cluster names are mutually exclusive")
	}
	if options.all && command.Flags().Changed("group") {
		return usageError("--all and --group are mutually exclusive")
	}
	interval, err := watchInterval(options.watchTwo, options.watch)
	if err != nil {
		return err
	}
	if application.root.output != "" && interval > 0 {
		return usageError("--output cannot be combined with topology watch mode")
	}
	operation := config.OperationClusterRead
	explicitScope := firstArgument(args)
	explicitGroup := optionalGroup(command, options.group)
	if options.all {
		operation, explicitScope, explicitGroup = config.OperationDiscover, "", nil
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: operation, explicitScope: explicitScope, explicitGroup: explicitGroup, allowMissingScope: true,
	})
	if err != nil {
		return err
	}
	defer closeCommandRuntime(runtime, &returnedError)
	if options.all {
		return runWatch(command.Context(), interval, func() error {
			result := runtime.service.TopologyAll(command.Context(), control.TopologyAllRequest{
				Context: runtime.target.Context, Namespace: runtime.target.Namespace,
				AllowUnsupportedRead: application.root.allowUnsupportedRead,
			})
			if application.root.output == "json" || application.root.output == "yaml" {
				return finishResult(application, application.root.output, runtime, machineTopologyListResult(result), "ClusterTopologyList", nil)
			}
			return finishResult(application, application.root.output, runtime, result, "ClusterTopologyList", func(writer io.Writer, data control.TopologyListData) error {
				for _, topology := range data.Topologies {
					if err := renderTopologyData(writer, topology); err != nil {
						return err
					}
				}
				return nil
			})
		})
	}
	clusterNames := append([]string(nil), args...)
	if len(clusterNames) == 0 && runtime.target.Scope != "" {
		clusterNames = []string{runtime.target.Scope}
	}
	if len(clusterNames) == 0 {
		_, _ = fmt.Fprintln(application.stderr, "Listing members: No cluster names were provided")
		return nil
	}
	if application.root.output != "" && (interval > 0 || len(clusterNames) != 1) {
		return usageError("--output topology requires exactly one cluster and no watch mode")
	}
	return runWatch(command.Context(), interval, func() error {
		for _, scope := range clusterNames {
			target := runtime.target
			target.Scope = scope
			if runtime.resolved.Citus && explicitGroup == nil {
				result := runtime.service.TopologyGroups(command.Context(), control.TopologyGroupsRequest{
					Target: target.Normalize(), AllowUnsupportedRead: application.root.allowUnsupportedRead,
				})
				if application.root.output == "json" || application.root.output == "yaml" {
					if err := finishResult(application, application.root.output, runtime, machineTopologyListResult(result), "ClusterTopologyList", nil); err != nil {
						return err
					}
					continue
				}
				if err := finishResult(application, application.root.output, runtime, result, "ClusterTopologyList", func(writer io.Writer, data control.TopologyListData) error {
					for _, topology := range data.Topologies {
						if err := renderTopologyData(writer, topology); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					return err
				}
				continue
			}
			result := runtime.service.Topology(command.Context(), control.TopologyRequest{
				Target: target.Normalize(), AllowUnsupportedRead: application.root.allowUnsupportedRead,
			})
			if err := finishResult(application, application.root.output, runtime, result, "ClusterTopology", func(writer io.Writer, data control.TopologyData) error {
				return renderTopologyData(writer, data)
			}); err != nil {
				return err
			}
		}
		return nil
	})
}

type machineDiscovery struct {
	Clusters []machineClusterSummary `json:"clusters" yaml:"clusters"`
}

type machineClusterSummary struct {
	Target            model.Target            `json:"target" yaml:"target"`
	DiscoveryState    model.DiscoveryState    `json:"discoveryState" yaml:"discoveryState"`
	ManagementState   model.ManagementState   `json:"managementState" yaml:"managementState"`
	ReachabilityState model.ReachabilityState `json:"reachabilityState" yaml:"reachabilityState"`
	HealthState       model.HealthState       `json:"healthState" yaml:"healthState"`
	Revision          int64                   `json:"revision" yaml:"revision"`
	MemberCount       int                     `json:"memberCount" yaml:"memberCount"`
	Leader            string                  `json:"leader,omitempty" yaml:"leader,omitempty"`
}

func machineDiscoveryResult(result control.Result[control.DiscoverData]) control.Result[machineDiscovery] {
	data := machineDiscovery{Clusters: make([]machineClusterSummary, 0, len(result.Data.Clusters))}
	for _, cluster := range result.Data.Clusters {
		data.Clusters = append(data.Clusters, machineClusterSummary{
			Target: cluster.Target, DiscoveryState: cluster.DiscoveryState, ManagementState: cluster.ManagementState,
			ReachabilityState: cluster.ReachabilityState, HealthState: cluster.HealthState,
			Revision: cluster.Revision, MemberCount: cluster.MemberCount, Leader: cluster.Leader,
		})
	}
	return control.Result[machineDiscovery]{
		OperationID: result.OperationID, Outcome: result.Outcome, Target: result.Target, Path: result.Path,
		Data: data, Evidence: append([]control.Evidence{}, result.Evidence...), Error: result.Error,
	}
}

type machineTopologyList struct {
	Topologies []machineTopology `json:"topologies" yaml:"topologies"`
}

type machineTopology struct {
	Cluster machineCluster          `json:"cluster" yaml:"cluster"`
	Members []machineTopologyMember `json:"members" yaml:"members"`
}

type machineTopologyMember struct {
	Member model.Member `json:"member" yaml:"member"`
	Parent string       `json:"parent,omitempty" yaml:"parent,omitempty"`
	Depth  int          `json:"depth" yaml:"depth"`
}

func machineTopologyListResult(result control.Result[control.TopologyListData]) control.Result[machineTopologyList] {
	data := machineTopologyList{Topologies: make([]machineTopology, 0, len(result.Data.Topologies))}
	for _, topology := range result.Data.Topologies {
		item := machineTopology{Cluster: projectMachineCluster(topology.Cluster), Members: make([]machineTopologyMember, 0, len(topology.Members))}
		for _, member := range topology.Members {
			item.Members = append(item.Members, machineTopologyMember{Member: member.Member, Parent: member.Parent, Depth: member.Depth})
		}
		data.Topologies = append(data.Topologies, item)
	}
	return control.Result[machineTopologyList]{
		OperationID: result.OperationID, Outcome: result.Outcome, Target: result.Target, Path: result.Path,
		Data: data, Evidence: append([]control.Evidence{}, result.Evidence...), Error: result.Error,
	}
}

func (application *adapter) runShowConfig(command *cobra.Command, args []string, options *showConfigOptions) (returnedError error) {
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationClusterRead, explicitScope: firstArgument(args), explicitGroup: optionalGroup(command, options.group), useConfiguredGroup: true,
	})
	if err != nil {
		return err
	}
	defer closeCommandRuntime(runtime, &returnedError)
	result := runtime.service.ShowConfig(command.Context(), control.ShowConfigRequest{
		Target: runtime.target, AllowUnsupportedRead: application.root.allowUnsupportedRead,
	})
	return finishResult(application, application.root.output, runtime, result, "DynamicConfig", func(writer io.Writer, data control.ConfigData) error {
		encoded, encodeError := yaml.Marshal(yamlCompatibleValue(data.Config))
		if encodeError != nil {
			return encodeError
		}
		_, writeError := fmt.Fprint(writer, internalsecret.Redact(string(encoded)))
		return writeError
	})
}

func (application *adapter) runVersion(command *cobra.Command, args []string, options *versionOptions) (returnedError error) {
	if len(args) == 0 {
		information := newMachineVersionInfo(nil)
		if application.root.output == "json" || application.root.output == "yaml" {
			document, documentError := newMachineSuccessEnvelope(machineKindVersionInfo, machineMetadata{
				RequestID: application.requestID(), ObservedAt: application.now().Format(time.RFC3339Nano), Warnings: []string{},
			}, information)
			if documentError != nil {
				return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "constructing version output failed", cause: documentError}
			}
			if err := encodeDocument(application.stdout, application.root.output, document); err != nil {
				return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "encoding version output failed", cause: err}
			}
			return nil
		}
		_, err := fmt.Fprintf(application.stdout, "patronictl version %s\n", version.String())
		return err
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationClusterRead, explicitScope: args[0], explicitGroup: optionalGroup(command, options.group),
	})
	if err != nil {
		return err
	}
	defer closeCommandRuntime(runtime, &returnedError)
	result := runtime.service.Version(command.Context(), control.VersionRequest{
		Target: runtime.target, Members: append([]string(nil), args[1:]...),
		Citus:                runtime.resolved.Citus,
		AllowUnsupportedRead: application.root.allowUnsupportedRead,
	})
	if application.root.output == "json" || application.root.output == "yaml" {
		return finishResult(application, application.root.output, runtime, machineVersionResult(result), "VersionInfo", nil)
	}
	return finishResult(application, application.root.output, runtime, result, "VersionInfo", func(writer io.Writer, data control.VersionData) error {
		if _, err := fmt.Fprintf(writer, "patronictl version %s\n", version.String()); err != nil {
			return err
		}
		if len(data.Members) > 0 {
			if _, err := fmt.Fprintln(writer); err != nil {
				return err
			}
		}
		for _, member := range data.Members {
			if member.Error != nil {
				if _, err := fmt.Fprintf(writer, "%s: failed to get version: %s\n", member.Target.Member, member.Error.Message); err != nil {
					return err
				}
				continue
			}
			postgresVersion := ""
			if member.PostgresVersion != "" {
				postgresVersion = " PostgreSQL " + member.PostgresVersion
			}
			if _, err := fmt.Fprintf(writer, "%s: Patroni %s%s\n", member.Target.Member, member.PatroniVersion, postgresVersion); err != nil {
				return err
			}
		}
		return nil
	})
}

// machineVersionInfo is adapter-owned so the stable machine contract does not
// serialize the control DTO directly. Local and cluster forms have one shape;
// members is always an array, including for `patronictl version` without a cluster.
type machineVersionInfo struct {
	Version          string                 `json:"version" yaml:"version"`
	Commit           string                 `json:"commit" yaml:"commit"`
	BuildTime        string                 `json:"buildTime" yaml:"buildTime"`
	GoVersion        string                 `json:"goVersion" yaml:"goVersion"`
	SupportedPatroni string                 `json:"supportedPatroni" yaml:"supportedPatroni"`
	MachineSchema    string                 `json:"machineSchema" yaml:"machineSchema"`
	Members          []machineMemberVersion `json:"members" yaml:"members"`
}

type machineMemberVersion struct {
	Target          model.Target  `json:"target" yaml:"target"`
	PatroniVersion  string        `json:"patroniVersion,omitempty" yaml:"patroniVersion,omitempty"`
	PostgresVersion string        `json:"postgresVersion,omitempty" yaml:"postgresVersion,omitempty"`
	HTTPStatus      int           `json:"httpStatus,omitempty" yaml:"httpStatus,omitempty"`
	Error           *machineError `json:"error,omitempty" yaml:"error,omitempty"`
}

func newMachineVersionInfo(members []control.MemberVersion) machineVersionInfo {
	information := version.Current()
	machineMembers := make([]machineMemberVersion, 0, len(members))
	for _, member := range members {
		machineMembers = append(machineMembers, machineMemberVersion{
			Target: member.Target, PatroniVersion: member.PatroniVersion, PostgresVersion: member.PostgresVersion,
			HTTPStatus: member.HTTPStatus, Error: newMachineError(member.Error),
		})
	}
	return machineVersionInfo{
		Version: information.Version, Commit: information.Commit, BuildTime: information.BuildTime,
		GoVersion: information.GoVersion, SupportedPatroni: information.SupportedPatroni,
		MachineSchema: information.MachineSchema, Members: machineMembers,
	}
}

func machineVersionResult(result control.Result[control.VersionData]) control.Result[machineVersionInfo] {
	return control.Result[machineVersionInfo]{
		OperationID: result.OperationID, Outcome: result.Outcome, Target: result.Target, Path: result.Path,
		Data: newMachineVersionInfo(result.Data.Members), Evidence: append([]control.Evidence{}, result.Evidence...), Error: result.Error,
	}
}

func (application *adapter) runHistory(command *cobra.Command, args []string, options *historyOptions) (returnedError error) {
	if !oneOf(options.format, "pretty", "tsv", "json", "yaml", "yml") {
		return usageError(fmt.Sprintf("invalid --format %q", options.format))
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationClusterRead, explicitScope: firstArgument(args), explicitGroup: optionalGroup(command, options.group), useConfiguredGroup: true,
	})
	if err != nil {
		return err
	}
	defer closeCommandRuntime(runtime, &returnedError)
	result := runtime.service.History(command.Context(), control.HistoryRequest{
		Target: runtime.target, AllowUnsupportedRead: application.root.allowUnsupportedRead,
	})
	return finishResult(application, application.root.output, runtime, result, "ClusterHistory", func(writer io.Writer, data control.HistoryData) error {
		headers := []string{"TL", "LSN", "Reason", "Timestamp", "New Leader"}
		rows := make([][]any, 0, len(data.Entries))
		for _, entry := range data.Entries {
			rows = append(rows, []any{entry.Timeline, entry.LSN, entry.Reason, entry.Timestamp, entry.NewLeader})
		}
		return renderRows(writer, headers, rows, options.format, "\t", "")
	})
}

func (application *adapter) querySQL(ctx context.Context, options *queryOptions) (string, error) {
	if options.file == "" {
		return options.command, nil
	}
	if err := ctx.Err(); err != nil {
		return "", &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "query input canceled", cause: err}
	}
	var data []byte
	var err error
	if options.file == "-" {
		data, err = io.ReadAll(application.stdin)
	} else {
		data, err = os.ReadFile(options.file)
	}
	if err != nil {
		return "", &exitError{category: control.CategoryConfig, code: control.ExitCode(control.CategoryConfig), message: "query file is not readable", cause: err}
	}
	if err := ctx.Err(); err != nil {
		return "", &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "query input canceled", cause: err}
	}
	return string(data), nil
}

func (application *adapter) promptPassword(label string) (string, error) {
	if err := application.promptUnavailableError(); err != nil {
		return "", usageError("password prompting requires a terminal and human output; use a PostgreSQL credential source")
	}
	if _, err := fmt.Fprintf(application.stderr, "%s: ", label); err != nil {
		return "", err
	}
	if file, ok := application.stdin.(*os.File); ok && isTerminalFile(file) {
		value, err := readPasswordFile(file)
		_, _ = fmt.Fprintln(application.stderr)
		if err != nil {
			return "", &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "password prompt failed", cause: err}
		}
		return strings.TrimSuffix(string(value), "\r"), nil
	}
	value, err := application.input.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "password prompt failed", cause: err}
	}
	return strings.TrimRight(value, "\r\n"), nil
}

func renderQueryData(writer io.Writer, data control.QueryData, format, delimiter string, observedAt time.Time) error {
	if data.Error != nil {
		return renderRows(writer, []string{"Timestamp", "Error"}, [][]any{{observedAt.Format("2006-01-02 15:04:05"), data.Error.Message}}, format, delimiter, "")
	}
	for _, set := range data.Result.Sets {
		if len(set.Columns) == 0 {
			continue
		}
		headers := make([]string, len(set.Columns))
		for index, column := range set.Columns {
			headers[index] = column.Name
		}
		rows := make([][]any, len(set.Rows))
		for rowIndex, row := range set.Rows {
			rows[rowIndex] = make([]any, len(row))
			for columnIndex, cell := range row {
				if cell.Null {
					rows[rowIndex][columnIndex] = nil
				} else {
					rows[rowIndex][columnIndex] = cell.Text
				}
			}
		}
		if err := renderRows(writer, headers, rows, format, delimiter, ""); err != nil {
			return err
		}
	}
	return nil
}

func renderDiscoveryData(writer io.Writer, data control.DiscoverData, format string) error {
	hasGroup := false
	for _, cluster := range data.Clusters {
		hasGroup = hasGroup || cluster.Target.Group != nil
	}
	headers := []string{"Scope", "Members", "Leader", "Reachability", "DCS Revision"}
	if hasGroup {
		headers = append(headers[:1], append([]string{"Group"}, headers[1:]...)...)
	}
	rows := make([][]any, 0, len(data.Clusters))
	for _, cluster := range data.Clusters {
		row := []any{cluster.Target.Scope, cluster.MemberCount, cluster.Leader, cluster.ReachabilityState, cluster.Revision}
		if hasGroup {
			row = append(row[:1], append([]any{groupValue(cluster.Target.Group)}, row[1:]...)...)
		}
		rows = append(rows, row)
	}
	return renderRows(writer, headers, rows, format, "\t", "")
}

func clusterListHasGroup(data control.ListData) bool {
	for _, cluster := range data.Clusters {
		if cluster.Target.Group != nil {
			return true
		}
	}
	return false
}

func renderListData(writer io.Writer, data control.ListData, citus, extended bool, format string) error {
	headers := []string{"Cluster", "Member", "Host", "Role", "State", "TL", "Receive LSN", "Receive Lag", "Replay LSN", "Replay Lag"}
	if citus {
		headers = append(headers[:1], append([]string{"Group"}, headers[1:]...)...)
	}
	showPending, showReason, showScheduled, showTags := extended, extended, extended, extended
	for _, cluster := range data.Clusters {
		for _, member := range cluster.Members {
			showPending = showPending || member.PendingRestart
			showReason = showReason || len(member.PendingRestartReason) > 0
			showScheduled = showScheduled || member.ScheduledRestart != nil
			showTags = showTags || len(member.Tags) > 0
		}
	}
	if showPending {
		headers = append(headers, "Pending restart")
	}
	if showReason {
		headers = append(headers, "Pending restart reason")
	}
	if showScheduled {
		headers = append(headers, "Scheduled restart")
	}
	if showTags {
		headers = append(headers, "Tags")
	}
	appendPort := false
	hostCounts := make(map[string]int)
	for _, cluster := range data.Clusters {
		for _, member := range cluster.Members {
			if member.Host != "" {
				hostCounts[member.Host]++
			}
			appendPort = appendPort || member.Port != 0 && member.Port != 5432
		}
	}
	for _, count := range hostCounts {
		appendPort = appendPort || count > 1
	}

	rows := make([][]any, 0)
	for _, cluster := range data.Clusters {
		for _, member := range cluster.Members {
			host := member.Host
			if appendPort && host != "" && member.Port != 0 {
				host = netJoinHostPort(host, member.Port)
			}
			receiveLSN, replayLSN := member.ReceiveLSN, member.ReplayLSN
			var receiveLag, replayLag any = "", ""
			if receiveLSN != "" {
				receiveLag = lagMB(member.ReceiveLagBytes)
				if replayLSN == "" {
					replayLSN = "unknown"
				}
			}
			if replayLSN != "" && replayLSN != "unknown" {
				replayLag = lagMB(member.ReplayLagBytes)
				if receiveLSN == "" {
					receiveLSN = "unknown"
				}
			}
			row := []any{cluster.Target.Scope, member.Name, host, roleTitle(member.Role), member.State, optionalIntValue(member.Timeline),
				receiveLSN, receiveLag, replayLSN, replayLag}
			if citus {
				row = append(row[:1], append([]any{groupValue(cluster.Target.Group)}, row[1:]...)...)
			}
			if showPending {
				value := ""
				if member.PendingRestart {
					value = "*"
				}
				row = append(row, value)
			}
			if showReason {
				row = append(row, pendingReason(member.PendingRestartReason))
			}
			if showScheduled {
				value := ""
				if member.ScheduledRestart != nil {
					value = member.ScheduledRestart.Schedule
					if member.ScheduledRestart.PostgresVersion != "" {
						value += " if version < " + member.ScheduledRestart.PostgresVersion
					}
				}
				row = append(row, value)
			}
			if showTags {
				row = append(row, member.Tags)
			}
			rows = append(rows, row)
		}
	}
	title := ""
	if len(data.Clusters) == 1 {
		title = fmt.Sprintf(" Cluster: %s (%s) ", data.Clusters[0].Target.Scope, clusterInitialize(data.Clusters[0].Initialize))
	}
	if (format == "pretty" || format == "topology") && len(data.Clusters) == 1 && !citus {
		headers = headers[1:]
		for index := range rows {
			rows[index] = rows[index][1:]
		}
	}
	return renderRows(writer, headers, rows, format, "\t", title)
}

func renderTopologyData(writer io.Writer, data control.TopologyData) error {
	clusterData := control.ListData{Clusters: []model.Cluster{data.Cluster}}
	byName := make(map[string]control.TopologyMember, len(data.Members))
	for _, member := range data.Members {
		byName[member.Member.Name] = member
	}
	for index := range clusterData.Clusters[0].Members {
		member := &clusterData.Clusters[0].Members[index]
		if topology, ok := byName[member.Name]; ok && topology.Depth > 0 {
			member.Name = strings.Repeat("  ", topology.Depth-1) + "+ " + member.Name
		}
	}
	return renderListData(writer, clusterData, data.Cluster.Target.Group != nil, false, "topology")
}

func parseRole(value string) (control.Role, error) {
	role := control.Role(strings.TrimSpace(value))
	switch role {
	case "", control.RoleLeader, control.RolePrimary, control.RoleStandbyLeader, control.RoleReplica, control.RoleStandby, control.RoleAny:
		return role, nil
	default:
		return "", usageError(fmt.Sprintf("invalid role %q", value))
	}
}

func optionalGroup(command *cobra.Command, value int) *int {
	if flag := command.Flags().Lookup("group"); flag == nil || !flag.Changed {
		return nil
	}
	copyValue := value
	return &copyValue
}

func firstArgument(args []string) string {
	if len(args) == 0 {
		return ""
	}
	return args[0]
}

func watchInterval(twoSeconds bool, seconds float64) (time.Duration, error) {
	if twoSeconds && seconds != 0 {
		return 0, usageError("-W and --watch are mutually exclusive")
	}
	if twoSeconds {
		return 2 * time.Second, nil
	}
	if seconds < 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0, usageError("--watch must be a finite non-negative number")
	}
	return time.Duration(seconds * float64(time.Second)), nil
}

func runWatch(ctx context.Context, interval time.Duration, action func() error) error {
	for {
		if err := action(); err != nil || interval <= 0 {
			return err
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "watch canceled", cause: ctx.Err()}
		case <-timer.C:
		}
	}
}

func oneOf(value string, values ...string) bool {
	for _, candidate := range values {
		if value == candidate {
			return true
		}
	}
	return false
}

func roleTitle(role model.MemberRole) string {
	words := strings.Fields(strings.ReplaceAll(string(role), "_", " "))
	for index := range words {
		words[index] = strings.ToUpper(words[index][:1]) + words[index][1:]
	}
	return strings.Join(words, " ")
}

func optionalIntValue(value *int) any {
	if value == nil {
		return ""
	}
	return *value
}

func groupValue(value *int) any {
	if value == nil {
		return ""
	}
	return *value
}

func lagMB(bytes int64) any {
	if bytes == 0 {
		return 0
	}
	return int64(math.Round(float64(bytes) / 1024 / 1024))
}

func pendingReason(reason map[string]any) string {
	keys := make([]string, 0, len(reason))
	for key := range reason {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	lines := make([]string, 0, len(keys))
	for _, key := range keys {
		values, _ := reason[key].(map[string]any)
		oldValue, newValue := fmt.Sprint(values["old_value"]), fmt.Sprint(values["new_value"])
		lines = append(lines, fmt.Sprintf("%s: %s->%s", key, oldValue, newValue))
	}
	return strings.Join(lines, "\n")
}

func clusterInitialize(value string) string {
	switch value {
	case "":
		return "initializing"
	default:
		return value
	}
}

func netJoinHostPort(host string, port uint16) string {
	if strings.Contains(host, ":") && !strings.HasPrefix(host, "[") {
		return "[" + host + "]:" + strconv.FormatUint(uint64(port), 10)
	}
	return host + ":" + strconv.FormatUint(uint64(port), 10)
}

func yamlCompatibleValue(value any) any {
	switch typed := value.(type) {
	case json.Number:
		if integer, err := typed.Int64(); err == nil {
			return integer
		}
		if number, err := typed.Float64(); err == nil {
			return number
		}
		return typed.String()
	case map[string]any:
		converted := make(map[string]any, len(typed))
		for key, item := range typed {
			converted[key] = yamlCompatibleValue(item)
		}
		return converted
	case []any:
		converted := make([]any, len(typed))
		for index, item := range typed {
			converted[index] = yamlCompatibleValue(item)
		}
		return converted
	default:
		return value
	}
}
