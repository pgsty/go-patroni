package cli

import (
	"github.com/spf13/cobra"
)

type dsnOptions struct {
	role   string
	member string
	group  int
}

type queryOptions struct {
	group     int
	format    string
	file      string
	password  bool
	username  string
	watchTwo  bool
	watch     float64
	role      string
	member    string
	delimiter string
	command   string
	database  string
}

type listOptions struct {
	group     int
	all       bool
	extended  bool
	timestamp bool
	format    string
	watchTwo  bool
	watch     float64
}

type topologyOptions struct {
	group    int
	all      bool
	watchTwo bool
	watch    float64
}

type discoverOptions struct{ format string }

type showConfigOptions struct{ group int }

type versionOptions struct{ group int }

type historyOptions struct {
	group  int
	format string
}

func (application *adapter) addReadCommands(root *cobra.Command) {
	root.AddCommand(
		application.newInspectConfigCommand(),
		application.newDiscoverCommand(),
		application.newDSNCommand(),
		application.newQueryCommand(),
		application.newListCommand(),
		application.newTopologyCommand(),
		application.newShowConfigCommand(),
		application.newVersionCommand(),
		application.newHistoryCommand(),
	)
}

func (application *adapter) newInspectConfigCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "inspect-config",
		Short: "Inspect the redacted effective configuration, sources, and current target",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return application.runInspectConfig(command)
		},
	}
}

func (application *adapter) newDiscoverCommand() *cobra.Command {
	options := &discoverOptions{format: "pretty"}
	command := &cobra.Command{
		Use:   "discover",
		Short: "Discover all Patroni clusters in the selected context and namespace",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			return application.runDiscover(command, options)
		},
	}
	command.Flags().StringVarP(&options.format, "format", "f", "pretty", "Output format (pretty, tsv, json, yaml)")
	return command
}

func (application *adapter) newDSNCommand() *cobra.Command {
	options := &dsnOptions{}
	command := &cobra.Command{
		Use:   "dsn [cluster_name]",
		Short: "Generate a dsn for the provided member, defaults to a dsn of the leader",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runDSN(command, args, options)
		},
	}
	command.Flags().StringVarP(&options.role, "role", "r", "", "Give a dsn of any member with this role")
	command.Flags().StringVarP(&options.member, "member", "m", "", "Generate a dsn for this member")
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	return command
}

func (application *adapter) newQueryCommand() *cobra.Command {
	options := &queryOptions{format: "tsv", delimiter: "\t"}
	command := &cobra.Command{
		Use:   "query [cluster_name]",
		Short: "Query a Patroni PostgreSQL member",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runQuery(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().StringVar(&options.format, "format", "tsv", "Output format (pretty, tsv, json, yaml)")
	command.Flags().StringVarP(&options.file, "file", "f", "", "Execute the SQL commands from this file")
	command.Flags().BoolVar(&options.password, "password", false, "Force password prompt")
	command.Flags().StringVarP(&options.username, "username", "U", "", "Database user name")
	command.Flags().BoolVarP(&options.watchTwo, "watch-2s", "W", false, "Auto update the screen every 2 seconds")
	command.Flags().Float64VarP(&options.watch, "watch", "w", 0, "Auto update the screen every X seconds")
	command.Flags().StringVarP(&options.role, "role", "r", "", "The role of the query")
	command.Flags().StringVarP(&options.member, "member", "m", "", "Query a specific member")
	command.Flags().StringVar(&options.delimiter, "delimiter", "\t", "The column delimiter")
	command.Flags().StringVarP(&options.command, "command", "c", "", "The SQL commands to execute")
	command.Flags().StringVarP(&options.database, "dbname", "d", "", "Database name to connect to")
	return command
}

func (application *adapter) newListCommand() *cobra.Command {
	options := &listOptions{format: "pretty"}
	command := &cobra.Command{
		Use:   "list [cluster_names...]",
		Short: "List the Patroni members for a given Patroni",
		Args:  cobra.ArbitraryArgs,
		RunE: func(command *cobra.Command, args []string) error {
			return application.runList(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().BoolVar(&options.all, "all", false, "List all clusters discovered in the selected context and namespace")
	command.Flags().BoolVarP(&options.extended, "extended", "e", false, "Show some extra information")
	command.Flags().BoolVarP(&options.timestamp, "timestamp", "t", false, "Print timestamp")
	command.Flags().StringVarP(&options.format, "format", "f", "pretty", "Output format")
	command.Flags().BoolVarP(&options.watchTwo, "watch-2s", "W", false, "Auto update the screen every 2 seconds")
	command.Flags().Float64VarP(&options.watch, "watch", "w", 0, "Auto update the screen every X seconds")
	return command
}

func (application *adapter) newTopologyCommand() *cobra.Command {
	options := &topologyOptions{}
	command := &cobra.Command{
		Use:   "topology [cluster_names...]",
		Short: "Prints ASCII topology for given cluster",
		Args:  cobra.ArbitraryArgs,
		RunE: func(command *cobra.Command, args []string) error {
			return application.runTopology(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().BoolVar(&options.all, "all", false, "Show topology for all clusters discovered in the selected context and namespace")
	command.Flags().BoolVarP(&options.watchTwo, "watch-2s", "W", false, "Auto update the screen every 2 seconds")
	command.Flags().Float64VarP(&options.watch, "watch", "w", 0, "Auto update the screen every X seconds")
	return command
}

func (application *adapter) newShowConfigCommand() *cobra.Command {
	options := &showConfigOptions{}
	command := &cobra.Command{
		Use:   "show-config [cluster_name]",
		Short: "Show cluster configuration",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runShowConfig(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	return command
}

func (application *adapter) newVersionCommand() *cobra.Command {
	options := &versionOptions{}
	command := &cobra.Command{
		Use:   "version [cluster_name] [member_names...]",
		Short: "Output version of patronictl or a running Patroni instance",
		Args:  cobra.ArbitraryArgs,
		RunE: func(command *cobra.Command, args []string) error {
			return application.runVersion(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	return command
}

func (application *adapter) newHistoryCommand() *cobra.Command {
	options := &historyOptions{format: "pretty"}
	command := &cobra.Command{
		Use:   "history [cluster_name]",
		Short: "Show the history of failovers/switchovers",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runHistory(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().StringVarP(&options.format, "format", "f", "pretty", "Output format")
	return command
}
