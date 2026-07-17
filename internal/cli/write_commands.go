package cli

import "github.com/spf13/cobra"

type removeOptions struct {
	group           int
	format          string
	confirmCluster  string
	acknowledgement string
	confirmLeader   string
}

type reloadOptions struct {
	group int
	role  string
	force bool
}

type restartOptions struct {
	group     int
	role      string
	any       bool
	scheduled string
	pgVersion string
	pending   bool
	timeout   string
	force     bool
}

type reinitOptions struct {
	group      int
	force      bool
	fromLeader bool
	wait       bool
}

type failoverOptions struct {
	group     int
	candidate string
	force     bool
}

type switchoverOptions struct {
	group     int
	leader    string
	candidate string
	scheduled string
	force     bool
}

type flushOptions struct {
	group int
	role  string
	force bool
}

type pauseOptions struct {
	group int
	wait  bool
}

type editConfigOptions struct {
	group       int
	quiet       bool
	settings    []string
	pgSettings  []string
	applyFile   string
	replaceFile string
	force       bool
}

type demoteClusterOptions struct {
	force           bool
	host            string
	port            int
	restoreCommand  string
	primarySlotName string
}

type promoteClusterOptions struct{ force bool }

func (application *adapter) addWriteCommands(root *cobra.Command) {
	root.AddCommand(
		application.newRemoveCommand(),
		application.newReloadCommand(),
		application.newRestartCommand(),
		application.newReinitCommand(),
		application.newFailoverCommand(),
		application.newSwitchoverCommand(),
		application.newFlushCommand(),
		application.newPauseCommand(false),
		application.newPauseCommand(true),
		application.newEditConfigCommand(),
		application.newDemoteClusterCommand(),
		application.newPromoteClusterCommand(),
	)
}

func (application *adapter) newRemoveCommand() *cobra.Command {
	options := &removeOptions{format: "pretty"}
	command := &cobra.Command{
		Use: "remove cluster_name", Short: "Remove cluster from DCS", Args: cobra.ExactArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runRemove(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().StringVarP(&options.format, "format", "f", "pretty", "Output format")
	command.Flags().StringVar(&options.confirmCluster, "confirm-cluster", "", "Explicitly confirm the exact cluster name without prompting")
	command.Flags().StringVar(&options.acknowledgement, "acknowledge-removal", "", "Explicitly provide the destructive removal acknowledgement without prompting")
	command.Flags().StringVar(&options.confirmLeader, "confirm-leader", "", "Explicitly confirm the current leader name without prompting")
	return command
}

func (application *adapter) newReloadCommand() *cobra.Command {
	options := &reloadOptions{role: "any"}
	command := &cobra.Command{
		Use: "reload cluster_name [member_names...]", Short: "Reload cluster member configuration", Args: cobra.MinimumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runReload(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().StringVarP(&options.role, "role", "r", "any", "Reload only members with this role")
	command.Flags().BoolVar(&options.force, "force", false, "Do not ask for confirmation at any point")
	return command
}

func (application *adapter) newRestartCommand() *cobra.Command {
	options := &restartOptions{role: "any"}
	command := &cobra.Command{
		Use: "restart cluster_name [member_names...]", Short: "Restart cluster member", Args: cobra.MinimumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runRestart(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().StringVarP(&options.role, "role", "r", "any", "Restart only members with this role")
	command.Flags().BoolVar(&options.any, "any", false, "Restart a single member only")
	command.Flags().StringVar(&options.scheduled, "scheduled", "", "Timestamp of a scheduled restart in unambiguous format (e.g. ISO 8601)")
	command.Flags().StringVar(&options.pgVersion, "pg-version", "", "Restart if the PostgreSQL version is less than provided (e.g. 9.5.2)")
	command.Flags().BoolVar(&options.pending, "pending", false, "Restart if pending")
	command.Flags().StringVar(&options.timeout, "timeout", "", "Return error and fail over if necessary when restarting takes longer than this.")
	command.Flags().BoolVar(&options.force, "force", false, "Do not ask for confirmation at any point")
	return command
}

func (application *adapter) newReinitCommand() *cobra.Command {
	options := &reinitOptions{}
	command := &cobra.Command{
		Use: "reinit cluster_name [member_names...]", Short: "Reinitialize cluster member", Args: cobra.MinimumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runReinit(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().BoolVar(&options.force, "force", false, "Do not ask for confirmation at any point")
	command.Flags().BoolVar(&options.fromLeader, "from-leader", false, "Get basebackup from leader")
	command.Flags().BoolVar(&options.wait, "wait", false, "Wait until reinitialization completes")
	return command
}

func (application *adapter) newFailoverCommand() *cobra.Command {
	options := &failoverOptions{}
	command := &cobra.Command{
		Use: "failover [cluster_name]", Short: "Failover to a replica", Args: cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runFailover(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().StringVar(&options.candidate, "candidate", "", "The name of the candidate")
	command.Flags().BoolVar(&options.force, "force", false, "Do not ask for confirmation at any point")
	return command
}

func (application *adapter) newSwitchoverCommand() *cobra.Command {
	options := &switchoverOptions{}
	command := &cobra.Command{
		Use: "switchover [cluster_name]", Short: "Switchover to a replica", Args: cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runSwitchover(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().StringVar(&options.leader, "leader", "", "The name of the current leader")
	command.Flags().StringVar(&options.leader, "primary", "", "The name of the current leader")
	command.Flags().StringVar(&options.candidate, "candidate", "", "The name of the candidate")
	command.Flags().StringVar(&options.scheduled, "scheduled", "", "Timestamp of a scheduled switchover in unambiguous format (e.g. ISO 8601)")
	command.Flags().BoolVar(&options.force, "force", false, "Do not ask for confirmation at any point")
	return command
}

func (application *adapter) newFlushCommand() *cobra.Command {
	options := &flushOptions{role: "any"}
	command := &cobra.Command{
		Use: "flush cluster_name [member_names...] target", Short: "Discard scheduled events", Args: cobra.MinimumNArgs(2),
		RunE: func(command *cobra.Command, args []string) error { return application.runFlush(command, args, options) },
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().StringVarP(&options.role, "role", "r", "any", "Flush only members with this role")
	command.Flags().BoolVar(&options.force, "force", false, "Do not ask for confirmation at any point")
	return command
}

func (application *adapter) newPauseCommand(resume bool) *cobra.Command {
	options := &pauseOptions{}
	name, short, waitHelp := "pause", "Disable auto failover", "Wait until pause is applied on all nodes"
	if resume {
		name, short, waitHelp = "resume", "Resume auto failover", "Wait until pause is cleared on all nodes"
	}
	command := &cobra.Command{
		Use: name + " [cluster_name]", Short: short, Args: cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runPause(command, args, options, resume)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().BoolVar(&options.wait, "wait", false, waitHelp)
	return command
}

func (application *adapter) newEditConfigCommand() *cobra.Command {
	options := &editConfigOptions{}
	command := &cobra.Command{
		Use: "edit-config [cluster_name]", Short: "Edit cluster configuration", Args: cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runEditConfig(command, args, options)
		},
	}
	command.Flags().IntVar(&options.group, "group", 0, "Citus group")
	command.Flags().BoolVarP(&options.quiet, "quiet", "q", false, "Do not show changes")
	command.Flags().StringArrayVarP(&options.settings, "set", "s", nil, "Set specific configuration value. Can be specified multiple times")
	command.Flags().StringArrayVarP(&options.pgSettings, "pg", "p", nil, "Set specific PostgreSQL parameter value. Can be specified multiple times")
	command.Flags().StringVar(&options.applyFile, "apply", "", "Apply configuration from file. Use - for stdin.")
	command.Flags().StringVar(&options.replaceFile, "replace", "", "Apply configuration from file, replacing existing configuration. Use - for stdin.")
	command.Flags().BoolVar(&options.force, "force", false, "Do not ask for confirmation at any point")
	return command
}

func (application *adapter) newDemoteClusterCommand() *cobra.Command {
	options := &demoteClusterOptions{}
	command := &cobra.Command{
		Use: "demote-cluster [cluster_name]", Short: "Demote cluster to a standby cluster", Args: cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runDemoteCluster(command, args, options)
		},
	}
	command.Flags().BoolVar(&options.force, "force", false, "Do not ask for confirmation at any point")
	command.Flags().StringVar(&options.host, "host", "", "Address of the remote node")
	command.Flags().IntVar(&options.port, "port", 0, "Port of the remote node")
	command.Flags().StringVar(&options.restoreCommand, "restore-command", "", "Command to restore WAL records from the remote primary")
	command.Flags().StringVar(&options.primarySlotName, "primary-slot-name", "", "Name of the slot on the remote node to use for replication")
	return command
}

func (application *adapter) newPromoteClusterCommand() *cobra.Command {
	options := &promoteClusterOptions{}
	command := &cobra.Command{
		Use: "promote-cluster [cluster_name]", Short: "Promote cluster, make it run standalone", Args: cobra.MaximumNArgs(1),
		RunE: func(command *cobra.Command, args []string) error {
			return application.runPromoteCluster(command, args, options)
		},
	}
	command.Flags().BoolVar(&options.force, "force", false, "Do not ask for confirmation at any point")
	return command
}
