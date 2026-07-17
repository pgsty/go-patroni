package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pgsty/go-patroni/config"
	"github.com/pgsty/go-patroni/control"
	"github.com/pgsty/go-patroni/model"
	"github.com/spf13/cobra"
)

func (application *adapter) runRemove(command *cobra.Command, args []string, options *removeOptions) error {
	if !oneOf(options.format, "pretty", "tsv", "json", "yaml", "yml") {
		return usageError(fmt.Sprintf("invalid --format %q", options.format))
	}
	explicitConfirmation := options.confirmCluster != "" || options.acknowledgement != "" || options.confirmLeader != ""
	if !explicitConfirmation {
		if err := application.promptUnavailableError(); err != nil {
			return err
		}
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: args[0], explicitGroup: optionalGroup(command, options.group),
	})
	if err != nil {
		return err
	}
	defer runtime.Close()

	request := control.RemoveRequest{Target: runtime.target, Citus: runtime.resolved.Citus}
	prepared := runtime.service.PrepareRemove(command.Context(), request)
	plan, err := application.preparedPlan(runtime, prepared)
	if err != nil {
		return err
	}
	if application.root.output == "" || application.root.output == "human" {
		listed := runtime.service.List(command.Context(), control.ListRequest{Targets: []model.Target{runtime.target}})
		if listed.Outcome == control.Succeeded {
			if err := renderListData(application.stdout, listed.Data, runtime.resolved.Citus, false, options.format); err != nil {
				return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "rendering remove target failed", cause: err}
			}
		}
	}
	clusterName, acknowledgement, leaderConfirmation := options.confirmCluster, options.acknowledgement, options.confirmLeader
	if explicitConfirmation {
		if clusterName == "" || acknowledgement == "" {
			return usageError("non-interactive remove requires --confirm-cluster and --acknowledge-removal")
		}
	} else {
		clusterName, err = application.promptValue("Please confirm the cluster name to remove", "")
		if err != nil {
			return err
		}
	}
	if clusterName != runtime.target.Scope {
		return usageError("Cluster names specified do not match")
	}
	if !explicitConfirmation {
		acknowledgement, err = application.promptValue(
			fmt.Sprintf("You are about to remove all information in DCS for %s, please type: %q", runtime.target.Scope, control.RemoveAcknowledgement), "",
		)
		if err != nil {
			return err
		}
	}
	if acknowledgement != control.RemoveAcknowledgement {
		return usageError(fmt.Sprintf("You did not exactly type %q", control.RemoveAcknowledgement))
	}
	leader, _ := planPrecondition(plan, "remove.leader")
	if leader != "" {
		if !explicitConfirmation {
			leaderConfirmation, err = application.promptValue("This cluster currently is healthy. Please specify the leader name to continue", "")
			if err != nil {
				return err
			}
		}
		if leaderConfirmation != leader {
			return usageError("You did not specify the current leader of the cluster")
		}
	}
	result := runtime.service.ExecuteRemove(command.Context(), request, control.RemoveConfirmation{
		ClusterName: clusterName, Acknowledgement: acknowledgement, Leader: leaderConfirmation,
	}, plan)
	return finishWriteResult(application, runtime, result, "RemoveResult", func(io.Writer, control.RemoveData) error { return nil })
}

func (application *adapter) runReload(command *cobra.Command, args []string, options *reloadOptions) error {
	role, err := parseRole(options.role)
	if err != nil {
		return err
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: args[0], explicitGroup: optionalGroup(command, options.group),
	})
	if err != nil {
		return err
	}
	defer runtime.Close()
	request := control.ReloadRequest{Target: runtime.target, Members: append([]string(nil), args[1:]...), Role: role, Citus: runtime.resolved.Citus}
	prepared := runtime.service.PrepareReload(command.Context(), request)
	plan, err := application.confirmPreparedPlan(runtime, prepared, options.force, "Are you sure you want to reload members %s?")
	if err != nil {
		return err
	}
	result := runtime.service.ExecuteReload(command.Context(), request, plan)
	loopWait := 10
	if value, ok := planPrecondition(plan, "cluster.loopWait"); ok {
		if parsed, parseError := strconv.Atoi(value); parseError == nil && parsed > 0 {
			loopWait = parsed
		}
	}
	return finishWriteResult(application, runtime, result, "ReloadResult", func(writer io.Writer, data control.BatchWriteData) error {
		return renderReloadWrite(writer, data, loopWait)
	})
}

func (application *adapter) runRestart(command *cobra.Command, args []string, options *restartOptions) error {
	role, err := parseRole(options.role)
	if err != nil {
		return err
	}
	scheduledText := options.scheduled
	if !options.force && !command.Flags().Changed("scheduled") {
		scheduledText, err = application.promptValue("When should the restart take place", "now")
		if err != nil {
			return err
		}
	}
	scheduledAt, err := parseScheduled(scheduledText)
	if err != nil {
		return err
	}
	postgresVersion := options.pgVersion
	if !options.force && !command.Flags().Changed("pg-version") {
		postgresVersion, err = application.promptValue("Restart if the PostgreSQL version is less than provided (e.g. 9.5.2)", "")
		if err != nil {
			return err
		}
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: args[0], explicitGroup: optionalGroup(command, options.group),
	})
	if err != nil {
		return err
	}
	defer runtime.Close()
	request := control.RestartRequest{
		Target: runtime.target, Members: append([]string(nil), args[1:]...), Role: role, Any: options.any,
		ScheduledAt: scheduledAt, PostgresVersion: postgresVersion, Pending: options.pending, Timeout: options.timeout, Force: options.force,
		Citus: runtime.resolved.Citus,
	}
	prepared := runtime.service.PrepareRestart(command.Context(), request)
	question := "Are you sure you want to restart members %s?"
	if scheduledAt != nil {
		question = "Are you sure you want to schedule restart of members %s?"
	}
	plan, err := application.confirmPreparedPlan(runtime, prepared, options.force, question)
	if err != nil {
		return err
	}
	result := runtime.service.ExecuteRestart(command.Context(), request, plan)
	return finishWriteResult(application, runtime, result, "RestartResult", renderRestartWrite)
}

func (application *adapter) runReinit(command *cobra.Command, args []string, options *reinitOptions) error {
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: args[0], explicitGroup: optionalGroup(command, options.group),
	})
	if err != nil {
		return err
	}
	defer runtime.Close()
	members := append([]string(nil), args[1:]...)
	if len(members) == 0 && !options.force {
		candidates, candidatesError := application.replicaNamesForPrompt(command.Context(), runtime)
		if candidatesError != nil {
			return candidatesError
		}
		selected, promptError := application.promptValue(
			fmt.Sprintf("Which member do you want to reinitialize [%s]?", strings.Join(candidates, ", ")), "",
		)
		if promptError != nil {
			return promptError
		}
		members = []string{selected}
	}
	request := control.ReinitializeRequest{
		Target: runtime.target, Members: members, Force: options.force, FromLeader: options.fromLeader, Wait: options.wait,
		Citus: runtime.resolved.Citus,
	}
	prepared := runtime.service.PrepareReinitialize(command.Context(), request)
	plan, err := application.confirmPreparedPlan(runtime, prepared, options.force, "Are you sure you want to reinitialize members %s?")
	if err != nil {
		return err
	}
	result := runtime.service.ExecuteReinitialize(command.Context(), request, plan)
	return finishWriteResult(application, runtime, result, "ReinitializeResult", renderReinitializeWrite)
}

func (application *adapter) runFailover(command *cobra.Command, args []string, options *failoverOptions) error {
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: firstArgument(args), explicitGroup: optionalGroup(command, options.group),
	})
	if err != nil {
		return err
	}
	defer runtime.Close()
	if err := application.resolveInteractiveGroup(command, runtime, options.force); err != nil {
		return err
	}
	candidate := strings.TrimSpace(options.candidate)
	if candidate == "" && !options.force {
		cluster, clusterError := application.clusterForPrompt(command.Context(), runtime)
		if clusterError != nil {
			return clusterError
		}
		candidate, err = application.promptValue("Candidate", strings.Join(failoverCandidates(cluster), ","))
		if err != nil {
			return err
		}
	}
	request := control.FailoverRequest{Target: runtime.target, Candidate: candidate, Force: options.force, Citus: runtime.resolved.Citus}
	prepared := runtime.service.PrepareFailover(command.Context(), request)
	plan, err := application.preparedPlan(runtime, prepared)
	if err != nil {
		return err
	}
	if !options.force {
		if syncEligible, _ := planPrecondition(plan, "candidate.syncEligible"); syncEligible == "false" {
			confirmed, confirmError := application.confirm("Candidate is not synchronous. Are you sure you want to fail over?")
			if confirmError != nil {
				return confirmError
			}
			if !confirmed {
				return abortedError("Aborting failover")
			}
		}
		if err := application.confirmPlan(plan, "Are you sure you want to failover cluster "+runtime.target.Scope+"?"); err != nil {
			return err
		}
	}
	result := runtime.service.ExecuteFailover(command.Context(), request, plan)
	return finishWriteResult(application, runtime, result, "FailoverResult", renderClusterWrite)
}

func (application *adapter) runSwitchover(command *cobra.Command, args []string, options *switchoverOptions) error {
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: firstArgument(args), explicitGroup: optionalGroup(command, options.group),
	})
	if err != nil {
		return err
	}
	defer runtime.Close()
	if err := application.resolveInteractiveGroup(command, runtime, options.force); err != nil {
		return err
	}
	leader, candidate := strings.TrimSpace(options.leader), strings.TrimSpace(options.candidate)
	if !options.force && (leader == "" || candidate == "") {
		cluster, clusterError := application.clusterForPrompt(command.Context(), runtime)
		if clusterError != nil {
			return clusterError
		}
		if leader == "" {
			leader, err = application.promptValue("Current leader", cluster.Leader)
			if err != nil {
				return err
			}
		}
		if candidate == "" {
			candidate, err = application.promptValue("Candidate", strings.Join(failoverCandidates(cluster), ","))
			if err != nil {
				return err
			}
		}
	}
	scheduledText := options.scheduled
	if !options.force && !command.Flags().Changed("scheduled") {
		scheduledText, err = application.promptValue("When should the switchover take place", "now")
		if err != nil {
			return err
		}
	}
	scheduledAt, err := parseScheduled(scheduledText)
	if err != nil {
		return err
	}
	request := control.SwitchoverRequest{
		Target: runtime.target, Leader: leader, Candidate: candidate, ScheduledAt: scheduledAt,
		Force: options.force, Citus: runtime.resolved.Citus,
	}
	prepared := runtime.service.PrepareSwitchover(command.Context(), request)
	plan, err := application.confirmPreparedPlan(runtime, prepared, options.force, "Are you sure you want to switchover cluster %s?")
	if err != nil {
		return err
	}
	result := runtime.service.ExecuteSwitchover(command.Context(), request, plan)
	return finishWriteResult(application, runtime, result, "SwitchoverResult", renderClusterWrite)
}

func (application *adapter) runFlush(command *cobra.Command, args []string, options *flushOptions) error {
	eventText := args[len(args)-1]
	if !oneOf(eventText, string(control.FlushRestart), string(control.FlushSwitchover)) {
		return usageError("flush target must be restart or switchover")
	}
	role, err := parseRole(options.role)
	if err != nil {
		return err
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: args[0], explicitGroup: optionalGroup(command, options.group),
	})
	if err != nil {
		return err
	}
	defer runtime.Close()
	request := control.FlushRequest{
		Target: runtime.target, Event: control.FlushEvent(eventText), Members: append([]string(nil), args[1:len(args)-1]...),
		Role: role, Force: options.force, Citus: runtime.resolved.Citus,
	}
	prepared := runtime.service.PrepareFlush(command.Context(), request)
	plan, err := application.confirmPreparedPlan(runtime, prepared, options.force, "Are you sure you want to flush members %s?")
	if err != nil {
		return err
	}
	result := runtime.service.ExecuteFlush(command.Context(), request, plan)
	return finishWriteResult(application, runtime, result, "FlushResult", renderFlushWrite)
}

func (application *adapter) runPause(command *cobra.Command, args []string, options *pauseOptions, resume bool) error {
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: firstArgument(args), explicitGroup: optionalGroup(command, options.group), useConfiguredGroup: true,
	})
	if err != nil {
		return err
	}
	defer runtime.Close()
	request := control.PauseRequest{Target: runtime.target, Wait: options.wait, Citus: runtime.resolved.Citus}
	var prepared control.Result[control.Plan]
	if resume {
		prepared = runtime.service.PrepareResume(command.Context(), request)
	} else {
		prepared = runtime.service.PreparePause(command.Context(), request)
	}
	plan, err := application.preparedPlan(runtime, prepared)
	if err != nil {
		return err
	}
	var result control.Result[control.PauseData]
	if resume {
		result = runtime.service.ExecuteResume(command.Context(), request, plan)
	} else {
		result = runtime.service.ExecutePause(command.Context(), request, plan)
	}
	kind := "PauseResult"
	if resume {
		kind = "ResumeResult"
	}
	return finishWriteResult(application, runtime, result, kind, renderPauseWrite)
}

func (application *adapter) runDemoteCluster(command *cobra.Command, args []string, options *demoteClusterOptions) error {
	if strings.TrimSpace(options.host) == "" && options.port == 0 && strings.TrimSpace(options.restoreCommand) == "" {
		return usageError("At least --host, --port or --restore-command should be specified")
	}
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: firstArgument(args),
	})
	if err != nil {
		return err
	}
	defer runtime.Close()
	request := control.DemoteClusterRequest{
		Target: runtime.target,
		Standby: control.StandbyConfig{
			Host: options.host, Port: options.port, RestoreCommand: options.restoreCommand, PrimarySlotName: options.primarySlotName,
		},
		Force: options.force, Citus: runtime.resolved.Citus,
	}
	prepared := runtime.service.PrepareDemoteCluster(command.Context(), request)
	plan, err := application.confirmPreparedPlan(runtime, prepared, options.force,
		"Are you sure you want to demote "+runtime.target.Scope+" cluster?")
	if err != nil {
		if err.Error() == "Aborted demote-cluster" {
			return abortedError("Aborted cluster demotion")
		}
		return err
	}
	result := runtime.service.ExecuteDemoteCluster(command.Context(), request, plan)
	return finishWriteResult(application, runtime, result, "DemoteClusterResult", renderClusterRoleWrite)
}

func (application *adapter) runPromoteCluster(command *cobra.Command, args []string, options *promoteClusterOptions) error {
	runtime, err := application.openRuntime(command, runtimeRequest{
		operation: config.OperationRESTWrite, explicitScope: firstArgument(args),
	})
	if err != nil {
		return err
	}
	defer runtime.Close()
	request := control.PromoteClusterRequest{Target: runtime.target, Force: options.force, Citus: runtime.resolved.Citus}
	prepared := runtime.service.PreparePromoteCluster(command.Context(), request)
	plan, err := application.confirmPreparedPlan(runtime, prepared, options.force, "Are you sure you want to promote cluster %s?")
	if err != nil {
		return err
	}
	result := runtime.service.ExecutePromoteCluster(command.Context(), request, plan)
	return finishWriteResult(application, runtime, result, "PromoteClusterResult", renderClusterRoleWrite)
}

func (application *adapter) preparedPlan(runtime *commandRuntime, result control.Result[control.Plan]) (control.Plan, error) {
	if err := result.Validate(); err != nil {
		return control.Plan{}, &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "control returned an invalid plan result", cause: err}
	}
	if result.Outcome != control.Succeeded {
		return control.Plan{}, finishResult(application, application.root.output, runtime, result, "Plan", func(io.Writer, control.Plan) error { return nil })
	}
	if err := result.Data.Validate(); err != nil {
		return control.Plan{}, &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "control returned an invalid plan", cause: err}
	}
	return result.Data, nil
}

func (application *adapter) confirmPreparedPlan(
	runtime *commandRuntime,
	result control.Result[control.Plan],
	force bool,
	question string,
) (control.Plan, error) {
	plan, err := application.preparedPlan(runtime, result)
	if err != nil {
		return control.Plan{}, err
	}
	if force {
		return plan, nil
	}
	if strings.Contains(question, "%s") {
		value := plan.Target.Scope
		if len(plan.Targets) > 0 {
			value = strings.Join(planMemberNames(plan), ", ")
		}
		question = fmt.Sprintf(question, value)
	}
	if err := application.confirmPlan(plan, question); err != nil {
		return control.Plan{}, err
	}
	return plan, nil
}

func (application *adapter) confirmPlan(plan control.Plan, question string) error {
	if err := application.promptUnavailableError(); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(application.stderr, "Plan: %s\nTarget: %s\n", plan.Summary, targetLabel(plan.Target)); err != nil {
		return err
	}
	confirmed, err := application.confirm(question)
	if err != nil {
		return err
	}
	if !confirmed {
		return abortedError("Aborted " + plan.Operation)
	}
	return nil
}

func (application *adapter) confirm(question string) (bool, error) {
	value, err := application.promptValue(question+" [y/N]", "")
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "y", "yes":
		return true, nil
	default:
		return false, nil
	}
}

func (application *adapter) promptValue(label, defaultValue string) (string, error) {
	if err := application.promptUnavailableError(); err != nil {
		return "", err
	}
	if defaultValue == "" {
		if _, err := fmt.Fprintf(application.stderr, "%s: ", label); err != nil {
			return "", err
		}
	} else if _, err := fmt.Fprintf(application.stderr, "%s [%s]: ", label, defaultValue); err != nil {
		return "", err
	}
	value, err := application.input.ReadString('\n')
	if err != nil && !errors.Is(err, io.EOF) {
		return "", &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: "interactive input failed", cause: err}
	}
	value = strings.TrimRight(value, "\r\n")
	if value == "" {
		value = defaultValue
	}
	return value, nil
}

func (application *adapter) promptUnavailableError() error {
	if application.root.output == "json" || application.root.output == "yaml" {
		return usageError("interactive prompts are disabled with machine output; provide explicit confirmation options")
	}
	if !application.isInteractive() {
		return usageError("interactive input requires a terminal; provide explicit options instead")
	}
	return nil
}

func abortedError(message string) error {
	return &exitError{category: control.CategoryFailed, code: control.ExitCode(control.CategoryFailed), message: message}
}

func finishWriteResult[T any](
	application *adapter,
	runtime *commandRuntime,
	result control.Result[T],
	kind string,
	render func(io.Writer, T) error,
) error {
	if application.root.output != "" && application.root.output != "human" {
		return finishResult(application, application.root.output, runtime, result, kind, render)
	}
	if err := result.Validate(); err != nil {
		return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "control returned an invalid result", cause: err}
	}
	if err := render(application.stdout, result.Data); err != nil {
		return &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "rendering command output failed", cause: err}
	}
	if result.Outcome != control.Succeeded {
		return exitForControl(result.Error, false)
	}
	return nil
}

func renderReloadWrite(writer io.Writer, data control.BatchWriteData, loopWait int) error {
	for _, member := range data.Members {
		var line string
		switch member.HTTPStatus {
		case 200:
			line = fmt.Sprintf("No changes to apply on member %s", member.Target.Member)
		case 202:
			line = fmt.Sprintf("Reload request received for member %s and will be processed within %d seconds", member.Target.Member, loopWait)
		default:
			line = fmt.Sprintf("Failed: reload for member %s, status code=%d", member.Target.Member, member.HTTPStatus)
			if member.HTTPStatus == 0 {
				line = fmt.Sprintf("Failed: reload for member %s (%s)", member.Target.Member, member.Summary)
			}
		}
		if _, err := fmt.Fprintln(writer, line); err != nil {
			return err
		}
	}
	return nil
}

func renderRestartWrite(writer io.Writer, data control.BatchWriteData) error {
	for _, member := range data.Members {
		var line string
		switch member.HTTPStatus {
		case 200:
			line = fmt.Sprintf("Success: restart on member %s", member.Target.Member)
		case 202:
			line = fmt.Sprintf("Success: restart scheduled on member %s", member.Target.Member)
		case 409:
			line = fmt.Sprintf("Failed: another restart is already scheduled on member %s", member.Target.Member)
		default:
			line = fmt.Sprintf("Failed: restart for member %s, status code=%d", member.Target.Member, member.HTTPStatus)
			if member.HTTPStatus == 0 {
				line = fmt.Sprintf("Failed: restart for member %s (%s)", member.Target.Member, member.Summary)
			}
		}
		if _, err := fmt.Fprintln(writer, line); err != nil {
			return err
		}
	}
	return nil
}

func renderReinitializeWrite(writer io.Writer, data control.BatchWriteData) error {
	for _, member := range data.Members {
		line := fmt.Sprintf("Success: reinitialize for member %s", member.Target.Member)
		if member.Outcome != control.Succeeded {
			line = fmt.Sprintf("Failed: reinitialize for member %s, status code=%d", member.Target.Member, member.HTTPStatus)
			if member.HTTPStatus == 0 {
				line = fmt.Sprintf("Failed: reinitialize for member %s (%s)", member.Target.Member, member.Summary)
			}
		}
		if _, err := fmt.Fprintln(writer, line); err != nil {
			return err
		}
	}
	return nil
}

func renderClusterWrite(writer io.Writer, data control.ClusterWriteData) error {
	_, err := fmt.Fprintf(writer, "leader=%s candidate=%s outcome=%s verification=%s\n", data.Leader, data.Candidate, data.RESTSendState, data.Verification)
	return err
}

func renderFlushWrite(writer io.Writer, data control.FlushData) error {
	if data.Noop {
		_, err := fmt.Fprintf(writer, "No pending scheduled %s\n", data.Event)
		return err
	}
	for _, result := range data.Results {
		line := fmt.Sprintf("Success: flush scheduled %s for member %s", data.Event, result.Target.Member)
		if result.Outcome != control.Succeeded {
			line = fmt.Sprintf("Failed: flush scheduled %s for member %s, status code=%d", data.Event, result.Target.Member, result.HTTPStatus)
		}
		if _, err := fmt.Fprintln(writer, line); err != nil {
			return err
		}
	}
	return nil
}

func renderPauseWrite(writer io.Writer, data control.PauseData) error {
	state := "resumed"
	if data.Paused {
		state = "paused"
	}
	_, err := fmt.Fprintf(writer, "Success: cluster management is %s\n", state)
	return err
}

func renderClusterRoleWrite(writer io.Writer, data control.ClusterRoleData) error {
	_, err := fmt.Fprintf(writer, "cluster leader=%s role=%s verification=%s\n", data.Leader, data.DesiredRole, data.Verification)
	return err
}

func parseScheduled(value string) (*time.Time, error) {
	value = strings.TrimSpace(value)
	if value == "" || strings.EqualFold(value, "now") {
		return nil, nil
	}
	formats := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02T15:04", "2006-01-02 15:04:05", "2006-01-02 15:04"}
	for _, layout := range formats {
		var parsed time.Time
		var err error
		if strings.Contains(layout, "Z07:00") {
			parsed, err = time.Parse(layout, value)
		} else {
			parsed, err = time.ParseInLocation(layout, value, time.Local)
		}
		if err == nil {
			return &parsed, nil
		}
	}
	return nil, usageError(fmt.Sprintf("unable to parse scheduled timestamp %q; use an unambiguous ISO 8601 value", value))
}

func planMemberNames(plan control.Plan) []string {
	names := make([]string, 0, len(plan.Targets))
	for _, target := range plan.Targets {
		names = append(names, target.Member)
	}
	return names
}

func planPrecondition(plan control.Plan, field string) (string, bool) {
	for _, precondition := range plan.Preconditions {
		if precondition.Field == field {
			return precondition.Expected, true
		}
	}
	return "", false
}

func targetLabel(target model.Target) string {
	target = target.Normalize()
	group := ""
	if target.Group != nil {
		group = strconv.Itoa(*target.Group)
	}
	return fmt.Sprintf("context=%s namespace=%s scope=%s group=%s member=%s", target.Context, target.Namespace, target.Scope, group, target.Member)
}

func (application *adapter) clusterForPrompt(ctx context.Context, runtime *commandRuntime) (model.Cluster, error) {
	result := runtime.service.List(ctx, control.ListRequest{Targets: []model.Target{runtime.target}})
	if result.Outcome != control.Succeeded {
		return model.Cluster{}, finishResult(application, application.root.output, runtime, result, "ClusterList", func(io.Writer, control.ListData) error { return nil })
	}
	if len(result.Data.Clusters) != 1 {
		return model.Cluster{}, &exitError{category: control.CategoryInternal, code: control.ExitCode(control.CategoryInternal), message: "cluster selection returned an invalid result"}
	}
	return result.Data.Clusters[0], nil
}

func (application *adapter) replicaNamesForPrompt(ctx context.Context, runtime *commandRuntime) ([]string, error) {
	result := runtime.service.List(ctx, control.ListRequest{
		Targets: []model.Target{runtime.target}, Citus: runtime.resolved.Citus,
	})
	if result.Outcome != control.Succeeded {
		return nil, finishResult(application, application.root.output, runtime, result, "ClusterList", func(io.Writer, control.ListData) error { return nil })
	}
	unique := make(map[string]struct{})
	for _, cluster := range result.Data.Clusters {
		for _, name := range memberNamesByRole(cluster, model.RoleReplica) {
			unique[name] = struct{}{}
		}
	}
	resultNames := make([]string, 0, len(unique))
	for name := range unique {
		resultNames = append(resultNames, name)
	}
	sort.Strings(resultNames)
	return resultNames, nil
}

func memberNamesByRole(cluster model.Cluster, role model.MemberRole) []string {
	result := make([]string, 0)
	for _, member := range cluster.Members {
		if member.Role == role {
			result = append(result, member.Name)
		}
	}
	return result
}

func failoverCandidates(cluster model.Cluster) []string {
	result := make([]string, 0)
	for _, member := range cluster.Members {
		if member.Name != cluster.Leader {
			result = append(result, member.Name)
		}
	}
	return result
}

func (application *adapter) resolveInteractiveGroup(command *cobra.Command, runtime *commandRuntime, force bool) error {
	if !runtime.resolved.Citus || runtime.target.Group != nil || force {
		return nil
	}
	value, err := application.promptValue("Citus group", "")
	if err != nil {
		return err
	}
	group, parseError := strconv.Atoi(strings.TrimSpace(value))
	if parseError != nil || group < 0 {
		return usageError("Citus group must be a non-negative integer")
	}
	runtime.target.Group = &group
	return nil
}
