package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
)

// doctorResult is deliberately stricter than status: unknown observations are
// actionable for an operator, while status remains a read-only evidence view.
type doctorResult struct {
	State   string
	Reasons []string
}

func summarizeDoctor(report statusReport) doctorResult {
	result := doctorResult{State: "healthy"}
	if report.Cluster.State == "degraded" || report.Health.State == "degraded" {
		result.State = "degraded"
	} else if report.Cluster.State != "available" || report.Health.State != "healthy" {
		result.State = "unknown"
	}

	seen := make(map[string]struct{})
	addReason := func(reason string) {
		reason = strings.TrimSpace(reason)
		if reason == "" {
			return
		}
		if _, ok := seen[reason]; ok {
			return
		}
		seen[reason] = struct{}{}
		result.Reasons = append(result.Reasons, reason)
	}
	for _, reason := range report.Cluster.Reasons {
		addReason(reason)
	}
	for _, reason := range report.Health.Reasons {
		addReason(reason)
	}
	addReason(report.LocalStorageError)
	addReason(report.LocalRepairError)
	if result.State == "unknown" && len(result.Reasons) == 0 {
		addReason("cluster health is not fully observed")
	}
	return result
}

func runDoctor(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	flags.SetOutput(stderr)
	store := flags.String("store", "", "existing managed Node store")
	server := flags.String("server", "", "reachable Node RPC address")
	jsonOutput := flags.Bool("json", false, "print the full machine-readable status report")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: dubi doctor [--store <path>] [--server <host:port>] [--json]")
	}

	// Let status own Store/context/address validation and the authenticated
	// Root/peer probe. This keeps doctor from drifting from status over time.
	statusArgs := make([]string, 0, 4)
	if flagWasSet(flags, "store") {
		statusArgs = append(statusArgs, "--store", *store)
	}
	if flagWasSet(flags, "server") {
		statusArgs = append(statusArgs, "--server", *server)
	}
	var raw bytes.Buffer
	if err := runStatus(ctx, statusArgs, &raw, stderr); err != nil {
		return err
	}
	var report statusReport
	if err := json.Unmarshal(raw.Bytes(), &report); err != nil {
		return fmt.Errorf("decode status report: %w", err)
	}
	result := summarizeDoctor(report)
	if *jsonOutput {
		_, writeErr := stdout.Write(raw.Bytes())
		return errors.Join(writeErr, doctorExitError(result))
	}

	if _, err := fmt.Fprintf(stdout, "DUBI doctor: %s\ncluster: %s\nnode: %s\n", result.State, report.Cluster.State, report.Health.State); err != nil {
		return err
	}
	if len(result.Reasons) == 0 {
		_, err := io.WriteString(stdout, "no actionable issues observed\n")
		return err
	}
	for _, reason := range result.Reasons {
		if _, err := fmt.Fprintf(stdout, "- %s\n", reason); err != nil {
			return err
		}
	}
	return doctorExitError(result)
}

func doctorExitError(result doctorResult) error {
	switch result.State {
	case "healthy":
		return nil
	case "degraded":
		return errors.New("cluster health is degraded")
	default:
		return errors.New("cluster health is not fully observed")
	}
}
