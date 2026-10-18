package command

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
)

// runBackup exposes only the cluster-level job control plane. Actual
// enumeration of Engine data and upload to a Repository is performed by the
// future worker; this command records a durable job that can be resumed by any
// current Node.
func runBackup(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: dubi backup run|list|status ...")
	}
	switch args[0] {
	case "run":
		return runBackupRun(ctx, args[1:], stdout, stderr)
	case "list":
		return runBackupList(ctx, args[1:], stdout, stderr)
	case "status":
		return runBackupStatus(ctx, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown backup command %q", args[0])
	}
}

func backupCommandFlags(name string, stderr io.Writer) (*flag.FlagSet, *string, *string) {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(stderr)
	store := flags.String("store", "", "existing Node store")
	server := flags.String("server", "", "reachable Node RPC address")
	return flags, store, server
}

func connectBackupCommand(ctx context.Context, flags *flag.FlagSet, store, server *string) (datawire.DataServiceClient, identity.Identity, io.Closer, error) {
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return nil, identity.Identity{}, nil, err
	}
	*store = resolved
	address, err := resolveServer(flags, *server, *store)
	if err != nil {
		return nil, identity.Identity{}, nil, err
	}
	*server = address
	if *store == "" || enrollment.ValidateNodeAddress(*server) != nil {
		return nil, identity.Identity{}, nil, errors.New("a local Node store and reachable server are required")
	}
	conn, id, err := connectDataNode(ctx, *store, *server)
	if err != nil {
		return nil, identity.Identity{}, nil, err
	}
	client := datawire.NewDataServiceClient(conn)
	return client, id, conn, nil
}

func runBackupRun(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags, store, server := backupCommandFlags("backup run", stderr)
	requestID := flags.String("request-id", "", "stable ID for retrying an uncertain request")
	jobID := flags.String("job-id", "", "optional durable Backup job ID; defaults to request ID")
	repositoryName := flags.String("repository", "", "Backup Repository name; defaults to the configured default")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: dubi backup run [--store path] [--server host:port] [--request-id ID] [--job-id ID] [--repository name]")
	}
	if err := ensureRequestID(requestID); err != nil {
		return err
	}
	conn, id, closer, err := connectBackupCommand(ctx, flags, store, server)
	if err != nil {
		return err
	}
	defer closer.Close()
	_, _ = fmt.Fprintf(stderr, "dubi: request-id %s\n", *requestID)
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := conn.CreateBackupJob(call, &datawire.CreateBackupJobRequest{ClusterId: id.ClusterID,
		FromRaftId: id.RaftID, RequestId: *requestID, JobId: *jobID, Repository: *repositoryName})
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(response)
}

func runBackupList(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags, store, server := backupCommandFlags("backup list", stderr)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("usage: dubi backup list [--store path] [--server host:port]")
	}
	conn, id, closer, err := connectBackupCommand(ctx, flags, store, server)
	if err != nil {
		return err
	}
	defer closer.Close()
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := conn.ListBackupJobs(call, &datawire.ListBackupJobsRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID})
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(response)
}

func runBackupStatus(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	flags, store, server := backupCommandFlags("backup status", stderr)
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("usage: dubi backup status [--store path] [--server host:port] <job-id>")
	}
	conn, id, closer, err := connectBackupCommand(ctx, flags, store, server)
	if err != nil {
		return err
	}
	defer closer.Close()
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	response, err := conn.GetBackupJob(call, &datawire.GetBackupJobRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, JobId: flags.Arg(0)})
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(response)
}
