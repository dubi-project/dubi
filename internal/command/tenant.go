package command

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
)

func runTenant(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) < 2 || args[0] != "token" || args[1] != "create" && args[1] != "revoke" {
		return errors.New("usage: dubi tenant token create|revoke [--store path] [--server host:port] [--request-id ID] <tenant|credential-id>")
	}
	operation := args[1]
	flags := flag.NewFlagSet("tenant token "+operation, flag.ContinueOnError)
	flags.SetOutput(stderr)
	store := flags.String("store", "", "existing Node store")
	server := flags.String("server", "", "reachable Node RPC address")
	requestID := flags.String("request-id", "", "stable ID for diagnosing an uncertain request")
	var ttl *time.Duration
	if operation == "create" {
		ttl = flags.Duration("ttl", 90*24*time.Hour, "Token lifetime (maximum 366 days)")
	}
	if err := flags.Parse(args[2:]); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return err
	}
	*store = resolved
	endpoint, err := resolveServer(flags, *server, *store)
	if err != nil {
		return err
	}
	*server = endpoint
	if flags.NArg() != 1 || *store == "" || identityenrollment.ValidateNodeAddress(*server) != nil {
		return errors.New("usage: dubi tenant token create|revoke [--store path] [--server host:port] <tenant|credential-id>")
	}
	if operation == "create" && (rootstate.ValidateTenantID(flags.Arg(0)) != nil || *ttl < time.Second ||
		*ttl > 366*24*time.Hour || *ttl%time.Second != 0) {
		return errors.New("Tenant ID or Token TTL is invalid")
	}
	if operation == "revoke" && rootstate.ValidateTenantCredentialID(flags.Arg(0)) != nil {
		return errors.New("invalid Tenant credential ID")
	}
	if err := ensureRequestID(requestID); err != nil {
		return err
	}
	conn, id, err := connectDataNode(ctx, *store, *server)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, _ = fmt.Fprintf(stderr, "dubi: request-id %s\n", *requestID)
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client := datawire.NewDataServiceClient(conn)
	if operation == "create" {
		response, err := client.CreateTenantToken(call, &datawire.CreateTenantTokenRequest{
			ClusterId: id.ClusterID, FromRaftId: id.RaftID, RequestId: *requestID,
			TenantId: flags.Arg(0), TtlSeconds: uint64(*ttl / time.Second)})
		if err != nil {
			return err
		}
		// stdout contains a secret; never duplicate it to diagnostics.
		return json.NewEncoder(stdout).Encode(response)
	}
	response, err := client.RevokeTenantToken(call, &datawire.RevokeTenantTokenRequest{
		ClusterId: id.ClusterID, FromRaftId: id.RaftID, RequestId: *requestID,
		CredentialId: flags.Arg(0)})
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(response)
}
