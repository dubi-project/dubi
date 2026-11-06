package command

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/auth"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
)

func runAccess(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || (args[0] != "create" && args[0] != "revoke") {
		return errors.New("usage: dubi access create --role admin|monitor | dubi access revoke <credential-id>")
	}
	f := flag.NewFlagSet("access "+args[0], flag.ContinueOnError)
	f.SetOutput(stderr)
	store := f.String("store", "", "Node metadata directory")
	endpoint := f.String("server", "", "Node RPC address")
	role := f.String("role", "admin", "admin or monitor")
	ttl := f.Duration("ttl", 90*24*time.Hour, "credential lifetime, maximum 366 days")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if args[0] == "create" && (f.NArg() != 0 || (*role != "admin" && *role != "monitor")) {
		return errors.New("create requires role admin or monitor and no positional arguments")
	}
	if args[0] == "revoke" && (f.NArg() != 1 || rootstate.ValidateTenantCredentialID(f.Arg(0)) != nil) {
		return errors.New("revoke requires a credential ID")
	}
	var err error
	*store, err = resolveStore(f, *store)
	if err != nil {
		return err
	}
	*endpoint, err = resolveServer(f, *endpoint, *store)
	if err != nil {
		return err
	}
	conn, id, err := connectDataNode(ctx, *store, *endpoint)
	if err != nil {
		return err
	}
	defer conn.Close()
	requestID := ""
	if err = ensureRequestID(&requestID); err != nil {
		return err
	}
	r := &datawire.AccessCredentialRequest{ClusterId: id.ClusterID, FromRaftId: id.RaftID, RequestId: requestID}
	var token string
	var value rootstate.AccessCredential
	if args[0] == "create" {
		token, value, err = auth.GenerateAccess(*role, "", time.Now(), *ttl)
		if err != nil {
			return err
		}
		r.CredentialJson, err = json.Marshal(value)
		if err != nil {
			return err
		}
	} else {
		r.RevokeId = f.Arg(0)
	}
	call, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err = datawire.NewDataServiceClient(conn).MutateAccessCredential(call, r)
	if err != nil {
		if value.CredentialID != "" {
			fmt.Fprintf(stderr, "unconfirmed credential: %s; revoke it before creating another\n", value.CredentialID)
		}
		return err
	}
	if args[0] == "revoke" {
		return json.NewEncoder(stdout).Encode(map[string]any{"credential_id": r.RevokeId, "revoked": true})
	}
	return json.NewEncoder(stdout).Encode(map[string]any{"credential_id": value.CredentialID, "role": value.Role, "token": token, "expires_unix": value.ExpiresUnix})
}
