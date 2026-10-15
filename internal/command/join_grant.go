package command

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
)

func runJoinGrant(ctx context.Context, operation string, args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("join "+operation, flag.ContinueOnError)
	flags.SetOutput(stderr)
	store := flags.String("store", "", "existing managed Node store for local operator authentication")
	address := flags.String("server", "", "existing enrollment authority RPC address")
	var ttl *time.Duration
	var grantID *string
	if operation == "token" {
		ttl = flags.Duration("ttl", identity.DefaultJoinGrantTTL, "one-time credential lifetime (maximum 24h)")
	} else {
		grantID = flags.String("grant", "", "unused credential's GrantID (not a Node/Raft ID)")
	}
	if err := flags.Parse(args); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return err
	}
	*store = resolved
	endpoint, err := resolveServer(flags, *address, *store)
	if err != nil {
		return err
	}
	*address = endpoint
	if flags.NArg() != 0 || strings.TrimSpace(*store) == "" {
		return errors.New("join credential management accepts flags only and requires an existing Store")
	}
	if operation == "revoke" {
		if err := enrollmentrpc.RevokeGrant(ctx, *store, *address, *grantID); err != nil {
			return err
		}
		_, err := fmt.Fprintln(stdout, "Join credential revoked. Existing enrolled Nodes are unchanged.")
		return err
	}
	if *ttl <= 0 {
		return errors.New("join credential TTL must be positive")
	}
	draft, err := identityenrollment.NewGrantDraft(*ttl, time.Time{})
	if err != nil {
		return err
	}
	ticket, err := enrollmentrpc.CreateTicket(ctx, *store, *address, draft)
	if err != nil {
		return err
	}
	encoded, err := identityenrollment.EncodeTicket(ticket, time.Time{})
	if err != nil {
		return err
	}
	// stdout is an intentional bearer-credential disclosure. All diagnostics
	// stay on stderr and never interpolate the encoded ticket or private draft.
	if _, err := fmt.Fprintf(stderr, "Join credential %s is secret; keep its output private. Expires %s.\n", ticket.GrantID(), time.Unix(draft.Request().ExpiresUnix, 0).UTC().Format(time.RFC3339)); err != nil {
		return err
	}
	_, err = fmt.Fprintln(stdout, encoded)
	return err
}
