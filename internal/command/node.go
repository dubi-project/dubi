package command

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/rpc/nodeadminrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/tlscredentials"
	"google.golang.org/grpc"
)

func runNode(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: dubi node remove <name-or-raft-id> | dubi node rename <name> [--clear]")
	}
	switch args[0] {
	case "remove":
		flags := flag.NewFlagSet("node remove", flag.ContinueOnError)
		flags.SetOutput(stderr)
		store := flags.String("store", "", "existing managed Node store")
		server := flags.String("server", "", "local Node RPC address")
		name := flags.String("name", "", "remove the Node with this name")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if (*name != "" && flags.NArg() != 0) || (*name == "" && flags.NArg() != 1) {
			return errors.New("usage: dubi node remove <raft-id-or-name> [--name <name>] [--store <path>] [--server <host:port>]")
		}
		target := uint64(0)
		var err error
		targetName := *name
		if targetName == "" {
			target, err = strconv.ParseUint(flags.Arg(0), 10, 64)
			if err != nil {
				targetName = flags.Arg(0)
			} else if target == 0 {
				return errors.New("raft-id must be a positive integer")
			}
		}
		if target == 0 {
			if err := rootstate.ValidateNodeName(targetName); err != nil || targetName == "" {
				return errors.New("target must be a positive raft-id or a valid Node name")
			}
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
		if ctx == nil || identityenrollment.ValidateNodeAddress(*server) != nil {
			return errors.New("invalid local Node address")
		}
		id, err := identity.Load(*store)
		if err != nil {
			return fmt.Errorf("load local Node identity: %w", err)
		}
		_, clientTLS, err := config.LoadTLS(*store, id)
		if err != nil {
			return err
		}
		conn, err := grpc.NewClient("passthrough:///"+*server,
			grpc.WithTransportCredentials(tlscredentials.NewTLS(clientTLS)),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(rootjoinrpc.MaxResponseSize), grpc.MaxCallSendMsgSize(rootjoinrpc.MaxRequestSize)))
		if err != nil {
			return err
		}
		defer conn.Close()
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if target != 0 {
			if err := nodeadminrpc.RemoveNode(callCtx, conn, id.ClusterID, id.RaftID, target); err != nil {
				return err
			}
		} else {
			target, err = nodeadminrpc.RemoveNodeByName(callCtx, conn, id.ClusterID, id.RaftID, targetName)
			if err != nil {
				return err
			}
		}
		return json.NewEncoder(stdout).Encode(struct {
			TargetRaftID uint64 `json:"target_raft_id"`
			TargetName   string `json:"target_name,omitempty"`
			Completed    bool   `json:"completed"`
		}{TargetRaftID: target, TargetName: targetName, Completed: true})
	case "rename":
		flags := flag.NewFlagSet("node rename", flag.ContinueOnError)
		flags.SetOutput(stderr)
		store := flags.String("store", "", "existing managed Node store")
		server := flags.String("server", "", "local Node RPC address")
		clear := flags.Bool("clear", false, "clear the current Node name")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if (*clear && flags.NArg() != 0) || (!*clear && flags.NArg() != 1) {
			return errors.New("usage: dubi node rename <name> [--clear] [--store <path>] [--server <host:port>]")
		}
		name := ""
		if !*clear {
			name = flags.Arg(0)
		}
		if err := rootstate.ValidateNodeName(name); err != nil {
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
		id, err := identity.Load(*store)
		if err != nil {
			return fmt.Errorf("load local Node identity: %w", err)
		}
		_, clientTLS, err := config.LoadTLS(*store, id)
		if err != nil {
			return err
		}
		conn, err := grpc.NewClient("passthrough:///"+*server, grpc.WithTransportCredentials(tlscredentials.NewTLS(clientTLS)), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(rootjoinrpc.MaxResponseSize), grpc.MaxCallSendMsgSize(rootjoinrpc.MaxRequestSize)))
		if err != nil {
			return err
		}
		defer conn.Close()
		callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := nodeadminrpc.RenameNode(callCtx, conn, id.ClusterID, id.RaftID, name); err != nil {
			return err
		}
		return json.NewEncoder(stdout).Encode(struct {
			Name      string `json:"name"`
			Completed bool   `json:"completed"`
		}{Name: name, Completed: true})
	default:
		return fmt.Errorf("unknown node command %q", args[0])
	}
}
