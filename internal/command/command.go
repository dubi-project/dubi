// Package command adapts the dubi CLI to bootstrap and server orchestration.
package command

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/capability/record"
	"github.com/dubi-project/dubi/internal/platform/cluster/capabilityruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"github.com/dubi-project/dubi/internal/server"
)

var Version = "dev"

func Run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if stdout == nil || stderr == nil {
		return 2
	}
	if len(args) == 0 {
		usage(stderr)
		return 2
	}
	var err error
	switch args[0] {
	case "init":
		err = runInit(args[1:], stdout, stderr)
	case "server":
		err = runServer(ctx, args[1:], stderr)
	case "status":
		err = runStatus(ctx, args[1:], stdout, stderr)
	case "doctor":
		err = runDoctor(ctx, args[1:], stdout, stderr)
	case "dataset":
		err = runDataset(ctx, args[1:], stdout, stderr)
	case "record":
		err = runRecord(ctx, args[1:], stdout, stderr)
	case "tenant":
		err = runTenant(ctx, args[1:], stdout, stderr)
	case "join":
		err = runJoin(ctx, args[1:], stdout, stderr)
	case "volume":
		err = runVolume(ctx, args[1:], stdout, stderr)
	case "node":
		err = runNode(ctx, args[1:], stdout, stderr)
	case "version":
		_, err = fmt.Fprintf(stdout, "dubi %s\n", Version)
	case "help", "-h", "--help":
		usage(stdout)
		return 0
	default:
		err = fmt.Errorf("unknown command %q", args[0])
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "dubi: %v\n", err)
		return 1
	}
	return 0
}

func runJoin(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return runJoinConnect(ctx, args, os.Stdin, stderr)
	}
	if strings.HasPrefix(args[0], "-") {
		return runJoinConnect(ctx, args, os.Stdin, stderr)
	}
	switch args[0] {
	case "connect":
		return runJoinConnect(ctx, args[1:], os.Stdin, stderr)
	case "token", "revoke":
		return runJoinGrant(ctx, args[0], args[1:], stdout, stderr)
	case "prepare":
		flags := flag.NewFlagSet("join prepare", flag.ContinueOnError)
		flags.SetOutput(stderr)
		storeRoot := flags.String("store", "", "durable Node store root")
		region := flags.String("region", "default", "deployment region")
		advertise := flags.String("advertise", "", "advertised internal host:port")
		var volumes volumePaths
		flags.Var(&volumes, "volume", "existing empty data directory; repeat for multiple volumes")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		resolved, err := resolveStore(flags, *storeRoot)
		if err != nil {
			return err
		}
		*storeRoot = resolved
		if flags.NArg() != 0 || strings.TrimSpace(*storeRoot) == "" {
			return errors.New("usage: dubi join prepare --advertise <host:port> [--store <path>] [--region <region>] [--volume <path> ...]")
		}
		prepared, request, err := identityenrollment.Prepare(*storeRoot, nodepki.DefaultServerName, *region, *advertise)
		if err != nil {
			return err
		}
		registry, err := volume.Initialize(*storeRoot, volumes)
		if err != nil {
			return err
		}
		if err := registry.Close(); err != nil {
			return err
		}
		if err := saveLocalContext(*storeRoot, prepared, "", "", ""); err != nil {
			return fmt.Errorf("save local Node context: %w", err)
		}
		encoded, err := identityenrollment.EncodeRequest(request)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "%s\n", encoded)
		return err
	case "install":
		flags := flag.NewFlagSet("join install", flag.ContinueOnError)
		flags.SetOutput(stderr)
		storeRoot := flags.String("store", "", "durable Node store root")
		bundlePath := flags.String("bundle", "", "issued enrollment bundle JSON")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		resolved, err := resolveStore(flags, *storeRoot)
		if err != nil {
			return err
		}
		*storeRoot = resolved
		if flags.NArg() != 0 || strings.TrimSpace(*storeRoot) == "" || strings.TrimSpace(*bundlePath) == "" {
			return errors.New("usage: dubi join install --store <path> --bundle <path>")
		}
		info, err := os.Stat(*bundlePath)
		if err != nil || !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > 1<<20 {
			return errors.Join(err, errors.New("enrollment bundle must be a non-empty regular file no larger than 1 MiB"))
		}
		encoded, err := os.ReadFile(*bundlePath)
		if err != nil {
			return fmt.Errorf("read enrollment bundle: %w", err)
		}
		bundle, err := identityenrollment.DecodeBundle(encoded)
		if err != nil {
			return err
		}
		material, err := identityenrollment.Install(*storeRoot, bundle, time.Now().UTC())
		if err != nil {
			return err
		}
		value, err := identity.Load(*storeRoot)
		if err != nil {
			return err
		}
		if err := saveLocalContext(*storeRoot, value, "", "", ""); err != nil {
			return fmt.Errorf("save local Node context: %w", err)
		}
		encoder := json.NewEncoder(stdout)
		encoder.SetIndent("", "  ")
		return encoder.Encode(struct {
			identity.Identity
			PKI nodepki.Paths `json:"pki"`
		}{Identity: value, PKI: material.Paths})
	default:
		return fmt.Errorf("unknown join command %q", args[0])
	}
}

func runInit(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("init", flag.ContinueOnError)
	flags.SetOutput(stderr)
	storeRoot := flags.String("store", "", "durable Node store root")
	var volumes volumePaths
	flags.Var(&volumes, "volume", "existing empty data directory; repeat for multiple volumes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	resolved, err := resolveStore(flags, *storeRoot)
	if err != nil {
		return err
	}
	*storeRoot = resolved
	if flags.NArg() != 0 || strings.TrimSpace(*storeRoot) == "" {
		return errors.New("usage: dubi init [--store <path>] [--volume <path> ...]")
	}
	value, err := identity.Bootstrap(*storeRoot)
	if err != nil {
		return err
	}
	if value.Origin == "" {
		// Legacy identities did not record their origin. Only an existing,
		// validated bootstrap signer permits resuming init; do not infer that
		// a bound identity with missing PKI is a new Cluster.
		if _, err := nodepki.OpenSigner(*storeRoot, value, time.Now().UTC()); err != nil {
			return fmt.Errorf("legacy identity has no verified bootstrap signer; refusing to initialize a new Root: %w", err)
		}
	}
	material, err := nodepki.Ensure(*storeRoot, value, nodepki.DefaultServerName, time.Now().UTC())
	if err != nil {
		return err
	}
	// Persist the initial Root membership here. server only recovers this
	// state; an empty or joined store can never silently create a new Root.
	if err := node.InitializeRoot(*storeRoot, volumes...); err != nil {
		return fmt.Errorf("initialize Root: %w", err)
	}
	if err := saveLocalContext(*storeRoot, value, "", "", ""); err != nil {
		return fmt.Errorf("save local Node context after init: %w", err)
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(struct {
		identity.Identity
		PKI nodepki.Paths `json:"pki"`
	}{Identity: value, PKI: material.Paths})
}

func runServer(ctx context.Context, args []string, stderr io.Writer) error {
	process, err := openServer(args, stderr)
	if err != nil {
		return err
	}
	return runManagedProcess(ctx, process, stderr)
}

func openServer(args []string, stderr io.Writer) (*server.Server, error) {
	return openServerWithMux(args, stderr, nil)
}

// A supplied mux is only used by in-package integration tests. Normal CLI
// startup always registers the built-in Record handler below.
func openServerWithMux(args []string, stderr io.Writer, mux *capabilityruntime.Mux) (*server.Server, error) {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	storeRoot := flags.String("store", "", "durable Node store root")
	listen := flags.String("listen", "", "internal RPC listen address")
	apiListen := flags.String("api-listen", "", "optional Tenant HTTPS listen address (identity probe only)")
	advertise := flags.String("advertise", "", "published host:port; automatically derived or reused by default")
	region := flags.String("region", "", "deployment region; reuse registration or default to default")
	name := flags.String("name", "", "optional operator-facing Node name")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	resolved, err := resolveStore(flags, *storeRoot)
	if err != nil {
		return nil, err
	}
	*storeRoot = resolved
	bound, err := resolveListen(flags, *listen, *storeRoot)
	if err != nil {
		return nil, err
	}
	*listen = bound
	knownAddress, err := resolveAdvertise(flags, *advertise, *storeRoot)
	if err != nil {
		return nil, err
	}
	*advertise = knownAddress
	if flags.NArg() != 0 || strings.TrimSpace(*storeRoot) == "" {
		return nil, errors.New("usage: dubi server [--store <path>] [--listen <host:port>] [--api-listen <host:port>] [--advertise <host:port>] [--region <region>] [--name <name>]")
	}
	if err := rootstate.ValidateNodeName(*name); err != nil {
		return nil, err
	}
	if err := config.ValidateListen(*listen); err != nil {
		return nil, err
	}
	if *apiListen != "" {
		if err := config.ValidateListen(*apiListen); err != nil {
			return nil, err
		}
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return nil, fmt.Errorf("listen for internal RPC: %w", err)
	}
	var publicListener net.Listener
	if *apiListen != "" {
		publicListener, err = net.Listen("tcp", *apiListen)
		if err != nil {
			_ = listener.Close()
			return nil, fmt.Errorf("listen for Tenant HTTPS: %w", err)
		}
	}
	return openManagedServerWithMuxAndPublic(*storeRoot, listener, publicListener,
		server.NetworkOptions{Advertise: *advertise, Region: *region, Name: *name}, stderr, mux)
}

// Takes ownership of listener on success and failure. Joining orchestration
// keeps its selected port reserved all the way through admission and startup.
func openManagedServer(storeRoot string, listener net.Listener, network server.NetworkOptions, stderr io.Writer) (_ *server.Server, returnErr error) {
	return openManagedServerWithMux(storeRoot, listener, network, stderr, nil)
}

func openManagedServerWithMux(storeRoot string, listener net.Listener, network server.NetworkOptions, stderr io.Writer, mux *capabilityruntime.Mux) (_ *server.Server, returnErr error) {
	return openManagedServerWithMuxAndPublic(storeRoot, listener, nil, network, stderr, mux)
}

func openManagedServerWithMuxAndPublic(storeRoot string, listener, publicListener net.Listener, network server.NetworkOptions, stderr io.Writer, mux *capabilityruntime.Mux) (_ *server.Server, returnErr error) {
	defer func() {
		if returnErr != nil {
			_ = listener.Close()
			if publicListener != nil {
				_ = publicListener.Close()
			}
		}
	}()
	value, err := identity.Load(storeRoot)
	if err != nil {
		return nil, fmt.Errorf("load Node identity (initialize a new cluster with 'dubi init', or complete enrollment): %w", err)
	}
	if value.ClusterID == "" {
		return nil, errors.New("Node enrollment is incomplete; complete enrollment before starting")
	}
	if value.Origin == identity.OriginJoin {
		if err := validateManagedJoinState(storeRoot); err != nil {
			return nil, fmt.Errorf("validate durable Root admission (resume enrollment with 'dubi join'): %w", err)
		}
	}
	serverTLS, clientTLS, err := config.LoadReloadableTLS(storeRoot, value)
	if err != nil {
		return nil, err
	}
	authorize, err := identity.PeerAuthorizer(value.ClusterID)
	if err != nil {
		return nil, err
	}
	var signer *nodepki.Signer
	signer, signerErr := nodepki.OpenSigner(storeRoot, value, time.Now().UTC())
	if signerErr != nil {
		if value.Origin == identity.OriginBootstrap {
			err = signerErr
			return nil, fmt.Errorf("load existing bootstrap enrollment authority: %w", err)
		}
		// Existing leaf-only Nodes remain valid and can start without an
		// authority envelope; a later transfer installs one durably.
		signer = nil
	}
	if mux == nil {
		registry := capabilityruntime.NewRegistry()
		if err := registry.Register(record.CapabilityID, record.Handler{}); err != nil {
			return nil, err
		}
		mux, err = registry.Seal()
		if err != nil {
			_ = listener.Close()
			return nil, err
		}
	}
	process, err := server.Open(server.Options{StoreRoot: storeRoot, Listener: listener, PublicListener: publicListener,
		ServerTLS: serverTLS, ClientTLS: clientTLS, AuthorizePeer: authorize, CapabilityMux: mux,
		EnrollmentSigner: signer, Network: &network,
		ReportError: func(err error) { _, _ = fmt.Fprintf(stderr, "dubi: background: %v\n", err) }})
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("recover Node (Root must be initialized or installed through cluster enrollment): %w", err)
	}
	return process, nil
}

func usage(writer io.Writer) {
	_, _ = io.WriteString(writer, "usage: dubi <command>\n\ncommands:\n  init     initialize the first node and cluster automatically\n  join     enroll and run a node; token/revoke manage credentials\n  server   run the DUBI node process\n  status   query the live Root directory using this Node's identity\n  doctor   explain whether the current Node and Cluster need attention\n  node remove <name-or-raft-id>  manually remove a failed Node and fence its old identity\n  node rename <name>     set the optional operator-facing Node name\n  tenant token create|revoke  manage Tenant application Tokens using Node identity\n  dataset create  register a Record Dataset (three-Node Region required)\n  record put|get  verify Record writes and reads using Node identity\n  volume   list/add/drain/remove/compact local data Volumes and Packs\n  version  print build version\n\nstart a new cluster:\n  dubi init\n  dubi server\n  dubi status\n  dubi doctor\n  dubi volume list\n\njoin another node:\n  dubi join --ticket-file <private-file>\n  dubi join   # resume after durable receipt, or run an admitted node\n\ninit/join select a local Node in ~/.dubi/current.json; server remembers its actual listen address.\nDefaults: --store, DUBI_STORE, local selection, /var/lib/dubi (in that order).\n--server overrides the remembered local endpoint for remote/debug access.\ninit and join accept repeatable --volume paths; otherwise use <store>/volume.\n")
}
