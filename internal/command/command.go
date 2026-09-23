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

	"github.com/dubi-project/dubi/internal/platform/cluster/capabilityruntime"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
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
	case "join":
		err = runJoin(args[1:], stdout, stderr)
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

func runJoin(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: dubi join <prepare|install>")
	}
	switch args[0] {
	case "prepare":
		flags := flag.NewFlagSet("join prepare", flag.ContinueOnError)
		flags.SetOutput(stderr)
		storeRoot := flags.String("store", "", "durable Node store root")
		region := flags.String("region", "", "failure-domain RegionID")
		advertise := flags.String("advertise", "", "advertised internal host:port")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 || strings.TrimSpace(*storeRoot) == "" {
			return errors.New("usage: dubi join prepare --store <path> --region <id> --advertise <host:port>")
		}
		_, request, err := identityenrollment.Prepare(*storeRoot, nodepki.DefaultServerName, *region, *advertise)
		if err != nil {
			return err
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
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*storeRoot) == "" {
		return errors.New("usage: dubi init --store <path>")
	}
	value, err := identity.Bootstrap(*storeRoot)
	if err != nil {
		return err
	}
	material, err := nodepki.Ensure(*storeRoot, value, nodepki.DefaultServerName, time.Now().UTC())
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(struct {
		identity.Identity
		PKI nodepki.Paths `json:"pki"`
	}{Identity: value, PKI: material.Paths})
}

func runServer(ctx context.Context, args []string, stderr io.Writer) error {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	path := flags.String("config", "", "server YAML configuration")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || strings.TrimSpace(*path) == "" {
		return errors.New("usage: dubi server --config <path>")
	}
	loaded, err := config.Load(*path)
	if err != nil {
		return err
	}
	value, err := identity.Load(loaded.StoreRoot)
	if err != nil {
		return fmt.Errorf("load Node identity (run 'dubi init' first): %w", err)
	}
	serverTLS, clientTLS, err := loaded.LoadTLS(value)
	if err != nil {
		return err
	}
	authorize, err := identity.PeerAuthorizer(value.ClusterID)
	if err != nil {
		return err
	}
	directory, err := loaded.BootstrapDirectory()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", loaded.Listen)
	if err != nil {
		return fmt.Errorf("listen for internal RPC: %w", err)
	}
	registry := capabilityruntime.NewRegistry()
	mux, err := registry.Seal()
	if err != nil {
		_ = listener.Close()
		return err
	}
	process, err := server.Open(server.Options{StoreRoot: loaded.StoreRoot, Listener: listener,
		ServerTLS: serverTLS, ClientTLS: clientTLS, AuthorizePeer: authorize, CapabilityMux: mux,
		RootBootstrapPeers: loaded.RootPeers(), BootstrapDirectory: directory,
		TickInterval: loaded.TickInterval, MembershipInterval: loaded.MembershipInterval,
		MaxMessageSize: loaded.MaxMessageSize,
		ReportError:    func(err error) { _, _ = fmt.Fprintf(stderr, "dubi: membership: %v\n", err) }})
	if err != nil {
		return err
	}
	return process.Run(ctx)
}

func usage(writer io.Writer) {
	_, _ = io.WriteString(writer, "usage: dubi <command>\n\ncommands:\n  init     create or load bootstrap Node identity and PKI\n  join     prepare a join request or install an issued bundle\n  server   run the DUBI node process\n  version  print build version\n")
}
