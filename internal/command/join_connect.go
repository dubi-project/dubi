package command

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/capabilityruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"github.com/dubi-project/dubi/internal/server"
)

func runJoinConnect(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer) error {
	process, err := openJoiningServer(ctx, args, stdin, stderr)
	if err != nil {
		return err
	}
	return runManagedProcess(ctx, process, stderr)
}

// An interrupted command resumes from durable request -> receipt -> admission,
// not a persisted ticket or in-memory progress. Once admitted, reopening never
// calls the issuer or Join RPC again (whole-cluster cold startup must work).
func openJoiningServer(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer) (process *server.Server, returnErr error) {
	return openJoiningServerWithMux(ctx, args, stdin, stderr, nil)
}

func openJoiningServerWithMux(ctx context.Context, args []string, stdin io.Reader, stderr io.Writer, mux *capabilityruntime.Mux) (process *server.Server, returnErr error) {
	flags := flag.NewFlagSet("join", flag.ContinueOnError)
	flags.SetOutput(stderr)
	store := flags.String("store", "", "durable joining Node store")
	listen := flags.String("listen", "", "internal RPC listen address")
	apiListen := flags.String("api-listen", "", "optional Tenant HTTPS listen address (identity probe only)")
	advertise := flags.String("advertise", "", "published host:port; derive from route to issuer by default")
	region := flags.String("region", "", "deployment region; reuse prepared request or default to default")
	name := flags.String("name", "", "optional operator-facing Node name")
	ticketFile := flags.String("ticket-file", "", "private one-time credential file; '-' reads stdin; optional after receipt")
	var volumes volumePaths
	flags.Var(&volumes, "volume", "existing empty data directory; repeat for initial volumes")
	if err := flags.Parse(args); err != nil {
		return nil, err
	}
	resolved, err := resolveStore(flags, *store)
	if err != nil {
		return nil, err
	}
	*store = resolved
	bound, err := resolveListen(flags, *listen, *store)
	if err != nil {
		return nil, err
	}
	*listen = bound
	if ctx == nil || flags.NArg() != 0 || strings.TrimSpace(*store) == "" {
		return nil, errors.New("join requires context, a Store and flags only")
	}
	if err := rootstate.ValidateNodeName(*name); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
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
	root, err := filepath.Abs(*store)
	if err != nil {
		return nil, err
	}
	request, receipt, err := inspectJoinIntent(root)
	if err != nil {
		return nil, err
	}
	var ticket identityenrollment.Ticket
	if *ticketFile != "" {
		encoded, err := readJoinTicket(*ticketFile, stdin)
		if err != nil {
			return nil, err
		}
		at := time.Now().UTC()
		if receipt != nil {
			at = time.Unix(receipt.AcceptedUnix, 0)
		}
		ticket, err = identityenrollment.DecodeTicket(encoded, at)
		if err != nil {
			return nil, err
		}
		if receipt != nil && (ticket.GrantID() != receipt.GrantID || ticket.ClusterID() != receipt.Bundle.ClusterID ||
			ticket.Address() != receipt.IssuerAddress || ticket.ValidateNodeBundle(receipt.Bundle, receipt.Request, at) != nil) {
			return nil, errors.New("credential differs from the durable enrollment receipt")
		}
	} else if receipt == nil {
		return nil, errors.New("first enrollment requires --ticket-file; after receipt it can resume without the credential")
	}
	listener, err := net.Listen("tcp", *listen)
	if err != nil {
		return nil, fmt.Errorf("listen before enrollment: %w", err)
	}
	var publicListener net.Listener
	if *apiListen != "" {
		publicListener, err = net.Listen("tcp", *apiListen)
		if err != nil {
			_ = listener.Close()
			return nil, fmt.Errorf("listen for Tenant HTTPS: %w", err)
		}
	}
	defer func() {
		if returnErr != nil {
			if process != nil {
				returnErr = errors.Join(returnErr, process.Close())
				process = nil
			} else {
				_ = listener.Close()
				if publicListener != nil {
					_ = publicListener.Close()
				}
			}
		}
	}()
	address, regionID := *advertise, *region
	if request != nil {
		if request.ServerName != nodepki.DefaultServerName || address != "" && address != request.Address || regionID != "" && regionID != request.RegionID {
			return nil, errors.New("join flags differ from the durable enrollment request")
		}
		_, boundPort, _ := net.SplitHostPort(listener.Addr().String())
		_, publishedPort, _ := net.SplitHostPort(request.Address)
		if address == "" && boundPort != publishedPort {
			return nil, errors.New("reuse the enrolled listen port, or explicitly confirm --advertise for intentional port mapping")
		}
		address, regionID = request.Address, request.RegionID
	} else {
		address, err = config.DiscoverAdvertise(ctx, listener.Addr().String(), address, ticket.Address())
		if err != nil {
			return nil, err
		}
		if regionID == "" {
			regionID = "default"
		}
	}
	if err := identityenrollment.ValidateNodeMetadata(regionID, address); err != nil {
		return nil, err
	}
	// All credential/network checks precede creating even the empty Store.
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() {
		return nil, errors.Join(err, errors.New("joining Store must be a physical directory"))
	}
	checkStore := func() error {
		current, err := os.Lstat(root)
		if err != nil || !current.IsDir() || !os.SameFile(info, current) {
			return errors.Join(err, errors.New("joining Store directory changed during enrollment"))
		}
		return ctx.Err()
	}
	defer func() {
		if returnErr == nil {
			returnErr = checkStore()
		}
	}()
	workflow, err := storelock.AcquireEnrollment(root)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, workflow.Release()) }()
	// Reject an already serving Store before any remote mutation. Publications
	// below take this lease independently; the separate workflow lease serializes
	// concurrent join commands without weakening their Store ownership checks.
	live, err := storelock.Acquire(root)
	if err != nil {
		return nil, err
	}
	defer func() { returnErr = errors.Join(returnErr, live.Release()) }()
	if err := checkStore(); err != nil {
		return nil, err
	}
	currentRequest, currentReceipt, err := inspectJoinIntent(root)
	if err != nil {
		return nil, err
	}
	if (request == nil) != (currentRequest == nil) || request != nil && *request != *currentRequest ||
		(receipt == nil) != (currentReceipt == nil) || receipt != nil && *receipt != *currentReceipt {
		return nil, errors.New("join state changed during preflight; retry the same command")
	}
	if _, err := os.Lstat(filepath.Join(root, rootjoin.LocalStateFile)); err == nil {
		// Validate the complete durable fence before any retry volume writes.
		report, err := volume.Inspect(root)
		if err != nil {
			return nil, err
		}
		journal, err := rootjoin.OpenLocalStore(root, report.StoreID)
		if err != nil {
			return nil, err
		}
		_, loadErr := journal.Load()
		if err := errors.Join(loadErr, journal.Close()); err != nil {
			return nil, err
		}
		if err := initializeJoinVolumes(root, volumes); err != nil {
			return nil, err
		}
		rememberJoinContext(root, listener, address, stderr)
		if err := errors.Join(checkStore(), live.Release()); err != nil {
			return nil, err
		}
		return openManagedServerWithMuxAndPublic(root, listener, publicListener,
			server.NetworkOptions{Advertise: address, Region: regionID, Name: *name}, stderr, mux)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if existing, err := raft.HasGroupResources(root); err != nil || existing {
		return nil, errors.Join(err, errors.New("existing consensus resources without a Root join fence; refusing enrollment"))
	}
	if receipt == nil {
		_, prepared, err := identityenrollment.Prepare(root, nodepki.DefaultServerName, regionID, address)
		if err != nil {
			return nil, err
		}
		request = &prepared
	}
	if err := initializeJoinVolumes(root, volumes); err != nil {
		return nil, err
	}
	rememberJoinContext(root, listener, address, stderr)
	if receipt == nil {
		bundle, err := enrollmentrpc.Enroll(ctx, ticket, *request)
		if err != nil {
			return nil, err
		}
		if err := errors.Join(checkStore(), live.Release()); err != nil {
			return nil, err
		}
		saved, err := identityenrollment.SaveReceipt(root, ticket, *request, bundle, time.Time{})
		if err != nil {
			return nil, err
		}
		receipt = &saved
	}
	if err := errors.Join(checkStore(), live.Release()); err != nil {
		return nil, err
	}
	if _, _, err := identityenrollment.ResumeReceipt(root, time.Time{}); err != nil {
		return nil, err
	}
	rememberJoinContext(root, listener, address, stderr)
	admission, err := rootjoinrpc.Join(ctx, root, nil)
	if err != nil {
		return nil, err
	}
	view, err := rootjoinrpc.DecodeView(admission.View, receipt.Bundle.ClusterID)
	if err != nil {
		return nil, err
	}
	if err := checkStore(); err != nil {
		return nil, err
	}
	if _, err := rootjoin.SaveLocalAdmission(root, view, admission.AdmittedIndex); err != nil {
		return nil, err
	}
	return openManagedServerWithMuxAndPublic(root, listener, publicListener,
		server.NetworkOptions{Advertise: address, Region: regionID, Name: *name}, stderr, mux)
}

func initializeJoinVolumes(store string, paths []string) error {
	registry, err := volume.Initialize(store, paths)
	if err != nil {
		return err
	}
	return registry.Close()
}

func rememberJoinContext(store string, listener net.Listener, advertise string, stderr io.Writer) {
	value, err := identity.Load(store)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "dubi: warning: cannot remember joining Node: %v\n", err)
		return
	}
	endpoint, err := localListenerEndpoint(listener.Addr().String())
	if err == nil {
		err = saveLocalContext(store, value, listener.Addr().String(), endpoint, advertise)
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "dubi: warning: cannot update ~/.dubi/current.json for joining Node: %v\n", err)
	}
}

// Production startup cannot use the runtime's legacy development-fixture
// recovery path to bypass a lost receipt/admission/activation record. Validate
// under the Store lease before opening or creating any consensus resources.
func validateManagedJoinState(store string) (returnErr error) {
	lock, err := storelock.Acquire(store)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, lock.Release()) }()
	report, err := volume.Inspect(store)
	if err != nil {
		return err
	}
	journal, err := rootjoin.OpenLocalStore(store, report.StoreID)
	if err != nil {
		return err
	}
	_, loadErr := journal.Load()
	return errors.Join(loadErr, journal.Close())
}

// Missing receipt is not an excuse to reinitialize an already bound identity.
// A directory without identity must be empty except for known operation locks;
// orphaned volume/Raft resources are corruption, not a new Node.
func inspectJoinIntent(store string) (*identityenrollment.Request, *identityenrollment.Receipt, error) {
	info, err := os.Lstat(store)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil || !info.IsDir() {
		return nil, nil, errors.Join(err, errors.New("joining Store must be a physical directory"))
	}
	id, err := identity.Load(store)
	if errors.Is(err, os.ErrNotExist) {
		entries, readErr := os.ReadDir(store)
		if readErr != nil {
			return nil, nil, readErr
		}
		for _, entry := range entries {
			if !entry.Type().IsRegular() || entry.Name() != storelock.EnrollmentFilename && entry.Name() != storelock.Filename && entry.Name() != ".DUBI_NODE_IDENTITY.lock" {
				return nil, nil, errors.New("Store has resources without a valid Node identity; refusing reinitialization")
			}
		}
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if id.Origin != identity.OriginJoin {
		return nil, nil, errors.New("bootstrap/legacy Node cannot be repurposed by join")
	}
	if _, err := os.Lstat(filepath.Join(store, identityenrollment.JoinReceiptName)); err == nil {
		receipt, err := identityenrollment.LoadReceipt(store)
		if err != nil {
			return nil, nil, err
		}
		return &receipt.Request, &receipt, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, err
	}
	if id.ClusterID != "" {
		return nil, nil, errors.New("bound joining Node is missing its durable enrollment receipt")
	}
	if _, err := os.Lstat(identityenrollment.StorePaths(store).Directory); errors.Is(err, os.ErrNotExist) {
		return nil, nil, nil
	} else if err != nil {
		return nil, nil, err
	}
	_, request, err := identityenrollment.LoadPrepared(store)
	if err != nil {
		return nil, nil, err
	}
	return &request, nil, nil
}
