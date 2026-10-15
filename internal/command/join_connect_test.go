package command

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"github.com/dubi-project/dubi/internal/server"
)

func startJoinTestServer(t *testing.T, process *server.Server) (func(), func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(10 * time.Second):
				t.Error("server shutdown timed out")
			}
		})
	}
	t.Cleanup(stop)
	wait := func() {
		t.Helper()
		select {
		case <-process.Ready():
		case <-time.After(20 * time.Second):
			t.Fatal("joining test server not ready")
		}
	}
	return stop, wait
}

func joinTestIssuer(t *testing.T) (string, string, func()) {
	t.Helper()
	store := filepath.Join(t.TempDir(), "issuer")
	if err := runInit([]string{"--store", store}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	process, err := openServer([]string{"--store", store, "--listen", "127.0.0.1:0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	stop, wait := startJoinTestServer(t, process)
	wait()
	node, found, err := process.Runtime.Root.Node(process.Runtime.Identity.RaftID)
	if err != nil || !found {
		t.Fatal("issuer registration missing", err)
	}
	return store, node.Address, stop
}

func joinTestTicket(t *testing.T, store, address string) (identityenrollment.Ticket, string) {
	t.Helper()
	draft, err := identityenrollment.NewGrantDraft(0, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ticket, err := enrollmentrpc.CreateTicket(ctx, store, address, draft)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := identityenrollment.EncodeTicket(ticket, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ticket")
	if err := os.WriteFile(path, []byte(encoded), 0o600); err != nil {
		t.Fatal(err)
	}
	return ticket, path
}

func loadJoinTestState(t *testing.T, store string) rootjoin.LocalState {
	t.Helper()
	lock, err := storelock.Acquire(store)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Release()
	report, err := volume.Inspect(store)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := rootjoin.OpenLocalStore(store, report.StoreID)
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	state, err := journal.Load()
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func TestJoinConnectAutomaticEnrollmentAndColdRestart(t *testing.T) {
	issuerStore, issuerAddress, stopIssuer := joinTestIssuer(t)
	_, ticketPath := joinTestTicket(t, issuerStore, issuerAddress)
	store := filepath.Join(t.TempDir(), "joiner")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	process, err := openJoiningServer(ctx, []string{"--store", store, "--listen", "127.0.0.1:0", "--ticket-file", ticketPath}, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	stop, wait := startJoinTestServer(t, process)
	wait()
	id := process.Runtime.Identity
	if id.Origin != identity.OriginJoin || id.ClusterID == "" {
		t.Fatal("identity was not enrolled automatically")
	}
	node, found, err := process.Runtime.Root.Node(id.RaftID)
	if err != nil || !found || node.RegionID != "default" {
		t.Fatal("joining network registration missing", err)
	}
	stop()
	before := loadJoinTestState(t, store)
	if before.ActivatedIndex == 0 {
		t.Fatal("ready without durable activation")
	}
	if _, err := os.Stat(nodepki.StorePaths(store).Envelope); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("joiner received signer", err)
	}
	if _, err := os.Stat(filepath.Join(store, "volume")); err != nil {
		t.Fatal("default volume missing", err)
	}

	// A consumed ticket is not needed ever again. Open the joiner while the
	// entire cluster is offline, then recover both nodes from local state.
	stopIssuer()
	process, err = openJoiningServer(ctx, []string{"--store", store, "--listen", node.Address}, nil, io.Discard)
	if err != nil {
		t.Fatal("cold open depended on remote issuer", err)
	}
	stop, wait = startJoinTestServer(t, process)
	issuer, err := openServer([]string{"--store", issuerStore, "--listen", issuerAddress}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, waitIssuer := startJoinTestServer(t, issuer)
	waitIssuer()
	wait()
	if process.Runtime.Identity != id {
		t.Fatal("restart changed identity")
	}
	stop()
	after := loadJoinTestState(t, store)
	if after.ActivatedIndex != before.ActivatedIndex {
		t.Fatal("restart replaced original activation")
	}

	marker := filepath.Join(store, rootjoin.LocalStateFile)
	if err := os.Rename(marker, marker+".test-backup"); err != nil {
		t.Fatal(err)
	}
	if process, err := openJoiningServer(ctx, []string{"--store", store, "--listen", node.Address}, nil, io.Discard); err == nil {
		process.Close()
		t.Fatal("existing consensus state accepted without its activation fence")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("missing fence was recreated", err)
	}
	if process, err := openServer([]string{"--store", store, "--listen", node.Address}, io.Discard); err == nil {
		process.Close()
		t.Fatal("server bypassed the missing admission fence")
	}
	if err := os.Rename(marker+".test-backup", marker); err != nil {
		t.Fatal(err)
	}
}

func TestJoinConnectResumesReceiptWithoutCredentialAndUsesExplicitVolumes(t *testing.T) {
	issuerStore, issuerAddress, _ := joinTestIssuer(t)
	ticket, _ := joinTestTicket(t, issuerStore, issuerAddress)
	store := filepath.Join(t.TempDir(), "joiner")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	id, request, err := identityenrollment.Prepare(store, nodepki.DefaultServerName, "east", address)
	if err != nil {
		t.Fatal(err)
	}
	first, second := t.TempDir(), t.TempDir()
	if err := initializeJoinVolumes(store, []string{first, second}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	bundle, err := enrollmentrpc.Enroll(ctx, ticket, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := identityenrollment.SaveReceipt(store, ticket, request, bundle, time.Time{}); err != nil {
		t.Fatal(err)
	}
	// Simulate exiting after the receipt is durable but before installing TLS.
	process, err := openJoiningServer(ctx, []string{"--store", store, "--listen", address}, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	stop, wait := startJoinTestServer(t, process)
	wait()
	if process.Runtime.Identity.NodeID != id.NodeID || process.Runtime.Identity.RaftID != id.RaftID {
		t.Fatal("resume changed CSR identity")
	}
	stop()
	if _, err := os.Stat(filepath.Join(store, "volume")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("explicit volumes also created default", err)
	}
	report, err := volume.Inspect(store)
	if err != nil || len(report.Volumes) != 2 {
		t.Fatal("explicit volumes not retained", err)
	}
	// Corruption is rejected before even considering a new volume flag.
	marker := filepath.Join(store, rootjoin.LocalStateFile)
	if err := os.WriteFile(marker, []byte("broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	extra := t.TempDir()
	if process, err := openJoiningServer(ctx, []string{"--store", store, "--listen", address, "--volume", extra}, nil, io.Discard); err == nil {
		process.Close()
		t.Fatal("corrupt join fence accepted")
	}
	entries, err := os.ReadDir(extra)
	if err != nil || len(entries) != 0 {
		t.Fatal("volume changed before fence validation", err)
	}
}

func TestJoinConnectInvalidPreflightDoesNotCreateStore(t *testing.T) {
	for _, extra := range [][]string{nil, {"--ticket-file", "absent"}, {"--listen", "invalid"}, {"--ticket-file", "-"}} {
		store := filepath.Join(t.TempDir(), "absent")
		args := append([]string{"--store", store}, extra...)
		if process, err := openJoiningServer(context.Background(), args, nil, io.Discard); err == nil {
			process.Close()
			t.Fatal("invalid join accepted")
		}
		if _, err := os.Stat(store); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("preflight created Store", err)
		}
	}
}

func TestJoinConnectRetriesInterruptedEnrollmentWithOriginalIdentity(t *testing.T) {
	issuerStore, issuerAddress, stopIssuer := joinTestIssuer(t)
	_, ticketPath := joinTestTicket(t, issuerStore, issuerAddress)
	stopIssuer()
	store := filepath.Join(t.TempDir(), "joining")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	process, err := openJoiningServer(ctx, []string{"--store", store, "--listen", "127.0.0.1:0", "--ticket-file", ticketPath}, nil, io.Discard)
	cancel()
	if err == nil {
		process.Close()
		t.Fatal("offline enrollment unexpectedly succeeded")
	}
	id, request, err := identityenrollment.LoadPrepared(store)
	if err != nil {
		t.Fatal("interruption lost prepared identity", err)
	}
	if _, err := identityenrollment.LoadReceipt(store); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("offline call published receipt", err)
	}
	issuer, err := openServer([]string{"--store", issuerStore, "--listen", issuerAddress}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	_, waitIssuer := startJoinTestServer(t, issuer)
	waitIssuer()
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	args := []string{"--store", store, "--listen", request.Address, "--ticket-file", ticketPath}
	// A parallel workflow or running Store must be rejected before contacting
	// the issuer. Neither failed attempt may consume the one-time credential.
	for _, acquire := range []func(string) (*storelock.Lock, error){storelock.AcquireEnrollment, storelock.Acquire} {
		lock, err := acquire(store)
		if err != nil {
			t.Fatal(err)
		}
		process, err := openJoiningServer(ctx, args, nil, io.Discard)
		lock.Release()
		if err == nil {
			process.Close()
			t.Fatal("join ignored exclusive ownership")
		}
		if _, err := identityenrollment.LoadReceipt(store); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("locked attempt consumed credential", err)
		}
	}
	process, err = openJoiningServer(ctx, args, nil, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	stop, wait := startJoinTestServer(t, process)
	wait()
	if process.Runtime.Identity.NodeID != id.NodeID || process.Runtime.Identity.RaftID != id.RaftID {
		t.Fatal("retry generated another identity")
	}
	receipt, err := identityenrollment.LoadReceipt(store)
	if err != nil || receipt.Request != request {
		t.Fatal("retry changed CSR/network intent", err)
	}
	stop()
}

func TestJoinConnectCommandDispatch(t *testing.T) {
	// Dispatch without a selected local Node must not depend on context
	// published by an earlier CLI test in this package.
	t.Setenv("HOME", t.TempDir())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, args := range [][]string{nil, {"--store", t.TempDir()}, {"connect", "--store", t.TempDir()}} {
		if err := runJoin(ctx, args, io.Discard, io.Discard); !errors.Is(err, context.Canceled) {
			t.Fatal("join did not dispatch automatic connection", err)
		}
	}
}
