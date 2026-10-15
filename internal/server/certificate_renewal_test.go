package server

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/storelock"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"go.etcd.io/raft/v3/raftpb"
	"golang.org/x/sys/unix"
)

func TestManagedCertificateRenewalBootstrapAndJoinedNode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	issued := time.Now().UTC().Add(-61 * 24 * time.Hour)
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	original, err := nodepki.Ensure(store, id, nodepki.DefaultServerName, issued)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(store, id, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	errorsSeen := make(chan error, 20)
	start := func(store string, id identity.Identity, listener net.Listener, signer *nodepki.Signer, bootstrap bool) (*Server, func()) {
		t.Helper()
		serverTLS, clientTLS, err := config.LoadReloadableTLS(store, id)
		if err != nil {
			t.Fatal(err)
		}
		authorize, err := identity.PeerAuthorizer(id.ClusterID)
		if err != nil {
			t.Fatal(err)
		}
		// Use the production Raft tick. A 1 ms tick implies 10–20 ms
		// elections, shorter than ordinary concurrent fsync/TLS work; it
		// manufactures leadership churn instead of testing managed renewal.
		options := Options{StoreRoot: store, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS, AuthorizePeer: authorize,
			CapabilityMux: serverTestMux(t), Network: &NetworkOptions{}, EnrollmentSigner: signer,
			ReportError: func(err error) {
				select {
				case errorsSeen <- err:
				default:
				}
			}}
		if bootstrap {
			options.RootBootstrapPeers = []uint64{id.RaftID}
		}
		process, err := Open(options)
		if err != nil {
			t.Fatal(err)
		}
		if process.certificates == nil {
			t.Fatal("managed process has no renewal worker")
		}
		runCtx, stop := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- process.Run(runCtx) }()
		stopped := false
		closeProcess := func() {
			if stopped {
				return
			}
			stopped = true
			stop()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(5 * time.Second):
				t.Error("renewing process did not join workers")
			}
		}
		t.Cleanup(closeProcess)
		select {
		case <-process.Ready():
		case err := <-done:
			stopped = true
			t.Fatal("start failed", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		return process, closeProcess
	}
	listen := func(address string) net.Listener {
		t.Helper()
		listener, err := net.Listen("tcp", address)
		if err != nil {
			t.Fatal(err)
		}
		return listener
	}
	pollRenewed := func(store string, id identity.Identity, previous nodepki.Material) nodepki.Material {
		t.Helper()
		for {
			current, err := nodepki.Load(store, id, nodepki.DefaultServerName, time.Time{})
			if err == nil && current.Certificate.NotAfter.After(previous.Certificate.NotAfter) {
				return current
			}
			select {
			case failure := <-errorsSeen:
				t.Fatal(failure)
			case <-ctx.Done():
				t.Fatal("renewal did not publish", ctx.Err(), err)
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	sourceListener := listen("127.0.0.1:0")
	_, stopSource := start(store, id, sourceListener, signer, true)
	defer stopSource()
	sourceUpdated := pollRenewed(store, id, original)
	if !bytes.Equal(sourceUpdated.CACertificate.Raw, original.CACertificate.Raw) {
		t.Fatal("bootstrap rotated CA")
	}
	draft, err := enrollment.NewGrantDraft(0, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := enrollmentrpc.CreateTicket(ctx, store, sourceListener.Addr().String(), draft)
	if err != nil {
		t.Fatal(err)
	}
	childStore := t.TempDir()
	childListener := listen("127.0.0.1:0")
	childAddress := childListener.Addr().String()
	_, request, err := enrollment.Prepare(childStore, nodepki.DefaultServerName, "default", childAddress)
	if err != nil {
		t.Fatal(err)
	}
	volumes, err := volume.Initialize(childStore, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := volumes.Close(); err != nil {
		t.Fatal(err)
	}
	bundle, err := enrollmentrpc.Enroll(ctx, ticket, request)
	if err != nil {
		t.Fatal(err)
	}
	// The fixture ages only this already-authorized leaf, using the same CA
	// and persisted CSR. No production clock or deployment input is exposed.
	bound := identity.Identity{Version: 1, ClusterID: id.ClusterID, NodeID: request.NodeID, RaftID: request.RaftID, CreatedAt: time.Unix(1, 0).UTC()}
	aged, err := signer.IssueNode(bound, nodepki.DefaultServerName, []byte(request.CSRPEM), issued)
	if err != nil {
		t.Fatal(err)
	}
	bundle.CertificatePEM = string(aged)
	if _, err := enrollment.SaveReceipt(childStore, ticket, request, bundle, time.Time{}); err != nil {
		t.Fatal(err)
	}
	_, childOriginal, err := enrollment.ResumeReceipt(childStore, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	childID, err := identity.Load(childStore)
	if err != nil {
		t.Fatal(err)
	}
	privateBefore, err := os.ReadFile(childOriginal.Paths.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	admission, err := rootjoinrpc.Join(ctx, childStore, nil)
	if err != nil {
		t.Fatal(err)
	}
	view, err := rootjoinrpc.DecodeView(admission.View, id.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rootjoin.SaveLocalAdmission(childStore, view, admission.AdmittedIndex); err != nil {
		t.Fatal(err)
	}
	_, stopChild := start(childStore, childID, childListener, nil, false)
	childUpdated := pollRenewed(childStore, childID, childOriginal)
	privateAfter, err := os.ReadFile(childUpdated.Paths.PrivateKey)
	if err != nil || !bytes.Equal(privateBefore, privateAfter) {
		t.Fatal("renewal replaced private key", err)
	}
	if !bytes.Equal(childUpdated.CACertificate.Raw, original.CACertificate.Raw) {
		t.Fatal("joined node rotated CA")
	}
	if _, err := os.Stat(childUpdated.Paths.Envelope); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("joined node received signer", err)
	}
	stopChild()
	_, stopReopened := start(childStore, childID, listen(childAddress), nil, false)
	defer stopReopened()
	reopened, err := nodepki.Load(childStore, childID, nodepki.DefaultServerName, time.Time{})
	if err != nil || !bytes.Equal(reopened.Certificate.Raw, childUpdated.Certificate.Raw) {
		t.Fatal("restart changed renewed leaf", err)
	}
}

type renewalReadFunc func(context.Context, string) (uint64, error)

func TestManagedCertificateRenewalConfirmsAlreadyVisibleLeaf(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	material, err := nodepki.Ensure(store, id, nodepki.DefaultServerName, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	// A visible, not-due leaf may be the outcome of an uncertain rename. Hold
	// the publication lease to prove the scheduler still enters durable install
	// confirmation instead of trusting expiry and returning success immediately.
	file, err := os.OpenFile(filepath.Join(material.Paths.Directory, ".leaf-renewal.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	worker := newCertificateRenewal(store, id, nil, nil, nil, nil)
	if err := worker.once(context.Background()); err == nil {
		t.Fatal("visible certificate bypassed durability confirmation")
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := worker.once(context.Background()); err != nil {
		t.Fatal(err)
	}
	current, err := nodepki.Load(store, id, nodepki.DefaultServerName, time.Time{})
	if err != nil || !bytes.Equal(current.Certificate.Raw, material.Certificate.Raw) {
		t.Fatal("confirmation replaced leaf", err)
	}
}

func (f renewalReadFunc) ReadIndex(ctx context.Context, group string) (uint64, error) {
	return f(ctx, group)
}

func TestManagedCertificateRenewalRetriesAndCancels(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	original, err := nodepki.Ensure(store, id, nodepki.DefaultServerName, time.Now().Add(-61*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(store, id, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, Type: rootstate.RegisterNode, RequestID: "renewal-retry-node", Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "local", Address: "127.0.0.1:7443"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Apply(1, command); err != nil {
		t.Fatal(err)
	}
	if err := root.ApplyMembership(2, &raftpb.ConfState{Voters: []uint64{id.RaftID}}); err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	injected := errors.New("temporarily unavailable Root")
	host := renewalReadFunc(func(ctx context.Context, _ string) (uint64, error) {
		if reads.Add(1) <= 2 {
			return 0, injected
		}
		return 2, ctx.Err()
	})
	reports := make(chan error, 4)
	worker := newCertificateRenewal(store, id, host, root, signer, func(err error) {
		select {
		case reports <- err:
		default:
		}
	})
	worker.retryMin, worker.retryMax = time.Millisecond, 2*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { worker.run(ctx); close(done) }()
	for range 2 {
		select {
		case err := <-reports:
			if !errors.Is(err, injected) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("retry did not report failure")
		}
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		material, err := nodepki.Load(store, id, nodepki.DefaultServerName, time.Time{})
		if err == nil && material.Certificate.NotAfter.After(original.Certificate.NotAfter) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("transient failure did not recover", err)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("sleeping renewal worker did not cancel")
	}
	material, err := nodepki.Load(store, id, nodepki.DefaultServerName, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if reads.Load() != 4 || !bytes.Equal(material.CACertificate.Raw, original.CACertificate.Raw) {
		t.Fatal("unexpected renewal attempts or changed CA", reads.Load())
	}
}

func TestServerCloseJoinsCertificateRenewalBeforeReleasingStore(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodepki.Ensure(store, id, nodepki.DefaultServerName, time.Now().Add(-61*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(store, id, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, clientTLS, err := config.LoadReloadableTLS(store, id)
	if err != nil {
		t.Fatal(err)
	}
	authorize, err := identity.PeerAuthorizer(id.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	process, err := Open(Options{StoreRoot: store, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS, AuthorizePeer: authorize,
		CapabilityMux: serverTestMux(t), RootBootstrapPeers: []uint64{id.RaftID}, TickInterval: time.Millisecond, Network: &NetworkOptions{}, EnrollmentSigner: signer})
	if err != nil {
		t.Fatal(err)
	}
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	process.certificates.host = renewalReadFunc(func(ctx context.Context, _ string) (uint64, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return 0, ctx.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	t.Cleanup(func() { unblock(); cancel(); _ = process.Close() })
	done := make(chan error, 1)
	go func() { done <- process.Run(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("renewal did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- process.Close() }()
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("close did not cancel renewal")
	}
	select {
	case err := <-closed:
		t.Fatal("close did not join renewal", err)
	default:
	}
	if lock, err := storelock.Acquire(store); err == nil {
		_ = lock.Release()
		t.Fatal("Store released before renewal returned")
	}
	if _, err := process.Runtime.Host.Status(raft.RootGroupID); err != nil {
		t.Fatal("Host closed before renewal returned", err)
	}
	unblock()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("close did not finish")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("Run did not finish")
	}
	lock, err := storelock.Acquire(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
}
