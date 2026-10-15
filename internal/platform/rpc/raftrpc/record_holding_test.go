package raftrpc

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type holdingVerifyFunc func(context.Context, records.Descriptor) (records.Holding, error)

func (f holdingVerifyFunc) Verify(ctx context.Context, d records.Descriptor) (records.Holding, error) {
	return f(ctx, d)
}

type holdingFixture struct {
	server               *RecordHoldingServer
	root                 *rootstate.State
	store                *records.Store
	volumes              *volume.Registry
	local, caller, other identity.Identity
	descriptor           records.Descriptor
	holding              records.Holding
	request              *raftwire.ObserveRecordHoldingRequest
	ctx                  context.Context
	pool                 *x509.CertPool
	certificates         map[uint64]tls.Certificate
	volumePath           string
}

func holdingCertificates(t *testing.T, ids ...identity.Identity) (*x509.CertPool, map[uint64]tls.Certificate) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	caTemplate := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "holding test CA"}, IsCA: true, BasicConstraintsValid: true,
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageCertSign}
	der, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, pub, key)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	certificates := make(map[uint64]tls.Certificate)
	for i, id := range ids {
		public, private, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		uri, err := identity.NodeIdentityURI(id)
		if err != nil {
			t.Fatal(err)
		}
		template := &x509.Certificate{SerialNumber: big.NewInt(int64(i + 2)), Subject: pkix.Name{CommonName: id.NodeID}, URIs: []*url.URL{uri}, DNSNames: []string{"localhost"},
			NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		leafDER, err := x509.CreateCertificate(rand.Reader, template, ca, public, key)
		if err != nil {
			t.Fatal(err)
		}
		leaf, err := x509.ParseCertificate(leafDER)
		if err != nil {
			t.Fatal(err)
		}
		certificates[id.RaftID] = tls.Certificate{Certificate: [][]byte{leafDER, der}, PrivateKey: private, Leaf: leaf}
	}
	return pool, certificates
}

func holdingPeerContext(t *testing.T, pool *x509.CertPool, certificate tls.Certificate) context.Context {
	t.Helper()
	chains, err := certificate.Leaf.Verify(x509.VerifyOptions{Roots: pool, CurrentTime: time.Now(), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}})
	if err != nil {
		t.Fatal(err)
	}
	return peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{PeerCertificates: []*x509.Certificate{certificate.Leaf}, VerifiedChains: chains}}})
}

func newHoldingFixture(t *testing.T) *holdingFixture {
	t.Helper()
	dir := t.TempDir()
	local, err := identity.Bootstrap(dir)
	if err != nil {
		t.Fatal(err)
	}
	volumes, err := volume.Initialize(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := pack.OpenPool(volumes)
	if err != nil {
		volumes.Close()
		t.Fatal(err)
	}
	store, err := records.OpenNodeStore(pool, volumes)
	if err != nil {
		pool.Close()
		volumes.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := errors.Join(store.Close(), pool.Close(), volumes.Close()); err != nil {
			t.Error(err)
		}
	})
	root, err := rootstate.Open(filepath.Join(dir, "root-state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	caller := identity.Identity{Version: 1, ClusterID: local.ClusterID, NodeID: fmt.Sprintf("node_%032x", 991), RaftID: 991, CreatedAt: time.Now(), Origin: identity.OriginJoin}
	other := caller
	other.NodeID, other.RaftID = fmt.Sprintf("node_%032x", 992), 992
	index := uint64(0)
	for i, id := range []identity.Identity{local, caller, other} {
		result := applyRootCommand(t, root, &index, rootstate.Command{Version: 1, RequestID: fmt.Sprintf("node-%d", i), Type: rootstate.RegisterNode,
			Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "west", Address: fmt.Sprintf("127.0.0.1:%d", 9000+i)}})
		if result.Error != "" {
			t.Fatal(result)
		}
	}
	if err := root.ApplyMembership(index+1, &raftpb.ConfState{Voters: []uint64{local.RaftID}, Learners: []uint64{caller.RaftID, other.RaftID}}); err != nil {
		t.Fatal(err)
	}
	d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", CapabilityID: "search", DatasetID: "index"}, "origin-not-hosted-here", []byte("opaque/key\x00"), []byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	h, err := store.Put(context.Background(), d, []byte("data"))
	if err != nil {
		t.Fatal(err)
	}
	authorize, err := identity.PeerAuthorizer(local.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewRecordHoldingServer(local, regionBarrierFunc(func(ctx context.Context, _ string) (uint64, error) { return root.AppliedIndex(), ctx.Err() }), root, store, authorize)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	certPool, certs := holdingCertificates(t, local, caller, other)
	return &holdingFixture{server: s, root: root, store: store, volumes: volumes, local: local, caller: caller, other: other, descriptor: d, holding: h,
		request: &raftwire.ObserveRecordHoldingRequest{ClusterId: local.ClusterID, FromRaftId: caller.RaftID, ToRaftId: local.RaftID, RecordDescriptor: encoded},
		ctx:     holdingPeerContext(t, certPool, certs[caller.RaftID]), pool: certPool, certificates: certs, volumePath: filepath.Join(dir, "volume")}
}

func TestRecordHoldingChecksStorageAndCurrentAuthorization(t *testing.T) {
	for _, scenario := range []string{"normal", "no-tls", "wrong-caller", "wrong-target", "wrong-cluster", "empty", "unknown-field", "oversized", "missing", "corrupt", "storage-error", "closed-store", "lost-volume", "replaced-volume-marker", "no-root-quorum", "root-behind", "caller-removed", "local-removed", "removed-during-io", "expired-during-io", "expired-missing", "wrong-caller-node", "wrong-local-node", "wrong-holding", "root-closed", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			f := newHoldingFixture(t)
			s := f.server
			want := codes.OK
			remove := func(local bool) {
				t.Helper()
				conf := &raftpb.ConfState{Voters: []uint64{f.local.RaftID}, Learners: []uint64{f.other.RaftID}}
				if local {
					conf = &raftpb.ConfState{Voters: []uint64{f.caller.RaftID}, Learners: []uint64{f.other.RaftID}}
				}
				if err := f.root.ApplyMembership(f.root.AppliedIndex()+1, conf); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "no-tls":
				f.ctx = context.Background()
				want = codes.Unauthenticated
			case "wrong-caller":
				f.request.FromRaftId = f.other.RaftID
				want = codes.PermissionDenied
			case "wrong-target":
				f.request.ToRaftId++
				want = codes.PermissionDenied
			case "wrong-cluster":
				f.request.ClusterId = "other"
				want = codes.PermissionDenied
			case "empty":
				f.request.RecordDescriptor = nil
				want = codes.InvalidArgument
			case "unknown-field":
				f.request.RecordDescriptor = append(bytes.TrimSuffix(f.request.RecordDescriptor, []byte("}")), []byte(`,"location":"foreign"}`)...)
				want = codes.InvalidArgument
			case "oversized":
				f.request.RecordDescriptor = make([]byte, recordHoldingMessageBytes)
				want = codes.ResourceExhausted
			case "missing":
				d := f.descriptor
				d.Key = []byte("missing")
				f.request.RecordDescriptor, _ = json.Marshal(d)
				want = codes.NotFound
			case "corrupt":
				s.store = holdingVerifyFunc(func(context.Context, records.Descriptor) (records.Holding, error) {
					return records.Holding{}, records.ErrCorrupt
				})
				want = codes.DataLoss
			case "storage-error":
				s.store = holdingVerifyFunc(func(context.Context, records.Descriptor) (records.Holding, error) {
					return records.Holding{}, errors.New("private storage path")
				})
				want = codes.FailedPrecondition
			case "closed-store":
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				want = codes.Unavailable
			case "lost-volume":
				if err := os.Rename(f.volumePath, f.volumePath+".missing"); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.Rename(f.volumePath+".missing", f.volumePath); err != nil {
						t.Error(err)
					}
				}()
				want = codes.Unavailable
			case "replaced-volume-marker":
				marker := filepath.Join(f.volumePath, "DUBI_VOLUME")
				content, err := os.ReadFile(marker)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Rename(marker, marker+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(marker, content, 0o600); err != nil {
					t.Fatal(err)
				}
				want = codes.Unavailable
			case "no-root-quorum":
				s.host = regionBarrierFunc(func(context.Context, string) (uint64, error) { return 0, errors.New("no quorum") })
				want = codes.Unavailable
			case "root-behind":
				s.host = regionBarrierFunc(func(context.Context, string) (uint64, error) { return f.root.AppliedIndex() + 1, nil })
				want = codes.Unavailable
			case "caller-removed":
				remove(false)
				want = codes.PermissionDenied
			case "local-removed":
				remove(true)
				want = codes.PermissionDenied
			case "removed-during-io":
				s.store = holdingVerifyFunc(func(context.Context, records.Descriptor) (records.Holding, error) {
					remove(false)
					return f.holding, nil
				})
				want = codes.PermissionDenied
			case "expired-during-io", "expired-missing":
				original := s.authorize
				expired := false
				s.authorize = func(ctx context.Context, id uint64) error {
					if expired {
						return errors.New("expired")
					}
					return original(ctx, id)
				}
				s.store = holdingVerifyFunc(func(context.Context, records.Descriptor) (records.Holding, error) {
					expired = true
					if scenario == "expired-missing" {
						return records.Holding{}, records.ErrNotHeld
					}
					return f.holding, nil
				})
				want = codes.PermissionDenied
			case "wrong-caller-node":
				wrong := f.caller
				wrong.NodeID = fmt.Sprintf("node_%032x", 999)
				roots, certs := holdingCertificates(t, wrong)
				f.ctx = holdingPeerContext(t, roots, certs[wrong.RaftID])
				want = codes.PermissionDenied
			case "wrong-local-node":
				s.local.NodeID = fmt.Sprintf("node_%032x", 999)
				want = codes.PermissionDenied
			case "wrong-holding":
				s.store = holdingVerifyFunc(func(context.Context, records.Descriptor) (records.Holding, error) {
					h := f.holding
					h.RecordID = "wrong"
					return h, nil
				})
				want = codes.FailedPrecondition
			case "root-closed":
				f.root.Close()
				want = codes.Unavailable
			case "canceled":
				ctx, cancel := context.WithCancel(f.ctx)
				cancel()
				f.ctx = ctx
				want = codes.Canceled
			}
			actual := s.store
			calls := 0
			s.store = holdingVerifyFunc(func(ctx context.Context, d records.Descriptor) (records.Holding, error) {
				calls++
				return actual.Verify(ctx, d)
			})
			response, err := s.Observe(f.ctx, f.request)
			if status.Code(err) != want {
				t.Fatal("wrong status", status.Code(err), want, err)
			}
			if len(s.slots) != 0 {
				t.Fatal("observation slot leaked")
			}
			if calls != 0 && (scenario == "no-tls" || scenario == "wrong-caller" || scenario == "caller-removed" || scenario == "local-removed" || scenario == "wrong-caller-node" || scenario == "wrong-local-node" || scenario == "root-behind" || scenario == "canceled") {
				t.Fatal("unqualified caller reached storage")
			}
			if err != nil {
				if response != nil || strings.Contains(err.Error(), "private storage") {
					t.Fatal("failed observation leaked state", response, err)
				}
				return
			}
			got, err := decodeHoldingObservation(f.local.ClusterID, f.local.RaftID, f.descriptor, response)
			if err != nil || got.Holding.RecordID != f.holding.RecordID || got.Holding.StoreID != f.holding.StoreID || got.RootConfirmedIndex == 0 {
				t.Fatal(got, err)
			}
			if len(s.slots) != 0 {
				t.Fatal("observation slot leaked")
			}
		})
	}
}

func TestRecordHoldingResponseRejectsForgedIdentityAndDescriptor(t *testing.T) {
	f := newHoldingFixture(t)
	response, err := f.server.Observe(f.ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	for _, damage := range []string{"version", "cluster", "raft", "node", "root", "store", "volume", "record", "scope", "origin", "key", "size", "digest", "unknown", "oversized"} {
		t.Run(damage, func(t *testing.T) {
			r := proto.Clone(response).(*raftwire.ObserveRecordHoldingResponse)
			d := f.descriptor
			switch damage {
			case "version":
				r.Version++
			case "cluster":
				r.ClusterId = "other"
			case "raft":
				r.RaftId++
			case "node":
				r.NodeId = "other"
			case "root":
				r.RootConfirmedIndex = 0
			case "store":
				r.StoreId = "store_invalid"
			case "volume":
				r.VolumeId = "volume_invalid"
			case "record":
				r.RecordId = "record_invalid"
			case "scope":
				d.Keyspace.TenantID = "other"
			case "origin":
				d.GroupID = "other"
			case "key":
				d.Key = []byte("other")
			case "size":
				d.Size++
			case "digest":
				d.SHA256 = strings.Repeat("0", 64)
			case "unknown":
				r.RecordDescriptor = append(bytes.TrimSuffix(r.RecordDescriptor, []byte("}")), []byte(`,"location":"foreign"}`)...)
			case "oversized":
				r.RecordDescriptor = make([]byte, recordHoldingMessageBytes)
			}
			if damage == "scope" || damage == "origin" || damage == "key" || damage == "size" || damage == "digest" {
				r.RecordDescriptor, _ = json.Marshal(d)
			}
			if _, err := decodeHoldingObservation(f.local.ClusterID, f.local.RaftID, f.descriptor, r); err == nil {
				t.Fatal("forged response accepted")
			}
		})
	}
}

func TestRecordHoldingAdmissionTimeoutAndCancellation(t *testing.T) {
	f := newHoldingFixture(t)
	entered := make(chan struct{}, recordHoldingConcurrency)
	release := make(chan struct{})
	f.server.store = holdingVerifyFunc(func(ctx context.Context, d records.Descriptor) (records.Holding, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return f.holding, nil
		case <-ctx.Done():
			return records.Holding{}, ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, recordHoldingConcurrency)
	for i := 0; i < recordHoldingConcurrency; i++ {
		go func() { _, err := f.server.Observe(ctx, f.request); done <- err }()
	}
	for i := 0; i < recordHoldingConcurrency; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("admission stalled")
		}
	}
	if _, err := f.server.Observe(f.ctx, f.request); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("unbounded observations", err)
	}
	cancel()
	for i := 0; i < recordHoldingConcurrency; i++ {
		select {
		case err := <-done:
			if status.Code(err) != codes.Canceled {
				t.Fatal(err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("cancel stalled")
		}
	}
	if len(f.server.slots) != 0 {
		t.Fatal("canceled slots leaked")
	}
	f.server.timeout = 20 * time.Millisecond
	if _, err := f.server.Observe(f.ctx, f.request); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal("internal deadline missing", err)
	}
	if len(f.server.slots) != 0 {
		t.Fatal("timed-out slot leaked")
	}
	close(release)
	if _, err := f.server.Observe(f.ctx, f.request); err != nil {
		t.Fatal("capacity did not recover", err)
	}
}

func TestRecordHoldingBlockedIOKeepsAdmissionUntilItActuallyReturns(t *testing.T) {
	f := newHoldingFixture(t)
	release := make(chan struct{})
	entered := make(chan struct{}, recordHoldingConcurrency)
	f.server.store = holdingVerifyFunc(func(context.Context, records.Descriptor) (records.Holding, error) {
		entered <- struct{}{}
		<-release
		return f.holding, nil
	})
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, recordHoldingConcurrency)
	for i := 0; i < recordHoldingConcurrency; i++ {
		go func() { _, err := f.server.Observe(ctx, f.request); done <- err }()
	}
	defer func() {
		close(release)
		for i := 0; i < recordHoldingConcurrency; i++ {
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("blocked verification did not leave")
			}
		}
	}()
	for i := 0; i < recordHoldingConcurrency; i++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("verification not entered")
		}
	}
	cancel()
	if len(f.server.slots) != recordHoldingConcurrency {
		t.Fatal("cancellation released an active IO slot")
	}
	if _, err := f.server.Observe(f.ctx, f.request); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("new IO admitted behind uninterruptible operations", err)
	}
	select {
	case <-done:
		t.Fatal("handler left while storage still running")
	default:
	}
}

func TestRecordHoldingClientChecksCurrentTLSValidity(t *testing.T) {
	f := newHoldingFixture(t)
	r, err := f.server.Observe(f.ctx, f.request)
	if err != nil {
		t.Fatal(err)
	}
	serverContext := holdingPeerContext(t, f.pool, f.certificates[f.local.RaftID])
	remote, ok := peer.FromContext(serverContext)
	if !ok {
		t.Fatal("missing fixture peer")
	}
	leaf := f.certificates[f.local.RaftID].Leaf
	for _, now := range []time.Time{leaf.NotBefore.Add(-time.Second), leaf.NotAfter, leaf.NotAfter.Add(time.Second)} {
		if _, err := authenticatedHoldingObservation(context.Background(), remote, f.local.ClusterID, f.local.RaftID, f.descriptor, r, now); err == nil {
			t.Fatal("invalid-time responder accepted", now)
		}
	}
	if _, err := authenticatedHoldingObservation(context.Background(), remote, f.local.ClusterID, f.local.RaftID, f.descriptor, r, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := authenticatedHoldingObservation(context.Background(), nil, f.local.ClusterID, f.local.RaftID, f.descriptor, r, time.Now()); err == nil {
		t.Fatal("missing responder accepted")
	}
	if _, err := authenticatedHoldingObservation(context.Background(), &peer.Peer{}, f.local.ClusterID, f.local.RaftID, f.descriptor, r, time.Now()); err == nil {
		t.Fatal("plaintext response accepted")
	}
}

func TestRecordHoldingConstructorRejectsMissingDependencies(t *testing.T) {
	f := newHoldingFixture(t)
	for _, kind := range []string{"identity", "host", "root", "store", "authorizer"} {
		local, host, root, store, authorize := f.local, f.server.host, f.root, f.store, f.server.authorize
		switch kind {
		case "identity":
			local.ClusterID = ""
		case "host":
			host = nil
		case "root":
			root = nil
		case "store":
			store = nil
		case "authorizer":
			authorize = nil
		}
		if _, err := NewRecordHoldingServer(local, host, root, store, authorize); err == nil {
			t.Fatal("missing dependency accepted", kind)
		}
	}
}

type holdingProxy struct {
	raftwire.UnimplementedRecordHoldingServiceServer
	delegate *RecordHoldingServer
	corrupt  atomic.Bool
}

func (p *holdingProxy) Observe(ctx context.Context, req *raftwire.ObserveRecordHoldingRequest) (*raftwire.ObserveRecordHoldingResponse, error) {
	r, err := p.delegate.Observe(ctx, req)
	if err == nil && p.corrupt.Load() {
		r.RecordId = "forged"
	}
	return r, err
}

func TestRecordHoldingMutualTLSBindsActualResponderAndMembership(t *testing.T) {
	for _, certificateMode := range []string{"correct", "other-raft-id", "other-node-id"} {
		t.Run(certificateMode, func(t *testing.T) {
			f := newHoldingFixture(t)
			if certificateMode == "other-node-id" {
				wrong := f.local
				wrong.NodeID = fmt.Sprintf("node_%032x", 993)
				f.pool, f.certificates = holdingCertificates(t, f.caller, f.other, wrong)
			}
			certificate := f.certificates[f.local.RaftID]
			if certificateMode == "other-raft-id" {
				certificate = f.certificates[f.other.RaftID]
			}
			server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: f.pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
			t.Cleanup(server.Stop)
			proxy := &holdingProxy{delegate: f.server}
			raftwire.RegisterRecordHoldingServiceServer(server, proxy)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			go func() { _ = server.Serve(listener) }()
			directory := NewStaticDirectory()
			if err := directory.Update(f.local.RaftID, listener.Addr().String()); err != nil {
				t.Fatal(err)
			}
			client, err := NewGroupClient(GroupClientOptions{ClusterID: f.local.ClusterID, RaftID: f.caller.RaftID, Directory: directory,
				Credentials: credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{f.certificates[f.caller.RaftID]}, RootCAs: f.pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			got, err := client.ObserveRecordHolding(ctx, f.local.RaftID, f.descriptor)
			if certificateMode != "correct" {
				if err == nil || !strings.Contains(err.Error(), "responder identity") {
					t.Fatal("trusted but wrong Node certificate accepted", got, err)
				}
				return
			}
			if err != nil || got.NodeID != f.local.NodeID || got.Holding.RecordID != f.holding.RecordID {
				t.Fatal(got, err)
			}
			proxy.corrupt.Store(true)
			if _, err := client.ObserveRecordHolding(ctx, f.local.RaftID, f.descriptor); err == nil {
				t.Fatal("forged record accepted")
			}
			proxy.corrupt.Store(false)
			if err := f.root.ApplyMembership(f.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{f.local.RaftID}, Learners: []uint64{f.other.RaftID}}); err != nil {
				t.Fatal(err)
			}
			if _, err := client.ObserveRecordHolding(ctx, f.local.RaftID, f.descriptor); status.Code(err) != codes.PermissionDenied {
				t.Fatal("removed caller reused TLS connection", err)
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := client.ObserveRecordHolding(ctx, f.local.RaftID, f.descriptor); err == nil {
				t.Fatal("closed client used")
			}
		})
	}
}
