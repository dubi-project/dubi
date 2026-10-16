package server

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootjoin"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/config"
	"github.com/dubi-project/dubi/internal/platform/identity"
	identityenrollment "github.com/dubi-project/dubi/internal/platform/identity/enrollment"
	"github.com/dubi-project/dubi/internal/platform/identity/nodepki"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentrpc"
	"github.com/dubi-project/dubi/internal/platform/rpc/enrollmentwire"
	"github.com/dubi-project/dubi/internal/platform/rpc/rootjoinrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// This exercises the formal Server/Node runtime and real mTLS, but explicitly
// registers the issuer's address. It is NOT evidence of automatic CLI joining,
// multi-host durability or certificate rotation. The joining runtime starts
// from a durable admission record, catches up from a compacted file snapshot,
// and restarts without bootstrap peers or another enrollment/Join RPC.
func TestManagedEnrollmentServerIssuesLeafThroughRoot(t *testing.T) {
	store := t.TempDir()
	id, err := identity.Bootstrap(store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodepki.Ensure(store, id, nodepki.DefaultServerName, time.Time{}); err != nil {
		t.Fatal(err)
	}
	signer, err := nodepki.OpenSigner(store, id, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	serverTLS, clientTLS, err := config.LoadTLS(store, id)
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
	process, err := Open(Options{StoreRoot: store, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS,
		AuthorizePeer: authorize, EngineMux: serverTestMux(t), RootBootstrapPeers: []uint64{id.RaftID},
		TickInterval: time.Millisecond, MembershipInterval: 10 * time.Millisecond, EnrollmentSigner: signer})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sourceCtx, stopSource := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- process.Run(sourceCtx) }()
	stopIssuer := func() {
		if process == nil {
			return
		}
		stopSource()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("source Node did not stop")
		}
		process = nil
	}
	t.Cleanup(stopIssuer)
	sourceAddress := listener.Addr().String()
	startSource := func() {
		t.Helper()
		listener, err = net.Listen("tcp", sourceAddress)
		if err != nil {
			t.Fatal(err)
		}
		process, err = Open(Options{StoreRoot: store, Listener: listener, ServerTLS: serverTLS, ClientTLS: clientTLS,
			AuthorizePeer: authorize, EngineMux: serverTestMux(t), TickInterval: 10 * time.Millisecond})
		if err != nil {
			t.Fatal("cold-open Root without signer/bootstrap", err)
		}
		sourceCtx, stopSource = context.WithCancel(ctx)
		done = make(chan error, 1)
		currentSource, sourceDone := process, done
		go func() { sourceDone <- currentSource.Run(sourceCtx) }()
	}
	select {
	case <-process.Ready():
	case <-ctx.Done():
		t.Fatal("Server did not become ready")
	}
	command, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, RequestID: "register-enrollment-issuer", Type: rootstate.RegisterNode,
		Node: &rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID, RegionID: "local", Address: listener.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Runtime.Host.Submit(raft.RootGroupID, command); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	draft, err := identityenrollment.NewGrantDraft(0, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	r := draft.Request()
	response, err := enrollmentwire.NewEnrollmentServiceClient(conn).CreateGrant(ctx, &enrollmentwire.CreateGrantRequest{
		ClusterId: id.ClusterID, FromRaftId: id.RaftID, GrantId: r.GrantID,
		SecretSha256: r.SecretSHA256, PublicKeyDer: r.PublicKeyDER, ExpiresUnix: r.ExpiresUnix})
	if err != nil {
		t.Fatal(err)
	}
	ticket, err := draft.Complete(identityenrollment.GrantBundle{ClusterID: response.ClusterId, GrantID: response.GrantId,
		IssuerAddress: response.IssuerAddress, ExpiresUnix: response.ExpiresUnix,
		CACertificatePEM: response.CaCertificatePem, CertificatePEM: response.CertificatePem}, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	joinStore := t.TempDir()
	joinListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	joinAddress := joinListener.Addr().String()
	t.Cleanup(func() { _ = joinListener.Close() })
	_, request, err := identityenrollment.Prepare(joinStore, nodepki.DefaultServerName, "r2", joinAddress)
	if err != nil {
		t.Fatal(err)
	}
	volumes, err := volume.Initialize(joinStore, nil)
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
	if _, err := identityenrollment.SaveReceipt(joinStore, ticket, request, bundle, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := identityenrollment.ResumeReceipt(joinStore, time.Time{}); err != nil {
		t.Fatal(err)
	}
	grant, found, err := process.Runtime.Root.JoinGrant(ticket.GrantID())
	if err != nil || !found || grant.State != rootstate.JoinGrantConsumed {
		t.Fatal("Root did not bind grant", found, err)
	}
	bound, err := identity.Load(joinStore)
	if err != nil || bound.ClusterID != id.ClusterID {
		t.Fatal("installed wrong Cluster", err)
	}
	if _, _, err := config.LoadTLS(joinStore, bound); err != nil {
		t.Fatal("installed Node TLS invalid", err)
	}
	// Simulate a client disappearing after its authenticated durable intent.
	// The formal Server scheduler must finish it without another client call.
	startCommand, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, RequestID: "interrupted-root-join", Type: rootstate.BeginRootJoin,
		RootJoinStart: &rootstate.RootJoinStart{GrantID: ticket.GrantID(), Request: request}})
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Runtime.Host.Submit(raft.RootGroupID, startCommand); err != nil {
		t.Fatal(err)
	}
	for {
		task, found, err := process.Runtime.Root.RootJoin(ticket.GrantID())
		if err != nil {
			t.Fatal(err)
		}
		if found && task.State == rootstate.RootJoinAdmitted {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("server scheduler did not recover Root join", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	admission, err := rootjoinrpc.Join(ctx, joinStore, nil)
	if err != nil || admission.AdmittedIndex == 0 {
		t.Fatal("formal server did not confirm Root learner admission", admission, err)
	}
	if len(admission.View.Voters) != 1 || admission.View.Voters[0] != id.RaftID || len(admission.View.Learners) != 1 || admission.View.Learners[0] != bound.RaftID {
		t.Fatal("admission altered voter quorum", admission.View)
	}
	view, err := rootjoinrpc.DecodeView(admission.View, bound.ClusterID)
	if err != nil {
		t.Fatal(err)
	}
	local, err := rootjoin.SaveLocalAdmission(joinStore, view, admission.AdmittedIndex)
	if err != nil || local.Phase != rootjoin.LocalCatchingUp || local.ActivatedIndex != 0 {
		t.Fatal("remote admission was not durably fenced as catching-up", local, err)
	}
	retry, err := rootjoinrpc.Join(ctx, joinStore, []string{listener.Addr().String()})
	if err != nil || retry.AdmittedIndex != admission.AdmittedIndex {
		t.Fatal("Root admission retry changed the durable task", retry, err)
	}
	if _, err := process.Runtime.Host.CreateSnapshot(raft.RootGroupID); err != nil {
		t.Fatal("compact Root before first learner startup", err)
	}
	joinServerTLS, joinClientTLS, err := config.LoadTLS(joinStore, bound)
	if err != nil {
		t.Fatal(err)
	}
	var joining *Server
	var joiningDone chan error
	var stopJoining context.CancelFunc
	stop := func() {
		if joining == nil {
			return
		}
		stopJoining()
		select {
		case err := <-joiningDone:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(10 * time.Second):
			t.Error("joining Node did not stop")
		}
		joining = nil
	}
	t.Cleanup(stop)
	// Interrupt the first empty-learner startup while the source is offline.
	// Remote admission cannot replace a local quorum barrier and activation.
	stopIssuer()
	joining, err = Open(Options{StoreRoot: joinStore, Listener: joinListener, ServerTLS: joinServerTLS, ClientTLS: joinClientTLS,
		AuthorizePeer: authorize, EngineMux: serverTestMux(t), TickInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	interruptedCtx, interruptedCancel := context.WithCancel(ctx)
	stopJoining = interruptedCancel
	joiningDone = make(chan error, 1)
	interruptedNode, interruptedResult := joining, joiningDone
	go func() { interruptedResult <- interruptedNode.Run(interruptedCtx) }()
	select {
	case <-joining.Ready():
		t.Fatal("empty learner became ready without source")
	case <-time.After(30 * time.Millisecond):
	}
	before, err := joining.Runtime.RootJoin.Load()
	if err != nil || before.Phase != rootjoin.LocalCatchingUp || before.ActivatedIndex != 0 {
		t.Fatal("offline join manufactured activation", before, err)
	}
	stop()
	joinListener, err = net.Listen("tcp", joinAddress)
	if err != nil {
		t.Fatal(err)
	}
	startSource()
	start := func(listener net.Listener) {
		t.Helper()
		var err error
		joining, err = Open(Options{StoreRoot: joinStore, Listener: listener, ServerTLS: joinServerTLS, ClientTLS: joinClientTLS,
			AuthorizePeer: authorize, EngineMux: serverTestMux(t), TickInterval: 10 * time.Millisecond})
		if err != nil {
			t.Fatal("open admitted learner without bootstrap options", err)
		}
		var runCtx context.Context
		runCtx, stopJoining = context.WithCancel(ctx)
		joiningDone = make(chan error, 1)
		current, result := joining, joiningDone
		go func() { result <- current.Run(runCtx) }()
		select {
		case <-joining.Ready():
		case err := <-result:
			joining = nil
			t.Fatal("joining learner exited before ready", err)
		case <-ctx.Done():
			t.Fatal("joining learner did not catch up", ctx.Err())
		}
		phase, err := joining.Runtime.RootJoin.Load()
		if err != nil || phase.Phase != rootjoin.LocalActive || phase.ActivatedIndex < admission.AdmittedIndex {
			t.Fatal("Ready without durable local activation", phase, err)
		}
		membership, err := joining.Runtime.Root.RootMembership()
		if err != nil || len(membership.GetVoters()) != 1 || membership.GetVoters()[0] != id.RaftID || len(membership.GetLearners()) != 1 || membership.GetLearners()[0] != bound.RaftID {
			t.Fatal("catch-up changed quorum", membership, err)
		}
	}
	start(joinListener)
	// The formal Runtime owns the shared record Store and registers its RPC.
	// A privileged Node can observe exact physical bytes without hosting the
	// originating Group. These observations are NOT regional quorum receipts.
	var holdingDescriptors []records.Descriptor
	var sourceHoldings []records.Holding
	for _, engine := range []string{"file", "cache", "message", "search"} {
		payload := []byte("opaque/" + engine)
		d, err := records.Describe(rootstate.Keyspace{TenantID: "holding-" + engine, EngineID: engine, DatasetID: "items"}, "origin-not-hosted", []byte("key"), payload)
		if err != nil {
			t.Fatal(err)
		}
		h, err := process.Runtime.Records.Put(ctx, d, payload)
		if err != nil {
			t.Fatal(err)
		}
		holdingDescriptors, sourceHoldings = append(holdingDescriptors, d), append(sourceHoldings, h)
	}
	checkHoldings := func() {
		t.Helper()
		for i, d := range holdingDescriptors {
			observed, err := joining.GroupClient.ObserveRecordHolding(ctx, id.RaftID, d)
			if err != nil || observed.NodeID != id.NodeID || observed.Holding.RecordID != sourceHoldings[i].RecordID || observed.Holding.StoreID != sourceHoldings[i].StoreID || observed.Holding.VolumeID != sourceHoldings[i].VolumeID {
				t.Fatal("formal Node holding observation changed", observed, err)
			}
			if _, err := joining.Runtime.Records.Verify(ctx, d); !errors.Is(err, records.ErrNotHeld) {
				t.Fatal("observation transferred data", err)
			}
		}
	}
	checkHoldings()
	absent := holdingDescriptors[0]
	absent.Key = []byte("missing")
	if _, err := joining.GroupClient.ObserveRecordHolding(ctx, id.RaftID, absent); status.Code(err) != codes.NotFound {
		t.Fatal("missing physical record was reported held", err)
	}
	// Transfer distinct records over the Runtime-registered service. Both ends
	// use the real shared Pack Store, actual Root ReadIndex and exact Node mTLS.
	// These records have no logical reference and make no durability quorum.
	var transferred []records.Descriptor
	var transferredPayloads [][]byte
	var transferredHoldings []records.Holding
	var fetched []records.Descriptor
	var fetchedHoldings []records.Holding
	for i, original := range holdingDescriptors {
		payload := bytes.Repeat([]byte("transfer/"+original.Keyspace.EngineID), 1+i*(1<<17))
		d, err := records.Describe(original.Keyspace, original.GroupID, []byte("transferred-key"), payload)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := process.Runtime.Records.Put(ctx, d, payload); err != nil {
			t.Fatal(err)
		}
		body, source, err := process.Runtime.Records.Read(ctx, d)
		if err != nil {
			t.Fatal(err)
		}
		got, err := process.GroupClient.PutRecord(ctx, rootstate.Node{NodeID: bound.NodeID, RaftID: bound.RaftID}, d, body)
		if err != nil || got.NodeID != bound.NodeID || got.Holding.StoreID == source.StoreID {
			t.Fatal("formal transfer failed", got, err)
		}
		transferred = append(transferred, d)
		transferredPayloads = append(transferredPayloads, payload)
		transferredHoldings = append(transferredHoldings, got.Holding)
		pull, err := records.Describe(original.Keyspace, original.GroupID, []byte("fetched-key"), payload)
		if err != nil {
			t.Fatal(err)
		}
		sourceHolding, err := process.Runtime.Records.Put(ctx, pull, payload)
		if err != nil {
			t.Fatal(err)
		}
		copied, err := joining.GroupClient.FetchRecord(ctx, rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID}, pull, joining.Runtime.Records)
		if err != nil || copied.Source.Holding.RecordID != sourceHolding.RecordID || copied.Source.Holding.StoreID != sourceHolding.StoreID || copied.Local.StoreID == sourceHolding.StoreID {
			t.Fatal("formal record fetch failed", copied, err)
		}
		fetched = append(fetched, pull)
		fetchedHoldings = append(fetchedHoldings, copied.Local)
	}
	checkTransferred := func() {
		t.Helper()
		for i, d := range transferred {
			body, h, err := joining.Runtime.Records.Read(ctx, d)
			if err != nil || !bytes.Equal(body, transferredPayloads[i]) || h.RecordID != transferredHoldings[i].RecordID || h.StoreID != transferredHoldings[i].StoreID || h.VolumeID != transferredHoldings[i].VolumeID {
				t.Fatal("transferred data changed", h, err)
			}
			got, err := process.GroupClient.PutRecord(ctx, rootstate.Node{NodeID: bound.NodeID, RaftID: bound.RaftID}, d, transferredPayloads[i])
			if err != nil || got.Holding.RecordID != h.RecordID || got.Holding.StoreID != h.StoreID {
				t.Fatal("formal transfer retry failed", got, err)
			}
			body, local, err := joining.Runtime.Records.Read(ctx, fetched[i])
			if err != nil || !bytes.Equal(body, transferredPayloads[i]) || local.StoreID != fetchedHoldings[i].StoreID || local.VolumeID != fetchedHoldings[i].VolumeID {
				t.Fatal("fetched bytes changed after restart", local, err)
			}
			copied, err := joining.GroupClient.FetchRecord(ctx, rootstate.Node{NodeID: id.NodeID, RaftID: id.RaftID}, fetched[i], joining.Runtime.Records)
			if err != nil || copied.Local.RecordID != local.RecordID || copied.Local.StoreID != local.StoreID {
				t.Fatal("formal fetch retry failed", copied, err)
			}
		}
	}
	checkTransferred()
	activated, err := joining.Runtime.RootJoin.Load()
	if err != nil {
		t.Fatal(err)
	}
	stop()
	joinListener, err = net.Listen("tcp", joinAddress)
	if err != nil {
		t.Fatal(err)
	}
	start(joinListener)
	checkHoldings()
	checkTransferred()
	restarted, err := joining.Runtime.RootJoin.Load()
	if err != nil || restarted.ActivatedIndex != activated.ActivatedIndex || restarted.GrantID != ticket.GrantID() {
		t.Fatal("restart repeated/replaced admission", restarted, err)
	}
	// Confirm a fresh Root command reaches the restarted learner through its
	// normal Raft transport, not merely a cached admission response.
	encoded, err := rootstate.EncodeCommand(rootstate.Command{Version: 1, RequestID: "after-learner-restart", Type: rootstate.RegisterTenant, Tenant: &rootstate.Tenant{TenantID: "after-restart"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := process.Runtime.Host.Submit(raft.RootGroupID, encoded); err != nil {
		t.Fatal(err)
	}
	if _, err := process.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	if _, err := joining.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	if _, found, err := joining.Runtime.Root.Tenant("after-restart"); err != nil || !found {
		t.Fatal("restarted learner did not apply fresh Root command", found, err)
	}
	// Stop EVERY node, then restart from the existing stores. Neither process
	// gets a signer, a ticket, bootstrap voters, or an external peer directory.
	// This is a single-voter + learner cold restart, not regional quorum proof.
	stop()
	stopIssuer()
	startSource()
	joinListener, err = net.Listen("tcp", joinAddress)
	if err != nil {
		t.Fatal(err)
	}
	start(joinListener)
	select {
	case <-process.Ready():
	case <-ctx.Done():
		t.Fatal("cold source not ready")
	}
	cold, err := joining.Runtime.RootJoin.Load()
	if err != nil || cold.ActivatedIndex != activated.ActivatedIndex {
		t.Fatal("cold start replaced admission", cold, err)
	}
	if _, found, err := joining.Runtime.Root.Tenant("after-restart"); err != nil || !found {
		t.Fatal("cold start lost Root data", found, err)
	}
	checkHoldings()
	checkTransferred()
	// A Root learner is also a data-holding Node. Its local Store is distinct;
	// neither voter status nor a Volume count is a data durability quorum.
	d := holdingDescriptors[0]
	localHolding, err := joining.Runtime.Records.Put(ctx, d, []byte("opaque/file"))
	if err != nil {
		t.Fatal(err)
	}
	observed, err := process.GroupClient.ObserveRecordHolding(ctx, bound.RaftID, d)
	if err != nil || observed.NodeID != bound.NodeID || observed.Holding.StoreID != localHolding.StoreID || observed.Holding.StoreID == sourceHoldings[0].StoreID {
		t.Fatal("learner holding confused with source Store", observed, err)
	}
}
