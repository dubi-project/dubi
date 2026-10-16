package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/engine/record"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginedata"
	"github.com/dubi-project/dubi/internal/platform/cluster/engineruntime"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rangebootstrap"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/regiongroups"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/server"
)

// Integration-only operation: publish a logical immutable reference through
// the same Raft mutation path without extending the user-facing Record module.
type referenceTestHandler struct{}

type immutableEngineTestHandler struct{}

func (immutableEngineTestHandler) Apply(tx *enginestate.Transaction, target router.Target, payload []byte) (enginestate.Outcome, error) {
	if bytes.Equal(payload, []byte("skip-reference")) {
		return enginestate.Outcome{}, nil
	}
	var descriptor records.Descriptor
	if err := json.Unmarshal(payload, &descriptor); err != nil {
		return enginestate.Outcome{}, err
	}
	return enginestate.Outcome{}, tx.SetRecordReference(target.RoutingKey, descriptor)
}

type rejectingRecordSender struct{ calls int }

type repairReportBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (r *repairReportBuffer) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.b.Write(p)
}

func (r *repairReportBuffer) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.b.String()
}

func (s *rejectingRecordSender) PutRecord(context.Context, rootstate.Node, records.Descriptor, []byte) (raftrpc.HoldingObservation, error) {
	s.calls++
	return raftrpc.HoldingObservation{}, errors.New("unexpected Pack transfer during confirmed write retry")
}

func (referenceTestHandler) Apply(tx *enginestate.Transaction, target router.Target, payload []byte) (enginestate.Outcome, error) {
	if len(payload) > 0 && payload[0] == 0x7f {
		descriptor, err := records.Describe(target.Keyspace, target.OwnerGroupID, target.RoutingKey, payload[1:])
		if err != nil {
			return enginestate.Outcome{}, err
		}
		return enginestate.Outcome{}, tx.SetRecordReference(target.RoutingKey, descriptor)
	}
	return record.Handler{}.Apply(tx, target, payload)
}

func (referenceTestHandler) Read(view enginestate.ReadView, target router.Target, payload []byte) (enginestate.Outcome, error) {
	return record.Handler{}.Read(view, target, payload)
}

func referenceTestMux(t *testing.T) *engineruntime.Mux {
	t.Helper()
	registry := engineruntime.NewRegistry()
	if err := registry.Register(record.EngineID, referenceTestHandler{}); err != nil {
		t.Fatal(err)
	}
	for _, engineID := range []string{"data-alpha", "data-beta"} {
		if err := registry.Register(engineID, immutableEngineTestHandler{}); err != nil {
			t.Fatal(err)
		}
	}
	mux, err := registry.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return mux
}

func activateEngineTestDataset(t *testing.T, ctx context.Context, first *server.Server,
	group string, space rootstate.Keyspace) {
	t.Helper()
	dataset := rootstate.Dataset{TenantID: space.TenantID, EngineID: space.EngineID,
		DatasetID: space.DatasetID, RangeID: "range-" + space.EngineID, OwnerGroupID: group}
	rootCommand, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "create-" + space.EngineID, Type: rootstate.CreateDataset, Dataset: &dataset})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Runtime.Host.Submit(raft.RootGroupID, rootCommand); err != nil {
		t.Fatal("submit engine Dataset", err)
	}
	for {
		if _, err := first.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			t.Fatal(err)
		}
		result, found, err := first.Runtime.Root.ResultForCommand(rootCommand)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			if result.Error != "" {
				t.Fatal("engine Dataset rejected", result.Error)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("engine Dataset registration did not commit", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	bootstrap, err := rangebootstrap.New(first.Runtime.Host, first.Runtime.Root,
		func(groupID string) (rangebootstrap.OwnerControl, bool) {
			owner, found := first.Runtime.Engines.Resolve(groupID)
			if !found {
				return nil, false
			}
			control, err := rangebootstrap.NewLocalOwner(first.Runtime.Host, owner)
			return control, err == nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.Reconcile(ctx, dataset.RangeID); err != nil {
		t.Fatal("activate engine Dataset", err)
	}
}

func TestEngineDataAnyNodeGateway(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()
	mux := referenceTestMux(t)
	store := filepath.Join(t.TempDir(), "owner-1")
	if err := runInit([]string{"--store", store}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	first, err := openServerWithMux([]string{"--store", store, "--listen", "127.0.0.1:0"}, io.Discard, mux)
	if err != nil {
		t.Fatal(err)
	}
	firstStop, wait := startJoinTestServer(t, first)
	wait()
	owners := map[uint64]func(){first.Runtime.Identity.RaftID: firstStop}
	ownerServers := map[uint64]*server.Server{first.Runtime.Identity.RaftID: first}
	firstNode, found, err := first.Runtime.Root.Node(first.Runtime.Identity.RaftID)
	if err != nil || !found {
		t.Fatal("initial Node is not registered", err)
	}
	for i := 0; i < 2; i++ {
		_, ticket := joinTestTicket(t, store, firstNode.Address)
		peerStore := filepath.Join(t.TempDir(), "owner")
		peer, err := openJoiningServerWithMux(ctx, []string{"--store", peerStore,
			"--listen", "127.0.0.1:0", "--ticket-file", ticket}, nil, io.Discard, mux)
		if err != nil {
			t.Fatal(err)
		}
		stop, ready := startJoinTestServer(t, peer)
		ready()
		owners[peer.Runtime.Identity.RaftID] = stop
		ownerServers[peer.Runtime.Identity.RaftID] = peer
	}
	group := regiongroups.GroupID("default")
	for {
		progress, ready, err := first.Runtime.Root.RegionProgress(group)
		if err == nil && ready && progress.Eligible && progress.Pending == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("Owner Region did not form", err)
		case <-time.After(25 * time.Millisecond):
		}
	}
	space := rootstate.Keyspace{TenantID: "gateway-demo", EngineID: "data-alpha", DatasetID: "shared"}
	// The test-owned control-plane setup does not turn the development Record
	// endpoint into a general Dataset API.
	tenantCommand, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
		RequestID: "gateway-demo-tenant", Type: rootstate.RegisterTenant,
		Tenant: &rootstate.Tenant{TenantID: space.TenantID}})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Runtime.Host.Submit(raft.RootGroupID, tenantCommand); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := first.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			t.Fatal(err)
		}
		if result, found, err := first.Runtime.Root.ResultForCommand(tenantCommand); err != nil {
			t.Fatal(err)
		} else if found {
			if result.Error != "" {
				t.Fatal(result.Error)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	activateEngineTestDataset(t, ctx, first, group, space)
	_, ticket := joinTestTicket(t, store, firstNode.Address)
	gatewayStore := filepath.Join(t.TempDir(), "gateway")
	gateway, err := openJoiningServerWithMux(ctx, []string{"--store", gatewayStore,
		"--listen", "127.0.0.1:0", "--region", "gateway-region", "--ticket-file", ticket}, nil, io.Discard, mux)
	if err != nil {
		t.Fatal(err)
	}
	_, ready := startJoinTestServer(t, gateway)
	ready()
	if _, found := gateway.Runtime.Engines.Resolve(group); found {
		t.Fatal("gateway unexpectedly hosts the Owner Group")
	}
	body := bytes.Repeat([]byte("streamed-gateway-body-"), 100000)
	request := enginedata.WriteRequest{RequestID: "gateway-any-node-write", Keyspace: space,
		Key: []byte("gateway-key"), Body: body,
		Publish: func(d records.Descriptor) ([]byte, error) { return json.Marshal(d) }}
	var written enginedata.WriteResult
	for {
		attempt, stop := context.WithTimeout(ctx, 12*time.Second)
		written, err = gateway.EngineData.Write(attempt, request)
		stop()
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("gateway did not route a durable write", written, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	if written.State != enginedata.WriteLocalReady || written.Local.ConfirmationIndex == 0 {
		t.Fatal("gateway returned without Owner LocalReady proof", written)
	}
	if _, err := gateway.EngineData.WriteLocal(ctx, request); !errors.Is(err, enginedata.ErrOwnerNotLocal) {
		t.Fatal("Owner-local entry did not distinguish a non-Owner gateway", err)
	}
	read, err := gateway.EngineData.Read(ctx, space, request.Key)
	if err != nil || !bytes.Equal(read.Body, body) || read.Reference.Index != written.Local.Reference.Index {
		t.Fatal("gateway did not stream verified current data from Owner", len(read.Body), err)
	}
	// Kill the first candidate advertised to the gateway, not just an
	// arbitrary Follower: a single Write call must try another Owner member.
	candidates := gateway.GroupClient.GroupCandidates(group)
	if len(candidates) != 3 || owners[candidates[0]] == nil {
		t.Fatal("gateway lacks the expected Owner candidates", candidates)
	}
	beforeConf, beforeErr := gateway.Runtime.Root.RootMembership()
	t.Logf("before Owner loss: candidates=%v stopped=%x RootConf=%v err=%v", candidates, candidates[0], beforeConf, beforeErr)
	owners[candidates[0]]()
	second := enginedata.WriteRequest{RequestID: "gateway-after-owner-loss", Keyspace: space,
		Key: []byte("gateway-key-after-loss"), Body: bytes.Repeat([]byte("surviving-owner-"), 80000),
		Publish: func(d records.Descriptor) ([]byte, error) { return json.Marshal(d) }}
	failoverCtx, failoverCancel := context.WithTimeout(ctx, 60*time.Second)
	defer failoverCancel()
	committed, err := gateway.EngineData.Write(failoverCtx, second)
	if err != nil || committed.State != enginedata.WriteLocalReady {
		conf, confErr := gateway.Runtime.Root.RootMembership()
		t.Logf("failed Owner=%x candidates=%v gateway RootConf=%v applied=%d err=%v", candidates[0], candidates, conf, gateway.Runtime.Root.AppliedIndex(), confErr)
		for _, id := range candidates[1:] {
			probe, done := context.WithTimeout(ctx, 2*time.Second)
			index, readErr := ownerServers[id].Runtime.Host.ReadIndex(probe, raft.RootGroupID)
			done()
			t.Logf("surviving Owner %x Root ReadIndex=%d err=%v", id, index, readErr)
			probe, done = context.WithTimeout(ctx, 2*time.Second)
			index, readErr = gateway.GroupClient.ReadIndex(probe, id, raft.RootGroupID)
			done()
			t.Logf("gateway to Owner %x remote Root ReadIndex=%d err=%v", id, index, readErr)
		}
		t.Fatal("gateway did not fail over to a surviving Owner", committed, err)
	}
	// Model a lost response at the caller: the write is committed, but the
	// caller repeats the exact RequestID/body instead of inventing a new one.
	retried, err := gateway.EngineData.Write(failoverCtx, second)
	if err != nil || retried.State != enginedata.WriteLocalReady ||
		retried.Local.Reference.Index != committed.Local.Reference.Index {
		t.Fatal("gateway retry did not recover the original publication", committed, retried, err)
	}
	afterLoss, err := gateway.EngineData.Read(failoverCtx, space, second.Key)
	if err != nil || !bytes.Equal(afterLoss.Body, second.Body) ||
		afterLoss.Reference.Index != committed.Local.Reference.Index {
		t.Fatal("gateway did not read through surviving Owner members", err)
	}
	// Without the remaining Owner majority the gateway must not claim a
	// successful publication, even if one physical node still has free space.
	owners[candidates[1]]()
	noMajorityCtx, noMajorityCancel := context.WithTimeout(ctx, 5*time.Second)
	defer noMajorityCancel()
	third := second
	third.RequestID = "gateway-without-owner-majority"
	third.Key = []byte("gateway-key-without-majority")
	if result, err := gateway.EngineData.Write(noMajorityCtx, third); err == nil ||
		result.State == enginedata.WriteLocalReady {
		t.Fatal("gateway claimed LocalReady without an Owner majority", result, err)
	}
}

func TestManagedRecordRoundTripAndRegionalReferenceFill(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	mux := referenceTestMux(t)
	type running struct {
		store, address string
		process        *server.Server
		stop           func()
	}
	nodes := make([]running, 4)
	nodes[0].store = filepath.Join(t.TempDir(), "n1")
	if err := runInit([]string{"--store", nodes[0].store}, io.Discard, io.Discard); err != nil {
		t.Fatal(err)
	}
	first, err := openServerWithMux([]string{"--store", nodes[0].store, "--listen", "127.0.0.1:0"}, io.Discard, mux)
	if err != nil {
		t.Fatal(err)
	}
	nodes[0].process = first
	stop, wait := startJoinTestServer(t, first)
	nodes[0].stop = stop
	wait()
	address := func(i int) {
		node, found, err := nodes[i].process.Runtime.Root.Node(nodes[i].process.Runtime.Identity.RaftID)
		if err != nil || !found {
			t.Fatal("Node registration missing", i, err)
		}
		nodes[i].address = node.Address
	}
	address(0)
	for i := 1; i < 3; i++ {
		_, ticket := joinTestTicket(t, nodes[0].store, nodes[0].address)
		nodes[i].store = filepath.Join(t.TempDir(), "n")
		joined, err := openJoiningServerWithMux(ctx, []string{"--store", nodes[i].store, "--listen", "127.0.0.1:0", "--ticket-file", ticket}, nil, io.Discard, mux)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i].process = joined
		nodes[i].stop, wait = startJoinTestServer(t, joined)
		wait()
		address(i)
	}
	group := regiongroups.GroupID("default")
	for {
		progress, ready, err := first.Runtime.Root.RegionProgress(group)
		if err == nil && ready && progress.Eligible && progress.Pending == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("regional data Group did not form", err)
		case <-time.After(25 * time.Millisecond):
		}
	}
	if err := saveLocalContext(nodes[0].store, first.Runtime.Identity, first.LocalAddress(), nodes[0].address, nodes[0].address); err != nil {
		t.Fatal("select local entry Node", err)
	}
	command := func(args ...string) []byte {
		t.Helper()
		var out, diagnostics bytes.Buffer
		if code := Run(ctx, args, &out, &diagnostics); code != 0 {
			t.Fatalf("%v failed: %s", args[:2], diagnostics.String())
		}
		return out.Bytes()
	}
	create := command("dataset", "create", "--request-id", "data-test-create", "demo", "items")
	var descriptor struct {
		RangeID      string `json:"range_id"`
		OwnerGroupID string `json:"owner_group_id"`
	}
	if err := json.Unmarshal(create, &descriptor); err != nil || descriptor.RangeID == "" || descriptor.OwnerGroupID != group {
		t.Fatal("Dataset was not activated", string(create), err)
	}
	// A engine implementation calls the same in-process data entry; the
	// platform routes it and verifies the exact reference published by its
	// opaque mutation. Both handlers share this Owner Group and Node Pack pool.
	engineCases := []struct {
		space rootstate.Keyspace
		body  []byte
	}{
		{space: rootstate.Keyspace{TenantID: "demo", EngineID: "data-alpha", DatasetID: "shared"}, body: []byte("alpha-immutable-body")},
		{space: rootstate.Keyspace{TenantID: "demo", EngineID: "data-beta", DatasetID: "shared"}, body: []byte("beta-immutable-body")},
	}
	for _, tc := range engineCases {
		dataset := rootstate.Dataset{TenantID: tc.space.TenantID, EngineID: tc.space.EngineID,
			DatasetID: tc.space.DatasetID, RangeID: "range-" + tc.space.EngineID, OwnerGroupID: group}
		rootCommand, err := rootstate.EncodeCommand(rootstate.Command{Version: rootstate.FormatVersion,
			RequestID: "create-" + tc.space.EngineID, Type: rootstate.CreateDataset, Dataset: &dataset})
		if err != nil {
			t.Fatal(err)
		}
		if err := first.Runtime.Host.Submit(raft.RootGroupID, rootCommand); err != nil {
			t.Fatal("submit engine Dataset", err)
		}
		for {
			if _, err := first.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
				t.Fatal(err)
			}
			result, found, err := first.Runtime.Root.ResultForCommand(rootCommand)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				if result.Error != "" {
					t.Fatal("engine Dataset rejected", result.Error)
				}
				break
			}
			select {
			case <-ctx.Done():
				t.Fatal("engine Dataset registration did not commit", ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
		bootstrap, err := rangebootstrap.New(first.Runtime.Host, first.Runtime.Root,
			func(groupID string) (rangebootstrap.OwnerControl, bool) {
				owner, found := first.Runtime.Engines.Resolve(groupID)
				if !found {
					return nil, false
				}
				control, err := rangebootstrap.NewLocalOwner(first.Runtime.Host, owner)
				return control, err == nil
			})
		if err != nil {
			t.Fatal(err)
		}
		if err := bootstrap.Reconcile(ctx, dataset.RangeID); err != nil {
			t.Fatal("activate engine Dataset", err)
		}
		write := enginedata.WriteRequest{
			RequestID: "write-" + tc.space.EngineID, Keyspace: tc.space,
			Key: []byte("same-key"), Body: tc.body,
			Publish: func(d records.Descriptor) ([]byte, error) { return json.Marshal(d) },
		}
		var written enginedata.WriteResult
		for {
			written, err = first.EngineData.WriteLocal(ctx, write)
			if err == nil {
				break
			}
			if written.State != enginedata.WritePublishedUnconfirmed {
				t.Fatal("engine write failed before recoverable confirmation", tc.space, written, err)
			}
			select {
			case <-ctx.Done():
				t.Fatal("engine confirmation did not recover with the same RequestID", tc.space, err)
			case <-time.After(25 * time.Millisecond):
			}
		}
		if err != nil || written.State != enginedata.WriteLocalReady || written.Local.ConfirmationIndex == 0 {
			t.Fatal("engine data did not reach LocalReady", tc.space, written, err)
		}
		stored, err := records.Describe(tc.space, group, []byte("same-key"), tc.body)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := first.Runtime.Records.Verify(ctx, stored); err != nil {
			t.Fatal("engine bytes missing from the shared local Pack pool", tc.space, err)
		}
		read, err := nodes[1].process.EngineData.ReadLocal(ctx, tc.space, []byte("same-key"))
		if err != nil || !bytes.Equal(read.Body, tc.body) {
			t.Fatal("second Node could not read engine data", tc.space, read, err)
		}
	}
	unpublished, err := first.EngineData.WriteLocal(ctx, enginedata.WriteRequest{
		RequestID: "write-without-reference", Keyspace: engineCases[0].space,
		Key: []byte("no-reference"), Body: []byte("not-durable-without-reference"),
		Publish: func(records.Descriptor) ([]byte, error) { return []byte("skip-reference"), nil },
	})
	if err == nil || unpublished.State != enginedata.WriteUnknown {
		t.Fatal("handler mutation without the exact reference was mistaken for LocalReady", unpublished, err)
	}
	if got, err := first.EngineData.ReadLocal(ctx, engineCases[0].space, []byte("no-reference")); err == nil || len(got.Body) != 0 {
		t.Fatal("unpublished immutable bytes leaked through the read gate", got, err)
	}
	packFiles, err := filepath.Glob(filepath.Join(nodes[0].store, "volume", "packs", "*.pack"))
	if err != nil || len(packFiles) != 1 {
		t.Fatal("two Engines did not use the same physical Pack", packFiles, err)
	}
	probeSpace := engineCases[0].space
	probeDescriptor, err := records.Describe(probeSpace, group, []byte("same-key"), engineCases[0].body)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := nodes[i].process.Runtime.Records.Put(ctx, probeDescriptor, engineCases[0].body); err != nil {
			t.Fatal("prepare physical holdings for disk loss", i, err)
		}
	}
	// A disappeared data Volume is not a new empty Volume just because the
	// original path becomes writable again. The Node may still participate in
	// Root/Owner Raft, but its Pack operations must fail closed. An ordinary
	// any-Node read can use the two surviving physical holders.
	volumePath := filepath.Join(nodes[2].store, "volume")
	offlinePath := volumePath + "-offline-test"
	if err := os.Rename(volumePath, offlinePath); err != nil {
		t.Fatal("detach third Node's data Volume", err)
	}
	volumeOffline := true
	defer func() {
		if volumeOffline {
			_ = os.Remove(volumePath)
			_ = os.Rename(offlinePath, volumePath)
		}
	}()
	if err := os.Mkdir(volumePath, 0o750); err != nil {
		t.Fatal("create empty replacement mountpoint", err)
	}
	volumeReport, err := nodes[2].process.Runtime.Volumes.Status()
	if err != nil || len(volumeReport.Volumes) != 1 || volumeReport.Volumes[0].Health != "invalid" {
		t.Fatal("replacement mountpoint was not distinguished from a missing Volume", volumeReport, err)
	}
	if _, err := nodes[2].process.Runtime.Records.Verify(ctx, probeDescriptor); err == nil {
		t.Fatal("replaced Volume proved the old physical holding")
	}
	newDescriptor, err := records.Describe(probeSpace, group, []byte("after-volume-loss"), []byte("new-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nodes[2].process.Runtime.Records.Put(ctx, newDescriptor, []byte("new-bytes")); err == nil {
		t.Fatal("replaced Volume accepted new physical bytes")
	}
	if _, err := nodes[2].process.EngineData.ReadLocal(ctx, probeSpace, []byte("same-key")); err == nil {
		t.Fatal("Owner-local read hid the missing Volume")
	}
	viaHealthyOwner, err := nodes[2].process.EngineData.Read(ctx, probeSpace, []byte("same-key"))
	if err != nil || !bytes.Equal(viaHealthyOwner.Body, engineCases[0].body) {
		t.Fatal("any-Node read did not use surviving physical holders", viaHealthyOwner, err)
	}
	entries, err := os.ReadDir(volumePath)
	if err != nil || len(entries) != 0 {
		t.Fatal("replacement mountpoint acquired data", entries, err)
	}
	if err := os.Remove(volumePath); err != nil {
		t.Fatal("remove empty replacement mountpoint", err)
	}
	if err := os.Rename(offlinePath, volumePath); err != nil {
		t.Fatal("reattach original data Volume", err)
	}
	volumeOffline = false
	if _, err := nodes[2].process.Runtime.Records.Verify(ctx, probeDescriptor); err != nil {
		t.Fatal("original Volume did not recover under its unchanged identity", err)
	}
	// The third Node still hosts the Owner Group, but its local immutable
	// Store has failed. ReadLocal must report the fault; the ordinary any-Node
	// entry must use another Owner, which independently checks the current
	// reference and the surviving two-of-three physical majority.
	if err := nodes[2].process.Runtime.Records.Close(); err != nil {
		t.Fatal("inject local immutable Store failure", err)
	}
	if _, err := nodes[2].process.EngineData.ReadLocal(ctx, probeSpace, []byte("same-key")); err == nil {
		t.Fatal("Owner-local read hid the failed Store")
	}
	viaHealthyOwner, err = nodes[2].process.EngineData.Read(ctx, probeSpace, []byte("same-key"))
	if err != nil || !bytes.Equal(viaHealthyOwner.Body, engineCases[0].body) {
		t.Fatal("any-Node read did not fall back to a healthy Owner", viaHealthyOwner, err)
	}
	// Routing cannot manufacture durability: if another physical holder now
	// leaves, only one of the original three Nodes can prove the bytes.
	nodes[1].stop()
	minorityCtx, minorityCancel := context.WithTimeout(ctx, 8*time.Second)
	minorityRead, minorityErr := nodes[2].process.EngineData.Read(minorityCtx, probeSpace, []byte("same-key"))
	minorityCancel()
	if minorityErr == nil || len(minorityRead.Body) != 0 {
		t.Fatal("Owner fallback returned data without a physical majority", minorityRead, minorityErr)
	}
	recoveredSecond, err := openServerWithMux([]string{"--store", nodes[1].store, "--listen", nodes[1].address}, io.Discard, mux)
	if err != nil {
		t.Fatal("restart surviving physical holder", err)
	}
	nodes[1].process = recoveredSecond
	nodes[1].stop, wait = startJoinTestServer(t, recoveredSecond)
	wait()
	nodes[2].stop()
	recoveredThird, err := openServerWithMux([]string{"--store", nodes[2].store, "--listen", nodes[2].address}, io.Discard, mux)
	if err != nil {
		t.Fatal("restart Node after local Store fault", err)
	}
	nodes[2].process = recoveredThird
	nodes[2].stop, wait = startJoinTestServer(t, recoveredThird)
	wait()
	command("record", "put", "--request-id", "data-test-put", "demo", "items", "greeting", "hello")
	read := func(i int) {
		t.Helper()
		encoded := command("record", "get", "--store", nodes[i].store, "--server", nodes[i].address,
			"demo", "items", "greeting")
		var got struct {
			Found bool   `json:"found"`
			Value []byte `json:"value"`
			Index uint64 `json:"confirmed_index"`
		}
		if err := json.Unmarshal(encoded, &got); err != nil || !got.Found || string(got.Value) != "hello" || got.Index == 0 {
			t.Fatal("Record was not readable", i, string(encoded), err)
		}
	}
	for i := 0; i < 3; i++ {
		read(i)
	}
	nodes[1].stop()
	remaining, stopWait := context.WithTimeout(ctx, 12*time.Second)
	defer stopWait()
	var left, right raft.GroupStatus
	for {
		left, _ = nodes[0].process.Runtime.Host.Status(group)
		right, _ = nodes[2].process.Runtime.Host.Status(group)
		if left.LeaderID != 0 && left.LeaderID != nodes[1].process.Runtime.Identity.RaftID && right.LeaderID == left.LeaderID {
			break
		}
		select {
		case <-remaining.Done():
			t.Fatalf("data Group did not re-elect with two of three Nodes: left=%+v right=%+v", left, right)
		case <-time.After(25 * time.Millisecond):
		}
	}
	read(0)
	command("record", "put", "--store", nodes[0].store, "--server", nodes[0].address,
		"--request-id", "data-test-two-online", "demo", "items", "failover", "two-online")
	var whileOneDown struct {
		Found bool   `json:"found"`
		Value []byte `json:"value"`
	}
	if encoded := command("record", "get", "--store", nodes[2].store, "--server", nodes[2].address,
		"demo", "items", "failover"); json.Unmarshal(encoded, &whileOneDown) != nil || !whileOneDown.Found || string(whileOneDown.Value) != "two-online" {
		t.Fatal("two live data voters did not accept and serve a new write", string(encoded))
	}
	restarted, err := openServerWithMux([]string{"--store", nodes[1].store, "--listen", nodes[1].address}, io.Discard, mux)
	if err != nil {
		t.Fatal(err)
	}
	nodes[1].process = restarted
	nodes[1].stop, wait = startJoinTestServer(t, restarted)
	wait()
	for {
		attempt, stop := context.WithTimeout(ctx, time.Second)
		var out, diagnostics bytes.Buffer
		code := Run(attempt, []string{"record", "get", "--store", nodes[1].store, "--server", nodes[1].address,
			"demo", "items", "greeting"}, &out, &diagnostics)
		stop()
		if code == 0 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("restarted Record Group did not recover", diagnostics.String())
		case <-time.After(25 * time.Millisecond):
		}
	}
	read(1)
	for _, tc := range engineCases {
		got, err := nodes[1].process.EngineData.ReadLocal(ctx, tc.space, []byte("same-key"))
		if err != nil || !bytes.Equal(got.Body, tc.body) {
			t.Fatal("restarted Node lost engine data", tc.space, got, err)
		}
	}
	whileOneDown = struct {
		Found bool   `json:"found"`
		Value []byte `json:"value"`
	}{}
	if encoded := command("record", "get", "--store", nodes[1].store, "--server", nodes[1].address,
		"demo", "items", "failover"); json.Unmarshal(encoded, &whileOneDown) != nil || !whileOneDown.Found || string(whileOneDown.Value) != "two-online" {
		t.Fatal("restarted voter did not catch up the two-live-node write", string(encoded))
	}
	// A changed payload may not reuse the previously confirmed RequestID.
	var out, diagnostics bytes.Buffer
	if code := Run(ctx, []string{"record", "put", "--store", nodes[0].store, "--server", nodes[0].address,
		"--request-id", "data-test-put", "demo", "items", "greeting", "changed"}, &out, &diagnostics); code == 0 {
		t.Fatal("RequestID reuse changed a committed Record")
	}
	read(2)
	if _, found, err := first.Runtime.Root.Tenant("demo"); err != nil || !found {
		t.Fatal("Tenant missing", err)
	}
	if _, found, err := first.Runtime.Root.RouteKeyspace(rootstate.Keyspace{TenantID: "demo", EngineID: "record", DatasetID: "items"}, []byte("greeting")); err != nil || !found {
		t.Fatal("Range missing", err)
	}
	if _, err := first.Runtime.Host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		t.Fatal(err)
	}
	// Each old Node initially owns a different physical record, including an
	// empty one. The logical references are replicated through the data Group,
	// so n4 must try multiple old sources while filling the sealed cut.
	space := rootstate.Keyspace{TenantID: "demo", EngineID: record.EngineID, DatasetID: "items"}
	type immutableRef struct {
		key, body  []byte
		source     int
		target     router.Target
		descriptor records.Descriptor
	}
	refs := []immutableRef{
		{key: []byte("immutable-one"), body: []byte("payload-owned-by-n1-before-growth"), source: 0},
		{key: []byte("immutable-two"), body: []byte("payload-owned-by-n2-before-growth"), source: 1},
		{key: []byte("immutable-empty"), body: []byte{}, source: 2},
	}
	routeResolver, err := router.New(first.Runtime.Host, first.Runtime.Root)
	if err != nil {
		t.Fatal(err)
	}
	publishReference := func(ref *immutableRef, requestID string) uint64 {
		t.Helper()
		payload := append([]byte{0x7f}, ref.body...)
		mutation, err := routedstate.EncodeMutation(requestID, ref.target, payload)
		if err != nil {
			t.Fatal(err)
		}
		if err := first.Runtime.Host.Submit(group, mutation); err != nil {
			t.Fatal("publish immutable reference through Raft", err)
		}
		for {
			state, found := first.Runtime.Engines.Resolve(group)
			if !found {
				t.Fatal("old regional Group disappeared")
			}
			if _, err := first.Runtime.Host.ReadIndex(ctx, group); err != nil {
				t.Fatal(err)
			}
			result, found, err := state.ResultForMutation(requestID, ref.target, payload)
			if err != nil {
				t.Fatal(err)
			}
			if found {
				if !result.Applied || result.Error != "" {
					t.Fatal("reference mutation rejected", result)
				}
				return result.Index
			}
			select {
			case <-ctx.Done():
				t.Fatal("reference mutation was not applied", ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	for i := range refs {
		ref := &refs[i]
		ref.target, err = routeResolver.ResolveTarget(ctx, space, ref.key)
		if err != nil {
			t.Fatal(err)
		}
		ref.descriptor, err = records.Describe(space, group, ref.key, ref.body)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := nodes[ref.source].process.Runtime.Records.Put(ctx, ref.descriptor, ref.body); err != nil {
			t.Fatal("store original immutable bytes", err)
		}
		// Before publication, the automatic live repair worker cannot
		// discover this record from the Owner reference index.
		for source := range 3 {
			if source == ref.source {
				continue
			}
			if _, err := nodes[source].process.Runtime.Records.Verify(ctx, ref.descriptor); !errors.Is(err, records.ErrNotHeld) {
				t.Fatal("another old Node already held the immutable bytes", source, err)
			}
		}
		publishReference(ref, fmt.Sprintf("reference-before-growth-%d", i))
	}
	// Exercise the new module-neutral pre-publication path over real TLS. This
	// prepares a physical local majority, but is not yet a durable LocalReady
	// receipt; the test handler publishes its logical reference separately.
	majority := immutableRef{key: []byte("immutable-majority"), body: []byte("prepared-across-a-stable-majority"), source: -1}
	majority.target, err = routeResolver.ResolveTarget(ctx, space, majority.key)
	if err != nil {
		t.Fatal(err)
	}
	majority.descriptor, err = records.Describe(space, group, majority.key, majority.body)
	if err != nil {
		t.Fatal(err)
	}
	staged, err := recordwrite.StageLocal(ctx, first.Runtime.Host, first.Runtime.Root,
		first.Runtime.Identity.RaftID, first.Runtime.Identity.NodeID, first.Runtime.Records, first.GroupClient,
		majority.target, majority.descriptor, majority.body)
	if err != nil || len(staged.Holdings) != staged.Plan.Required || staged.Plan.Required != 2 {
		t.Fatal("did not prepare physical bytes on a stable local majority", staged, err)
	}
	for _, held := range staged.Holdings {
		found := false
		for _, node := range nodes[:3] {
			if node.process.Runtime.Identity.RaftID != held.RaftID {
				continue
			}
			found = true
			if _, err := node.process.Runtime.Records.Verify(ctx, majority.descriptor); err != nil {
				t.Fatal("prepared Node did not really hold bytes", held.RaftID, err)
			}
		}
		if !found {
			t.Fatal("prepared holding belongs to no old Node", held.RaftID)
		}
	}
	publicationIndex := publishReference(&majority, "reference-majority-before-growth")
	owner, found := first.Runtime.Engines.Resolve(group)
	if !found {
		t.Fatal("regional Owner Group disappeared after immutable publication")
	}
	observed, err := recordwrite.ObservePublishedLocal(ctx, first.Runtime.Host, first.Runtime.Root, owner,
		first.Runtime.Identity.RaftID, first.Runtime.Identity.NodeID, first.Runtime.Records, first.GroupClient,
		majority.target, majority.descriptor, "reference-majority-before-growth", publicationIndex)
	if err != nil || observed.Reference.Index != publicationIndex || len(observed.Holdings) != observed.Plan.Required {
		t.Fatal("published immutable reference did not retain a stable local physical majority", observed, err)
	}
	claim, err := recordwrite.ConfirmationClaim(majority.target, observed)
	if err != nil {
		t.Fatal("build stable local confirmation", err)
	}
	confirmation, err := routedstate.EncodeLocalConfirmation("confirm-majority-before-growth", claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Runtime.Host.Submit(group, confirmation); err != nil {
		t.Fatal("commit local physical confirmation through Owner Raft", err)
	}
	for {
		if _, err := first.Runtime.Host.ReadIndex(ctx, group); err != nil {
			t.Fatal(err)
		}
		result, found, err := owner.ResultForLocalConfirmation("confirm-majority-before-growth", claim)
		if err != nil {
			t.Fatal(err)
		}
		if found {
			if !result.Applied || result.Error != "" {
				t.Fatal("local physical confirmation rejected", result)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("local physical confirmation was not applied", ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err := owner.ReadRouted(ctx, first.Runtime.Host, majority.target, func(view enginestate.ReadView) error {
		receipt, found, err := view.LocalConfirmation(majority.key)
		if err != nil || !found || receipt.Claim.ReferenceIndex != publicationIndex || receipt.Claim.Generation != observed.Plan.Generation {
			return fmt.Errorf("local physical confirmation missing from Owner cut: found=%t record=%+v error=%v", found, receipt, err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	confirmed, err := recordwrite.CheckCommittedLocal(ctx, first.Runtime.Host, first.Runtime.Root, owner,
		first.Runtime.Identity.RaftID, first.Runtime.Identity.NodeID, first.Runtime.Records, first.GroupClient,
		majority.target, majority.descriptor, "reference-majority-before-growth", publicationIndex,
		"confirm-majority-before-growth", claim)
	if err != nil || len(confirmed.Holdings) != confirmed.Plan.Required {
		t.Fatal("final committed local majority gate failed", confirmed, err)
	}
	refs = append(refs, majority)
	// Exercise the assembled internal write protocol and an identical retry.
	// The payload remains test-owned; the platform only checks that it really
	// published the expected immutable reference before returning success.
	flow := immutableRef{key: []byte("immutable-flow"), body: []byte("coordinated-and-retryable-local-write"), source: -1}
	flow.target, err = routeResolver.ResolveTarget(ctx, space, flow.key)
	if err != nil {
		t.Fatal(err)
	}
	flow.descriptor, err = records.Describe(space, group, flow.key, flow.body)
	if err != nil {
		t.Fatal(err)
	}
	lostConfirmation := errors.New("simulated lost confirmation acknowledgement")
	loseOneConfirmation := true
	writer := recordwrite.Writer{Barrier: first.Runtime.Host, Root: first.Runtime.Root, Owner: owner,
		LocalID: first.Runtime.Identity.RaftID, LocalNodeID: first.Runtime.Identity.NodeID,
		Store: first.Runtime.Records, Sender: first.GroupClient, Observer: first.GroupClient,
		Submit: func(call context.Context, groupID string, encoded []byte) error {
			if err := first.Runtime.Host.Submit(groupID, encoded); err != nil {
				return err
			}
			command, err := routedstate.DecodeCommand(encoded)
			if err != nil || command.Type != routedstate.ConfirmLocal || !loseOneConfirmation {
				return err
			}
			loseOneConfirmation = false
			for {
				if _, err := first.Runtime.Host.ReadIndex(call, groupID); err != nil {
					return err
				}
				if result, found, err := owner.ResultForLocalConfirmation(command.RequestID, *command.Local); err != nil {
					return err
				} else if found {
					if !result.Applied {
						return fmt.Errorf("confirmation rejected before simulated lost ACK: %s", result.Error)
					}
					return lostConfirmation
				}
				select {
				case <-call.Done():
					return call.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}
		}}
	writeRequest := recordwrite.WriteRequest{RequestID: "immutable-flow-write", Target: flow.target,
		Descriptor: flow.descriptor, Body: flow.body, Payload: append([]byte{0x7f}, flow.body...)}
	if _, err := writer.WriteLocal(ctx, writeRequest); !errors.Is(err, lostConfirmation) {
		t.Fatal("first write did not simulate a lost acknowledgement after durable confirmation", err)
	}
	completed, err := writer.WriteLocal(ctx, writeRequest)
	if err != nil || completed.Reference.Index == 0 || completed.ConfirmationIndex == 0 {
		t.Fatal("internal local write did not reach committed majority", completed, err)
	}
	rejectingSender := &rejectingRecordSender{}
	writer.Sender = rejectingSender
	replayed, err := writer.WriteLocal(ctx, writeRequest)
	if err != nil || replayed.Reference.Index != completed.Reference.Index || replayed.ConfirmationIndex != completed.ConfirmationIndex || rejectingSender.calls != 0 {
		t.Fatal("identical write retry did not recover prior confirmation", replayed, err)
	}
	corruptRetry := writeRequest
	corruptRetry.Body = []byte("different-content-with-same-descriptor")
	if got, err := writer.WriteLocal(ctx, corruptRetry); err == nil || got.Reference.Index != 0 || rejectingSender.calls != 0 {
		t.Fatal("confirmed write retry accepted a body differing from descriptor", got, err)
	}
	outsideWriter := writer
	outsideWriter.LocalID = 999
	outsideWriter.LocalNodeID = "outside-region"
	if got, err := outsideWriter.WriteLocal(ctx, writeRequest); err == nil || got.Reference.Index != 0 || rejectingSender.calls != 0 {
		t.Fatal("confirmed write retry accepted a Node outside the stable cohort", got, err)
	}
	writer.Sender = first.GroupClient
	readerIndex := -1
	for i := 1; i < 3; i++ {
		if _, err := nodes[i].process.Runtime.Records.Verify(ctx, flow.descriptor); errors.Is(err, records.ErrNotHeld) {
			readerIndex = i
		} else if err != nil {
			t.Fatal("cannot inspect physical placement before read", i, err)
		}
	}
	// The in-process repair worker may fill the third Node before this test
	// reaches the read gate. In that case, still exercise the confirmed read.
	if readerIndex < 0 {
		readerIndex = 1
	}
	readerNode := nodes[readerIndex].process
	readerOwner, found := readerNode.Runtime.Engines.Resolve(group)
	if !found {
		t.Fatal("unheld Node lacks regional Owner Group")
	}
	reader := recordwrite.Reader{Barrier: readerNode.Runtime.Host, Root: readerNode.Runtime.Root,
		Owner: readerOwner, LocalID: readerNode.Runtime.Identity.RaftID,
		LocalNodeID: readerNode.Runtime.Identity.NodeID, Store: readerNode.Runtime.Records,
		Observer: readerNode.GroupClient, Fetcher: readerNode.GroupClient}
	readFlow, err := reader.ReadLocal(ctx, flow.target, flow.key)
	if err != nil || !bytes.Equal(readFlow.Body, flow.body) || readFlow.Reference.Index != completed.Reference.Index {
		t.Fatal("unheld Node did not read confirmed bytes through a verified source", readFlow, err)
	}
	if _, err := readerNode.Runtime.Records.Verify(ctx, flow.descriptor); err != nil {
		t.Fatal("remote read did not persist data into the local shared Pack", err)
	}
	changed := writeRequest
	changed.Body = []byte("different-content-with-reused-request")
	changed.Payload = append([]byte{0x7f}, changed.Body...)
	changed.Descriptor, err = records.Describe(space, group, flow.key, changed.Body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.WriteLocal(ctx, changed); !errors.Is(err, enginestate.ErrRequestIDReused) {
		t.Fatal("changed immutable write reused a committed RequestID", err)
	}
	if _, err := first.Runtime.Records.Verify(ctx, changed.Descriptor); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("conflicting RequestID reached physical Pack staging", err)
	}
	refs = append(refs, flow)
	// The managed process must resume this debt on its own; no CLI action or
	// explicit one-shot RepairCurrentLocal call may be needed for coverage.
	for {
		missing := false
		for _, ref := range refs {
			if _, err := readerNode.Runtime.Records.Verify(ctx, ref.descriptor); errors.Is(err, records.ErrNotHeld) {
				missing = true
			} else if err != nil {
				t.Fatal("automatic repair found a damaged local record", string(ref.key), err)
			}
		}
		if !missing {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("managed Node did not automatically repair current references", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	repair, err := recordrepair.RepairCurrentLocal(ctx, readerNode.Runtime.Host,
		readerNode.Runtime.Root, readerOwner, group,
		readerNode.Runtime.Identity.RaftID, readerNode.Runtime.Identity.NodeID,
		readerNode.Runtime.Records, readerNode.GroupClient)
	if err != nil || repair.Cut.Count != uint64(len(refs)+len(engineCases)) ||
		repair.Fetched+repair.AlreadyHeld != repair.Cut.Count {
		t.Fatal("live Owner reference cut did not repair missing local Pack data", repair, err)
	}
	for _, ref := range refs {
		if _, err := readerNode.Runtime.Records.Verify(ctx, ref.descriptor); err != nil {
			t.Fatal("live repair did not persist referenced bytes", string(ref.key), err)
		}
	}
	// The fourth Node joins a formed Region. Root must not publish generation 2
	// until the new Node has completed its sealed-cut physical fill RPC.
	_, ticket := joinTestTicket(t, nodes[0].store, nodes[0].address)
	nodes[3].store = filepath.Join(t.TempDir(), "n4")
	joined, err := openJoiningServerWithMux(ctx, []string{"--store", nodes[3].store, "--listen", "127.0.0.1:0", "--ticket-file", ticket}, nil, io.Discard, mux)
	if err != nil {
		t.Fatal(err)
	}
	nodes[3].process = joined
	for _, ref := range refs {
		if _, err := joined.Runtime.Records.Verify(ctx, ref.descriptor); !errors.Is(err, records.ErrNotHeld) {
			t.Fatal("new Node already held immutable bytes before regional growth", string(ref.key), err)
		}
	}
	nodes[3].stop, wait = startJoinTestServer(t, joined)
	wait()
	address(3)
	for {
		progress, ready, err := first.Runtime.Root.RegionProgress(group)
		if err == nil && ready && progress.Eligible && progress.Pending == nil && progress.Current.Generation == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("fourth Node did not complete regional data handoff", err)
		case <-time.After(25 * time.Millisecond):
		}
	}
	if _, err := recordwrite.CheckCommittedLocal(ctx, first.Runtime.Host, first.Runtime.Root, owner,
		first.Runtime.Identity.RaftID, first.Runtime.Identity.NodeID, first.Runtime.Records, first.GroupClient,
		majority.target, majority.descriptor, "reference-majority-before-growth", publicationIndex,
		"confirm-majority-before-growth", claim); err == nil {
		t.Fatal("historical local confirmation was accepted after Region generation changed")
	}
	// The leader's background pass must refresh the current-generation
	// confirmation after sealed-cut physical fill, without a client retry.
	for {
		fresh, err := reader.ReadLocal(ctx, flow.target, flow.key)
		if err == nil {
			if !bytes.Equal(fresh.Body, flow.body) || fresh.RegionGeneration != 2 {
				t.Fatal("automatic reconfirmation returned wrong data or Region", fresh)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("managed Node did not refresh local confirmation after growth", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
	refreshed, err := writer.WriteLocal(ctx, writeRequest)
	if err != nil || refreshed.Reference.Index != completed.Reference.Index ||
		refreshed.RegionGeneration != 2 || refreshed.ConfirmationIndex <= completed.ConfirmationIndex {
		t.Fatal("original RequestID did not refresh local confirmation after Region growth", refreshed, err)
	}
	readAfterGrowth, err := reader.ReadLocal(ctx, flow.target, flow.key)
	if err != nil || !bytes.Equal(readAfterGrowth.Body, flow.body) || readAfterGrowth.RegionGeneration != 2 {
		t.Fatal("read gate did not recover after current-generation confirmation", readAfterGrowth, err)
	}
	read(3)
	verifyNewNode := func() {
		t.Helper()
		state, found := nodes[3].process.Runtime.Engines.Resolve(group)
		if !found {
			t.Fatal("new Node did not open the regional data Group")
		}
		ready, cancel := context.WithTimeout(ctx, 12*time.Second)
		defer cancel()
		for {
			_, err := state.ReadRouted(ready, nodes[3].process.Runtime.Host, refs[0].target, func(view enginestate.ReadView) error {
				for _, expected := range refs {
					ref, found, err := view.RecordReference(expected.key)
					if err != nil || !found || !reflect.DeepEqual(ref.Descriptor, expected.descriptor) {
						return fmt.Errorf("new Node lacks replicated immutable reference %q: found=%t error=%v", expected.key, found, err)
					}
				}
				return nil
			})
			if err == nil {
				break
			}
			select {
			case <-ready.Done():
				t.Fatal("new Node did not resume quorum reads", err)
			case <-time.After(25 * time.Millisecond):
			}
		}
		for _, ref := range refs {
			if copied, _, err := nodes[3].process.Runtime.Records.Read(ctx, ref.descriptor); err != nil || !bytes.Equal(copied, ref.body) {
				t.Fatal("new Node did not fetch referenced Pack bytes", string(ref.key), err)
			}
		}
	}
	verifyNewNode()
	nodes[3].stop()
	restartedFourth, err := openServerWithMux([]string{"--store", nodes[3].store, "--listen", nodes[3].address}, io.Discard, mux)
	if err != nil {
		t.Fatal(err)
	}
	nodes[3].process = restartedFourth
	nodes[3].stop, wait = startJoinTestServer(t, restartedFourth)
	wait()
	verifyNewNode()
	// A current member that restarts after a reference was published must
	// rebuild its debt from Owner state. Take the original physical source
	// offline only after a second old Node has copied the record, so the
	// restarted member must fall back within the unchanged four-Node cohort.
	nodes[3].stop()
	for {
		progress, ready, err := first.Runtime.Root.RegionProgress(group)
		if err != nil {
			t.Fatal(err)
		}
		if ready && progress.Eligible && progress.Pending == nil && progress.Head.PendingGeneration == 0 && progress.Current.Generation == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("Region growth did not activate before recovery reference", progress, ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	// Root confirmation and Owner activation are separate durable steps. A
	// reference mutation before activation is correctly rejected by the Owner
	// write fence, so wait for the latter before publishing the next value.
	for {
		owner, found := first.Runtime.Engines.Resolve(group)
		if !found {
			t.Fatal("regional Owner disappeared after growth")
		}
		if _, err := first.Runtime.Host.ReadIndex(ctx, group); err != nil {
			t.Fatal(err)
		}
		view, err := owner.RegionFence()
		if err != nil {
			t.Fatal(err)
		}
		if view.Active && view.Handoff != nil && view.Handoff.Activation != nil && view.Record.Fence.Generation == 2 {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("confirmed Region growth did not activate its Owner write fence", ctx.Err())
		case <-time.After(25 * time.Millisecond):
		}
	}
	recovery := immutableRef{key: []byte("immutable-source-failover"),
		body: []byte("recover-after-source-and-destination-restart"), source: 0}
	recovery.target, err = routeResolver.ResolveTarget(ctx, space, recovery.key)
	if err != nil {
		t.Fatal(err)
	}
	recovery.descriptor, err = records.Describe(space, group, recovery.key, recovery.body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Runtime.Records.Put(ctx, recovery.descriptor, recovery.body); err != nil {
		t.Fatal("store sole initial recovery source", err)
	}
	publishReference(&recovery, "reference-source-failover")
	for {
		if _, err := nodes[1].process.Runtime.Records.Verify(ctx, recovery.descriptor); err == nil {
			break
		} else if !errors.Is(err, records.ErrNotHeld) {
			t.Fatal("second old Node could not verify repair", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("second old Node did not create a fallback source", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	// Keep both other old members online for Root/Owner quorum, but make their
	// physical Stores unavailable. The restarted destination must finish an
	// actual failed background sweep without claiming this missing record.
	if err := nodes[1].process.Runtime.Records.Close(); err != nil {
		t.Fatal("close first recovery source Store", err)
	}
	if err := nodes[2].process.Runtime.Records.Close(); err != nil {
		t.Fatal("close second recovery source Store", err)
	}
	nodes[0].stop()
	reports := &repairReportBuffer{}
	interruptedFourth, err := openServerWithMux([]string{"--store", nodes[3].store, "--listen", nodes[3].address}, reports, mux)
	if err != nil {
		t.Fatal("restart destination with unavailable physical sources", err)
	}
	nodes[3].process = interruptedFourth
	nodes[3].stop, wait = startJoinTestServer(t, interruptedFourth)
	wait()
	for !strings.Contains(reports.String(), "reference-source-failover") {
		select {
		case <-ctx.Done():
			t.Fatal("background repair did not report its unresolved reference", reports.String(), ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	if _, err := interruptedFourth.Runtime.Records.Verify(ctx, recovery.descriptor); !errors.Is(err, records.ErrNotHeld) {
		t.Fatal("failed repair sweep fabricated destination bytes", err)
	}
	nodes[3].stop()
	for _, i := range []int{1, 2} {
		nodes[i].stop()
		restored, err := openServerWithMux([]string{"--store", nodes[i].store, "--listen", nodes[i].address}, io.Discard, mux)
		if err != nil {
			t.Fatal("restore failed recovery source", i, err)
		}
		nodes[i].process = restored
		nodes[i].stop, wait = startJoinTestServer(t, restored)
		wait()
	}
	recoveredFourth, err := openServerWithMux([]string{"--store", nodes[3].store, "--listen", nodes[3].address}, io.Discard, mux)
	if err != nil {
		t.Fatal("restart repair destination after source outage", err)
	}
	nodes[3].process = recoveredFourth
	nodes[3].stop, wait = startJoinTestServer(t, recoveredFourth)
	wait()
	for {
		if _, err := recoveredFourth.Runtime.Records.Verify(ctx, recovery.descriptor); err == nil {
			break
		} else if !errors.Is(err, records.ErrNotHeld) {
			t.Fatal("restarted Node could not verify fallback repair", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("restarted Node did not automatically repair from surviving source", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	for {
		if _, err := nodes[2].process.Runtime.Records.Verify(ctx, recovery.descriptor); err == nil {
			break
		} else if !errors.Is(err, records.ErrNotHeld) {
			t.Fatal("second old member could not verify repair", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("second old member did not restore the physical majority", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
	recoveryOwner, found := recoveredFourth.Runtime.Engines.Resolve(group)
	if !found {
		t.Fatal("restarted repair destination lacks Owner Group")
	}
	recoveryReader := recordwrite.Reader{Barrier: recoveredFourth.Runtime.Host, Root: recoveredFourth.Runtime.Root,
		Owner: recoveryOwner, LocalID: recoveredFourth.Runtime.Identity.RaftID,
		LocalNodeID: recoveredFourth.Runtime.Identity.NodeID, Store: recoveredFourth.Runtime.Records,
		Observer: recoveredFourth.GroupClient, Fetcher: recoveredFourth.GroupClient}
	for {
		ready, err := recoveryReader.ReadLocal(ctx, recovery.target, recovery.key)
		if err == nil {
			if !bytes.Equal(ready.Body, recovery.body) || ready.RegionGeneration != 2 {
				t.Fatal("recovered local read differs from immutable source", ready)
			}
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("surviving three-Node majority did not reconfirm repaired data", err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}
