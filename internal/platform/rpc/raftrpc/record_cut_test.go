package raftrpc

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

const testCutClusterID = "cluster_00000000000000000000000000000001"

func recordCutArtifacts(t *testing.T) *snapshot.Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "artifacts")
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := snapshot.OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

func recordCutFixture(t *testing.T) (enginestate.RecordCut, []byte) {
	t.Helper()
	s, _, state, _, _ := regionObservationFixture(t)
	sealObservationState(t, state)
	seal, found, err := state.RegionHandoff()
	if err != nil || !found {
		t.Fatal(seal, found, err)
	}
	artifacts := recordCutArtifacts(t)
	p, err := state.PrepareRecordCut(context.Background(), s.host, seal)
	if err != nil {
		t.Fatal(err)
	}
	cut, err := p.Build(context.Background(), artifacts)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	reader, err := artifacts.Open(cut.Artifact)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	return cut, encoded
}

type cutProxy struct {
	raftwire.UnimplementedRecordCutServiceServer
	cut   enginestate.RecordCut
	body  []byte
	mode  string
	calls atomic.Int32
}

func (p *cutProxy) Get(req *raftwire.GetRecordCutRequest, stream raftwire.RecordCutService_GetServer) error {
	p.calls.Add(1)
	seal, err := decodeRecordCutSeal(req.SealRecord)
	if err != nil || req.FromRaftId != 991 || req.ToRaftId != 1 || req.GroupId != "data-1" || seal.SealRequestID != p.cut.Seal.SealRequestID {
		return status.Error(codes.InvalidArgument, "unexpected record cut request")
	}
	descriptor, _ := json.Marshal(p.cut)
	header := &raftwire.RecordCutHeader{ClusterId: testCutClusterID, RaftId: 1, NodeId: fmt.Sprintf("node_%032x", 1), RootConfirmedIndex: 51, CutDescriptor: descriptor}
	if p.mode == "wrong-header" {
		header.NodeId = fmt.Sprintf("node_%032x", 2)
	}
	if err := stream.Send(&raftwire.GetRecordCutResponse{Payload: &raftwire.GetRecordCutResponse_Header{Header: header}}); err != nil {
		return err
	}
	if p.mode == "wrong-header" {
		return nil
	}
	body := p.body
	if p.mode == "truncated" {
		body = body[:len(body)/2]
	}
	if p.mode == "changed-footer" {
		body = bytes.Clone(body)
		body[len(body)-1] ^= 1
	}
	for offset := 0; offset < len(body); offset += 53 {
		end := min(offset+53, len(body))
		if err := stream.Send(&raftwire.GetRecordCutResponse{Payload: &raftwire.GetRecordCutResponse_Data{Data: body[offset:end]}}); err != nil {
			return err
		}
	}
	if p.mode == "final-error" {
		return status.Error(codes.PermissionDenied, "caller removed before completion")
	}
	return nil
}

func TestFetchRecordCutAuthenticatedStreamingAndIncompleteReject(t *testing.T) {
	cut, body := recordCutFixture(t)
	for _, mode := range []string{"good", "wrong-header", "truncated", "changed-footer", "final-error", "wrong-cert"} {
		t.Run(mode, func(t *testing.T) {
			ids := []identity.Identity{
				{Version: 1, ClusterID: testCutClusterID, NodeID: fmt.Sprintf("node_%032x", 1), RaftID: 1, CreatedAt: time.Now()},
				{Version: 1, ClusterID: testCutClusterID, NodeID: fmt.Sprintf("node_%032x", 2), RaftID: 2, CreatedAt: time.Now()},
				{Version: 1, ClusterID: testCutClusterID, NodeID: fmt.Sprintf("node_%032x", 991), RaftID: 991, CreatedAt: time.Now()},
			}
			pool, certificates := holdingCertificates(t, ids...)
			certificate := certificates[1]
			if mode == "wrong-cert" {
				certificate = certificates[2]
			}
			server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
			proxy := &cutProxy{cut: cut, body: body, mode: mode}
			raftwire.RegisterRecordCutServiceServer(server, proxy)
			t.Cleanup(server.Stop)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			go func() { _ = server.Serve(listener) }()
			directory := NewStaticDirectory()
			if err := directory.Update(1, listener.Addr().String()); err != nil {
				t.Fatal(err)
			}
			client, err := NewGroupClient(GroupClientOptions{ClusterID: testCutClusterID, RaftID: 991, Directory: directory,
				Credentials: credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[991]}, RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			target := rootstate.Node{NodeID: fmt.Sprintf("node_%032x", 1), RaftID: 1, RegionID: "west"}
			destination := recordCutArtifacts(t)
			got, err := client.FetchRecordCut(ctx, target, cut.Seal, destination)
			if mode == "good" {
				if err != nil || got.RecordCutSummary != cut.RecordCutSummary || enginestate.WalkVerifiedRecordCut(ctx, destination, got, nil) != nil {
					t.Fatal("authenticated cut did not import", got, err)
				}
				return
			}
			if err == nil {
				t.Fatal("incomplete or mismatched cut accepted", mode, got)
			}
			if mode == "wrong-cert" && proxy.calls.Load() != 0 {
				t.Fatal("wrong TLS Node received an application request")
			}
			if _, err := destination.Open(cut.Artifact); err == nil {
				t.Fatal("failed transfer published a complete cut")
			}
		})
	}
}

func TestRecordCutSealDecodeRejectsNoncanonicalAndInvalid(t *testing.T) {
	cut, _ := recordCutFixture(t)
	encoded, _ := json.Marshal(cut.Seal)
	if got, err := decodeRecordCutSeal(encoded); err != nil || got.SealRequestID != cut.Seal.SealRequestID {
		t.Fatal(got, err)
	}
	for _, candidate := range [][]byte{nil, append(bytes.Clone(encoded), ' '), append(bytes.Clone(encoded), 'x'), []byte("{}")} {
		if _, err := decodeRecordCutSeal(candidate); err == nil {
			t.Fatal("bad seal accepted", string(candidate))
		}
	}
	activated := cut.Seal
	activated.Activation = &regionfence.Activation{}
	data, _ := json.Marshal(activated)
	if _, err := decodeRecordCutSeal(data); err == nil {
		t.Fatal("activated seal accepted")
	}
}

type cutServerStream struct {
	grpc.ServerStream
	mu     sync.Mutex
	ctx    context.Context
	frames []*raftwire.GetRecordCutResponse
	onSend func(int)
}

func (s *cutServerStream) Context() context.Context { return s.ctx }
func (s *cutServerStream) Send(frame *raftwire.GetRecordCutResponse) error {
	s.mu.Lock()
	s.frames = append(s.frames, frame)
	count := len(s.frames)
	s.mu.Unlock()
	if s.onSend != nil {
		s.onSend(count)
	}
	return nil
}

func recordCutServerFixture(t *testing.T) (*RecordCutServer, *rootstate.State, regionfence.Handoff, context.Context) {
	t.Helper()
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	index := uint64(0)
	apply := func(name string, command rootstate.Command) {
		command.Version, command.RequestID = rootstate.FormatVersion, name
		if r := applyRootCommand(t, root, &index, command); r.Error != "" {
			t.Fatal(name, r.Error)
		}
	}
	for _, id := range []uint64{1, 2, 3, 991} {
		apply(fmt.Sprintf("register-%d", id), rootstate.Command{Type: rootstate.RegisterNode, Node: &rootstate.Node{
			NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "west", Address: fmt.Sprintf("127.0.0.1:%d", 9000+id),
		}})
	}
	if err := root.ApplyMembership(index+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{991}}); err != nil {
		t.Fatal(err)
	}
	index++
	apply("group", rootstate.Command{Type: rootstate.RegisterGroup, Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateEngine}})
	members := []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}
	apply("members", rootstate.Command{Type: rootstate.PublishGroupMembers, Membership: &rootstate.GroupMembership{GroupID: "data-1", ExpectedEpoch: 1, Members: members}})
	apply("plan-initial", rootstate.Command{Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{RegionID: "west", GroupID: "data-1", ExpectedGeneration: 0, Nodes: []uint64{1, 2, 3}}})
	initial, found, err := root.RegionCohort("west", 1)
	if err != nil || !found {
		t.Fatal(initial, found, err)
	}
	state, err := enginestate.Open(t.TempDir(), "data-1", groupRPCHandler{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	if err := state.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	descriptor := regionfence.Descriptor{RegionID: "west", GroupID: "data-1", Generation: 1, PlannedIndex: initial.PlannedIndex,
		GroupEpoch: 2, MembershipIndex: 1, Members: members}
	if err := state.ApplyRegionFence(2, "fence", descriptor); err != nil {
		t.Fatal(err)
	}
	apply("confirm-initial", rootstate.Command{Type: rootstate.ConfirmRegionCohort, RegionConfirmation: &rootstate.RegionCohortConfirmation{
		RegionID: "west", GroupID: "data-1", Generation: 1, GroupEpoch: 2, GroupFenceRequestID: "fence", GroupFenceIndex: 2,
	}})
	apply("plan-growth", rootstate.Command{Type: rootstate.PlanRegionCohort, RegionPlan: &rootstate.RegionCohortPlan{RegionID: "west", GroupID: "data-1", ExpectedGeneration: 1, Nodes: []uint64{1, 2, 3, 991}}})
	pending, found, err := root.RegionCohort("west", 2)
	if err != nil || !found {
		t.Fatal(pending, found, err)
	}
	view, err := state.RegionFence()
	if err != nil {
		t.Fatal(err)
	}
	intent := regionfence.HandoffIntent{Previous: view.Record, Next: pending}
	if err := state.ApplyRegionHandoff(3, "seal", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &intent}); err != nil {
		t.Fatal(err)
	}
	seal, found, err := state.RegionHandoff()
	if err != nil || !found {
		t.Fatal(seal, found, err)
	}
	host := regionBarrierFunc(func(ctx context.Context, group string) (uint64, error) {
		if group == raft.RootGroupID {
			return root.AppliedIndex(), ctx.Err()
		}
		return state.AppliedIndex(), ctx.Err()
	})
	local := identity.Identity{Version: 1, ClusterID: testCutClusterID, NodeID: fmt.Sprintf("node_%032x", 1), RaftID: 1, CreatedAt: time.Now()}
	caller := identity.Identity{Version: 1, ClusterID: testCutClusterID, NodeID: fmt.Sprintf("node_%032x", 991), RaftID: 991, CreatedAt: time.Now()}
	pool, certs := holdingCertificates(t, local, caller)
	authorize, err := identity.PeerAuthorizer(testCutClusterID)
	if err != nil {
		t.Fatal(err)
	}
	auth := &RecordHoldingServer{local: local, host: host, root: root, authorize: authorize}
	service, err := NewRecordCutServer(auth, func(group string) (*enginestate.State, bool) { return state, group == "data-1" }, recordCutArtifacts(t))
	if err != nil {
		t.Fatal(err)
	}
	return service, root, seal, holdingPeerContext(t, pool, certs[caller.RaftID])
}

func TestRecordCutServerBuildsFreshAuthorizedStreamAndFencesRemoval(t *testing.T) {
	service, root, seal, ctx := recordCutServerFixture(t)
	encoded, _ := json.Marshal(seal)
	request := &raftwire.GetRecordCutRequest{ClusterId: testCutClusterID, GroupId: "data-1", FromRaftId: 991, ToRaftId: 1, SealRecord: encoded}
	stream := &cutServerStream{ctx: ctx}
	if err := service.Get(request, stream); err != nil || len(stream.frames) < 2 {
		t.Fatal("valid sealed cut was not streamed", err, len(stream.frames))
	}
	var cut enginestate.RecordCut
	if err := json.Unmarshal(stream.frames[0].GetHeader().CutDescriptor, &cut); err != nil || cut.Count != 0 || cut.Seal.SealRequestID != seal.SealRequestID {
		t.Fatal("wrong cut header", cut, err)
	}
	var body []byte
	for _, frame := range stream.frames[1:] {
		body = append(body, frame.GetData()...)
	}
	destination := recordCutArtifacts(t)
	if err := destination.Import(context.Background(), cut.Artifact, bytes.NewReader(body)); err != nil || enginestate.WalkVerifiedRecordCut(context.Background(), destination, cut, nil) != nil {
		t.Fatal("server streamed an invalid cut", err)
	}
	stream = &cutServerStream{ctx: ctx, onSend: func(n int) {
		if n == 1 {
			if err := root.ApplyMembership(root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
				t.Fatal(err)
			}
		}
	}}
	if err := service.Get(request, stream); status.Code(err) != codes.PermissionDenied {
		t.Fatal("removed caller got complete cut", err)
	}
}

func TestRecordCutSendTimeoutRetainsActualWorkerBudget(t *testing.T) {
	service := &RecordCutServer{writers: make(chan struct{}, 2)}
	blocked := make(chan struct{})
	stream := &cutServerStream{ctx: context.Background(), onSend: func(int) { <-blocked }}
	frame := &raftwire.GetRecordCutResponse{Payload: &raftwire.GetRecordCutResponse_Data{Data: []byte("x")}}
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		err := service.send(ctx, stream, frame)
		cancel()
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatal("stalled send did not time out", err)
		}
	}
	if err := service.send(context.Background(), stream, frame); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("stalled worker budget was released early", err)
	}
	close(blocked)
	deadline := time.After(time.Second)
	for len(service.writers) != 0 {
		select {
		case <-deadline:
			t.Fatal("send workers did not exit")
		case <-time.After(time.Millisecond):
		}
	}
}
