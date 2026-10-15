package raftrpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/groupmembership"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type guardedMembershipBackend struct {
	*membershipBackend
	root         *rootstate.State
	onRead       func(context.Context, string) error
	beforeChange func()
}

func (b *guardedMembershipBackend) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if b.onRead != nil {
		if err := b.onRead(ctx, group); err != nil {
			return 0, err
		}
	}
	if group == raft.RootGroupID {
		return b.root.AppliedIndex(), nil
	}
	cut, err := b.MembershipCut(group)
	return cut.Index, err
}
func (b *guardedMembershipBackend) ChangeMembership(group string, cut raft.MembershipCut, id uint64, op raftpb.ConfChangeType) error {
	if b.beforeChange != nil {
		b.beforeChange()
	}
	return b.membershipBackend.ChangeMembership(group, cut, id, op)
}
func membershipFenceFixture(t *testing.T) (*MembershipServer, *guardedMembershipBackend, context.Context, *raftwire.GroupMembershipChangeRequest) {
	t.Helper()
	observation, root, _, ctx, _ := regionObservationFixture(t)
	index := root.AppliedIndex()
	members := []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}
	for _, cmd := range []rootstate.Command{
		{Version: 1, RequestID: "publish", Type: rootstate.PublishGroupMembers, Membership: &rootstate.GroupMembership{GroupID: "data-1", ExpectedEpoch: 1, Members: members}},
		{Version: 1, RequestID: "plan", Type: rootstate.PlanGroupMembers, GroupPlan: &rootstate.GroupChangePlan{ChangeID: "add-node", GroupID: "data-1", ExpectedEpoch: 2, Members: append(members, rootstate.GroupMember{RaftID: 991, Role: rootstate.GroupMemberLearner})}},
	} {
		if r := applyRootCommand(t, root, &index, cmd); r.Error != "" {
			t.Fatal(r)
		}
	}
	shared := &sharedMembership{conf: &raftpb.ConfState{Voters: []uint64{1, 2, 3}}, leader: 1, index: 1, term: 1, prepared: map[uint64]bool{991: true}}
	b := &guardedMembershipBackend{membershipBackend: &membershipBackend{local: 1, shared: shared}, root: root}
	s, err := NewMembershipServer("cluster", 1, b, &membershipJoiner{local: 1, shared: shared}, root, observation.authorize)
	if err != nil {
		t.Fatal(err)
	}
	conf, err := proto.Marshal(shared.conf)
	if err != nil {
		t.Fatal(err)
	}
	r := &raftwire.GroupMembershipChangeRequest{ClusterId: "cluster", GroupId: "data-1", FromRaftId: 991, ToRaftId: 1, Operation: raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_ADD_LEARNER, MemberRaftId: 991, ChangeId: "add-node", ExpectedEpoch: 2, ExpectedMembershipIndex: 1, ExpectedConfState: conf}
	return s, b, ctx, r
}
func removeMutationCaller(t *testing.T, root *rootstate.State) {
	t.Helper()
	if err := root.ApplyMembership(root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
}

func TestMembershipFenceRejectsUnboundAndStaleChanges(t *testing.T) {
	for _, mode := range []string{"normal", "no-tls", "old-request", "wrong-task", "wrong-epoch", "root-group", "wrong-operation", "not-desired", "promote-undesired-role", "stale-index", "wrong-conf", "oversized", "root-unavailable", "removed-before", "removed-after-wait", "expired-after-wait", "membership-aba", "completed-task", "removed-after-apply"} {
		t.Run(mode, func(t *testing.T) {
			s, b, ctx, r := membershipFenceFixture(t)
			want := codes.OK
			switch mode {
			case "no-tls":
				ctx = context.Background()
				want = codes.Unauthenticated
			case "old-request":
				r.ChangeId = ""
				r.ExpectedMembershipIndex = 0
				want = codes.InvalidArgument
			case "wrong-task":
				r.ChangeId = "old"
				want = codes.PermissionDenied
			case "wrong-epoch":
				r.ExpectedEpoch++
				want = codes.PermissionDenied
			case "root-group":
				r.GroupId = raft.RootGroupID
				want = codes.InvalidArgument
			case "wrong-operation":
				r.Operation = raftwire.GroupMembershipOperation(99)
				want = codes.InvalidArgument
			case "not-desired":
				r.MemberRaftId = 66
				want = codes.PermissionDenied
			case "promote-undesired-role":
				r.Operation = raftwire.GroupMembershipOperation_GROUP_MEMBERSHIP_OPERATION_PROMOTE_LEARNER
				want = codes.PermissionDenied
			case "stale-index":
				r.ExpectedMembershipIndex++
				want = codes.FailedPrecondition
			case "wrong-conf":
				r.ExpectedConfState, _ = proto.Marshal(&raftpb.ConfState{Voters: []uint64{1}})
				want = codes.FailedPrecondition
			case "oversized":
				r.ChangeId = strings.Repeat("x", membershipMessageBytes)
				want = codes.ResourceExhausted
			case "root-unavailable":
				b.onRead = func(context.Context, string) error { return errors.New("no quorum") }
				want = codes.Unavailable
			case "removed-before":
				removeMutationCaller(t, b.root)
				want = codes.PermissionDenied
			case "removed-after-wait":
				b.onRead = func(context.Context, string) error { removeMutationCaller(t, b.root); return nil }
				want = codes.PermissionDenied
			case "expired-after-wait":
				expired := false
				s.authorize = func(context.Context, uint64) error {
					if expired {
						return errors.New("expired")
					}
					return nil
				}
				b.onRead = func(context.Context, string) error { expired = true; return nil }
				want = codes.PermissionDenied
			case "membership-aba":
				b.beforeChange = func() { b.shared.mu.Lock(); b.shared.index += 2; b.shared.mu.Unlock() }
				want = codes.FailedPrecondition
			case "completed-task":
				index := b.root.AppliedIndex()
				result := applyRootCommand(t, b.root, &index, rootstate.Command{Version: 1, RequestID: "complete", Type: rootstate.CompleteGroupMembers, GroupCompletion: &rootstate.GroupChangeCompletion{GroupID: "data-1", ChangeID: "add-node"}})
				if result.Error != "" {
					t.Fatal(result)
				}
				want = codes.PermissionDenied
			case "removed-after-apply":
				b.onRead = func(_ context.Context, group string) error {
					if group == "data-1" {
						removeMutationCaller(t, b.root)
					}
					return nil
				}
				want = codes.PermissionDenied
			}
			response, err := s.Change(ctx, r)
			if status.Code(err) != want || (response != nil) != (want == codes.OK) {
				t.Fatal(response, err, want)
			}
			cut, _ := b.MembershipCut("data-1")
			changed := len(cut.Conf.Learners) > 0
			if changed != (mode == "normal" || mode == "removed-after-apply") {
				t.Fatal("rejected mutation changed state", mode, cut)
			}
			if len(s.changes) != 0 {
				t.Fatal("mutation admission leaked")
			}
		})
	}
}

func TestMembershipFenceLocalControlRechecksAfterDataWait(t *testing.T) {
	for _, mode := range []string{"normal", "removed", "completed", "aba"} {
		t.Run(mode, func(t *testing.T) {
			s, b, _, _ := membershipFenceFixture(t)
			control, err := groupmembership.NewLocalControl(b, s.joiner, b.root, 1)
			if err != nil {
				t.Fatal(err)
			}
			mutation := rootstate.GroupMutation{GroupID: "data-1", ChangeID: "add-node", ExpectedEpoch: 2, Member: 991, Operation: raftpb.ConfChangeType_ConfChangeAddLearnerNode}
			b.onRead = func(_ context.Context, group string) error {
				if group != "data-1" {
					return nil
				}
				if mode == "removed" {
					if err := b.root.ApplyMembership(b.root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{2, 3}, Learners: []uint64{991}}); err != nil {
						t.Fatal(err)
					}
				}
				if mode == "completed" {
					index := b.root.AppliedIndex()
					r := applyRootCommand(t, b.root, &index, rootstate.Command{Version: 1, RequestID: "complete", Type: rootstate.CompleteGroupMembers, GroupCompletion: &rootstate.GroupChangeCompletion{GroupID: "data-1", ChangeID: "add-node"}})
					if r.Error != "" {
						t.Fatal(r)
					}
				}
				return nil
			}
			if mode == "aba" {
				b.beforeChange = func() { b.shared.mu.Lock(); b.shared.index += 2; b.shared.mu.Unlock() }
			}
			err = control.ChangeMembership(context.Background(), mutation)
			if (err == nil) != (mode == "normal") {
				t.Fatal(mode, err)
			}
			cut, _ := b.MembershipCut("data-1")
			if (len(cut.Conf.Learners) > 0) != (mode == "normal") {
				t.Fatal(cut)
			}
		})
	}
}

func TestMembershipFenceAdmissionCancellationAndResponseBinding(t *testing.T) {
	s, b, ctx, r := membershipFenceFixture(t)
	entered := make(chan struct{}, 8)
	b.onRead = func(ctx context.Context, _ string) error { entered <- struct{}{}; <-ctx.Done(); return ctx.Err() }
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, 8)
	for range 8 {
		go func() { _, err := s.Change(ctx, r); results <- err }()
	}
	for range 8 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("mutation did not enter")
		}
	}
	if _, err := s.Change(ctx, r); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	cancel()
	for range 8 {
		select {
		case err := <-results:
			if status.Code(err) != codes.Canceled {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("mutation did not cancel")
		}
	}
	if len(s.changes) != 0 {
		t.Fatal("slots leaked")
	}
	response := &raftwire.GroupMembershipResponse{ClusterId: "cluster", GroupId: "data-1", RaftId: 1, LeaderRaftId: 1, MembershipIndex: 1, ConfirmedIndex: 1, LeaderTerm: 1, ConfState: r.ExpectedConfState}
	if _, err := decodeMembershipResponse("cluster", 1, "data-1", response); err != nil {
		t.Fatal(err)
	}
	for _, change := range []func(*raftwire.GroupMembershipResponse){func(r *raftwire.GroupMembershipResponse) { r.ClusterId = "foreign" }, func(r *raftwire.GroupMembershipResponse) { r.GroupId = "wrong" }, func(r *raftwire.GroupMembershipResponse) { r.RaftId = 2 }, func(r *raftwire.GroupMembershipResponse) { r.MembershipIndex = 0 }, func(r *raftwire.GroupMembershipResponse) { r.ConfirmedIndex = 0 }, func(r *raftwire.GroupMembershipResponse) { r.ConfState = []byte{255} }, func(r *raftwire.GroupMembershipResponse) { r.ConfState = make([]byte, membershipConfBytes+1) }} {
		bad := proto.Clone(response).(*raftwire.GroupMembershipResponse)
		change(bad)
		if _, err := decodeMembershipResponse("cluster", 1, "data-1", bad); err == nil {
			t.Fatal("unbound response accepted")
		}
	}
}

func TestMembershipFenceMutualTLSReuseAfterRemoval(t *testing.T) {
	s, b, _, request := membershipFenceFixture(t)
	pool, certificates := testCertificates(t, []uint64{1, 991})
	s.authorize = func(ctx context.Context, id uint64) error {
		cert, err := identity.VerifiedPeerCertificate(ctx, time.Now())
		if err != nil {
			return err
		}
		if cert.Subject.CommonName != fmt.Sprintf("raft-%d", id) {
			return errors.New("wrong sender")
		}
		return nil
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[1]}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
	t.Cleanup(server.Stop)
	raftwire.RegisterGroupMembershipServiceServer(server, s)
	go server.Serve(listener)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[991]}, RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := raftwire.NewGroupMembershipServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := client.Change(ctx, request); err != nil {
		t.Fatal(err)
	}
	removeMutationCaller(t, b.root)
	cut, _ := b.MembershipCut("data-1")
	request.ExpectedMembershipIndex = cut.Index
	request.ExpectedConfState, _ = proto.Marshal(cut.Conf)
	if _, err := client.Change(ctx, request); status.Code(err) != codes.PermissionDenied {
		t.Fatal("removed caller reused connection", err)
	}
	after, _ := b.MembershipCut("data-1")
	if after.Index != cut.Index {
		t.Fatal("revoked call mutated state")
	}
}
