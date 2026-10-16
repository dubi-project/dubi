package raftrpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
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

type regionBarrierFunc func(context.Context, string) (uint64, error)

func (f regionBarrierFunc) ReadIndex(ctx context.Context, group string) (uint64, error) {
	return f(ctx, group)
}

func regionObservationFixture(t *testing.T) (*RegionFenceServer, *rootstate.State, *enginestate.State, context.Context, *raftwire.ObserveRegionFenceRequest) {
	t.Helper()
	root, err := rootstate.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Close() })
	index := uint64(0)
	for _, id := range []uint64{1, 2, 3, 991} {
		r := applyRootCommand(t, root, &index, rootstate.Command{Version: 1, RequestID: fmt.Sprintf("node-%d", id), Type: rootstate.RegisterNode,
			Node: &rootstate.Node{NodeID: fmt.Sprintf("node_%032x", id), RaftID: id, RegionID: "west", Address: fmt.Sprintf("127.0.0.1:%d", 9000+id)}})
		if r.Error != "" {
			t.Fatal(r)
		}
	}
	if err := root.ApplyMembership(index+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}, Learners: []uint64{991}}); err != nil {
		t.Fatal(err)
	}
	index++
	r := applyRootCommand(t, root, &index, rootstate.Command{Version: 1, RequestID: "group", Type: rootstate.RegisterGroup, Group: &rootstate.Group{GroupID: "data-1", StateKind: rootstate.GroupStateEngine}})
	if r.Error != "" {
		t.Fatal(r)
	}
	state, err := enginestate.Open(t.TempDir(), "data-1", groupRPCHandler{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { state.Close() })
	if err := state.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	d := regionfence.Descriptor{RegionID: "west", GroupID: "data-1", Generation: 1, PlannedIndex: 50, GroupEpoch: 2, MembershipIndex: 1,
		Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}, {RaftID: 2, Role: rootstate.GroupMemberVoter}, {RaftID: 3, Role: rootstate.GroupMemberVoter}}}
	if err := state.ApplyRegionFence(2, "fence", d); err != nil {
		t.Fatal(err)
	}
	host := regionBarrierFunc(func(ctx context.Context, group string) (uint64, error) {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if group == raft.RootGroupID {
			return root.AppliedIndex(), nil
		}
		return state.AppliedIndex(), nil
	})
	s, err := NewRegionFenceServer("cluster", 1, host, root, func(group string) (*enginestate.State, bool) { return state, group == "data-1" }, func(_ context.Context, id uint64) error {
		if id != 991 {
			return errors.New("wrong certificate identity")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, root, state, snapshotStream(nil).ctx, &raftwire.ObserveRegionFenceRequest{ClusterId: "cluster", GroupId: "data-1", FromRaftId: 991, ToRaftId: 1}
}

func TestRegionObservationChecksQuorumsMembershipAndFreshAuthorization(t *testing.T) {
	for _, scenario := range []string{"normal", "no-tls", "wrong-sender", "wrong-target", "wrong-cluster", "root-group", "oversized", "unregistered", "not-hosted", "no-root-quorum", "caller-removed", "local-removed", "removed-after-read", "expired-after-read", "membership-changed", "state-closed", "unapplied-barrier"} {
		t.Run(scenario, func(t *testing.T) {
			s, root, state, ctx, request := regionObservationFixture(t)
			expect := codes.OK
			original := s.host
			switch scenario {
			case "no-tls":
				ctx = context.Background()
				expect = codes.Unauthenticated
			case "wrong-sender":
				request.FromRaftId = 992
				expect = codes.PermissionDenied
			case "wrong-target":
				request.ToRaftId = 2
				expect = codes.PermissionDenied
			case "wrong-cluster":
				request.ClusterId = "foreign"
				expect = codes.PermissionDenied
			case "root-group":
				request.GroupId = "root"
				expect = codes.InvalidArgument
			case "oversized":
				request.GroupId = strings.Repeat("a", regionObserveRequestBytes)
				expect = codes.ResourceExhausted
			case "unregistered":
				request.GroupId = "unknown"
				expect = codes.FailedPrecondition
			case "not-hosted":
				s.states = func(string) (*enginestate.State, bool) { return nil, false }
				expect = codes.NotFound
			case "no-root-quorum":
				s.host = regionBarrierFunc(func(context.Context, string) (uint64, error) { return 0, errors.New("quorum gone") })
				expect = codes.Unavailable
			case "caller-removed", "local-removed":
				conf := &raftpb.ConfState{Voters: []uint64{1, 2, 3}}
				if scenario == "local-removed" {
					conf = &raftpb.ConfState{Voters: []uint64{2, 3}, Learners: []uint64{991}}
				}
				if err := root.ApplyMembership(root.AppliedIndex()+1, conf); err != nil {
					t.Fatal(err)
				}
				expect = codes.PermissionDenied
			case "removed-after-read", "expired-after-read", "membership-changed", "unapplied-barrier":
				expired := false
				if scenario == "expired-after-read" {
					s.authorize = func(context.Context, uint64) error {
						if expired {
							return errors.New("expired")
						}
						return nil
					}
				}
				s.host = regionBarrierFunc(func(ctx context.Context, group string) (uint64, error) {
					if group == "data-1" {
						switch scenario {
						case "removed-after-read":
							if err := root.ApplyMembership(root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
								t.Fatal(err)
							}
						case "expired-after-read":
							expired = true
						case "membership-changed":
							if err := state.ApplyMembership(3, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
								t.Fatal(err)
							}
						case "unapplied-barrier":
							return 99, nil
						}
					}
					return original.ReadIndex(ctx, group)
				})
				if scenario == "unapplied-barrier" {
					expect = codes.FailedPrecondition
				} else if scenario != "membership-changed" {
					expect = codes.PermissionDenied
				}
			case "state-closed":
				state.Close()
				expect = codes.FailedPrecondition
			}
			response, err := s.Observe(ctx, request)
			if status.Code(err) != expect {
				t.Fatal(scenario, response, err)
			}
			if expect != codes.OK && response != nil {
				t.Fatal("failed observation leaked success", response)
			}
			if expect == codes.OK {
				observed, err := decodeRegionObservation("cluster", 1, "data-1", response)
				if err != nil || !observed.View.Found || observed.View.Active != (scenario != "membership-changed") {
					t.Fatal(observed, err)
				}
			}
			if len(s.slots) != 0 {
				t.Fatal("slot leaked")
			}
		})
	}
}

func TestRegionObservationAdmissionTimeoutAndCancellation(t *testing.T) {
	s, _, _, ctx, request := regionObservationFixture(t)
	entered := make(chan struct{}, regionObserveConcurrency)
	original := s.host
	s.host = regionBarrierFunc(func(ctx context.Context, _ string) (uint64, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return 0, ctx.Err()
	})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, regionObserveConcurrency)
	for n := 0; n < regionObserveConcurrency; n++ {
		go func() { _, err := s.Observe(ctx, request); done <- err }()
	}
	for n := 0; n < regionObserveConcurrency; n++ {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("admission did not enter")
		}
	}
	if _, err := s.Observe(ctx, request); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	cancel()
	for n := 0; n < regionObserveConcurrency; n++ {
		if err := <-done; status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
	}
	if len(s.slots) != 0 {
		t.Fatal("canceled requests retained slots")
	}
	s.timeout = 20 * time.Millisecond
	ctx = snapshotStream(nil).ctx
	if _, err := s.Observe(ctx, request); status.Code(err) != codes.DeadlineExceeded {
		t.Fatal(err)
	}
	s.host = original
	if response, err := s.Observe(ctx, request); err != nil || !response.Active {
		t.Fatal(response, err)
	}
}

func TestRegionObservationRejectsMalformedOrMisboundResponses(t *testing.T) {
	s, _, _, ctx, request := regionObservationFixture(t)
	good, err := s.Observe(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	for name, damage := range map[string]func(*raftwire.ObserveRegionFenceResponse){
		"old-format":       func(r *raftwire.ObserveRegionFenceResponse) { r.StateFormat = 4 },
		"missing-format":   func(r *raftwire.ObserveRegionFenceResponse) { r.StateFormat = 0 },
		"cluster":          func(r *raftwire.ObserveRegionFenceResponse) { r.ClusterId = "another" },
		"target":           func(r *raftwire.ObserveRegionFenceResponse) { r.RaftId++ },
		"group":            func(r *raftwire.ObserveRegionFenceResponse) { r.GroupId = "another" },
		"root-barrier":     func(r *raftwire.ObserveRegionFenceResponse) { r.RootConfirmedIndex = 0 },
		"group-barrier":    func(r *raftwire.ObserveRegionFenceResponse) { r.GroupConfirmedIndex = 0 },
		"unapplied":        func(r *raftwire.ObserveRegionFenceResponse) { r.GroupConfirmedIndex = r.AppliedIndex + 1 },
		"membership":       func(r *raftwire.ObserveRegionFenceResponse) { r.MembershipIndex = r.AppliedIndex + 1 },
		"empty-membership": func(r *raftwire.ObserveRegionFenceResponse) { r.ConfState = nil },
		"inactive-lie":     func(r *raftwire.ObserveRegionFenceResponse) { r.Active = false },
		"missing-fence":    func(r *raftwire.ObserveRegionFenceResponse) { r.FenceRecord = nil },
		"trailing-json":    func(r *raftwire.ObserveRegionFenceResponse) { r.FenceRecord = append(r.FenceRecord, []byte(" {}")...) },
		"unknown-json": func(r *raftwire.ObserveRegionFenceResponse) {
			r.FenceRecord = append(r.FenceRecord[:len(r.FenceRecord)-1], []byte(`,"unknown":1}`)...)
		},
		"oversized-record": func(r *raftwire.ObserveRegionFenceResponse) {
			r.FenceRecord = make([]byte, regionfence.MaxRecordBytes+1)
		},
		"wrong-record-group": func(r *raftwire.ObserveRegionFenceResponse) {
			r.FenceRecord = []byte(strings.Replace(string(r.FenceRecord), "data-1", "data-2", 1))
		},
		"joint": func(r *raftwire.ObserveRegionFenceResponse) {
			r.ConfState, _ = proto.Marshal(&raftpb.ConfState{Voters: []uint64{1, 2, 3}, VotersOutgoing: []uint64{1, 2, 3}})
		},
		"foreign-participant": func(r *raftwire.ObserveRegionFenceResponse) {
			r.ConfState, _ = proto.Marshal(&raftpb.ConfState{Voters: []uint64{2, 3, 4}})
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := proto.Clone(good).(*raftwire.ObserveRegionFenceResponse)
			damage(bad)
			if _, err := decodeRegionObservation("cluster", 1, "data-1", bad); err == nil {
				t.Fatal("bad response accepted")
			}
		})
	}
	good.FenceRecord = nil
	good.Active = false
	if observation, err := decodeRegionObservation("cluster", 1, "data-1", good); err != nil || observation.View.Found {
		t.Fatal(observation, err)
	}
}

type regionObserveProxy struct {
	raftwire.UnimplementedRegionFenceServiceServer
	delegate *RegionFenceServer
	corrupt  atomic.Bool
}

func (p *regionObserveProxy) Observe(ctx context.Context, req *raftwire.ObserveRegionFenceRequest) (*raftwire.ObserveRegionFenceResponse, error) {
	r, err := p.delegate.Observe(ctx, req)
	if err == nil && p.corrupt.Load() {
		r.RaftId++
	}
	return r, err
}

func TestRegionObservationOverMutualTLSBindsSenderAndResponder(t *testing.T) {
	s, root, state, _, _ := regionObservationFixture(t)
	sealObservationState(t, state)
	pool, certificates := testCertificates(t, []uint64{1, 991})
	s.authorize = func(ctx context.Context, claimed uint64) error {
		leaf, err := identity.VerifiedPeerCertificate(ctx, time.Now())
		if err != nil {
			return err
		}
		if leaf.Subject.CommonName != fmt.Sprintf("raft-%d", claimed) {
			return errors.New("certificate mismatch")
		}
		return nil
	}
	proxy := &regionObserveProxy{delegate: s}
	rpc := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[1]}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
	t.Cleanup(rpc.Stop)
	raftwire.RegisterRegionFenceServiceServer(rpc, proxy)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() { _ = rpc.Serve(listener) }()
	directory := NewStaticDirectory()
	if err := directory.Update(1, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	clientTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[991]}, RootCAs: pool, ServerName: "localhost", MinVersion: tls.VersionTLS13})
	client, err := NewGroupClient(GroupClientOptions{ClusterID: "cluster", RaftID: 991, Directory: directory, Credentials: clientTLS})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if observed, err := client.ObserveRegionFence(ctx, 1, "data-1"); err != nil || !observed.View.Active || observed.View.Handoff == nil || observed.View.Handoff.Activation != nil {
		t.Fatal(observed, err)
	}
	proxy.corrupt.Store(true)
	if _, err := client.ObserveRegionFence(ctx, 1, "data-1"); err == nil {
		t.Fatal("mismatched responder accepted")
	}
	proxy.corrupt.Store(false)
	spoof, err := NewGroupClient(GroupClientOptions{ClusterID: "cluster", RaftID: 992, Directory: directory, Credentials: clientTLS})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { spoof.Close() })
	if _, err := spoof.ObserveRegionFence(ctx, 1, "data-1"); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	if err := root.ApplyMembership(root.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3}}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ObserveRegionFence(ctx, 1, "data-1"); status.Code(err) != codes.PermissionDenied {
		t.Fatal("removed caller reused TLS connection", err)
	}
}

func sealObservationState(t *testing.T, state *enginestate.State) regionfence.HandoffIntent {
	t.Helper()
	v, err := state.RegionFence()
	if err != nil {
		t.Fatal(err)
	}
	i := regionfence.HandoffIntent{Previous: v.Record, Next: rootstate.RegionCohort{RegionID: "west", GroupID: "data-1", Generation: 2, PlannedIndex: 51, Nodes: []uint64{1, 2, 3, 991}, State: rootstate.RegionCohortPlanned}}
	if err := state.ApplyRegionHandoff(state.AppliedIndex()+1, "seal", regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &i}); err != nil {
		t.Fatal(err)
	}
	return i
}

func TestRegionObservationHandoffBindsOneCutAndRejectsDamage(t *testing.T) {
	for _, phase := range []string{"sealed", "candidate", "activated", "invalidated"} {
		t.Run(phase, func(t *testing.T) {
			s, _, state, ctx, request := regionObservationFixture(t)
			i := sealObservationState(t, state)
			if phase != "sealed" {
				if err := state.ApplyMembership(state.AppliedIndex()+1, &raftpb.ConfState{Voters: []uint64{1, 2, 3, 991}}); err != nil {
					t.Fatal(err)
				}
				d := i.Previous.Fence
				d.Generation = 2
				d.PlannedIndex = 51
				d.GroupEpoch = 3
				d.MembershipIndex = state.AppliedIndex()
				d.Members = append(d.Members, rootstate.GroupMember{RaftID: 991, Role: rootstate.GroupMemberVoter})
				if err := state.ApplyRegionFence(state.AppliedIndex()+1, "candidate", d); err != nil {
					t.Fatal(err)
				}
				if phase != "candidate" {
					v, _ := state.RegionFence()
					confirmed := i.Next
					confirmed.State = rootstate.RegionCohortConfirmed
					confirmed.ConfirmedIndex = 52
					confirmed.GroupEpoch = 3
					confirmed.GroupFenceRequestID = v.Record.RequestID
					confirmed.GroupFenceIndex = v.Record.Index
					if err := state.ApplyRegionHandoff(state.AppliedIndex()+1, "activate", regionfence.HandoffCommand{Action: regionfence.Activate, Activation: &regionfence.Activation{Candidate: v.Record, Confirmed: confirmed}}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if phase == "invalidated" {
				if err := state.ApplyMembership(state.AppliedIndex()+1, state.ConfState()); err != nil {
					t.Fatal(err)
				}
			}
			response, err := s.Observe(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeRegionObservation("cluster", 1, "data-1", response)
			if err != nil {
				t.Fatal(err)
			}
			local, err := state.RegionFence()
			if err != nil || !reflect.DeepEqual(local, decoded.View) || decoded.View.Handoff == nil {
				t.Fatal("inconsistent combined cut", local, decoded.View, err)
			}
			for _, damage := range []string{"oversized", "trailing", "unknown", "orphan", "wrong-generation", "future-seal", "missing-handoff"} {
				if phase == "sealed" && damage == "missing-handoff" {
					continue
				}
				bad := proto.Clone(response).(*raftwire.ObserveRegionFenceResponse)
				switch damage {
				case "oversized":
					bad.HandoffRecord = make([]byte, regionfence.MaxRecordBytes+1)
				case "trailing":
					bad.HandoffRecord = append(bad.HandoffRecord, []byte(" {}")...)
				case "unknown":
					bad.HandoffRecord = append(bad.HandoffRecord[:len(bad.HandoffRecord)-1], []byte(`,"unknown":true}`)...)
				case "orphan":
					bad.FenceRecord = nil
					bad.Active = false
				case "missing-handoff":
					bad.HandoffRecord = nil
				default:
					var h regionfence.Handoff
					json.Unmarshal(bad.HandoffRecord, &h)
					if damage == "wrong-generation" {
						h.Intent.Next.Generation++
					} else {
						h.SealIndex = bad.AppliedIndex + 1
					}
					bad.HandoffRecord, _ = json.Marshal(h)
				}
				if _, err := decodeRegionObservation("cluster", 1, "data-1", bad); err == nil {
					t.Fatal("bad handoff accepted", phase, damage)
				}
			}
		})
	}
}
