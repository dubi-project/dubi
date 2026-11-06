package raftrpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"strings"
	"testing"
	"time"
)

type mirrorBatchBackend struct {
	state             *enginestate.State
	reads, submits    int
	pending, rejected string
	lost              bool
	cut               uint64
}

func (b *mirrorBatchBackend) Submit(g string, data []byte) error {
	b.submits++
	c, e := routedstate.DecodeCommand(data)
	if e != nil {
		return e
	}
	if c.RequestID == b.pending {
		return nil
	}
	if c.RequestID == b.rejected {
		return status.Error(codes.PermissionDenied, "denied")
	}
	return b.state.ApplyGeoMetadataMirror(b.state.AppliedIndex()+1, c.RequestID, *c.GeoMetadata)
}
func (b *mirrorBatchBackend) ReadIndex(context.Context, string) (uint64, error) {
	b.reads++
	if b.lost {
		return 0, errors.New("quorum lost")
	}
	if b.cut != 0 {
		return b.cut, nil
	}
	return b.state.AppliedIndex(), nil
}
func (b *mirrorBatchBackend) Status(string) (raft.GroupStatus, error) { return raft.GroupStatus{}, nil }
func mirrorBatchCommands(t *testing.T, n int) [][]byte {
	t.Helper()
	var out [][]byte
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("item-%02d", i))
		scope := rootstate.Keyspace{TenantID: "tenant-a", EngineID: "record", ModuleID: "items"}
		d, e := records.Describe(scope, "origin", k, []byte("body"))
		if e != nil {
			t.Fatal(e)
		}
		id, _ := d.ID()
		m := routedstate.GeoMetadataMirror{Target: router.Target{Keyspace: scope, RangeID: "range-a", OwnerGroupID: "owner", Epoch: 1, RoutingKey: k}, Key: k, DestinationGroupID: "data-1", RegionID: "west", Generation: 1, GroupEpoch: 2, FenceIndex: 2, RecordID: id, ReferenceRequestID: "publication", ReferenceIndex: 9, ReferenceCommandHash: strings.Repeat("a", 64), Descriptor: d}
		b, e := routedstate.EncodeGeoMetadataMirror(fmt.Sprintf("mirror-%02d", i), m)
		if e != nil {
			t.Fatal(e)
		}
		out = append(out, b)
	}
	return out
}
func mirrorBatchServer(t *testing.T) (*EngineServer, *mirrorBatchBackend, context.Context, *raftwire.EngineMirrorBatchRequest) {
	t.Helper()
	s, e := enginestate.Open(t.TempDir(), "data-1", groupRPCHandler{})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { s.Close() })
	if e = s.ApplyMembership(1, &raftpb.ConfState{Voters: []uint64{1}}); e != nil {
		t.Fatal(e)
	}
	d := regionfence.Descriptor{RegionID: "west", GroupID: "data-1", Generation: 1, PlannedIndex: 1, GroupEpoch: 2, MembershipIndex: 1, Members: []rootstate.GroupMember{{RaftID: 1, Role: rootstate.GroupMemberVoter}}}
	encoded, e := routedstate.EncodeRegionFence("fence", d)
	if e != nil {
		t.Fatal(e)
	}
	a, e := routedstate.New(s)
	if e != nil {
		t.Fatal(e)
	}
	if e = a.Apply(2, encoded); e != nil {
		t.Fatal(e)
	}
	b := &mirrorBatchBackend{state: s}
	server, e := NewEngineServer("cluster", 1, b, func(g string) (*enginestate.State, bool) { return s, g == "data-1" }, func(context.Context, uint64) error { return nil }, 0)
	if e != nil {
		t.Fatal(e)
	}
	cert := &x509.Certificate{}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}}})
	return server, b, ctx, &raftwire.EngineMirrorBatchRequest{ClusterId: "cluster", GroupId: "data-1", FromRaftId: 2, ToRaftId: 1, RoutedCommands: mirrorBatchCommands(t, 8)}
}
func TestMirrorBatchOneCutPartialResultsAndIdempotentReplay(t *testing.T) {
	s, b, ctx, r := mirrorBatchServer(t)
	b.pending = "mirror-03"
	got, e := s.MirrorGeoBatch(ctx, r)
	if e != nil || b.reads != 1 {
		t.Fatal(e, b.reads)
	}
	commands, _ := decodeMirrorBatch(r.GroupId, r.RoutedCommands)
	rows, e := decodeMirrorBatchResults(commands, got)
	if e != nil {
		t.Fatal(e)
	}
	for i, row := range rows {
		if i == 3 {
			if status.Code(row.Err) != codes.Unavailable {
				t.Fatal("missing row was completed", row)
			}
			continue
		}
		if row.Err != nil || !row.Result.Applied {
			t.Fatal(i, row)
		}
	}
	old := rows[0].Result.Index
	b.pending = ""
	got, e = s.MirrorGeoBatch(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	rows, e = decodeMirrorBatchResults(commands, got)
	if e != nil || !rows[3].Result.Applied || rows[0].Result.Index != old || b.reads != 2 {
		t.Fatal(e, rows, b.reads)
	}
	// Exact hash conflicts and durable fence rejections are independent rows.
	c := commands[0]
	c.GeoMetadata.ReferenceIndex++
	r.RoutedCommands[0], _ = routedstate.EncodeGeoMetadataMirror(c.RequestID, *c.GeoMetadata)
	c = commands[1]
	c.GeoMetadata.Generation++
	r.RoutedCommands[1], _ = routedstate.EncodeGeoMetadataMirror("new-stale-fence", *c.GeoMetadata)
	got, e = s.MirrorGeoBatch(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	if got.Items[0].ErrorCode != uint32(codes.FailedPrecondition) || got.Items[1].Result.Applied || got.Items[1].Result.Error == "" || !got.Items[2].Result.Applied {
		t.Fatal(got)
	}
}
func TestMirrorBatchRejectsUntrustedOrOversizedEnvelopeBeforeSubmit(t *testing.T) {
	for _, mode := range []string{"no-tls", "unauthorized", "wrong-cluster", "wrong-destination", "unknown", "count", "bytes", "duplicate", "mutation", "busy"} {
		t.Run(mode, func(t *testing.T) {
			s, b, ctx, r := mirrorBatchServer(t)
			switch mode {
			case "no-tls":
				ctx = context.Background()
			case "unauthorized":
				s.authorize = func(context.Context, uint64) error { return errors.New("fenced") }
			case "wrong-cluster":
				r.ClusterId = "other"
			case "wrong-destination":
				r.GroupId = "elsewhere"
			case "unknown":
				r.ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "count":
				r.RoutedCommands = make([][]byte, 33)
			case "bytes":
				r.RoutedCommands = [][]byte{make([]byte, GeoMirrorBatchBytes+1)}
			case "duplicate":
				r.RoutedCommands[1] = r.RoutedCommands[0]
			case "mutation":
				c, _ := routedstate.DecodeCommand(r.RoutedCommands[0])
				r.RoutedCommands[0], _ = routedstate.EncodeMutation("mutate", c.GeoMetadata.Target, []byte("x"))
			case "busy":
				s.mirrorSlots <- struct{}{}
				s.mirrorSlots <- struct{}{}
			}
			if _, e := s.MirrorGeoBatch(ctx, r); e == nil || b.submits != 0 || b.reads != 0 {
				t.Fatal("invalid batch reached proposal", e, b.submits, b.reads)
			}
		})
	}
}
func TestMirrorBatchRequiresQuorumAndAppliedCut(t *testing.T) {
	for _, mode := range []string{"quorum", "behind"} {
		t.Run(mode, func(t *testing.T) {
			s, b, ctx, r := mirrorBatchServer(t)
			if mode == "quorum" {
				b.lost = true
			} else {
				b.cut = 999
			}
			ctx, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
			defer cancel()
			if got, e := s.MirrorGeoBatch(ctx, r); e == nil || got != nil {
				t.Fatal("unconfirmed batch returned results", got, e)
			}
		})
	}
}
func TestMirrorBatchRejectsIncompleteReorderedOrAmbiguousResponse(t *testing.T) {
	s, _, ctx, r := mirrorBatchServer(t)
	response, e := s.MirrorGeoBatch(ctx, r)
	if e != nil {
		t.Fatal(e)
	}
	commands, _ := decodeMirrorBatch(r.GroupId, r.RoutedCommands)
	for _, mode := range []string{"missing", "reordered", "unknown", "ambiguous", "zero-index"} {
		t.Run(mode, func(t *testing.T) {
			got := proto.Clone(response).(*raftwire.EngineMirrorBatchResponse)
			switch mode {
			case "missing":
				got.Items = got.Items[:7]
			case "reordered":
				got.Items[0], got.Items[1] = got.Items[1], got.Items[0]
			case "unknown":
				got.Items[0].ProtoReflect().SetUnknown([]byte{0x78, 1})
			case "ambiguous":
				got.Items[0].ErrorCode = uint32(codes.Unavailable)
				got.Items[0].Error = "pending"
			case "zero-index":
				got.Items[0].Result.AppliedIndex = 0
			}
			if _, e := decodeMirrorBatchResults(commands, got); status.Code(e) != codes.DataLoss {
				t.Fatal(e)
			}
		})
	}
}
