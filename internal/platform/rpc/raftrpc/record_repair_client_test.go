package raftrpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type repairProxy struct {
	raftwire.UnimplementedRecordRepairServiceServer
	mode  string
	calls atomic.Int32
}

func (p *repairProxy) Fill(_ context.Context, request *raftwire.FillRecordCutRequest) (*raftwire.FillRecordCutResponse, error) {
	p.calls.Add(1)
	seal, err := DecodeRecordCutSeal(request.SealRecord)
	if err != nil || request.ClusterId != testCutClusterID || request.FromRaftId != 1 || request.ToRaftId != 991 ||
		request.GroupId != seal.Intent.Previous.Fence.GroupID || request.VerifyCandidate != (p.mode == "verify-good" || p.mode == "verify-old") {
		return nil, fmt.Errorf("wrong pending repair request: %v", err)
	}
	response := &raftwire.FillRecordCutResponse{ClusterId: testCutClusterID, RaftId: 991,
		NodeId: fmt.Sprintf("node_%032x", 991), RootConfirmedIndex: 60, References: 2, AlreadyHeld: 1, Fetched: 1, ReferencedBytes: 10}
	if p.mode == "verify-good" {
		response.AlreadyHeld, response.Fetched = 2, 0
		response.CandidateVerified = true
	}
	if p.mode == "wrong-response" {
		response.NodeId = fmt.Sprintf("node_%032x", 2)
	}
	if p.mode == "bad-counts" {
		response.Fetched = 2
	}
	if p.mode == "stale-root" {
		response.RootConfirmedIndex = 1
	}
	if p.mode == "empty-with-bytes" {
		response.References, response.AlreadyHeld, response.Fetched = 0, 0, 0
	}
	return response, nil
}

func (p *repairProxy) Repair(_ context.Context, request *raftwire.RepairRecordRequest) (*raftwire.RepairRecordResponse, error) {
	p.calls.Add(1)
	var d records.Descriptor
	if err := json.Unmarshal(request.RecordDescriptor, &d); err != nil || request.ClusterId != testCutClusterID ||
		request.GroupId != "data-1" || request.FromRaftId != 1 || request.ToRaftId != 991 || request.SourceRaftId != 2 || d.Validate() != nil {
		return nil, fmt.Errorf("wrong record repair request: %v", err)
	}
	id, err := d.ID()
	if err != nil {
		return nil, err
	}
	response := &raftwire.RepairRecordResponse{ClusterId: testCutClusterID, RaftId: 991,
		NodeId: fmt.Sprintf("node_%032x", 991), RootConfirmedIndex: 60, RecordId: id,
		StoreId: fmt.Sprintf("store_%032x", 991), VolumeId: fmt.Sprintf("volume_%032x", 991)}
	if p.mode == "wrong-response" {
		response.NodeId = fmt.Sprintf("node_%032x", 2)
	}
	return response, nil
}

func TestFillPendingRecordCutPinsTargetAndChecksReport(t *testing.T) {
	cut, _ := recordCutFixture(t)
	for _, mode := range []string{"good", "verify-good", "verify-old", "wrong-cert", "wrong-response", "bad-counts", "stale-root", "empty-with-bytes"} {
		t.Run(mode, func(t *testing.T) {
			ids := []identity.Identity{
				{Version: 1, ClusterID: testCutClusterID, NodeID: fmt.Sprintf("node_%032x", 1), RaftID: 1, CreatedAt: time.Now()},
				{Version: 1, ClusterID: testCutClusterID, NodeID: fmt.Sprintf("node_%032x", 2), RaftID: 2, CreatedAt: time.Now()},
				{Version: 1, ClusterID: testCutClusterID, NodeID: fmt.Sprintf("node_%032x", 991), RaftID: 991, CreatedAt: time.Now()},
			}
			pool, certificates := holdingCertificates(t, ids...)
			certificate := certificates[991]
			if mode == "wrong-cert" {
				certificate = certificates[2]
			}
			server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificate},
				ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert, MinVersion: tls.VersionTLS13})))
			proxy := &repairProxy{mode: mode}
			raftwire.RegisterRecordRepairServiceServer(server, proxy)
			t.Cleanup(server.Stop)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { listener.Close() })
			go func() { _ = server.Serve(listener) }()
			directory := NewStaticDirectory()
			if err := directory.Update(991, listener.Addr().String()); err != nil {
				t.Fatal(err)
			}
			client, err := NewGroupClient(GroupClientOptions{ClusterID: testCutClusterID, RaftID: 1, Directory: directory,
				Credentials: credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{certificates[1]}, RootCAs: pool,
					ServerName: "localhost", MinVersion: tls.VersionTLS13})})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { client.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			target := rootstate.Node{RaftID: 991, NodeID: ids[2].NodeID, RegionID: "west"}
			var report RecordFillReport
			if mode == "verify-good" || mode == "verify-old" {
				report, err = client.VerifyPendingRecordCut(ctx, target, cut.Seal)
			} else {
				report, err = client.FillPendingRecordCut(ctx, target, cut.Seal)
			}
			if mode == "good" {
				if err != nil || report != (RecordFillReport{References: 2, AlreadyHeld: 1, Fetched: 1, ReferencedBytes: 10}) {
					t.Fatal(report, err)
				}
			} else if mode == "verify-good" {
				if err != nil || report != (RecordFillReport{References: 2, AlreadyHeld: 2, ReferencedBytes: 10}) {
					t.Fatal(report, err)
				}
			} else if err == nil {
				t.Fatal("bad repair report accepted", mode, report)
			}
			if mode == "wrong-cert" && proxy.calls.Load() != 0 {
				t.Fatal("wrong TLS target received an application request")
			}
			if mode == "good" {
				d, err := records.Describe(rootstate.Keyspace{TenantID: "tenant", EngineID: "record", DatasetID: "data"}, "data-1", []byte("repair-key"), []byte("repair-body"))
				if err != nil {
					t.Fatal(err)
				}
				holding, err := client.RepairRecordOnNode(ctx, target, rootstate.Node{RaftID: 2, NodeID: fmt.Sprintf("node_%032x", 2), RegionID: "west"}, "data-1", d)
				if err != nil || holding.RecordID == "" || holding.StoreID == "" || holding.VolumeID == "" {
					t.Fatal("remote repair request failed", holding, err)
				}
			}
		})
	}
}
