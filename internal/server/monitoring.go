package server

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/management"
	"github.com/dubi-project/dubi/internal/platform/rpc/datawire"
	"github.com/dubi-project/dubi/internal/platform/telemetry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (s *dataService) ObserveMonitor(ctx context.Context, r *datawire.MonitorRequest) (*datawire.MonitorResponse, error) {
	if r == nil || proto.Size(r) > 4096 || len(r.ProtoReflect().GetUnknown()) > 0 || r.ClusterId != s.runtime.Identity.ClusterID || r.FromRaftId == 0 || r.EndUnix > time.Now().Add(time.Minute).Unix() || r.EndUnix < time.Now().Add(-time.Minute).Unix() || (r.Seconds != 900 && r.Seconds != 3600 && r.Seconds != 21600) {
		return nil, status.Error(codes.InvalidArgument, "invalid monitoring request")
	}
	leaf, err := identity.VerifiedPeerCertificate(ctx, time.Now())
	if err != nil {
		return nil, status.Error(codes.PermissionDenied, "verified Node certificate required")
	}
	caller, err := identity.ParseNodeCertificate(leaf)
	if err != nil || caller.ClusterID != r.ClusterId || caller.RaftID != r.FromRaftId {
		return nil, status.Error(codes.PermissionDenied, "monitoring identity mismatch")
	}
	member, found, err := s.runtime.Root.RootMember(caller.RaftID)
	if err != nil || !found || member.NodeID != caller.NodeID {
		return nil, status.Error(codes.PermissionDenied, "current Root member required")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return s.localMonitor(r.EndUnix, r.Seconds)
}
func (s *dataService) localMonitor(end, seconds int64) (*datawire.MonitorResponse, error) {
	if s.monitor == nil {
		return nil, status.Error(codes.Unavailable, "monitoring not initialized")
	}
	state, err := s.localStatusResponse(true)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(s.monitor.Query(end, seconds))
	if err != nil {
		return nil, err
	}
	return &datawire.MonitorResponse{TelemetryJson: b, Status: state}, nil
}
func (s managementSource) Probe(ctx context.Context, m management.Member, end, seconds int64) (management.MonitorProbe, error) {
	var r *datawire.MonitorResponse
	var err error
	if m.NodeID == s.data.runtime.Identity.NodeID {
		if err = ctx.Err(); err == nil {
			r, err = s.data.localMonitor(end, seconds)
		}
	} else {
		if s.data.client == nil {
			return management.MonitorProbe{}, management.ErrUnavailable
		}
		conn, e := s.data.client.NodeAdminConnection(rootstate.Node{NodeID: m.NodeID, RaftID: m.RaftID, RegionID: m.RegionID, Name: m.Name, Address: m.Address})
		if e != nil {
			return management.MonitorProbe{}, e
		}
		r, err = datawire.NewDataServiceClient(conn).ObserveMonitor(ctx, &datawire.MonitorRequest{ClusterId: s.data.runtime.Identity.ClusterID, FromRaftId: s.data.runtime.Identity.RaftID, EndUnix: end, Seconds: seconds}, grpc.MaxCallRecvMsgSize(2<<20), grpc.MaxCallSendMsgSize(4096))
	}
	if err != nil {
		return management.MonitorProbe{}, err
	}
	if r == nil || len(r.TelemetryJson) > 256<<10 {
		return management.MonitorProbe{}, errors.New("invalid monitoring response")
	}
	var v telemetry.View
	if json.Unmarshal(r.TelemetryJson, &v) != nil || !v.Valid(m.NodeID, end, seconds) {
		return management.MonitorProbe{}, errors.New("invalid monitoring series")
	}
	o, err := managementObservation(m, r.Status)
	if err != nil {
		return management.MonitorProbe{}, err
	}
	return management.MonitorProbe{Observation: o, Telemetry: v}, nil
}
