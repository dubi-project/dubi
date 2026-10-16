package raftrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"slices"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const recordCutFrameBytes = 1 << 20
const recordCutHeaderBytes = regionfence.MaxRecordBytes + 4096
const recordCutTimeout = 30 * time.Minute

type recordCutResolver func(string) (*enginestate.State, bool)

// RecordCutServer only serves a fresh, locally read-barriered cut of a pending
// handoff to a current Node in its next cohort. It neither transfers payloads
// nor grants a retention/coverage receipt.
type RecordCutServer struct {
	raftwire.UnimplementedRecordCutServiceServer
	auth      *RecordHoldingServer
	resolve   recordCutResolver
	artifacts *snapshot.Store
	slots     chan struct{}
	writers   chan struct{}
}

func NewRecordCutServer(auth *RecordHoldingServer, resolve recordCutResolver, artifacts *snapshot.Store) (*RecordCutServer, error) {
	if auth == nil || resolve == nil || artifacts == nil {
		return nil, errors.New("record cut service requires Node authorization, Group state and artifacts")
	}
	return &RecordCutServer{auth: auth, resolve: resolve, artifacts: artifacts, slots: make(chan struct{}, 2), writers: make(chan struct{}, 2)}, nil
}

func (s *RecordCutServer) send(ctx context.Context, stream raftwire.RecordCutService_GetServer, frame *raftwire.GetRecordCutResponse) error {
	ctx, cancel := context.WithTimeout(ctx, recordCutIdleTimeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return status.FromContextError(err).Err()
	}
	select {
	case s.writers <- struct{}{}:
	default:
		return status.Error(codes.ResourceExhausted, "record cut send workers are busy")
	}
	owned := proto.Clone(frame).(*raftwire.GetRecordCutResponse)
	done := make(chan error, 1)
	go func() { err := stream.Send(owned); <-s.writers; done <- err }()
	select {
	case <-ctx.Done():
		return status.FromContextError(ctx.Err()).Err()
	case err := <-done:
		if err := ctx.Err(); err != nil {
			return status.FromContextError(err).Err()
		}
		return err
	}
}

func decodeRecordCutSeal(encoded []byte) (regionfence.Handoff, error) {
	var seal regionfence.Handoff
	if len(encoded) == 0 || len(encoded) > regionfence.MaxRecordBytes {
		return seal, errors.New("invalid record cut seal size")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&seal) != nil || decoder.Decode(new(struct{})) != io.EOF || seal.Validate() != nil || seal.Activation != nil {
		return regionfence.Handoff{}, errors.New("invalid record cut seal")
	}
	canonical, _ := json.Marshal(seal)
	if !bytes.Equal(canonical, encoded) {
		return regionfence.Handoff{}, errors.New("noncanonical record cut seal")
	}
	return seal, nil
}

// DecodeRecordCutSeal is shared by the cut and repair endpoints so both
// accept the same bounded, canonical handoff representation.
func DecodeRecordCutSeal(encoded []byte) (regionfence.Handoff, error) {
	return decodeRecordCutSeal(encoded)
}

func (s *RecordCutServer) qualify(ctx context.Context, caller uint64, group string, seal regionfence.Handoff) (rootstate.RegionProgress, uint64, error) {
	var progress rootstate.RegionProgress
	if _, index, err := s.auth.qualify(ctx, caller); err != nil {
		return progress, 0, err
	} else {
		p, found, err := s.auth.root.RegionProgress(group)
		if err != nil {
			return progress, 0, status.Error(codes.Unavailable, "regional progress unavailable")
		}
		if !found || !p.Eligible || p.Pending == nil || !reflect.DeepEqual(*p.Pending, seal.Intent.Next) ||
			p.Current.RegionID != seal.Intent.Previous.Fence.RegionID || p.Current.GroupID != group ||
			p.Current.Generation != seal.Intent.Previous.Fence.Generation || p.Current.PlannedIndex != seal.Intent.Previous.Fence.PlannedIndex ||
			p.Current.GroupFenceRequestID != seal.Intent.Previous.RequestID || p.Current.GroupFenceIndex != seal.Intent.Previous.Index ||
			!slices.Equal(p.Current.Nodes, seal.Intent.Previous.Fence.Nodes()) ||
			!slices.Contains(p.Current.Nodes, s.auth.local.RaftID) || !slices.Contains(p.Pending.Nodes, caller) {
			return progress, 0, status.Error(codes.FailedPrecondition, "record cut handoff is not the current authorized Region plan")
		}
		return p, index, nil
	}
}

func (s *RecordCutServer) Get(req *raftwire.GetRecordCutRequest, stream raftwire.RecordCutService_GetServer) error {
	if req == nil || proto.Size(req) > recordCutHeaderBytes || len(req.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "invalid record cut request")
	}
	if req.ClusterId != s.auth.local.ClusterID || req.FromRaftId == 0 || req.ToRaftId != s.auth.local.RaftID {
		return status.Error(codes.PermissionDenied, "record cut identity mismatch")
	}
	seal, err := decodeRecordCutSeal(req.SealRecord)
	if err != nil || seal.Intent.Previous.Fence.GroupID != req.GroupId {
		return status.Error(codes.InvalidArgument, "invalid record cut handoff")
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return status.Error(codes.ResourceExhausted, "record cut service is busy")
	}
	ctx, cancel := context.WithTimeout(stream.Context(), recordCutTimeout)
	defer cancel()
	before, _, err := s.qualify(ctx, req.FromRaftId, req.GroupId, seal)
	if err != nil {
		return err
	}
	state, found := s.resolve(req.GroupId)
	if !found || state == nil {
		return status.Error(codes.FailedPrecondition, "record cut Group is not held locally")
	}
	prepared, err := state.PrepareRecordCut(ctx, s.auth.host, seal)
	if err != nil {
		return status.Error(codes.FailedPrecondition, "record cut cannot be prepared at the current Group barrier")
	}
	cut, buildErr := prepared.Build(ctx, s.artifacts)
	if err := errors.Join(buildErr, prepared.Close()); err != nil {
		return status.Error(codes.FailedPrecondition, "record cut cannot be published")
	}
	if err := enginestate.WalkVerifiedRecordCut(ctx, s.artifacts, cut, nil); err != nil {
		return status.Error(codes.DataLoss, "record cut artifact failed full validation")
	}
	after, index, err := s.qualify(ctx, req.FromRaftId, req.GroupId, seal)
	if err != nil {
		return err
	}
	before.AppliedIndex, after.AppliedIndex = 0, 0
	if !reflect.DeepEqual(before, after) {
		return status.Error(codes.FailedPrecondition, "record cut Root plan changed while building")
	}
	encoded, err := json.Marshal(cut)
	if err != nil || len(encoded) > recordCutHeaderBytes {
		return status.Error(codes.Internal, "record cut descriptor exceeds bound")
	}
	if err := s.send(ctx, stream, &raftwire.GetRecordCutResponse{Payload: &raftwire.GetRecordCutResponse_Header{Header: &raftwire.RecordCutHeader{
		ClusterId: s.auth.local.ClusterID, RaftId: s.auth.local.RaftID, NodeId: s.auth.local.NodeID,
		RootConfirmedIndex: index, CutDescriptor: encoded,
	}}}); err != nil {
		return err
	}
	reader, err := s.artifacts.Open(cut.Artifact)
	if err != nil {
		return status.Error(codes.DataLoss, "record cut artifact disappeared")
	}
	buffer := make([]byte, recordCutFrameBytes)
	for {
		if err := ctx.Err(); err != nil {
			_ = reader.Close()
			return status.FromContextError(err).Err()
		}
		n, readErr := reader.Read(buffer)
		if n > 0 {
			if err := s.send(ctx, stream, &raftwire.GetRecordCutResponse{Payload: &raftwire.GetRecordCutResponse_Data{Data: buffer[:n]}}); err != nil {
				_ = reader.Close()
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil || n == 0 {
			_ = reader.Close()
			return status.Error(codes.DataLoss, "record cut artifact read failed")
		}
	}
	if err := reader.Close(); err != nil {
		return status.Error(codes.DataLoss, "record cut artifact close failed")
	}
	final, _, err := s.qualify(ctx, req.FromRaftId, req.GroupId, seal)
	if err != nil {
		return err
	}
	final.AppliedIndex = 0
	if !reflect.DeepEqual(after, final) {
		return status.Error(codes.FailedPrecondition, "record cut Root plan changed during transfer")
	}
	if _, err := identity.VerifiedPeerCertificate(ctx, time.Now()); err != nil {
		return status.Error(codes.PermissionDenied, "record cut caller certificate expired")
	}
	return nil
}
