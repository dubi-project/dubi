package raftrpc

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const recordDescriptorBytes = 16 << 10
const recordHoldingMessageBytes = recordDescriptorBytes + 4096

// A holding proof brackets disk verification with two quorum-confirmed Root
// reads. On a low-cost multi-Region cluster each Root cut can take several
// seconds, so a 10s end-to-end cap falsely marked healthy holders unavailable
// and prevented physical majorities from being confirmed.
const recordHoldingTimeout = 60 * time.Second
const recordHoldingConcurrency = 2

type RecordHoldingVerifier interface {
	Verify(context.Context, records.Descriptor) (records.Holding, error)
}

type RecordHoldingServer struct {
	raftwire.UnimplementedRecordHoldingServiceServer
	local     identity.Identity
	host      regionReadBarrier
	root      *rootstate.State
	store     RecordHoldingVerifier
	authorize AuthorizePeer
	slots     chan struct{}
	timeout   time.Duration // Shortened only in tests; no deployment setting.
}

func NewRecordHoldingServer(local identity.Identity, host regionReadBarrier, root *rootstate.State, store *records.Store, authorize AuthorizePeer) (*RecordHoldingServer, error) {
	if _, err := identity.NodeIdentityURI(local); err != nil || host == nil || root == nil || store == nil || authorize == nil {
		return nil, errors.New("record observation requires bound Node identity, Root, Host, Store and peer authorization")
	}
	return &RecordHoldingServer{local: local, host: host, root: root, store: store, authorize: authorize, slots: make(chan struct{}, recordHoldingConcurrency), timeout: recordHoldingTimeout}, nil
}

func decodeHoldingDescriptor(encoded []byte) (records.Descriptor, error) {
	var d records.Descriptor
	if len(encoded) == 0 || len(encoded) > recordDescriptorBytes {
		return d, errors.New("invalid record descriptor size")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&d) != nil || decoder.Decode(new(struct{})) != io.EOF || d.Validate() != nil {
		return records.Descriptor{}, errors.New("invalid record descriptor")
	}
	return d, nil
}

func (s *RecordHoldingServer) Observe(ctx context.Context, req *raftwire.ObserveRecordHoldingRequest) (*raftwire.ObserveRecordHoldingResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "record observation request missing")
	}
	if proto.Size(req) > recordHoldingMessageBytes {
		return nil, status.Error(codes.ResourceExhausted, "record observation request too large")
	}
	if req.ClusterId != s.local.ClusterID || req.FromRaftId == 0 || req.ToRaftId != s.local.RaftID {
		return nil, status.Error(codes.PermissionDenied, "record observation identity mismatch")
	}
	d, err := decodeHoldingDescriptor(req.RecordDescriptor)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid record observation descriptor")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return nil, status.Error(codes.ResourceExhausted, "record observation is busy")
	}
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	before, _, err := s.qualify(ctx, req.FromRaftId)
	if err != nil {
		return nil, err
	}
	holding, verifyErr := s.store.Verify(ctx, d)
	// Revalidate even after a missing/corrupt result. Do not disclose current
	// storage information to a peer whose authorization expired during IO.
	after, index, err := s.qualify(ctx, req.FromRaftId)
	if err != nil {
		return nil, err
	}
	if before != after {
		return nil, status.Error(codes.FailedPrecondition, "record observation Node registration changed")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if verifyErr != nil {
		switch {
		case errors.Is(verifyErr, context.Canceled), errors.Is(verifyErr, context.DeadlineExceeded):
			return nil, status.FromContextError(verifyErr).Err()
		case errors.Is(verifyErr, records.ErrNotHeld):
			return nil, status.Error(codes.NotFound, "record is not held on this Node")
		case errors.Is(verifyErr, records.ErrCorrupt):
			return nil, status.Error(codes.DataLoss, "record integrity check failed")
		case errors.Is(verifyErr, records.ErrUnavailable):
			return nil, status.Error(codes.Unavailable, "record Store is unavailable")
		default:
			return nil, status.Error(codes.FailedPrecondition, "record Store cannot provide a verified holding")
		}
	}
	return s.holdingResponse(d, holding, index)
}

func (s *RecordHoldingServer) holdingResponse(d records.Descriptor, holding records.Holding, index uint64) (*raftwire.ObserveRecordHoldingResponse, error) {
	wantID, _ := d.ID()
	actualID, err := holding.Descriptor.ID()
	if err != nil || actualID != wantID || holding.RecordID != wantID || !validHoldingID(holding.StoreID, "store_") || !validHoldingID(holding.VolumeID, "volume_") {
		return nil, status.Error(codes.FailedPrecondition, "record Store returned an inconsistent holding")
	}
	encoded, err := json.Marshal(holding.Descriptor)
	if err != nil {
		return nil, status.Error(codes.Internal, "cannot encode record holding")
	}
	response := &raftwire.ObserveRecordHoldingResponse{Version: 1, ClusterId: s.local.ClusterID, RaftId: s.local.RaftID, NodeId: s.local.NodeID,
		RootConfirmedIndex: index, RecordDescriptor: encoded, RecordId: holding.RecordID, StoreId: holding.StoreID, VolumeId: holding.VolumeID}
	if _, err := decodeHoldingObservation(s.local.ClusterID, s.local.RaftID, d, response); err != nil {
		return nil, status.Error(codes.FailedPrecondition, "record Store returned an inconsistent holding")
	}
	return response, nil
}

// RootMembers reads BOTH identities from one cut. The two checks bracket IO;
// they are point-in-time qualification, not atomic global revocation or a
// promise of future membership. The caller's NodeID must match its TLS leaf,
// not just a claimed RaftID. The local identity is the Runtime's durable one.
func (s *RecordHoldingServer) qualify(ctx context.Context, caller uint64) ([2]rootstate.Node, uint64, error) {
	var nodes [2]rootstate.Node
	if err := ctx.Err(); err != nil {
		return nodes, 0, status.FromContextError(err).Err()
	}
	if err := authorizeSender(ctx, caller, s.authorize); err != nil {
		return nodes, 0, err
	}
	index, err := s.host.ReadIndex(ctx, raft.RootGroupID)
	if err != nil {
		return nodes, 0, observationWaitError(err)
	}
	if index == 0 || s.root.AppliedIndex() < index {
		return nodes, 0, status.Error(codes.Unavailable, "Root authorization cut is not applied")
	}
	qualified, found, err := s.root.RootMembers(caller, s.local.RaftID)
	if err != nil {
		return nodes, 0, status.Error(codes.Unavailable, "Root authorization unavailable")
	}
	if !found || len(qualified) != 2 {
		return nodes, 0, status.Error(codes.PermissionDenied, "record observation requires current Root members")
	}
	copy(nodes[:], qualified)
	if err := authorizeSender(ctx, caller, s.authorize); err != nil {
		return nodes, 0, err
	}
	cert, err := identity.VerifiedPeerCertificate(ctx, time.Now())
	if err != nil {
		return nodes, 0, status.Error(codes.PermissionDenied, "record observation certificate is no longer valid")
	}
	remote, err := identity.ParseNodeCertificate(cert)
	if err != nil || remote.ClusterID != s.local.ClusterID || remote.RaftID != caller || remote.NodeID != nodes[0].NodeID || nodes[1].NodeID != s.local.NodeID {
		return nodes, 0, status.Error(codes.PermissionDenied, "record observation certificate and Node registration differ")
	}
	return nodes, index, nil
}

// HoldingObservation proves only this authenticated Node's completed physical
// check. Neither Root index nor existence of bytes proves a current logical
// reference, stable Region generation, LocalReady or GeoReady. Multiple
// Volumes of this Node remain ONE participant for any future quorum protocol.
type HoldingObservation struct {
	NodeID             string
	RaftID             uint64
	RootConfirmedIndex uint64
	Holding            records.Holding
}

func validHoldingID(value, prefix string) bool {
	if len(value) != len(prefix)+32 || value[:len(prefix)] != prefix {
		return false
	}
	decoded, err := hex.DecodeString(value[len(prefix):])
	return err == nil && hex.EncodeToString(decoded) == value[len(prefix):]
}

func decodeHoldingObservation(cluster string, target uint64, expected records.Descriptor, r *raftwire.ObserveRecordHoldingResponse) (HoldingObservation, error) {
	bad := errors.New("invalid record holding observation")
	if r == nil || r.Version != 1 || proto.Size(r) > recordHoldingMessageBytes || r.ClusterId != cluster || r.RaftId != target || target == 0 || r.RootConfirmedIndex == 0 || !validHoldingID(r.NodeId, "node_") || !validHoldingID(r.StoreId, "store_") || !validHoldingID(r.VolumeId, "volume_") {
		return HoldingObservation{}, bad
	}
	d, err := decodeHoldingDescriptor(r.RecordDescriptor)
	if err != nil {
		return HoldingObservation{}, bad
	}
	want, err := expected.ID()
	got, gotErr := d.ID()
	if err != nil || gotErr != nil || got != want || r.RecordId != want {
		return HoldingObservation{}, bad
	}
	return HoldingObservation{NodeID: r.NodeId, RaftID: target, RootConfirmedIndex: r.RootConfirmedIndex,
		Holding: records.Holding{RecordID: got, Descriptor: d, StoreID: r.StoreId, VolumeID: r.VolumeId}}, nil
}

// Direct-to-Node, with no leader fallback. Bind the responder's ACTUAL TLS
// identity, not merely the RaftID it claims in a response. The peer may hold
// data whose originating Group it does not host; no origin-Group read is used.
func (client *GroupClient) ObserveRecordHolding(ctx context.Context, target uint64, d records.Descriptor) (HoldingObservation, error) {
	if client == nil || ctx == nil || target == 0 || target == client.options.RaftID {
		return HoldingObservation{}, errors.New("invalid record observation target")
	}
	d.Key = bytes.Clone(d.Key)
	if err := d.Validate(); err != nil {
		return HoldingObservation{}, err
	}
	encoded, err := json.Marshal(d)
	if err != nil || len(encoded) > recordDescriptorBytes {
		return HoldingObservation{}, errors.New("invalid record observation descriptor")
	}
	ctx, cancel := context.WithTimeout(ctx, recordHoldingTimeout)
	defer cancel()
	if _, err := client.peer(target); err != nil {
		return HoldingObservation{}, err
	}
	client.mu.Lock()
	if client.closed || client.peers[target] == nil {
		client.mu.Unlock()
		return HoldingObservation{}, errors.New("Group client is closed")
	}
	remote := raftwire.NewRecordHoldingServiceClient(client.peers[target].conn)
	client.mu.Unlock()
	var transportPeer peer.Peer
	response, err := remote.Observe(ctx, &raftwire.ObserveRecordHoldingRequest{ClusterId: client.options.ClusterID, FromRaftId: client.options.RaftID, ToRaftId: target, RecordDescriptor: encoded},
		grpc.Peer(&transportPeer), grpc.MaxCallSendMsgSize(recordHoldingMessageBytes), grpc.MaxCallRecvMsgSize(recordHoldingMessageBytes))
	if err != nil {
		return HoldingObservation{}, err
	}
	if err := ctx.Err(); err != nil {
		return HoldingObservation{}, err
	}
	return authenticatedHoldingObservation(ctx, &transportPeer, client.options.ClusterID, target, d, response, time.Now())
}

func authenticatedHoldingObservation(ctx context.Context, transportPeer *peer.Peer, cluster string, target uint64, d records.Descriptor, response *raftwire.ObserveRecordHoldingResponse, now time.Time) (HoldingObservation, error) {
	if transportPeer == nil {
		return HoldingObservation{}, errors.New("record observation has no responder transport identity")
	}
	cert, err := identity.VerifiedPeerCertificate(peer.NewContext(ctx, transportPeer), now)
	if err != nil {
		return HoldingObservation{}, errors.New("record observation has no current verified responder certificate")
	}
	actual, err := identity.ParseNodeCertificate(cert)
	if err != nil || actual.ClusterID != cluster || actual.RaftID != target || response == nil || actual.NodeID != response.NodeId {
		return HoldingObservation{}, errors.New("record observation responder identity mismatch")
	}
	return decodeHoldingObservation(cluster, target, d, response)
}
