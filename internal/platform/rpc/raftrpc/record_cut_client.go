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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const recordCutHeaderTimeout = 10 * time.Second
const recordCutIdleTimeout = 15 * time.Second

// FetchRecordCut imports a fully validated manifest from the exact old Node
// named by the caller. The returned cut is authenticated transport evidence
// from that Node, not proof that it remains the current Root authority; the
// coordinator must revalidate its pending handoff before using it for repair.
func (c *GroupClient) FetchRecordCut(ctx context.Context, source rootstate.Node, seal regionfence.Handoff, artifacts *snapshot.Store) (enginestate.RecordCut, error) {
	var zero enginestate.RecordCut
	if c == nil || ctx == nil || artifacts == nil || seal.Validate() != nil || seal.Activation != nil ||
		!slices.Contains(seal.Intent.Previous.Fence.Nodes(), source.RaftID) ||
		!slices.Contains(seal.Intent.Next.Nodes, c.options.RaftID) ||
		source.RegionID != seal.Intent.Previous.Fence.RegionID {
		return zero, errors.New("record cut source, destination or seal is invalid")
	}
	select {
	case c.recordSlots <- struct{}{}:
		defer func() { <-c.recordSlots }()
	default:
		return zero, status.Error(codes.ResourceExhausted, "record transfers are busy")
	}
	ctx, cancel := context.WithTimeout(ctx, recordCutTimeout)
	defer cancel()
	conn, err := c.recordConnection(source)
	if err != nil {
		return zero, err
	}
	encodedSeal, err := json.Marshal(seal)
	if err != nil || len(encodedSeal) > regionfence.MaxRecordBytes {
		return zero, errors.New("record cut seal exceeds request bound")
	}
	remote := raftwire.NewRecordCutServiceClient(conn)
	stream, err := remote.Get(ctx, &raftwire.GetRecordCutRequest{ClusterId: c.options.ClusterID, GroupId: seal.Intent.Previous.Fence.GroupID,
		FromRaftId: c.options.RaftID, ToRaftId: source.RaftID, SealRecord: encodedSeal},
		grpc.MaxCallSendMsgSize(recordCutHeaderBytes), grpc.MaxCallRecvMsgSize(recordCutFrameBytes+recordCutHeaderBytes))
	if err != nil {
		return zero, err
	}
	first, err := c.receiveRecordCut(ctx, stream, recordCutHeaderTimeout)
	if err != nil {
		return zero, err
	}
	if first == nil || first.GetHeader() == nil || len(first.ProtoReflect().GetUnknown()) != 0 ||
		len(first.GetHeader().ProtoReflect().GetUnknown()) != 0 || proto.Size(first) > recordCutHeaderBytes {
		return zero, errors.New("invalid record cut response header")
	}
	header := first.GetHeader()
	if header.ClusterId != c.options.ClusterID || header.RaftId != source.RaftID || header.NodeId != source.NodeID ||
		header.RootConfirmedIndex == 0 || header.RootConfirmedIndex < seal.Intent.Next.PlannedIndex ||
		len(header.CutDescriptor) == 0 || len(header.CutDescriptor) > recordCutHeaderBytes {
		return zero, errors.New("record cut response identity or Root position mismatch")
	}
	transportPeer, ok := peer.FromContext(stream.Context())
	if !ok || recordCutPeer(ctx, transportPeer, c.options.ClusterID, source) != nil {
		return zero, errors.New("record cut responder TLS identity mismatch")
	}
	decoder := json.NewDecoder(bytes.NewReader(header.CutDescriptor))
	decoder.DisallowUnknownFields()
	var cut enginestate.RecordCut
	if decoder.Decode(&cut) != nil || decoder.Decode(new(struct{})) != io.EOF || cut.Validate() != nil ||
		!reflect.DeepEqual(cut.Seal, seal) || cut.Artifact.GroupID != seal.Intent.Previous.Fence.GroupID {
		return zero, errors.New("record cut descriptor differs from requested handoff")
	}
	canonical, _ := json.Marshal(cut)
	if !bytes.Equal(canonical, header.CutDescriptor) {
		return zero, errors.New("noncanonical record cut descriptor")
	}
	reader := &recordCutStreamReader{ctx: ctx, client: c, stream: stream, transportPeer: transportPeer,
		cluster: c.options.ClusterID, source: source}
	if err := artifacts.Import(ctx, cut.Artifact, reader); err != nil {
		return zero, err
	}
	if err := enginestate.WalkVerifiedRecordCut(ctx, artifacts, cut, nil); err != nil {
		return zero, errors.New("imported record cut failed full validation")
	}
	if err := recordCutPeer(ctx, transportPeer, c.options.ClusterID, source); err != nil {
		return zero, err
	}
	return cut, nil
}

func recordCutPeer(ctx context.Context, transportPeer *peer.Peer, cluster string, source rootstate.Node) error {
	if transportPeer == nil {
		return errors.New("record cut transport peer is missing")
	}
	leaf, err := identity.VerifiedPeerCertificate(peer.NewContext(ctx, transportPeer), time.Now())
	if err != nil {
		return errors.New("record cut responder certificate is not current")
	}
	actual, err := identity.ParseNodeCertificate(leaf)
	if err != nil || actual.ClusterID != cluster || actual.RaftID != source.RaftID || actual.NodeID != source.NodeID {
		return errors.New("record cut responder identity differs from selected source")
	}
	return nil
}

func (c *GroupClient) receiveRecordCut(ctx context.Context, stream raftwire.RecordCutService_GetClient, timeout time.Duration) (*raftwire.GetRecordCutResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case c.recordReaders <- struct{}{}:
	default:
		return nil, status.Error(codes.ResourceExhausted, "record cut receive workers are busy")
	}
	type result struct {
		frame *raftwire.GetRecordCutResponse
		err   error
	}
	done := make(chan result, 1)
	go func() { frame, err := stream.Recv(); <-c.recordReaders; done <- result{frame, err} }()
	select {
	case <-ctx.Done():
		return nil, status.FromContextError(ctx.Err()).Err()
	case r := <-done:
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
		return r.frame, r.err
	}
}

// Import requires exact successful EOF. This reader never manufactures EOF
// from a short stream, duplicated header or a final gRPC error.
type recordCutStreamReader struct {
	ctx           context.Context
	client        *GroupClient
	stream        raftwire.RecordCutService_GetClient
	transportPeer *peer.Peer
	cluster       string
	source        rootstate.Node
	frame         []byte
}

func (r *recordCutStreamReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if len(r.frame) == 0 {
		response, err := r.client.receiveRecordCut(r.ctx, r.stream, recordCutIdleTimeout)
		if errors.Is(err, io.EOF) {
			if validityErr := recordCutPeer(r.ctx, r.transportPeer, r.cluster, r.source); validityErr != nil {
				return 0, validityErr
			}
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		if response == nil || response.GetHeader() != nil || len(response.GetData()) == 0 ||
			len(response.GetData()) > recordCutFrameBytes || len(response.ProtoReflect().GetUnknown()) != 0 {
			return 0, errors.New("invalid record cut data frame")
		}
		r.frame = response.GetData()
	}
	n := copy(p, r.frame)
	r.frame = r.frame[n:]
	return n, nil
}
