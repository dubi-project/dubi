package raftrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"

	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	snapshotWireMagic       = "DUBISW01"
	snapshotWirePrefixSize  = len(snapshotWireMagic) + 4
	maxSnapshotEnvelope     = 64 << 10
	snapshotWireBufferBytes = 2 << 20
	maxSnapshotSenders      = 2
)

// The header digest covers the bounded prefix+MsgSnap envelope. The canonical
// reference inside that envelope binds the complete file's size and SHA-256.
// A file cannot be swapped between Groups or applied cuts without detection.
func snapshotEnvelope(message []byte) ([]byte, error) {
	if len(message) == 0 || len(message) > maxSnapshotEnvelope {
		return nil, errors.New("file snapshot Raft envelope exceeds bound")
	}
	envelope := make([]byte, snapshotWirePrefixSize+len(message))
	copy(envelope, snapshotWireMagic)
	binary.BigEndian.PutUint32(envelope[len(snapshotWireMagic):], uint32(len(message)))
	copy(envelope[snapshotWirePrefixSize:], message)
	return envelope, nil
}

func (t *Transport) deliverFileSnapshot(stream raftwire.RaftService_InstallSnapshotClient, request outbound) (returnErr error) {
	envelope, err := snapshotEnvelope(request.data)
	if err != nil {
		return err
	}
	ref := *request.reference
	reader, err := t.options.OpenSnapshot(request.groupID, ref)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			returnErr = errors.Join(returnErr, reader.Close())
		}
	}()
	digest := sha256.Sum256(envelope)
	if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Header{
		Header: &raftwire.SnapshotHeader{ClusterId: t.options.ClusterID, GroupId: request.groupID,
			FromRaftId: t.options.RaftID, ToRaftId: request.to, EncodedSize: uint64(len(envelope)) + ref.Size,
			Sha256Digest: digest[:]}}}); err != nil {
		return err
	}
	if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: envelope}}); err != nil {
		return err
	}
	remaining := ref.Size
	for remaining != 0 {
		// A fresh immutable frame: gRPC instrumentation may retain sent messages,
		// so a reused io.Copy scratch buffer must not overwrite their contents.
		frame := make([]byte, min(remaining, uint64(DefaultSnapshotFrame)))
		if _, err := io.ReadFull(reader, frame); err != nil {
			return err
		}
		if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: frame}}); err != nil {
			return err
		}
		remaining -= uint64(len(frame))
	}
	var extra [1]byte
	if n, err := reader.Read(extra[:]); n != 0 || !errors.Is(err, io.EOF) {
		return errors.Join(err, errors.New("snapshot source lacks exact EOF"))
	}
	err = reader.Close()
	closed = true
	if err != nil {
		return err
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if response.GetAppliedIndex() < request.index {
		return errors.New("file snapshot ACK is behind sent index")
	}
	return nil
}

func (s *Server) installFileSnapshot(stream raftwire.RaftService_InstallSnapshotServer, header *raftwire.SnapshotHeader,
	reader *snapshotFrameReader, prefix []byte) error {
	installer, ok := s.stepper.(SnapshotStreamInstaller)
	if !ok {
		return status.Error(codes.Unimplemented, "file snapshot installer is unavailable")
	}
	size := uint64(binary.BigEndian.Uint32(prefix[len(snapshotWireMagic):]))
	if size == 0 || size > maxSnapshotEnvelope || size >= reader.remaining {
		return status.Error(codes.InvalidArgument, "invalid file snapshot envelope size")
	}
	envelope := make([]byte, len(prefix)+int(size))
	copy(envelope, prefix)
	if _, err := io.ReadFull(reader, envelope[len(prefix):]); err != nil {
		return err
	}
	digest := sha256.Sum256(envelope)
	if !bytes.Equal(digest[:], header.GetSha256Digest()) {
		return status.Error(codes.DataLoss, "file snapshot envelope digest mismatch")
	}
	var message raftpb.Message
	if err := proto.Unmarshal(envelope[len(prefix):], &message); err != nil {
		return status.Error(codes.InvalidArgument, "invalid file snapshot Raft message")
	}
	if message.GetType() != raftpb.MessageType_MsgSnap || etcdraft.IsEmptySnap(message.GetSnapshot()) ||
		message.GetFrom() != header.GetFromRaftId() || message.GetTo() != header.GetToRaftId() {
		return status.Error(codes.InvalidArgument, "file snapshot envelope does not match sender/target or MsgSnap")
	}
	ref, recognized, err := snapshot.DecodeReference(message.GetSnapshot().GetData())
	if err != nil || !recognized || ref.GroupID != header.GetGroupId() ||
		ref.Index != message.GetSnapshot().GetMetadata().GetIndex() || ref.Size != reader.remaining {
		return status.Error(codes.InvalidArgument, "file snapshot reference does not match Group/cut/payload")
	}
	index, err := installer.InstallSnapshotStream(stream.Context(), header.GetGroupId(), &message, reader)
	if err != nil {
		if code := status.Code(err); code == codes.PermissionDenied || code == codes.Unauthenticated || code == codes.DeadlineExceeded || code == codes.Canceled || code == codes.ResourceExhausted {
			return status.Error(code, "snapshot stream is no longer available")
		}
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return status.FromContextError(err).Err()
		}
		return status.Errorf(codes.Unavailable, "install file snapshot: %v", err)
	}
	if err := reader.finish(); err != nil {
		return err
	}
	if index < ref.Index {
		return status.Error(codes.Internal, "file snapshot was not durably applied")
	}
	return stream.SendAndClose(&raftwire.InstallSnapshotResponse{AppliedIndex: index})
}

// A reader holds at most one gRPC data frame. Frame boundaries are irrelevant
// to the envelope and artifact formats. Exact declared size AND stream EOF
// are required; a second header, empty frame or trailing frame is invalid.
type snapshotFrameReader struct {
	stream    raftwire.RaftService_InstallSnapshotServer
	remaining uint64
	buffer    []byte
	verified  bool
	failed    error
}

func (r *snapshotFrameReader) Read(target []byte) (int, error) {
	if len(target) == 0 {
		return 0, nil
	}
	if r.failed != nil {
		return 0, r.failed
	}
	if r.remaining == 0 {
		if err := r.finish(); err != nil {
			return 0, err
		}
		return 0, io.EOF
	}
	if len(r.buffer) == 0 {
		frame, err := r.stream.Recv()
		if errors.Is(err, io.EOF) {
			err = status.Error(codes.InvalidArgument, "incomplete Raft snapshot stream")
		}
		if err != nil {
			r.failed = err
			return 0, err
		}
		data := frame.GetData()
		if len(data) == 0 || len(data) > DefaultSnapshotFrame || uint64(len(data)) > r.remaining {
			r.failed = status.Error(codes.InvalidArgument, "invalid Raft snapshot data frame")
			return 0, r.failed
		}
		r.buffer = data
	}
	n := copy(target, r.buffer)
	r.buffer = r.buffer[n:]
	if len(r.buffer) == 0 {
		r.buffer = nil
	}
	r.remaining -= uint64(n)
	return n, nil
}

func (r *snapshotFrameReader) finish() error {
	if r.failed != nil {
		return r.failed
	}
	if r.remaining != 0 {
		return status.Error(codes.InvalidArgument, "snapshot installer did not consume complete stream")
	}
	if r.verified {
		return nil
	}
	_, err := r.stream.Recv()
	if !errors.Is(err, io.EOF) {
		if err == nil {
			err = status.Error(codes.InvalidArgument, "Raft snapshot exceeds declared size")
		}
		r.failed = err
		return err
	}
	r.verified = true
	return nil
}
