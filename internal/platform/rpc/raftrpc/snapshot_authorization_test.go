package raftrpc

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type authorizationBoundaryStream struct {
	*snapshotTestStream
	afterReceive func(*raftwire.InstallSnapshotRequest, error)
}

func (s *authorizationBoundaryStream) Recv() (*raftwire.InstallSnapshotRequest, error) {
	frame, err := s.snapshotTestStream.Recv()
	s.afterReceive(frame, err)
	return frame, err
}

func TestSnapshotAuthorizationRefreshesAfterReceiveAndBeforeACK(t *testing.T) {
	for _, boundary := range []string{"data", "eof", "ack"} {
		t.Run(boundary, func(t *testing.T) {
			expired, installed := false, false
			backend := &snapshotBackend{install: func(m *raftpb.Message) (uint64, error) {
				installed = true
				if boundary == "ack" {
					expired = true
				}
				return m.GetSnapshot().GetMetadata().GetIndex(), nil
			}}
			s, err := NewServer("cluster", 1, backend, func(context.Context, uint64) error {
				if expired {
					return errors.New("peer authorization expired")
				}
				return nil
			}, 0)
			if err != nil {
				t.Fatal(err)
			}
			data, err := proto.Marshal(&raftpb.Message{Type: raftpb.MessageType_MsgSnap.Enum(), From: proto.Uint64(2), To: proto.Uint64(1),
				Snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(5), Term: proto.Uint64(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2}}}}})
			if err != nil {
				t.Fatal(err)
			}
			stream := &authorizationBoundaryStream{snapshotTestStream: snapshotStream(data), afterReceive: func(frame *raftwire.InstallSnapshotRequest, err error) {
				if boundary == "data" && len(frame.GetData()) > 0 || boundary == "eof" && errors.Is(err, io.EOF) {
					expired = true
				}
			}}
			stream.frames <- &raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: data}}
			close(stream.frames)
			if err := s.InstallSnapshot(stream); status.Code(err) != codes.PermissionDenied {
				t.Fatal("stream kept expired authorization", err)
			}
			if installed != (boundary == "ack") {
				t.Fatal("unexpected installation boundary", installed)
			}
			if stream.ack != 0 || s.SnapshotResources().UsedBytes != 0 || len(s.snapshotSlots) != 0 {
				t.Fatal("expired stream acknowledged or leaked leases")
			}
		})
	}
}

func TestFileSnapshotAuthorizationExpiryDoesNotPublishArtifact(t *testing.T) {
	for _, boundary := range []string{"payload", "eof"} {
		t.Run(boundary, func(t *testing.T) {
			source, destination := snapshotArtifacts(t), snapshotArtifacts(t)
			ref := wireArtifact(t, source, 1, 16)
			reader, err := source.Open(ref)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := io.ReadAll(reader)
			if closeErr := reader.Close(); err != nil || closeErr != nil {
				t.Fatal(err, closeErr)
			}
			encoded, err := proto.Marshal(fileMessage(t, ref, 2, 1))
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := snapshotEnvelope(encoded)
			if err != nil {
				t.Fatal(err)
			}
			expired := false
			backend := &fileSnapshotBackend{importSnapshot: func(ctx context.Context, _ string, _ *raftpb.Message, reader io.Reader) (uint64, error) {
				if err := destination.Import(ctx, ref, reader); err != nil {
					return 0, fmt.Errorf("import artifact: %w", err)
				}
				t.Error("expired stream reached durable publication")
				return ref.Index, nil
			}}
			s, err := NewServer("cluster", 1, backend, func(context.Context, uint64) error {
				if expired {
					return errors.New("expired")
				}
				return nil
			}, 0)
			if err != nil {
				t.Fatal(err)
			}
			frames := 0
			stream := &authorizationBoundaryStream{snapshotTestStream: snapshotStream(nil), afterReceive: func(frame *raftwire.InstallSnapshotRequest, err error) {
				if len(frame.GetData()) > 0 {
					frames++
				}
				if boundary == "payload" && frames == 2 || boundary == "eof" && errors.Is(err, io.EOF) {
					expired = true
				}
			}}
			stream.header.EncodedSize = uint64(len(envelope) + len(payload))
			digest := sha256.Sum256(envelope)
			stream.header.Sha256Digest = digest[:]
			stream.frames = make(chan *raftwire.InstallSnapshotRequest, 2)
			for _, data := range [][]byte{envelope, payload} {
				stream.frames <- &raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: data}}
			}
			close(stream.frames)
			if err := s.InstallSnapshot(stream); status.Code(err) != codes.PermissionDenied {
				t.Fatal("expiry misclassified", err)
			}
			if artifact, err := destination.Open(ref); !errors.Is(err, os.ErrNotExist) {
				if artifact != nil {
					artifact.Close()
				}
				t.Fatal("unauthorized artifact published", err)
			}
			if stream.ack != 0 || s.SnapshotResources().UsedBytes != 0 || len(s.snapshotSlots) != 0 {
				t.Fatal("expiry leaked/acknowledged")
			}
		})
	}
}
