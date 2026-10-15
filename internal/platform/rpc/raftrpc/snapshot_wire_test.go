package raftrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type fileSnapshotBackend struct {
	importSnapshot func(context.Context, string, *raftpb.Message, io.Reader) (uint64, error)
	heartbeat      func()
}

func (b *fileSnapshotBackend) Step(string, *raftpb.Message) error {
	if b.heartbeat != nil {
		b.heartbeat()
	}
	return nil
}
func (*fileSnapshotBackend) InstallSnapshot(string, *raftpb.Message) (uint64, error) {
	return 0, errors.New("unexpected inline snapshot installation")
}
func (b *fileSnapshotBackend) InstallSnapshotStream(ctx context.Context, group string, message *raftpb.Message, reader io.Reader) (uint64, error) {
	return b.importSnapshot(ctx, group, message, reader)
}

func snapshotArtifacts(t *testing.T) *snapshot.Store {
	t.Helper()
	store, err := snapshot.OpenStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	return store
}

func wireArtifact(t *testing.T, store *snapshot.Store, records, valueBytes int) snapshot.Reference {
	t.Helper()
	value := bytes.Repeat([]byte("v"), valueBytes)
	ref, err := store.Create(context.Background(), snapshot.Header{Version: snapshot.StreamVersion, Format: "opaque-v1", GroupID: "data", Index: 5},
		snapshot.MaxStreamBytes, func(e *snapshot.Encoder) error {
			for i := range records {
				if err := e.Add([]byte(fmt.Sprintf("key-%06d", i)), value); err != nil {
					return err
				}
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func fileMessage(t *testing.T, ref snapshot.Reference, from, to uint64) *raftpb.Message {
	t.Helper()
	encoded, err := ref.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return &raftpb.Message{Type: raftpb.MessageType_MsgSnap.Enum(), From: proto.Uint64(from), To: proto.Uint64(to),
		Snapshot: &raftpb.Snapshot{Data: encoded, Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(ref.Index), Term: proto.Uint64(2),
			ConfState: &raftpb.ConfState{Voters: []uint64{from, to}}}}}
}

func TestFileSnapshotWireRejectsInvalidStreams(t *testing.T) {
	for _, damage := range []string{"none", "envelope-digest", "envelope-size", "group", "index", "payload-size", "artifact", "truncated", "trailing", "second-header", "behind-ack", "unconsumed", "bare-reference"} {
		t.Run(damage, func(t *testing.T) {
			source, destination := snapshotArtifacts(t), snapshotArtifacts(t)
			ref := wireArtifact(t, source, 1, 12)
			reader, err := source.Open(ref)
			if err != nil {
				t.Fatal(err)
			}
			payload, err := io.ReadAll(reader) // Tiny adversarial fixture only.
			if closeErr := reader.Close(); err != nil || closeErr != nil {
				t.Fatal(err, closeErr)
			}
			message := fileMessage(t, ref, 2, 1)
			switch damage {
			case "group":
				ref.GroupID = "other"
			case "index":
				ref.Index++
			case "payload-size":
				ref.Size++
			}
			message.Snapshot.Data, _ = ref.Encode()
			encoded, err := proto.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			envelope, err := snapshotEnvelope(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if damage == "envelope-size" {
				binary.BigEndian.PutUint32(envelope[len(snapshotWireMagic):], maxSnapshotEnvelope+1)
			}
			wire := append(bytes.Clone(envelope), payload...)
			stream := snapshotStream(wire)
			digest := sha256.Sum256(envelope)
			stream.header.Sha256Digest = digest[:]
			if damage == "envelope-digest" {
				stream.header.Sha256Digest = make([]byte, 32)
			}
			if damage == "bare-reference" {
				wire = encoded
				stream = snapshotStream(wire)
			}
			if damage == "artifact" {
				wire[len(wire)-1] ^= 1
			}
			if damage == "truncated" {
				wire = wire[:len(wire)-1]
			}
			stream.frames = make(chan *raftwire.InstallSnapshotRequest, len(wire)+1)
			// Split every prefix/header/body byte to prove frame-boundary neutrality.
			for _, b := range wire {
				stream.frames <- &raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: []byte{b}}}
			}
			if damage == "trailing" {
				stream.frames <- &raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: []byte("x")}}
			}
			if damage == "second-header" {
				stream.frames <- &raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Header{Header: &raftwire.SnapshotHeader{}}}
			}
			close(stream.frames)
			installed := false
			backend := &fileSnapshotBackend{importSnapshot: func(ctx context.Context, group string, message *raftpb.Message, payload io.Reader) (uint64, error) {
				if damage == "unconsumed" {
					return 5, nil
				}
				ref, _, err := snapshot.DecodeReference(message.GetSnapshot().GetData())
				if err != nil || group != ref.GroupID {
					return 0, errors.New("wrong reference")
				}
				if err := destination.Import(ctx, ref, payload); err != nil {
					return 0, err
				}
				installed = true
				if damage == "behind-ack" {
					return 4, nil
				}
				return ref.Index, nil
			}}
			server, err := NewServer("cluster", 1, backend, func(context.Context, uint64) error { return nil }, 0)
			if err != nil {
				t.Fatal(err)
			}
			err = server.InstallSnapshot(stream)
			if damage == "none" {
				if err != nil || stream.ack != 5 || !installed {
					t.Fatal("valid stream failed", stream.ack, err)
				}
			} else if err == nil || stream.ack != 0 || (installed && damage != "behind-ack") {
				t.Fatal("invalid stream was installed/ACKed", installed, stream.ack, err)
			}
			if server.SnapshotResources().UsedBytes != 0 || len(server.snapshotSlots) != 0 {
				t.Fatal("stream leaked admission resources")
			}
		})
	}
}

func TestFileSnapshotWireSenderBudgetsAndQueueRelease(t *testing.T) {
	transport := budgetTransport(t)
	transport.options.OpenSnapshot = func(string, snapshot.Reference) (io.ReadCloser, error) {
		t.Error("queued snapshot was opened before execution")
		return nil, errors.New("unreachable")
	}
	ref := snapshot.Reference{Version: 1, GroupID: "data", Index: 5, Size: 80 << 20,
		SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	message := fileMessage(t, ref, 1, 2)
	if err := transport.Send("data", message); err != nil {
		t.Fatal(err)
	}
	used := transport.ResourceStatus().Snapshots.UsedBytes
	if used != uint64(proto.Size(message))+snapshotWireBufferBytes || len(transport.snapshotSlots) != 1 {
		t.Fatal("sender did not reserve stream buffer/slot", used)
	}
	if err := transport.Send("data", message); err == nil || len(transport.snapshotSlots) != 1 || transport.ResourceStatus().Snapshots.UsedBytes != used {
		t.Fatal("full queue leaked stream lease", err)
	}
	message.To = proto.Uint64(3)
	if err := transport.Send("data", message); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send("data", message); err == nil || len(transport.snapshotSlots) != maxSnapshotSenders {
		t.Fatal("global sender concurrency exceeded", err)
	}
	if err := transport.Send("different", message); err == nil {
		t.Fatal("cross-Group reference accepted")
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if transport.ResourceStatus().Snapshots.UsedBytes != 0 || len(transport.snapshotSlots) != 0 {
		t.Fatal("close leaked queued file snapshot leases")
	}
}

func TestFileSnapshotWireCancellationDuringImport(t *testing.T) {
	source, destination := snapshotArtifacts(t), snapshotArtifacts(t)
	ref := wireArtifact(t, source, 1, 1024)
	encoded, err := proto.Marshal(fileMessage(t, ref, 2, 1))
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := snapshotEnvelope(encoded)
	if err != nil {
		t.Fatal(err)
	}
	stream := snapshotStream(envelope)
	stream.header.EncodedSize += ref.Size
	ctx, cancel := context.WithCancel(stream.ctx)
	stream.ctx = ctx
	defer cancel()
	stream.frames <- &raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: envelope}}
	entered := make(chan struct{})
	backend := &fileSnapshotBackend{importSnapshot: func(ctx context.Context, _ string, _ *raftpb.Message, reader io.Reader) (uint64, error) {
		close(entered)
		return 0, destination.Import(ctx, ref, reader)
	}}
	server, err := NewServer("cluster", 1, backend, func(context.Context, uint64) error { return nil }, 0)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- server.InstallSnapshot(stream) }()
	awaitSignal(t, entered)
	cancel()
	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled file receiver did not stop")
	}
	if stream.ack != 0 || len(server.snapshotSlots) != 0 || server.SnapshotResources().UsedBytes != 0 {
		t.Fatal("canceled receive ACKed or leaked resources")
	}
	if reader, err := destination.Open(ref); err == nil {
		reader.Close()
		t.Fatal("canceled receive published artifact")
	}
}

type closeFailureReader struct{ io.Reader }

func (closeFailureReader) Close() error { return errors.New("late digest failure") }

type fileSendProbe struct {
	raftwire.RaftService_InstallSnapshotClient
	acked bool
}

func (*fileSendProbe) Send(*raftwire.InstallSnapshotRequest) error { return nil }
func (s *fileSendProbe) CloseAndRecv() (*raftwire.InstallSnapshotResponse, error) {
	s.acked = true
	return &raftwire.InstallSnapshotResponse{AppliedIndex: 5}, nil
}

func TestFileSnapshotWireSenderChecksReaderCloseBeforeACK(t *testing.T) {
	transport := budgetTransport(t)
	transport.options.OpenSnapshot = func(string, snapshot.Reference) (io.ReadCloser, error) {
		return closeFailureReader{bytes.NewReader([]byte("payload"))}, nil
	}
	ref := snapshot.Reference{Version: 1, GroupID: "data", Index: 5, Size: 7,
		SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}
	message, err := proto.Marshal(fileMessage(t, ref, 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	stream := new(fileSendProbe)
	if err := transport.deliverFileSnapshot(stream, outbound{groupID: "data", to: 2, index: 5, data: message, reference: &ref}); err == nil || stream.acked {
		t.Fatal("late artifact verification failure was ACKed", err)
	}
}

func TestFileSnapshotWireOverMutualTLS(t *testing.T) {
	source, destination := snapshotArtifacts(t), snapshotArtifacts(t)
	ref := wireArtifact(t, source, 80, 1<<20)
	if ref.Size <= DefaultMaxSnapshotSize {
		t.Fatal("test did not exceed inline limit")
	}
	certPool, certificates := testCertificates(t, []uint64{1, 2})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	imported, allowACK, heartbeat := make(chan struct{}), make(chan struct{}), make(chan struct{}, 1)
	backend := &fileSnapshotBackend{heartbeat: func() { heartbeat <- struct{}{} },
		importSnapshot: func(ctx context.Context, _ string, message *raftpb.Message, reader io.Reader) (uint64, error) {
			ref, _, err := snapshot.DecodeReference(message.GetSnapshot().GetData())
			if err != nil {
				return 0, err
			}
			if err := destination.Import(ctx, ref, reader); err != nil {
				return 0, err
			}
			close(imported)
			select {
			case <-allowACK:
				return ref.Index, nil
			case <-ctx.Done():
				return 0, ctx.Err()
			}
		}}
	server, err := NewServer("cluster", 2, backend, func(ctx context.Context, claimed uint64) error {
		remote, _ := peer.FromContext(ctx)
		info := remote.AuthInfo.(credentials.TLSInfo)
		if claimed != 1 || info.State.PeerCertificates[0].Subject.CommonName != "raft-1" {
			return errors.New("identity mismatch")
		}
		return nil
	}, 0)
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{certificates[2]}, ClientCAs: certPool, ClientAuth: tls.RequireAndVerifyClientCert})))
	defer grpcServer.Stop()
	raftwire.RegisterRaftServiceServer(grpcServer, server)
	go grpcServer.Serve(listener)
	directory := NewStaticDirectory()
	if err := directory.Update(2, listener.Addr().String()); err != nil {
		t.Fatal(err)
	}
	result := make(chan bool, 1)
	transport, err := NewTransport(TransportOptions{ClusterID: "cluster", RaftID: 1, Directory: directory,
		Credentials:  credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, ServerName: "localhost", RootCAs: certPool, Certificates: []tls.Certificate{certificates[1]}}),
		OpenSnapshot: func(group string, ref snapshot.Reference) (io.ReadCloser, error) { return source.Open(ref) },
		OnSnapshot:   func(_ string, _ uint64, success bool) { result <- success }})
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	if err := transport.Send("data", fileMessage(t, ref, 1, 2)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-imported:
	case success := <-result:
		t.Fatal("transfer ended before durable import", success)
	case <-time.After(30 * time.Second):
		t.Fatal("file transfer timed out")
	}
	select {
	case <-result:
		t.Fatal("artifact receipt was prematurely ACKed")
	default:
	}
	if server.SnapshotResources().UsedBytes != snapshotWireBufferBytes || transport.ResourceStatus().Snapshots.UsedBytes == 0 {
		t.Fatal("file transfer lease was released before installation")
	}
	if err := transport.Send("data", &raftpb.Message{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)}); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, heartbeat)
	close(allowACK)
	select {
	case success := <-result:
		if !success {
			t.Fatal("durable snapshot reported failure")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no durable snapshot ACK")
	}
	if len(transport.snapshotSlots) != 0 || transport.ResourceStatus().Snapshots.UsedBytes != 0 {
		t.Fatal("completed sender leaked resources")
	}
	reader, err := destination.Open(ref)
	if err != nil {
		t.Fatal(err)
	}
	count, err := io.Copy(io.Discard, reader)
	if closeErr := reader.Close(); err != nil || closeErr != nil || uint64(count) != ref.Size {
		t.Fatal("destination artifact does not verify", count, err, closeErr)
	}
}

func TestFileSnapshotWireRejectsOversizedDeclarationWithoutReceiving(t *testing.T) {
	server := budgetServer(t, &snapshotBackend{})
	stream := snapshotStream(nil)
	stream.header.EncodedSize = snapshot.MaxStreamBytes + uint64(maxSnapshotEnvelope+snapshotWirePrefixSize) + 1
	if err := server.InstallSnapshot(stream); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	select {
	case <-stream.entered:
		t.Fatal("oversized stream consumed data")
	default:
	}
}
