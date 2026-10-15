package raftrpc

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func budgetTransport(t *testing.T) *Transport {
	t.Helper()
	directory := NewStaticDirectory()
	for _, id := range []uint64{2, 3} {
		if err := directory.Update(id, "127.0.0.1:1"); err != nil {
			t.Fatal(err)
		}
	}
	transport, err := NewTransport(TransportOptions{ClusterID: "cluster", RaftID: 1,
		Directory: directory, Credentials: credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13}),
		OnSnapshot: func(string, uint64, bool) {}})
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately leave workers stopped so queue assertions do not depend on
	// network timing. The lane test below separately starts real workers.
	for _, id := range []uint64{2, 3} {
		transport.peers[id] = &peerSender{control: make(chan outbound, 1), queue: make(chan outbound, 1), snapshots: make(chan outbound, 1)}
	}
	t.Cleanup(func() {
		if err := transport.Close(); err != nil {
			t.Error(err)
		}
	})
	return transport
}

func logMessage(to uint64) *raftpb.Message {
	return &raftpb.Message{Type: raftpb.MessageType_MsgApp.Enum(), From: proto.Uint64(1), To: proto.Uint64(to),
		Entries: []*raftpb.Entry{{Index: proto.Uint64(1), Data: []byte("payload")}}}
}

func TestTransportReadIndexUsesReservedControlLane(t *testing.T) {
	for _, kind := range []raftpb.MessageType{raftpb.MessageType_MsgReadIndex, raftpb.MessageType_MsgReadIndexResp} {
		t.Run(kind.String(), func(t *testing.T) {
			transport := budgetTransport(t)
			data := logMessage(2)
			transport.messageBudget = resource.NewByteBudget(uint64(proto.Size(data)))
			if err := transport.Send("data", data); err != nil {
				t.Fatal(err)
			}
			// ReadIndex carries its token in Entries, but it is not log data.
			message := &raftpb.Message{Type: kind.Enum(), From: proto.Uint64(1), To: proto.Uint64(2),
				Entries: []*raftpb.Entry{{Data: []byte("0123456789abcdef")}}}
			if err := transport.Send("data", message); err != nil {
				t.Fatal("read barrier blocked by log-data budget", err)
			}
			if len(transport.peers[2].control) != 1 || transport.ResourceStatus().Control.UsedBytes != uint64(proto.Size(message)) {
				t.Fatal("read barrier did not reserve control bytes")
			}
		})
	}
}

func TestTransportBudgetsAreGlobalAndReleasedOnEveryExit(t *testing.T) {
	transport := budgetTransport(t)
	message := logMessage(2)
	size := uint64(proto.Size(message))
	transport.messageBudget = resource.NewByteBudget(size)
	if err := transport.Send("data", message); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send("data", logMessage(3)); !errors.Is(err, resource.ErrBudgetExceeded) {
		t.Fatal("another peer bypassed node budget", err)
	}
	if got := transport.ResourceStatus().Messages; got.UsedBytes != size || got.Rejected != 1 {
		t.Fatal(got)
	}
	// Empty log data is independent of a saturated log-data budget.
	heartbeat := &raftpb.Message{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)}
	if err := transport.Send("data", heartbeat); err != nil {
		t.Fatal(err)
	}
	controlUsed := transport.ResourceStatus().Control.UsedBytes
	if err := transport.Send("data", heartbeat); err == nil {
		t.Fatal("accepted full control queue")
	}
	if got := transport.ResourceStatus().Control.UsedBytes; got != controlUsed {
		t.Fatal("full queue leaked lease", got)
	}
	if err := transport.Send("bad/group", heartbeat); err == nil {
		t.Fatal("invalid group accepted")
	}
	snapshot := &raftpb.Message{Type: raftpb.MessageType_MsgSnap.Enum(), From: proto.Uint64(1), To: proto.Uint64(2),
		Snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(5)}}}
	transport.snapshotBudget = resource.NewByteBudget(uint64(proto.Size(snapshot)))
	if err := transport.Send("data", snapshot); err != nil {
		t.Fatal(err)
	}
	snapshot.To = proto.Uint64(3)
	if err := transport.Send("data", snapshot); !errors.Is(err, resource.ErrBudgetExceeded) {
		t.Fatal("snapshot peer bypassed node budget", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if err := transport.Send("data", logMessage(3)); err == nil {
		t.Fatal("accepted after close")
	}
	for _, got := range []resource.BudgetStatus{transport.ResourceStatus().Control, transport.ResourceStatus().Messages, transport.ResourceStatus().Snapshots} {
		if got.UsedBytes != 0 {
			t.Fatal("close leaked bytes", got)
		}
	}
}

type laneClient struct {
	raftwire.RaftServiceClient
	dataEntered    chan struct{}
	controlEntered chan struct{}
	allowData      chan struct{}
	dataOnce       sync.Once
}

func (c *laneClient) Step(ctx context.Context, request *raftwire.StepRequest, _ ...grpc.CallOption) (*raftwire.StepResponse, error) {
	var message raftpb.Message
	if err := proto.Unmarshal(request.GetMessage(), &message); err != nil {
		return nil, err
	}
	if len(message.GetEntries()) > 0 {
		c.dataOnce.Do(func() { close(c.dataEntered) })
		select {
		case <-c.allowData:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	} else {
		c.controlEntered <- struct{}{}
	}
	return &raftwire.StepResponse{}, nil
}

func awaitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for worker")
	}
}

func TestTransportControlLaneBypassesBlockedLogAndActiveBytesStayReserved(t *testing.T) {
	transport := budgetTransport(t)
	client := &laneClient{dataEntered: make(chan struct{}), controlEntered: make(chan struct{}, 1), allowData: make(chan struct{})}
	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(transport.options.Credentials))
	if err != nil {
		t.Fatal(err)
	}
	sender := transport.peers[2]
	sender.conn, sender.address, sender.client = conn, "127.0.0.1:1", client
	transport.workers.Add(2)
	go transport.runPeer(2, sender, sender.control)
	go transport.runPeer(2, sender, sender.queue)
	message := logMessage(2)
	transport.messageBudget = resource.NewByteBudget(uint64(proto.Size(message)))
	if err := transport.Send("data", message); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, client.dataEntered)
	if err := transport.Send("data", logMessage(3)); !errors.Is(err, resource.ErrBudgetExceeded) {
		t.Fatal("active request unaccounted", err)
	}
	if err := transport.Send("data", &raftpb.Message{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: proto.Uint64(1), To: proto.Uint64(2)}); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, client.controlEntered)
	// Close cancels the blocked data RPC and must release its active lease.
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if got := transport.ResourceStatus().Messages.UsedBytes; got != 0 {
		t.Fatal("active request leaked on cancel", got)
	}
}

type snapshotTestStream struct {
	grpc.ServerStream
	ctx     context.Context
	header  *raftwire.SnapshotHeader
	frames  chan *raftwire.InstallSnapshotRequest
	entered chan struct{}
	once    sync.Once
	ack     uint64
}

func (s *snapshotTestStream) Context() context.Context { return s.ctx }
func (s *snapshotTestStream) Recv() (*raftwire.InstallSnapshotRequest, error) {
	if s.header != nil {
		header := s.header
		s.header = nil
		return &raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Header{Header: header}}, nil
	}
	s.once.Do(func() { close(s.entered) })
	select {
	case frame, ok := <-s.frames:
		if !ok {
			return nil, io.EOF
		}
		return frame, nil
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}
func (s *snapshotTestStream) SendAndClose(response *raftwire.InstallSnapshotResponse) error {
	s.ack = response.GetAppliedIndex()
	return nil
}

func snapshotStream(data []byte) *snapshotTestStream {
	cert := &x509.Certificate{}
	ctx := peer.NewContext(context.Background(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{
		VerifiedChains: [][]*x509.Certificate{{cert}}, PeerCertificates: []*x509.Certificate{cert}}}})
	digest := sha256.Sum256(data)
	return &snapshotTestStream{ctx: ctx, frames: make(chan *raftwire.InstallSnapshotRequest, 1), entered: make(chan struct{}),
		header: &raftwire.SnapshotHeader{ClusterId: "cluster", GroupId: "data", FromRaftId: 2, ToRaftId: 1,
			EncodedSize: uint64(len(data)), Sha256Digest: digest[:]}}
}

type snapshotBackend struct {
	install func(*raftpb.Message) (uint64, error)
}

func (*snapshotBackend) Step(string, *raftpb.Message) error { return nil }
func (b *snapshotBackend) InstallSnapshot(_ string, m *raftpb.Message) (uint64, error) {
	return b.install(m)
}

func budgetServer(t *testing.T, backend *snapshotBackend) *Server {
	t.Helper()
	server, err := NewServer("cluster", 1, backend, func(context.Context, uint64) error { return nil }, 0)
	if err != nil {
		t.Fatal(err)
	}
	return server
}

func TestSnapshotReceiveLimitsAndCancellation(t *testing.T) {
	server := budgetServer(t, &snapshotBackend{install: func(*raftpb.Message) (uint64, error) { t.Error("incomplete snapshot reached installer"); return 0, nil }})
	server.snapshotBudget = resource.NewByteBudget(8)
	var results []chan error
	var cancels []context.CancelFunc
	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()
	for range maxSnapshotReceivers {
		stream := snapshotStream([]byte("data"))
		ctx, cancel := context.WithCancel(stream.ctx)
		stream.ctx = ctx
		cancels = append(cancels, cancel)
		result := make(chan error, 1)
		results = append(results, result)
		go func() { result <- server.InstallSnapshot(stream) }()
		awaitSignal(t, stream.entered)
	}
	if err := server.InstallSnapshot(snapshotStream([]byte("data"))); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("excess receiver admitted", err)
	}
	if got := server.SnapshotResources().UsedBytes; got != 8 {
		t.Fatal(got)
	}
	// Consensus traffic still passes while all snapshot slots are held.
	message, _ := proto.Marshal(&raftpb.Message{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: proto.Uint64(2), To: proto.Uint64(1)})
	if _, err := server.Step(snapshotStream(nil).ctx, &raftwire.StepRequest{ClusterId: "cluster", GroupId: "data", FromRaftId: 2, ToRaftId: 1, Message: message}); err != nil {
		t.Fatal(err)
	}
	for _, cancel := range cancels {
		cancel()
	}
	for _, result := range results {
		if err := <-result; status.Code(err) != codes.Canceled {
			t.Fatal(err)
		}
	}
	if got := server.SnapshotResources().UsedBytes; got != 0 {
		t.Fatal("cancel leaked bytes", got)
	}
	if len(server.snapshotSlots) != 0 {
		t.Fatal("cancel leaked slot")
	}
	// The byte limit is independent of the concurrency limit, and rejection
	// before allocation must return its slot as well.
	if err := server.InstallSnapshot(snapshotStream([]byte("too large"))); status.Code(err) != codes.ResourceExhausted {
		t.Fatal(err)
	}
	if len(server.snapshotSlots) != 0 || server.SnapshotResources().UsedBytes != 0 {
		t.Fatal("budget rejection leaked resources")
	}
}

func TestSnapshotLeaseHeldUntilInstallerReturns(t *testing.T) {
	entered, allow := make(chan struct{}), make(chan struct{})
	server := budgetServer(t, &snapshotBackend{install: func(m *raftpb.Message) (uint64, error) {
		close(entered)
		<-allow
		return m.GetSnapshot().GetMetadata().GetIndex(), nil
	}})
	data, err := proto.Marshal(&raftpb.Message{Type: raftpb.MessageType_MsgSnap.Enum(), From: proto.Uint64(2), To: proto.Uint64(1),
		Snapshot: &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{Index: proto.Uint64(5), Term: proto.Uint64(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}})
	if err != nil {
		t.Fatal(err)
	}
	stream := snapshotStream(data)
	stream.frames <- &raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: data}}
	close(stream.frames)
	result := make(chan error, 1)
	go func() { result <- server.InstallSnapshot(stream) }()
	awaitSignal(t, entered)
	used, slots := server.SnapshotResources().UsedBytes, len(server.snapshotSlots)
	close(allow)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if used != uint64(len(data)) || slots != 1 {
		t.Fatal("lease released before durable install", used, slots)
	}
	if server.SnapshotResources().UsedBytes != 0 || len(server.snapshotSlots) != 0 || stream.ack != 5 {
		t.Fatal("install did not release/ACK")
	}
}

func TestSnapshotReceiveValidationReleasesResources(t *testing.T) {
	for _, test := range []struct {
		name  string
		frame []byte
		want  codes.Code
	}{
		{"incomplete", nil, codes.InvalidArgument},
		{"oversized", []byte("abcde"), codes.InvalidArgument},
		{"digest", []byte("bad!"), codes.DataLoss},
		{"protobuf", []byte("data"), codes.InvalidArgument},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := budgetServer(t, &snapshotBackend{install: func(*raftpb.Message) (uint64, error) { t.Error("invalid snapshot reached install"); return 0, nil }})
			stream := snapshotStream([]byte("data"))
			if test.frame != nil {
				stream.frames <- &raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{Data: test.frame}}
			}
			close(stream.frames)
			if err := server.InstallSnapshot(stream); status.Code(err) != test.want {
				t.Fatal(err)
			}
			if server.SnapshotResources().UsedBytes != 0 || len(server.snapshotSlots) != 0 {
				t.Fatal("invalid snapshot leaked resources")
			}
		})
	}
}
