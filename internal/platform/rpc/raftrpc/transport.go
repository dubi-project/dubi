package raftrpc

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultRPCTimeout    = 2 * time.Second
	DefaultPeerQueueSize = 256
)

type TransportOptions struct {
	ClusterID   string
	RaftID      uint64
	Directory   Directory
	Credentials credentials.TransportCredentials
	RPCTimeout  time.Duration
	QueueSize   int
	MaxSize     int
	// OnFailure is invoked after an asynchronous delivery fails. The caller
	// should report the destination unreachable to the owning Raft Group.
	OnFailure func(groupID string, raftID uint64)
	// OnSnapshot reports the durable installation result to Raft.
	OnSnapshot func(groupID string, raftID uint64, success bool)
}

type outbound struct {
	groupID string
	to      uint64
	data    []byte
	index   uint64
}

type peerSender struct {
	queue     chan outbound
	snapshots chan outbound
	address   string
	conn      *grpc.ClientConn
	client    raftwire.RaftServiceClient
}

// Transport shares one bounded sender queue per remote Node across Groups.
// It does not send snapshots until durable streamed installation exists.
type Transport struct {
	mu      sync.Mutex
	options TransportOptions
	peers   map[uint64]*peerSender
	ctx     context.Context
	cancel  context.CancelFunc
	workers sync.WaitGroup
	closed  bool
}

func NewTransport(options TransportOptions) (*Transport, error) {
	if options.ClusterID == "" || options.RaftID == 0 || options.Directory == nil || options.Credentials == nil {
		return nil, errors.New("Raft transport requires ClusterID, RaftID, Directory and TLS credentials")
	}
	if options.Credentials.Info().SecurityProtocol != "tls" {
		return nil, errors.New("Raft transport requires TLS credentials")
	}
	if options.RPCTimeout <= 0 {
		options.RPCTimeout = DefaultRPCTimeout
	}
	if options.QueueSize <= 0 {
		options.QueueSize = DefaultPeerQueueSize
	}
	if options.MaxSize <= 0 {
		options.MaxSize = DefaultMaxMessageSize
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Transport{options: options, peers: make(map[uint64]*peerSender), ctx: ctx, cancel: cancel}, nil
}

func (t *Transport) Send(groupID string, message *raftpb.Message) error {
	if err := raft.ValidateGroupID(groupID); err != nil {
		return err
	}
	if message == nil || message.GetFrom() != t.options.RaftID || message.GetTo() == 0 || message.GetTo() == t.options.RaftID {
		return errors.New("Raft message has an invalid sender or target")
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("encode Raft message: %w", err)
	}
	isSnapshot := message.GetType() == raftpb.MessageType_MsgSnap
	if isSnapshot && (message.GetSnapshot() == nil || len(data) > DefaultMaxSnapshotSize || t.options.OnSnapshot == nil) {
		return errors.New("Raft snapshot exceeds inline limit or has no completion reporter")
	}
	if !isSnapshot && len(data) > t.options.MaxSize {
		return fmt.Errorf("encoded Raft message exceeds %d-byte limit", t.options.MaxSize)
	}
	if _, ok := t.options.Directory.Resolve(message.GetTo()); !ok {
		return fmt.Errorf("Raft peer %d has no address", message.GetTo())
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errors.New("Raft transport is closed")
	}
	peer := t.peers[message.GetTo()]
	if peer == nil {
		peer = &peerSender{queue: make(chan outbound, t.options.QueueSize), snapshots: make(chan outbound, 1)}
		t.peers[message.GetTo()] = peer
		t.workers.Add(2)
		go t.runPeer(message.GetTo(), peer)
		go t.runSnapshots(message.GetTo(), peer)
	}
	request := outbound{groupID: groupID, to: message.GetTo(), data: data}
	if isSnapshot {
		request.index = message.GetSnapshot().GetMetadata().GetIndex()
		select {
		case peer.snapshots <- request:
			return nil
		default:
			return fmt.Errorf("Raft peer %d snapshot queue is full", message.GetTo())
		}
	}
	select {
	case peer.queue <- request:
		return nil
	default:
		return fmt.Errorf("Raft peer %d queue is full", message.GetTo())
	}
}

func (t *Transport) runSnapshots(raftID uint64, peer *peerSender) {
	defer t.workers.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case request := <-peer.snapshots:
			err := t.deliverSnapshot(request)
			t.options.OnSnapshot(request.groupID, raftID, err == nil)
		}
	}
}

func (t *Transport) deliverSnapshot(request outbound) error {
	address, ok := t.options.Directory.Resolve(request.to)
	if !ok {
		return fmt.Errorf("Raft peer %d has no address", request.to)
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(t.options.Credentials),
		grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(DefaultSnapshotFrame+1024)))
	if err != nil {
		return err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.ctx, 2*time.Minute)
	defer cancel()
	stream, err := raftwire.NewRaftServiceClient(conn).InstallSnapshot(ctx)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(request.data)
	if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Header{
		Header: &raftwire.SnapshotHeader{ClusterId: t.options.ClusterID, GroupId: request.groupID,
			FromRaftId: t.options.RaftID, ToRaftId: request.to, EncodedSize: uint64(len(request.data)),
			Sha256Digest: digest[:]}}}); err != nil {
		return err
	}
	for offset := 0; offset < len(request.data); offset += DefaultSnapshotFrame {
		end := min(offset+DefaultSnapshotFrame, len(request.data))
		if err := stream.Send(&raftwire.InstallSnapshotRequest{Payload: &raftwire.InstallSnapshotRequest_Data{
			Data: request.data[offset:end]}}); err != nil {
			return err
		}
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	if response.GetAppliedIndex() < request.index {
		return errors.New("Raft snapshot ACK is behind sent index")
	}
	return nil
}

func (t *Transport) runPeer(raftID uint64, peer *peerSender) {
	defer t.workers.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case request := <-peer.queue:
			if err := t.deliver(peer, request); err != nil && t.options.OnFailure != nil {
				t.options.OnFailure(request.groupID, raftID)
			}
		}
	}
}

func (t *Transport) deliver(peer *peerSender, request outbound) error {
	address, ok := t.options.Directory.Resolve(request.to)
	if !ok {
		return fmt.Errorf("Raft peer %d has no address", request.to)
	}
	if peer.conn == nil || peer.address != address {
		if peer.conn != nil {
			_ = peer.conn.Close()
		}
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(t.options.Credentials),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(t.options.MaxSize+1024)))
		if err != nil {
			peer.conn = nil
			return err
		}
		peer.address, peer.conn, peer.client = address, conn, raftwire.NewRaftServiceClient(conn)
	}
	ctx, cancel := context.WithTimeout(t.ctx, t.options.RPCTimeout)
	defer cancel()
	_, err := peer.client.Step(ctx, &raftwire.StepRequest{ClusterId: t.options.ClusterID,
		GroupId: request.groupID, FromRaftId: t.options.RaftID, ToRaftId: request.to,
		Message: request.data})
	return err
}

func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.cancel()
	t.mu.Unlock()
	t.workers.Wait()
	var failures []error
	for _, peer := range t.peers {
		if peer.conn != nil {
			failures = append(failures, peer.conn.Close())
		}
	}
	return errors.Join(failures...)
}
