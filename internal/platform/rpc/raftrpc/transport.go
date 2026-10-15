package raftrpc

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
)

const (
	DefaultRPCTimeout    = 2 * time.Second
	DefaultPeerQueueSize = 256
	controlQueueSize     = 64
	controlBudgetBytes   = 8 << 20
	messageBudgetBytes   = 64 << 20
	snapshotBudgetBytes  = 128 << 20
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
	// OpenSnapshot opens the node-owned immutable artifact. The reader must
	// verify its digest and report incomplete/bad reads on Close as well.
	// Keep its owning Store alive until Transport.Close has completed.
	OpenSnapshot func(groupID string, reference snapshot.Reference) (io.ReadCloser, error)
}

type outbound struct {
	groupID   string
	to        uint64
	data      []byte
	index     uint64
	release   func()
	reference *snapshot.Reference
}

type peerSender struct {
	mu        sync.Mutex // Shared connection, used concurrently by control/data lanes.
	control   chan outbound
	queue     chan outbound
	snapshots chan outbound
	address   string
	conn      *grpc.ClientConn
	client    raftwire.RaftServiceClient
}

// Transport separates consensus control, log data and snapshot workers per
// peer. Queued AND active wire buffers share lane budgets across all peers.
type Transport struct {
	mu             sync.Mutex
	options        TransportOptions
	peers          map[uint64]*peerSender
	ctx            context.Context
	cancel         context.CancelFunc
	workers        sync.WaitGroup
	closed         bool
	controlBudget  *resource.ByteBudget
	messageBudget  *resource.ByteBudget
	snapshotBudget *resource.ByteBudget
	snapshotSlots  chan struct{}
	snapshotLimits func(uint64) snapshotDeadlines // nil uses automatic policy; test override before Send.
}

type TransportResources struct {
	Control   resource.BudgetStatus `json:"control"`
	Messages  resource.BudgetStatus `json:"messages"`
	Snapshots resource.BudgetStatus `json:"snapshots"`
}

func (t *Transport) ResourceStatus() TransportResources {
	return TransportResources{Control: t.controlBudget.Status(), Messages: t.messageBudget.Status(), Snapshots: t.snapshotBudget.Status()}
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
	return &Transport{options: options, peers: make(map[uint64]*peerSender), ctx: ctx, cancel: cancel,
		controlBudget: resource.NewByteBudget(controlBudgetBytes), messageBudget: resource.NewByteBudget(messageBudgetBytes),
		snapshotBudget: resource.NewByteBudget(snapshotBudgetBytes), snapshotSlots: make(chan struct{}, maxSnapshotSenders)}, nil
}

func (t *Transport) Send(groupID string, message *raftpb.Message) error {
	if err := raft.ValidateGroupID(groupID); err != nil {
		return err
	}
	if message == nil || message.GetFrom() != t.options.RaftID || message.GetTo() == 0 || message.GetTo() == t.options.RaftID {
		return errors.New("Raft message has an invalid sender or target")
	}
	// Size and reserve before allocating another serialized copy. The caller
	// owns message and must not mutate it concurrently with Send.
	size := proto.Size(message)
	isSnapshot := message.GetType() == raftpb.MessageType_MsgSnap
	if isSnapshot && (message.GetSnapshot() == nil || size > DefaultMaxSnapshotSize || t.options.OnSnapshot == nil) {
		return errors.New("Raft snapshot exceeds inline limit or has no completion reporter")
	}
	if !isSnapshot && size > t.options.MaxSize {
		return fmt.Errorf("encoded Raft message exceeds %d-byte limit", t.options.MaxSize)
	}
	var reference *snapshot.Reference
	if isSnapshot {
		ref, recognized, err := snapshot.DecodeReference(message.GetSnapshot().GetData())
		if err != nil {
			return err
		}
		if recognized {
			if ref.GroupID != groupID || ref.Index != message.GetSnapshot().GetMetadata().GetIndex() ||
				size > maxSnapshotEnvelope || t.options.OpenSnapshot == nil {
				return errors.New("file snapshot reference does not match cut, envelope bound or artifact opener")
			}
			reference = &ref
		}
	}
	if _, ok := t.options.Directory.Resolve(message.GetTo()); !ok {
		return fmt.Errorf("Raft peer %d has no address", message.GetTo())
	}
	// ReadIndex tokens live in Entries, but are consensus control traffic, not
	// replicated log payloads. They must retain headroom during catch-up.
	control := !isSnapshot && (len(message.GetEntries()) == 0 ||
		message.GetType() == raftpb.MessageType_MsgReadIndex || message.GetType() == raftpb.MessageType_MsgReadIndexResp)
	budget := t.messageBudget
	if isSnapshot {
		budget = t.snapshotBudget
	} else if control {
		budget = t.controlBudget
	}
	reserved := uint64(size)
	if reference != nil {
		reserved += snapshotWireBufferBytes
	}
	releaseBytes, err := budget.TryAcquire(reserved)
	if err != nil {
		return err
	}
	release := releaseBytes
	if reference != nil {
		select {
		case t.snapshotSlots <- struct{}{}:
			var once sync.Once
			release = func() { once.Do(func() { <-t.snapshotSlots; releaseBytes() }) }
		default:
			releaseBytes()
			return errors.New("file snapshot sender is busy")
		}
	}
	queued := false
	defer func() {
		if !queued {
			release()
		}
	}()
	data, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("encode Raft message: %w", err)
	}
	if len(data) != size {
		return errors.New("Raft message changed during serialization")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return errors.New("Raft transport is closed")
	}
	peer := t.peers[message.GetTo()]
	if peer == nil {
		peer = &peerSender{control: make(chan outbound, controlQueueSize), queue: make(chan outbound, t.options.QueueSize), snapshots: make(chan outbound, 1)}
		t.peers[message.GetTo()] = peer
		t.workers.Add(3)
		go t.runPeer(message.GetTo(), peer, peer.control)
		go t.runPeer(message.GetTo(), peer, peer.queue)
		go t.runSnapshots(message.GetTo(), peer)
	}
	request := outbound{groupID: groupID, to: message.GetTo(), data: data, release: release, reference: reference}
	if isSnapshot {
		request.index = message.GetSnapshot().GetMetadata().GetIndex()
		select {
		case peer.snapshots <- request:
			queued = true
			return nil
		default:
			return fmt.Errorf("Raft peer %d snapshot queue is full", message.GetTo())
		}
	}
	queue := peer.queue
	if control {
		queue = peer.control
	}
	select {
	case queue <- request:
		queued = true
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
			request.data = nil
			request.release()
			t.options.OnSnapshot(request.groupID, raftID, err == nil)
		}
	}
}

func (t *Transport) deliverSnapshot(request outbound) (returnErr error) {
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
	size := uint64(len(request.data))
	if request.reference != nil {
		size += request.reference.Size + uint64(snapshotWirePrefixSize)
	}
	limits := deadlinesForSnapshot(size)
	if t.snapshotLimits != nil {
		limits = t.snapshotLimits(size)
	}
	progress := newSnapshotProgress(t.ctx, limits)
	defer func() {
		if returnErr != nil && progress.ctx.Err() != nil {
			returnErr = errors.Join(returnErr, context.Cause(progress.ctx))
		}
		progress.Close()
	}()
	rawStream, err := raftwire.NewRaftServiceClient(conn).InstallSnapshot(progress.ctx)
	if err != nil {
		return err
	}
	stream := &progressSnapshotClient{RaftService_InstallSnapshotClient: rawStream, progress: progress}
	if request.reference != nil {
		return t.deliverFileSnapshot(stream, request)
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

func (t *Transport) runPeer(raftID uint64, peer *peerSender, queue <-chan outbound) {
	defer t.workers.Done()
	for {
		select {
		case <-t.ctx.Done():
			return
		case request := <-queue:
			err := t.deliver(peer, request)
			request.data = nil
			request.release()
			if err != nil && t.options.OnFailure != nil {
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
	peer.mu.Lock()
	if peer.conn == nil || peer.address != address {
		if peer.conn != nil {
			_ = peer.conn.Close()
		}
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(t.options.Credentials),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(t.options.MaxSize+1024)))
		if err != nil {
			peer.conn = nil
			peer.mu.Unlock()
			return err
		}
		peer.address, peer.conn, peer.client = address, conn, raftwire.NewRaftServiceClient(conn)
	}
	client := peer.client
	peer.mu.Unlock()
	ctx, cancel := context.WithTimeout(t.ctx, t.options.RPCTimeout)
	defer cancel()
	_, err := client.Step(ctx, &raftwire.StepRequest{ClusterId: t.options.ClusterID,
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
		// No worker or Send can own queued requests after the barrier above.
		for _, queue := range []chan outbound{peer.control, peer.queue, peer.snapshots} {
			for len(queue) > 0 {
				request := <-queue
				request.release()
			}
		}
		if peer.conn != nil {
			failures = append(failures, peer.conn.Close())
		}
	}
	return errors.Join(failures...)
}
