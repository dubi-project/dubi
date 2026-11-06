package raftrpc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type queueDeadlineClient struct {
	raftwire.RaftServiceClient
	calls     atomic.Int32
	delivered chan struct{}
	deadline  time.Time
}

func (c *queueDeadlineClient) Step(ctx context.Context, _ *raftwire.StepRequest, _ ...grpc.CallOption) (*raftwire.StepResponse, error) {
	c.calls.Add(1)
	c.deadline, _ = ctx.Deadline()
	c.delivered <- struct{}{}
	return &raftwire.StepResponse{}, nil
}

func TestRegionalControlRPCSharesQueueDeadline(t *testing.T) {
	transport := budgetTransport(t)
	sender := transport.peers[2]
	client := &queueDeadlineClient{delivered: make(chan struct{}, 1)}
	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(transport.options.Credentials))
	if err != nil {
		t.Fatal(err)
	}
	sender.controlLane = peerLane{address: "127.0.0.1:1", conn: conn, client: client}
	before := time.Now()
	if err := transport.Send("regional", &raftpb.Message{Type: raftpb.MessageType_MsgReadIndex.Enum(), From: proto.Uint64(1), To: proto.Uint64(2), Entries: []*raftpb.Entry{{Data: []byte("read-token")}}}); err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	request := <-sender.control
	defer request.release()
	if err := transport.deliverMode(sender, request, true, false); err != nil {
		t.Fatal(err)
	}
	// Delivery may consume the remaining budget but may never restart it.
	if client.deadline.Before(before.Add(transport.options.RPCTimeout)) || client.deadline.After(after.Add(transport.options.RPCTimeout)) {
		t.Fatal("RPC deadline did not start when the read barrier was queued")
	}
}

func TestRegionalControlQueueExpiresBacklogBeforeDelivery(t *testing.T) {
	transport := budgetTransport(t)
	transport.options.RPCTimeout = 30 * time.Millisecond
	sender := transport.peers[2]
	sender.control = make(chan outbound, controlQueueSize)
	client := &queueDeadlineClient{delivered: make(chan struct{}, controlQueueSize+1)}
	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(transport.options.Credentials))
	if err != nil {
		t.Fatal(err)
	}
	sender.controlLane = peerLane{address: "127.0.0.1:1", conn: conn, client: client}
	failed := make(chan struct{}, controlQueueSize)
	transport.options.OnFailure = func(group string, target uint64) {
		if group != "regional" || target != 2 {
			t.Errorf("wrong failure destination: %s/%d", group, target)
		}
		failed <- struct{}{}
	}
	for range controlQueueSize {
		if err := transport.Send("regional", &raftpb.Message{Type: raftpb.MessageType_MsgHeartbeat.Enum(), From: proto.Uint64(1), To: proto.Uint64(2), Term: proto.Uint64(1)}); err != nil {
			t.Fatal(err)
		}
	}
	// Pause the sender longer than its entire delivery budget. Recovery must
	// discard expired heartbeats, not restart a full RPC timeout for each one.
	time.Sleep(2 * transport.options.RPCTimeout)
	transport.workers.Add(1)
	go transport.runPeer(2, sender, sender.control, true)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for range controlQueueSize {
		select {
		case <-failed:
		case <-client.delivered:
			t.Fatal("expired heartbeat reached the RPC client")
		case <-deadline.C:
			t.Fatal("expired backlog did not drain")
		}
	}
	if client.calls.Load() != 0 || transport.ResourceStatus().Control.UsedBytes != 0 {
		t.Fatal("expired backlog retained RPC work or byte reservations")
	}
	// New election traffic must still traverse the same worker after recovery.
	transport.options.RPCTimeout = time.Second
	if err := transport.Send("regional", &raftpb.Message{Type: raftpb.MessageType_MsgVote.Enum(), From: proto.Uint64(1), To: proto.Uint64(2), Term: proto.Uint64(2)}); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, client.delivered)
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	if client.calls.Load() != 1 || transport.ResourceStatus().Control.UsedBytes != 0 {
		t.Fatal("fresh election was lost or leaked its reservation")
	}
}
