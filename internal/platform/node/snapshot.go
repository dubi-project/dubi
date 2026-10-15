package node

import (
	"context"
	"errors"
	"io"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
)

type snapshotStepper struct {
	*raft.Host
	runtime *Runtime
}

var _ raftrpc.SnapshotStreamInstaller = snapshotStepper{}

// Import runs outside Host/Group locks. Only the verified, durable reference
// then enters Raft; its state restore completes before returning an ACK index.
// Artifact GC is deliberately disabled, including orphaned failed imports.
func (stepper snapshotStepper) InstallSnapshotStream(ctx context.Context, groupID string, message *raftpb.Message, reader io.Reader) (uint64, error) {
	if ctx == nil || reader == nil || message == nil || message.GetType() != raftpb.MessageType_MsgSnap {
		return 0, errors.New("file snapshot requires context, reader and MsgSnap")
	}
	runtime := stepper.runtime
	if !stepper.Host.HasGroup(groupID) {
		return 0, errors.New("snapshot Group is not open")
	}
	// Both formal Node adapters recover durable references. Unknown Groups
	// must not persist a format their current state adapter cannot replay.
	if groupID != raft.RootGroupID {
		if _, found := runtime.Capabilities.Resolve(groupID); !found {
			return 0, errors.New("Group has no file snapshot state adapter")
		}
	}
	ref, recognized, err := snapshot.DecodeReference(message.GetSnapshot().GetData())
	if err != nil || !recognized || ref.GroupID != groupID || ref.Index != message.GetSnapshot().GetMetadata().GetIndex() {
		return 0, errors.Join(err, errors.New("file snapshot reference does not match Group/cut"))
	}
	importContext, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(runtime.recoveryContext, cancel)
	defer stop()
	defer cancel()
	if err := runtime.Snapshots.Import(importContext, ref, reader); err != nil {
		return 0, err
	}
	if err := importContext.Err(); err != nil {
		return 0, err
	}
	// Once Raft saves the incoming reference, recovery belongs to the Node
	// lifecycle, not this RPC deadline. A disconnect cannot roll it back.
	return stepper.Host.InstallSnapshot(groupID, message)
}

// OpenSnapshot is the transport's local artifact opener. Its result owns an
// independent FD and verifies complete bytes on Read/Close.
func (runtime *Runtime) OpenSnapshot(groupID string, ref snapshot.Reference) (io.ReadCloser, error) {
	if runtime == nil || runtime.Snapshots == nil || groupID != ref.GroupID {
		return nil, errors.New("invalid snapshot artifact source")
	}
	return runtime.Snapshots.Open(ref)
}
