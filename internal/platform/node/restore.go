package node

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

// InstallRestoreSnapshot installs one already verified Cluster Backup
// artifact through the normal Raft snapshot path. The caller must have
// started the new cluster with the destination membership and opened the
// corresponding Group; this method never edits Raft log files directly.
func (runtime *Runtime) InstallRestoreSnapshot(ctx context.Context, groupID string, index, term uint64, data []byte, conf *raftpb.ConfState) error {
	if runtime == nil || runtime.Host == nil || runtime.Identity.RaftID == 0 || raft.ValidateGroupID(groupID) != nil || index == 0 || term == 0 || len(data) == 0 || conf == nil {
		return errors.New("invalid Cluster Restore snapshot")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	messageType := raftpb.MessageType_MsgSnap
	raftID := runtime.Identity.RaftID
	message := &raftpb.Message{Type: &messageType, From: &raftID, To: &raftID,
		Snapshot: &raftpb.Snapshot{Data: data, Metadata: &raftpb.SnapshotMetadata{Index: &index, Term: &term, ConfState: conf}}}
	_, err := runtime.Host.InstallSnapshot(groupID, message)
	return err
}
