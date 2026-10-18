package node

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"go.etcd.io/raft/v3/raftpb"
)

type RestoreGroupSnapshot struct {
	GroupID string
	Index   uint64
	Term    uint64
	Data    []byte
	Conf    *raftpb.ConfState
}

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

// InstallRestoreSnapshots installs a Root snapshot first, then opens local
// Engine Groups from the restored Root catalog and installs their snapshots.
// The caller must have bootstrapped the destination Nodes and Root quorum.
func (runtime *Runtime) InstallRestoreSnapshots(ctx context.Context, snapshots []RestoreGroupSnapshot) error {
	if runtime == nil || runtime.Root == nil || runtime.Host == nil || len(snapshots) == 0 {
		return errors.New("Cluster Restore snapshots and Runtime are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ordered := append([]RestoreGroupSnapshot(nil), snapshots...)
	sort.SliceStable(ordered, func(i, j int) bool {
		leftRoot := ordered[i].GroupID == raft.RootGroupID
		rightRoot := ordered[j].GroupID == raft.RootGroupID
		if leftRoot != rightRoot {
			return leftRoot
		}
		return ordered[i].GroupID < ordered[j].GroupID
	})
	for _, snapshot := range ordered {
		if err := ctx.Err(); err != nil {
			return err
		}
		if snapshot.GroupID != raft.RootGroupID && !runtime.Host.HasGroup(snapshot.GroupID) {
			if err := runtime.RecoverLocalGroups(ctx); err != nil {
				return fmt.Errorf("open restored Engine Groups: %w", err)
			}
		}
		if !runtime.Host.HasGroup(snapshot.GroupID) {
			return fmt.Errorf("restore Group %q is not open", snapshot.GroupID)
		}
		if err := runtime.InstallRestoreSnapshot(ctx, snapshot.GroupID, snapshot.Index, snapshot.Term, snapshot.Data, snapshot.Conf); err != nil {
			return fmt.Errorf("install restored Group %q: %w", snapshot.GroupID, err)
		}
	}
	return nil
}
