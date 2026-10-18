package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/pack"
	"go.etcd.io/raft/v3/raftpb"
)

type RestoreGroupSnapshot struct {
	GroupID string
	Index   uint64
	Term    uint64
	Data    []byte
	Conf    *raftpb.ConfState
}

type RestorePackImage struct {
	VolumeID string
	PackID   string
	Reader   io.Reader
	Size     int64
}

// RestorePlacement supplies the only topology-specific part of physical
// restore.  The base layer keeps the old Volume identity as an input and the
// coordinator chooses the destination Volume/Store mapping for the new Node.
type RestorePlacement struct {
	Volume   func(string) (string, error)
	Location func(pack.Location) (pack.Location, error)
}

// ImportRestoreArtifact routes the two node-local physical artifacts emitted
// by the backup worker.  Root/Engine state artifacts remain Raft snapshot
// inputs and are deliberately handled by InstallRestoreSnapshots instead.
func (runtime *Runtime) ImportRestoreArtifact(ctx context.Context, name, kind string, reader io.Reader, size int64, placement RestorePlacement) error {
	if runtime == nil || reader == nil || strings.TrimSpace(name) != name || name == "" {
		return errors.New("invalid restore artifact")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	switch kind {
	case "pack":
		parts := strings.Split(name, "/")
		if len(parts) != 4 || parts[0] != "volume" || parts[2] != "pack" || placement.Volume == nil {
			return errors.New("invalid Pack restore artifact name")
		}
		volumeID, err := placement.Volume(parts[1])
		if err != nil {
			return err
		}
		return runtime.ImportRestorePacks(ctx, []RestorePackImage{{VolumeID: volumeID, PackID: parts[3], Reader: reader, Size: size}})
	case "records-index":
		if name != "records/index" || placement.Location == nil || size < 0 || size > 1<<30 {
			return errors.New("invalid records index restore artifact")
		}
		image, err := io.ReadAll(io.LimitReader(reader, size+1))
		if err != nil {
			return err
		}
		if int64(len(image)) != size {
			return errors.New("records index restore size differs from manifest")
		}
		return runtime.RestoreRecordIndex(ctx, image, placement.Location)
	default:
		return fmt.Errorf("restore artifact kind %q is not installed by Node Runtime", kind)
	}
}

// ImportRestorePacks installs node-local sealed Pack images before any
// records index or Engine snapshot is applied.  VolumeID is the destination
// mapping chosen by the restore coordinator; this method never guesses one.
func (runtime *Runtime) ImportRestorePacks(ctx context.Context, images []RestorePackImage) error {
	if runtime == nil || runtime.Packs == nil || len(images) == 0 {
		return errors.New("restore Pack images and Runtime are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ordered := append([]RestorePackImage(nil), images...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].VolumeID != ordered[j].VolumeID {
			return ordered[i].VolumeID < ordered[j].VolumeID
		}
		return ordered[i].PackID < ordered[j].PackID
	})
	for _, image := range ordered {
		if err := ctx.Err(); err != nil {
			return err
		}
		if image.Reader == nil || image.VolumeID == "" || image.PackID == "" || image.Size <= 0 {
			return errors.New("invalid restore Pack image")
		}
		if err := runtime.Packs.ImportSealed(ctx, image.VolumeID, image.PackID, image.Reader, image.Size); err != nil {
			return fmt.Errorf("import Pack %s on Volume %s: %w", image.PackID, image.VolumeID, err)
		}
	}
	return nil
}

// RestoreRecordIndex installs the portable immutable-record index after its
// Pack images have been imported.  The location mapper owns old-to-new
// Store/Volume placement and must retain PackID and physical offsets.
func (runtime *Runtime) RestoreRecordIndex(ctx context.Context, image []byte, mapLocation func(pack.Location) (pack.Location, error)) error {
	if runtime == nil || runtime.Records == nil {
		return errors.New("restore records index and Runtime are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return runtime.Records.RestoreIndex(ctx, image, mapLocation)
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
