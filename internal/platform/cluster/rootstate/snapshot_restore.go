package rootstate

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var restoreFenceKey = []byte("meta/stream-restore")

type restoreFence struct {
	Version int                `json:"version"`
	Ref     snapshot.Reference `json:"reference"`
	Conf    []byte             `json:"conf"`
}

func (s *State) loadRestoreFence() error {
	encoded, found, err := s.get(restoreFenceKey)
	if err != nil || !found {
		return err
	}
	var fence restoreFence
	if len(encoded) > snapshot.MaxHeaderBytes+2048 || decodeStreamJSON(encoded, &fence) != nil ||
		fence.Version != 1 || fence.Ref.Validate() != nil || fence.Ref.GroupID != raft.RootGroupID ||
		fence.Ref.Index <= s.applied {
		return errors.New("invalid Root snapshot restore fence")
	}
	if err := proto.Unmarshal(fence.Conf, new(raftpb.ConfState)); err != nil {
		return fmt.Errorf("Root restore membership: %w", err)
	}
	s.restoring, s.applied = &fence, 0
	return nil
}

// RestoreFrom requires this exact reference already be durable in the Raft
// log. Full artifact and directory validation precede any live mutation.
// Bounded installation is hidden behind a synced fence; failures after that
// fence require reopening and replaying the SAME reference and membership.
func (s *State) RestoreFrom(ctx context.Context, artifacts *snapshot.Store, index uint64, encoded []byte, conf *raftpb.ConfState) (returnErr error) {
	if ctx == nil || artifacts == nil || conf == nil {
		return errors.New("Root file restore requires context, artifacts and membership")
	}
	ref, recognized, err := snapshot.DecodeReference(encoded)
	if err != nil || !recognized || ref.GroupID != raft.RootGroupID || ref.Index != index {
		return errors.Join(err, errors.New("invalid Root snapshot reference"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed != nil {
		return errors.New("Root file restore requires open, non-failed state")
	}
	confBytes, err := proto.Marshal(conf)
	if err != nil {
		return err
	}
	fence := restoreFence{Version: 1, Ref: ref, Conf: confBytes}
	if s.restoring != nil {
		pending := new(raftpb.ConfState)
		if proto.Unmarshal(s.restoring.Conf, pending) != nil || s.restoring.Ref != ref || !proto.Equal(pending, conf) {
			return errors.New("pending Root restore must resume its exact reference and membership")
		}
	} else if index <= s.applied {
		return errors.New("Root file snapshot is stale")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	disk, err := diskpressure.Open(s.root)
	if err != nil {
		return err
	}
	defer disk.Close()
	if err := disk.Check(); err != nil {
		return err
	}
	staged, err := snapshot.Stage(ctx, s.root, artifacts, ref, func(header snapshot.Header) error {
		storedConf := new(raftpb.ConfState)
		if header.GroupID != raft.RootGroupID || header.Index != index || header.Format != fmt.Sprintf("rootstate-v%d", FormatVersion) ||
			proto.Unmarshal(header.Conf, storedConf) != nil || !proto.Equal(storedConf, conf) {
			return errors.New("Root stream metadata does not match Raft snapshot")
		}
		return nil
	})
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, staged.Close()) }()
	view := staged.View()
	if err := snapshot.Walk(ctx, view, func(key, value []byte) error { return validateStreamEntry(view, index, key, value) }); err != nil {
		return err
	}
	if err := s.restoreFault("after-validation"); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	defer func() {
		if returnErr != nil {
			s.failed = returnErr
		}
	}()
	clear := s.db.NewBatch()
	defer clear.Close()
	for _, prefix := range []string{"data", "result"} {
		if err := clear.DeleteRange([]byte(prefix+"/"), []byte(prefix+"0"), nil); err != nil {
			return err
		}
	}
	if err := setJSON(clear, restoreFenceKey, fence); err != nil {
		return err
	}
	if err := s.commitBatch(clear); err != nil {
		return err
	}
	s.restoring = &fence
	if err := s.restoreFault("after-fence"); err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	flush := func() error {
		if batch.Count() == 0 {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := disk.Check(); err != nil {
			return err
		}
		// Final Sync synchronizes all earlier WAL batches before deleting fence.
		if err := batch.Commit(pebble.NoSync); err != nil {
			return err
		}
		batch.Reset()
		return s.restoreFault("after-batch")
	}
	if err := snapshot.Walk(ctx, view, func(key, value []byte) error {
		if uint64(batch.Len())+uint64(len(key)+len(value)) > snapshot.StagingBatchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		return batch.Set(key, value, nil)
	}); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if err := s.restoreFault("before-final"); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := batch.Set(confKey, confBytes, nil); err != nil {
		return err
	}
	var applied [8]byte
	binary.BigEndian.PutUint64(applied[:], index)
	if err := batch.Set(appliedKey, applied[:], nil); err != nil {
		return err
	}
	if err := batch.Delete(restoreFenceKey, nil); err != nil {
		return err
	}
	if err := s.commitBatch(batch); err != nil {
		return err
	}
	s.applied, s.conf, s.restoring = index, proto.Clone(conf).(*raftpb.ConfState), nil
	return s.restoreFault("after-final")
}

func (s *State) restoreFault(stage string) error {
	if s.restoreHook != nil {
		return s.restoreHook(stage)
	}
	return nil
}
