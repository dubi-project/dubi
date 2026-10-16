package enginestate

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

var restoreFenceKey = []byte("meta/stream-restore")

// The durable artifact, not a temporary staging DB, is the restart authority.
// This marker is committed atomically with clearing the old logical image.
// Until final publication all public state operations fail closed. Reopening
// advertises AppliedIndex=0 so Raft must reconcile its durable snapshot first.
type restoreFence struct {
	Version int                `json:"version"`
	Ref     snapshot.Reference `json:"reference"`
	Conf    []byte             `json:"conf"`
	StoreID string             `json:"store_id"`
}

func (s *State) packStoreID() string {
	if s.packs == nil {
		return ""
	}
	return s.packs.StoreID()
}

func (s *State) loadRestoreFence() error {
	encoded, found, err := s.get(restoreFenceKey)
	if err != nil || !found {
		return err
	}
	var fence restoreFence
	if len(encoded) > snapshot.MaxHeaderBytes+2048 || decodePackJSON(encoded, &fence) != nil ||
		fence.Version != 1 || fence.Ref.Validate() != nil || fence.Ref.GroupID != s.groupID ||
		fence.Ref.Index <= s.applied || fence.StoreID != s.packStoreID() {
		return errors.New("invalid snapshot restore fence or mismatched destination Store")
	}
	conf := new(raftpb.ConfState)
	if err := proto.Unmarshal(fence.Conf, conf); err != nil {
		return fmt.Errorf("decode snapshot restore fence membership: %w", err)
	}
	s.restoring = &fence
	s.applied = 0
	return nil
}

// RestoreFrom is the snapshot-enabled engine runtime's file API.
// The caller must first persist the SAME reference in its Raft log and
// retain the artifact through recovery. The complete stream (including its
// SHA, membership and logical fences) is checked on a private, disk-backed
// staging DB before changing live state or appending Pack records.
//
// Installation uses bounded batches under a durable restore fence, not an
// image-sized Pebble Batch. Failure after clearing requires reopen + replay
// of the exact reference. Temporary staging costs additional disk IO/space;
// this is deliberately not an in-place format migration or an SST fast path.
func (s *State) RestoreFrom(ctx context.Context, artifacts *snapshot.Store, index uint64, encoded []byte, conf *raftpb.ConfState) (returnErr error) {
	if ctx == nil || artifacts == nil || conf == nil {
		return errors.New("file restore requires context, artifacts and membership")
	}
	ref, recognized, err := snapshot.DecodeReference(encoded)
	if err != nil || !recognized || ref.GroupID != s.groupID || ref.Index != index {
		return errors.Join(err, errors.New("invalid engine snapshot reference"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.failed != nil {
		return errors.New("file restore requires an open, non-failed engine state")
	}
	confBytes, err := proto.Marshal(conf)
	if err != nil {
		return err
	}
	fence := restoreFence{Version: 1, Ref: ref, Conf: confBytes, StoreID: s.packStoreID()}
	if s.restoring != nil {
		pendingConf := new(raftpb.ConfState)
		if proto.Unmarshal(s.restoring.Conf, pendingConf) != nil || s.restoring.Ref != ref ||
			s.restoring.StoreID != fence.StoreID || !proto.Equal(pendingConf, conf) {
			return errors.New("pending snapshot restore must resume its exact durable reference and membership")
		}
	} else if index <= s.applied {
		return errors.New("engine snapshot is stale")
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
		if header.GroupID != s.groupID || header.Index != ref.Index || header.Format != fmt.Sprintf("enginestate-v%d", FormatVersion) ||
			proto.Unmarshal(header.Conf, storedConf) != nil || !proto.Equal(storedConf, conf) {
			return errors.New("engine stream metadata does not match Raft snapshot")
		}
		return nil
	})
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, staged.Close()) }()
	view := staged.View()
	if err := s.validateRegionSnapshot(view, index, conf); err != nil {
		return err
	}
	if err := snapshot.Walk(ctx, view, func(key, value []byte) error {
		return s.validateStreamEntry(view, index, key, value)
	}); err != nil {
		return err
	}
	if err := s.restoreFault("after-validation"); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// All preflight validation is complete. Every error from here onwards
	// poisons this process's view, even if the final commit actually landed.
	defer func() {
		if returnErr != nil {
			s.failed = returnErr
		}
	}()
	clear := s.db.NewBatch()
	defer clear.Close()
	for _, bounds := range [][2][]byte{{[]byte("data/"), []byte("data0")}, {[]byte("result/"), []byte("result0")}, {packRefPrefix, packRefEnd}} {
		if err := clear.DeleteRange(bounds[0], bounds[1], nil); err != nil {
			return err
		}
	}
	if err := clear.Delete(packBindingKey, nil); err != nil {
		return err
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
		// NoSync is safe only behind the durable fence; final pebble.Sync
		// publishes the image and synchronizes preceding WAL writes too.
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
		if bytes.HasPrefix(key, valuePrefix) {
			return s.stageValue(batch, key, value)
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
