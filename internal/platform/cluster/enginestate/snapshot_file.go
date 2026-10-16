package enginestate

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// PrepareSnapshotTo captures a logical Pebble cut without holding the state
// lock during file IO. The caller owns artifacts and the Pack Pool, keeping
// both alive until the returned preparation is closed. Snapshot-enabled
// engine runtimes connect it to Raft's two-phase SnapshotPreparer; legacy
// standalone adapters retain their bounded inline format.
func (s *State) PrepareSnapshotTo(artifacts *snapshot.Store) (*raft.PreparedSnapshot, error) {
	if artifacts == nil {
		return nil, errors.New("snapshot artifact Store is required")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	if s.applied == 0 {
		return nil, errors.New("cannot capture an unapplied engine state")
	}
	conf, err := proto.Marshal(s.conf)
	if err != nil {
		return nil, err
	}
	index := s.applied
	capturedConf := proto.Clone(s.conf).(*raftpb.ConfState)
	view := s.db.NewSnapshot()
	s.snapshotWG.Add(1)
	var mu sync.Mutex
	var closed, built bool
	var closeErr error
	return &raft.PreparedSnapshot{Index: index, Conf: proto.Clone(s.conf).(*raftpb.ConfState),
		Build: func(ctx context.Context) ([]byte, error) {
			mu.Lock()
			defer mu.Unlock()
			if closed || built {
				return nil, errors.New("engine snapshot preparation is closed or already built")
			}
			built = true
			header := snapshot.Header{Version: snapshot.StreamVersion, Format: fmt.Sprintf("enginestate-v%d", FormatVersion),
				GroupID: s.groupID, Index: index, Conf: conf}
			ref, err := artifacts.Create(ctx, header, snapshot.MaxStreamBytes, func(encoder *snapshot.Encoder) error {
				if err := s.validateRegionSnapshot(view, index, capturedConf); err != nil {
					return err
				}
				return s.writeSnapshotView(view, index, encoder)
			})
			if err != nil {
				return nil, err
			}
			return ref.Encode()
		}, Close: func() error {
			mu.Lock()
			defer mu.Unlock()
			if !closed {
				closed = true
				closeErr = view.Close()
				s.snapshotWG.Done()
			}
			return closeErr
		}}, nil
}

// Merge inline values and references by their LOGICAL key. Physical placement
// cannot affect stream ordering or digest, and a conflicting duplicate fails.
func (s *State) writeSnapshotView(view *pebble.Snapshot, index uint64, encoder *snapshot.Encoder) (returnErr error) {
	inline, err := view.NewIter(&pebble.IterOptions{LowerBound: []byte("data/"), UpperBound: []byte("data0")})
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, inline.Close()) }()
	refs, err := view.NewIter(&pebble.IterOptions{LowerBound: packRefPrefix, UpperBound: packRefEnd})
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, refs.Close()) }()
	a, b := inline.First(), refs.First()
	for a || b {
		var refKey []byte
		if b {
			refKey = refs.Key()[len(packRefPrefix):]
		}
		if a && b && bytes.Equal(inline.Key(), refKey) {
			return errors.New("snapshot has conflicting inline and Pack values")
		}
		var key, value []byte
		useInline := a && (!b || bytes.Compare(inline.Key(), refKey) < 0)
		if useInline {
			key, value = inline.Key(), inline.Value()
		} else {
			record, _, err := s.readReference(refKey, refs.Value())
			if err != nil {
				return err
			}
			key, value = refKey, record.Payload
		}
		if err := s.validateStreamEntry(view, index, key, value); err != nil {
			return err
		}
		if err := encoder.Add(key, value); err != nil {
			return err
		}
		if useInline {
			a = inline.Next()
		} else {
			b = refs.Next()
		}
	}
	if err := errors.Join(inline.Error(), refs.Error()); err != nil {
		return err
	}
	results, err := view.NewIter(&pebble.IterOptions{LowerBound: []byte("result/"), UpperBound: []byte("result0")})
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, results.Close()) }()
	for results.First(); results.Valid(); results.Next() {
		if err := s.validateStreamEntry(view, index, results.Key(), results.Value()); err != nil {
			return err
		}
		if err := encoder.Add(results.Key(), results.Value()); err != nil {
			return err
		}
	}
	return results.Error()
}

// Semantic checks use bounded point lookups in the same Pebble snapshot,
// rather than retaining a map of every key, Range or Dataset in memory.
func (s *State) validateStreamEntry(view *pebble.Snapshot, index uint64, key, value []byte) error {
	if len(key) > snapshot.MaxKeyBytes || len(value) > snapshot.MaxValueBytes {
		return errors.New("snapshot logical record exceeds bound")
	}
	readRange := func(rangeID string) (rootstate.Range, error) {
		var descriptor rootstate.Range
		if rootstate.ValidateRangeID(rangeID) != nil {
			return descriptor, errors.New("invalid snapshot Range reference")
		}
		encoded, err := snapshotValue(view, fenceRangeKey(rangeID))
		if err != nil {
			return descriptor, err
		}
		if err := decodePackJSON(encoded, &descriptor); err != nil {
			return descriptor, err
		}
		if descriptor.RangeID != rangeID || descriptor.Validate() != nil || descriptor.OwnerGroupID != s.groupID ||
			descriptor.State != rootstate.RangeStateActive || descriptor.Epoch != 1 || len(descriptor.Start) != 0 || len(descriptor.End) != 0 {
			return descriptor, errors.New("invalid snapshot Range fence")
		}
		return descriptor, nil
	}
	switch {
	case bytes.HasPrefix(key, localConfirmationPrefix):
		record, err := decodeLocalConfirmation(key, value, s.groupID, index)
		if err != nil {
			return err
		}
		return validateLocalConfirmationLinks(record, func(key []byte) ([]byte, bool, error) { return snapshotReferenceMetadata(view, key) }, s.groupID, index)
	case bytes.HasPrefix(key, geoCoveragePrefix):
		record, err := decodeGeoCoverage(key, value, s.groupID, index)
		if err != nil {
			return err
		}
		return validateGeoCoverageLinks(record, func(key []byte) ([]byte, bool, error) { return snapshotReferenceMetadata(view, key) }, s.groupID, index)
	case bytes.HasPrefix(key, geoMetadataPrefix):
		if _, err := decodeGeoMetadata(key, value, s.groupID, index); err != nil {
			return err
		}
	case bytes.HasPrefix(key, recordReferencePrefix):
		ref, err := decodeRecordReference(key, value, s.groupID, index)
		if err != nil {
			return err
		}
		return validateRecordReferenceLinks(ref, func(key []byte) ([]byte, bool, error) { return snapshotReferenceMetadata(view, key) })
	case bytes.Equal(key, regionFenceKey), bytes.Equal(key, membershipIndexKey), bytes.Equal(key, regionHandoffKey):
		// Validated together against the same immutable cut and ConfState by
		// validateRegionSnapshot before stream generation or installation.
		return nil
	case bytes.HasPrefix(key, valuePrefix):
		scope, err := splitValueKey(key)
		if err != nil {
			return err
		}
		rangeID, err := snapshotValue(view, fenceKeyspaceKey(scope))
		if err != nil {
			return err
		}
		descriptor, err := readRange(string(rangeID))
		if err != nil {
			return err
		}
		if descriptor.Keyspace() != scope {
			return errors.New("snapshot value escaped Keyspace fence")
		}
	case bytes.HasPrefix(key, []byte("data/fence/range/")):
		descriptor, err := readRange(strings.TrimPrefix(string(key), "data/fence/range/"))
		if err != nil {
			return err
		}
		rangeID, err := snapshotValue(view, fenceKeyspaceKey(descriptor.Keyspace()))
		if err != nil {
			return err
		}
		if string(rangeID) != descriptor.RangeID {
			return errors.New("snapshot Range lacks matching Keyspace fence")
		}
	case bytes.HasPrefix(key, []byte("data/fence/keyspace/")):
		parts := strings.Split(strings.TrimPrefix(string(key), "data/fence/keyspace/"), "/")
		if len(parts) != 3 {
			return errors.New("invalid snapshot Keyspace fence")
		}
		scope := rootstate.Keyspace{TenantID: parts[0], EngineID: parts[1], DatasetID: parts[2]}
		descriptor, err := readRange(string(value))
		if err != nil {
			return err
		}
		if scope.Validate() != nil || descriptor.Keyspace() != scope {
			return errors.New("snapshot Keyspace lacks matching Range fence")
		}
	case bytes.HasPrefix(key, []byte("result/")):
		var result Result
		if decodePackJSON(value, &result) != nil || rootstate.ValidateRequestID(result.RequestID) != nil || !bytes.Equal(key, resultKey(result.RequestID)) ||
			result.Index == 0 || result.Index > index || (result.Applied && result.Error != "") || (!result.Applied && result.Error == "") {
			return errors.New("invalid snapshot request result")
		}
		digest, err := hex.DecodeString(result.Hash)
		if err != nil || len(digest) != 32 {
			return errors.New("invalid snapshot result digest")
		}
	default:
		return errors.New("unsupported snapshot logical namespace")
	}
	return nil
}

// Reference backlinks share the snapshot's immutable cut. Result rows may have
// opaque application values larger than fence metadata; bound them by the
// stream's per-value limit rather than incorrectly applying the 64 KiB fence
// limit. Validation reads only one bounded row at a time.
func snapshotReferenceMetadata(view *pebble.Snapshot, key []byte) ([]byte, bool, error) {
	value, closer, err := view.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	bound := 64 << 10
	if bytes.HasPrefix(key, []byte("result/")) {
		bound = snapshot.MaxValueBytes
	}
	if len(value) > bound {
		return nil, false, errors.Join(errors.New("record reference backlink exceeds snapshot bound"), closer.Close())
	}
	copy := bytes.Clone(value)
	return copy, true, closer.Close()
}

func snapshotValue(view *pebble.Snapshot, key []byte) ([]byte, error) {
	value, closer, err := view.Get(key)
	if err != nil {
		return nil, err
	}
	// Only metadata lookups use this helper, not large application values.
	if len(value) > 64<<10 {
		return nil, errors.Join(errors.New("snapshot fence metadata exceeds bound"), closer.Close())
	}
	copy := bytes.Clone(value)
	return copy, closer.Close()
}
