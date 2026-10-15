package capstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"math"
	"reflect"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

const recordCutFormat = "capstate-record-cut-v1"
const maxRecordCutMetadataBytes = regionfence.MaxRecordBytes + 4096

var recordCutMetadataKey = []byte("cut/metadata")
var recordCutSummaryKey = []byte("summary")
var ErrRecordCutBusy = errors.New("a sealed record cut is already active")

// RecordCut is a completed immutable reference manifest, NOT data coverage,
// a retention lease, authority to activate a generation, or a Raft snapshot.
// Artifact binds the exact captured Raft cut. SHA256 separately binds the seal
// and reference set, so unrelated later Raft entries cannot change set identity.
type RecordCut struct {
	Version  int                 `json:"version"`
	Artifact snapshot.Reference  `json:"artifact"`
	Seal     regionfence.Handoff `json:"seal"`
	RecordCutSummary
}
type RecordCutSummary struct {
	Count        uint64 `json:"count"`
	PayloadBytes uint64 `json:"payload_bytes"`
	SHA256       string `json:"sha256"`
}
type recordCutMetadata struct {
	ReadIndex uint64              `json:"read_index"`
	Seal      regionfence.Handoff `json:"seal"`
}

func (c RecordCut) Validate() error {
	if len(c.SHA256) != sha256.Size*2 {
		return errors.New("invalid sealed record cut digest length")
	}
	digest, err := hex.DecodeString(c.SHA256)
	if c.Version != 1 || c.Artifact.Validate() != nil || c.Seal.Validate() != nil || c.Seal.Activation != nil || c.Artifact.GroupID != c.Seal.Intent.Previous.Fence.GroupID || c.Artifact.Index < c.Seal.SealIndex || c.Count > snapshot.MaxStreamBytes || c.Count == 0 && c.PayloadBytes != 0 || err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != c.SHA256 {
		return errors.New("invalid sealed record cut")
	}
	return nil
}

func cloneCutSeal(h regionfence.Handoff) regionfence.Handoff {
	h.Intent.Previous.Fence.Members = append([]rootstate.GroupMember(nil), h.Intent.Previous.Fence.Members...)
	h.Intent.Next.Nodes = append([]uint64(nil), h.Intent.Next.Nodes...)
	return h // All callers require Activation == nil.
}

type PreparedRecordCut struct {
	mu            sync.Mutex
	state         *State
	view          *pebble.Snapshot
	header        snapshot.Header
	metadata      recordCutMetadata
	closed, built bool
	closeErr      error
}

// PrepareRecordCut obtains an actual Group read barrier, then captures ONE
// Pebble snapshot and validates the exact unactivated seal against that cut.
// Only one preparation per State is admitted (including its barrier wait).
// Caller MUST Close it; State.Close waits without closing the underlying DB.
// Capturing a seal does not keep the live Group sealed: a coordinator must
// requalify the current handoff before using this artifact for backfill/coverage.
func (s *State) PrepareRecordCut(ctx context.Context, barrier ReadBarrier, seal regionfence.Handoff) (_ *PreparedRecordCut, returnErr error) {
	if ctx == nil || barrier == nil || seal.Validate() != nil || seal.Activation != nil || seal.Intent.Previous.Fence.GroupID != s.groupID {
		return nil, errors.New("record cut requires a read barrier and exact pending seal")
	}
	seal = cloneCutSeal(seal)
	s.mu.Lock()
	if err := s.usable(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if s.recordCutActive {
		s.mu.Unlock()
		return nil, ErrRecordCutBusy
	}
	s.recordCutActive = true
	s.snapshotWG.Add(1)
	s.mu.Unlock()
	p := &PreparedRecordCut{state: s}
	success := false
	defer func() {
		if !success {
			returnErr = errors.Join(returnErr, p.Close())
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	index, err := barrier.ReadIndex(ctx, s.groupID)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	if err := s.usable(); err != nil {
		s.mu.RUnlock()
		return nil, err
	}
	if index == 0 || s.applied < index {
		s.mu.RUnlock()
		return nil, ErrStateBehind
	}
	capturedConf := proto.Clone(s.conf).(*raftpb.ConfState)
	// This is not an installable Raft snapshot and carries no ConfState.
	p.header = snapshot.Header{Version: snapshot.StreamVersion, Format: recordCutFormat, GroupID: s.groupID, Index: s.applied}
	p.view = s.db.NewSnapshot()
	s.mu.RUnlock()
	if err := p.header.Validate(); err != nil {
		return nil, err
	}
	lookup := func(key []byte) ([]byte, bool, error) { return recordCutSmallLookup(p.view, key) }
	r, found, _, err := s.validateRegionMetadata(p.header.Index, capturedConf, lookup)
	if err != nil {
		return nil, err
	}
	h, hasSeal, err := s.validateHandoff(p.header.Index, r, found, lookup)
	if err != nil {
		return nil, err
	}
	if !found || !hasSeal || h.Activation != nil || !reflect.DeepEqual(h, seal) {
		return nil, errors.New("record cut seal differs from captured state")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p.metadata = recordCutMetadata{ReadIndex: index, Seal: seal}
	success = true
	return p, nil
}

func recordCutSmallLookup(view *pebble.Snapshot, key []byte) ([]byte, bool, error) {
	v, closer, err := view.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if len(v) > regionfence.MaxRecordBytes {
		return nil, false, errors.Join(errors.New("record cut metadata exceeds bound"), closer.Close())
	}
	copy := bytes.Clone(v)
	return copy, true, closer.Close()
}

// Keep borrowed backlink values alive only through one validation. In
// particular, do not clone a possibly 64 MiB Result for every reference.
func validateCutReference(view *pebble.Snapshot, ref RecordReference) (returnErr error) {
	var closers []io.Closer
	defer func() {
		for _, closer := range closers {
			returnErr = errors.Join(returnErr, closer.Close())
		}
	}()
	return validateRecordReferenceLinks(ref, func(key []byte) ([]byte, bool, error) {
		v, closer, err := view.Get(key)
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		closers = append(closers, closer)
		limit := 64 << 10
		if bytes.HasPrefix(key, []byte("result/")) {
			limit = MaxResultBytes
		}
		if len(v) > limit {
			return nil, false, errors.New("record cut backlink exceeds bound")
		}
		return v, true, nil
	})
}

func cutSetHash(seal regionfence.Handoff) hash.Hash {
	h := sha256.New()
	h.Write([]byte("DUBI sealed record reference set v1\x00"))
	encoded, _ := json.Marshal(seal)
	cutHashField(h, encoded)
	return h
}
func cutHashField(h hash.Hash, b []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(b)))
	h.Write(n[:])
	h.Write(b)
}
func addCutReference(summary *RecordCutSummary, h hash.Hash, key, value []byte, ref RecordReference) error {
	if summary.Count == math.MaxUint64 || ref.Descriptor.Size > math.MaxUint64-summary.PayloadBytes {
		return errors.New("record cut summary overflow")
	}
	cutHashField(h, key)
	cutHashField(h, value)
	summary.Count++
	summary.PayloadBytes += ref.Descriptor.Size
	return nil
}

// Build publishes only after every reference and its Range/Result backlinks
// have passed validation at the captured cut. Repeated Build is rejected even
// after failure; Close and recapture rather than reuse a partial writer.
func (p *PreparedRecordCut) Build(ctx context.Context, artifacts *snapshot.Store) (RecordCut, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.built || ctx == nil || artifacts == nil {
		return RecordCut{}, errors.New("record cut preparation is closed, used or invalid")
	}
	p.built = true
	var summary RecordCutSummary
	ref, err := artifacts.Create(ctx, p.header, snapshot.MaxStreamBytes, func(encoder *snapshot.Encoder) (returnErr error) {
		encoded, err := json.Marshal(p.metadata)
		if err != nil || len(encoded) > maxRecordCutMetadataBytes {
			return errors.New("record cut header exceeds bound")
		}
		if err := encoder.Add(recordCutMetadataKey, encoded); err != nil {
			return err
		}
		iterator, err := p.view.NewIter(&pebble.IterOptions{LowerBound: recordReferencePrefix, UpperBound: []byte("data/record-reference0")})
		if err != nil {
			return err
		}
		defer func() { returnErr = errors.Join(returnErr, iterator.Close()) }()
		h := cutSetHash(p.metadata.Seal)
		for iterator.First(); iterator.Valid(); iterator.Next() {
			if err := ctx.Err(); err != nil {
				return err
			}
			key, value := iterator.Key(), iterator.Value()
			if len(key) > snapshot.MaxKeyBytes {
				return errors.New("record cut key exceeds bound")
			}
			r, err := decodeRecordReference(key, value, p.header.GroupID, p.header.Index)
			if err != nil {
				return err
			}
			if r.Index >= p.metadata.Seal.SealIndex {
				return errors.New("record publication is not before its seal")
			}
			if err := validateCutReference(p.view, r); err != nil {
				return err
			}
			canonical, err := json.Marshal(r)
			if err != nil {
				return err
			}
			if err := addCutReference(&summary, h, key, canonical, r); err != nil {
				return err
			}
			if err := encoder.Add(key, canonical); err != nil {
				return err
			}
		}
		if err := iterator.Error(); err != nil {
			return err
		}
		summary.SHA256 = hex.EncodeToString(h.Sum(nil))
		encoded, err = json.Marshal(summary)
		if err != nil {
			return err
		}
		return encoder.Add(recordCutSummaryKey, encoded)
	})
	if err != nil {
		return RecordCut{}, err
	}
	if err := ctx.Err(); err != nil {
		return RecordCut{}, err
	}
	result := RecordCut{Version: 1, Artifact: ref, Seal: cloneCutSeal(p.metadata.Seal), RecordCutSummary: summary}
	return result, result.Validate()
}

func (p *PreparedRecordCut) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return p.closeErr
	}
	p.closed = true
	if p.view != nil {
		p.closeErr = p.view.Close()
	}
	p.state.mu.Lock()
	p.state.recordCutActive = false
	p.state.mu.Unlock()
	p.state.snapshotWG.Done()
	return p.closeErr
}

// WalkVerifiedRecordCut does not invoke the caller until the entire artifact,
// including its final digest and close check, has been verified. A repair or
// coverage caller may leave harmless copied bytes on a later failure, but it
// must never act on a prefix of an invalid manifest. The artifact is checked
// again while visiting, so a concurrent replacement cannot produce success.
// This does not authenticate who supplied the cut or authorize activation.
func WalkVerifiedRecordCut(ctx context.Context, artifacts *snapshot.Store, cut RecordCut, visit func(RecordReference) error) error {
	if err := WalkRecordCut(ctx, artifacts, cut, nil); err != nil {
		return err
	}
	if visit == nil {
		return nil
	}
	return WalkRecordCut(ctx, artifacts, cut, visit)
}

// WalkRecordCut validates framing, exact seal, canonical references, summary
// and artifact SHA-256/EOF. A visitor may see a prefix before a later failure:
// never publish coverage/activation or delete data until this returns nil.
// Visiting a descriptor does not prove its current authority or physical data.
func WalkRecordCut(ctx context.Context, artifacts *snapshot.Store, cut RecordCut, visit func(RecordReference) error) (returnErr error) {
	if ctx == nil || artifacts == nil || cut.Validate() != nil {
		return errors.New("invalid record cut reader")
	}
	cut.Seal = cloneCutSeal(cut.Seal)
	if err := ctx.Err(); err != nil {
		return err
	}
	reader, err := artifacts.Open(cut.Artifact)
	if err != nil {
		return err
	}
	defer func() { returnErr = errors.Join(returnErr, reader.Close()) }()
	decoder, err := snapshot.NewBoundedDecoder(reader, cut.Artifact.Size, snapshot.MaxKeyBytes, maxRecordCutMetadataBytes)
	if err != nil {
		return err
	}
	header := decoder.Header()
	if header.Format != recordCutFormat || header.GroupID != cut.Artifact.GroupID || header.Index != cut.Artifact.Index || len(header.Conf) != 0 {
		return errors.New("record cut artifact format or identity differs")
	}
	key, value, err := decoder.Next()
	if err != nil {
		return err
	}
	var meta recordCutMetadata
	if !bytes.Equal(key, recordCutMetadataKey) || decodePackJSON(value, &meta) != nil || meta.ReadIndex == 0 || meta.ReadIndex > header.Index || !reflect.DeepEqual(meta.Seal, cut.Seal) {
		return errors.New("record cut header differs from expected seal")
	}
	canonical, _ := json.Marshal(meta)
	if !bytes.Equal(canonical, value) {
		return errors.New("noncanonical record cut metadata")
	}
	h := cutSetHash(meta.Seal)
	var summary RecordCutSummary
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		key, value, err = decoder.Next()
		if err != nil {
			return errors.Join(errors.New("record cut summary missing"), err)
		}
		if bytes.Equal(key, recordCutSummaryKey) {
			var stored RecordCutSummary
			summary.SHA256 = hex.EncodeToString(h.Sum(nil))
			if len(value) > 4096 || decodePackJSON(value, &stored) != nil || stored != summary || stored != cut.RecordCutSummary {
				return errors.New("record cut summary differs from contents")
			}
			canonical, _ := json.Marshal(stored)
			if !bytes.Equal(canonical, value) {
				return errors.New("noncanonical record cut summary")
			}
			if _, _, err := decoder.Next(); !errors.Is(err, io.EOF) {
				return errors.Join(errors.New("record cut has trailing records or invalid footer"), err)
			}
			return ctx.Err()
		}
		r, err := decodeRecordReference(key, value, header.GroupID, header.Index)
		if err != nil {
			return err
		}
		if r.Index >= cut.Seal.SealIndex {
			return errors.New("record publication is not before its seal")
		}
		canonical, err := json.Marshal(r)
		if err != nil {
			return err
		}
		if !bytes.Equal(canonical, value) {
			return errors.New("noncanonical record cut reference")
		}
		if err := addCutReference(&summary, h, key, value, r); err != nil {
			return err
		}
		if visit != nil {
			if err := visit(r.clone()); err != nil {
				return err
			}
		}
	}
}
