package capstate

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
)

// LiveReferenceCut walks one fixed, quorum-confirmed Owner state cut. It is a
// module-neutral source for repair discovery, not a durable retention lease
// or a physical coverage report. A callback may have run before a later
// validation failure, so only a nil final error completes a full pass.
type LiveReferenceCut struct {
	Index uint64
	Count uint64
}

// LiveReferencePage is one bounded Owner snapshot. Cursor is an opaque key to
// pass to the next page; each page has its own cut, so pages are not a single
// durable snapshot. New references below Cursor are found on the next sweep.
type LiveReferencePage struct {
	Cut    LiveReferenceCut
	Cursor []byte
	Done   bool
}

const MaxLiveReferencePage = 1024

// WalkCurrentReferences does not hold the apply lock during callbacks or
// network IO. Every reference and its Range/Keyspace/Result backlinks are
// checked against the SAME Pebble snapshot, even if live mutations continue.
// State.Close waits for this walk to release its snapshot.
func (s *State) WalkCurrentReferences(ctx context.Context, barrier ReadBarrier,
	visit func(RecordReference) error) (cut LiveReferenceCut, returnErr error) {
	page, err := s.walkCurrentReferences(ctx, barrier, nil, 0, visit)
	return page.Cut, err
}

// WalkCurrentReferencesPage bounds the number of callbacks and the lifetime
// of a repair snapshot. A nil Cursor starts a sweep; Done ends it. The caller
// should restart from nil after Done, or after a failed page. A page is only
// complete if its returned error is nil.
func (s *State) WalkCurrentReferencesPage(ctx context.Context, barrier ReadBarrier,
	cursor []byte, limit uint32, visit func(RecordReference) error) (LiveReferencePage, error) {
	if limit == 0 || limit > MaxLiveReferencePage ||
		(len(cursor) != 0 && (!bytes.HasPrefix(cursor, recordReferencePrefix) ||
			len(cursor) > maxLiveReferenceKeyBytes())) {
		return LiveReferencePage{}, errors.New("invalid current reference page limit or cursor")
	}
	return s.walkCurrentReferences(ctx, barrier, cursor, limit, visit)
}

func maxLiveReferenceKeyBytes() int {
	return len(recordReferencePrefix) + 3*128 + 3 + maxCapabilityKeyBytes
}

func (s *State) walkCurrentReferences(ctx context.Context, barrier ReadBarrier,
	cursor []byte, limit uint32, visit func(RecordReference) error) (page LiveReferencePage, returnErr error) {
	if s == nil || ctx == nil || barrier == nil || visit == nil {
		return page, errors.New("live record walk requires a context, Owner barrier and callback")
	}
	index, err := barrier.ReadIndex(ctx, s.groupID)
	if err != nil {
		return page, err
	}
	s.mu.RLock()
	if err := s.usable(); err != nil {
		s.mu.RUnlock()
		return page, err
	}
	if index == 0 || s.applied < index {
		s.mu.RUnlock()
		return page, ErrStateBehind
	}
	page.Cut.Index = s.applied
	view := s.db.NewSnapshot()
	s.snapshotWG.Add(1)
	s.mu.RUnlock()
	defer s.snapshotWG.Done()
	defer func() { returnErr = errors.Join(returnErr, view.Close()) }()
	upper := append(bytes.Clone(recordReferencePrefix[:len(recordReferencePrefix)-1]), '0')
	iterator, err := view.NewIter(&pebble.IterOptions{LowerBound: recordReferencePrefix, UpperBound: upper})
	if err != nil {
		return LiveReferencePage{}, err
	}
	defer func() { returnErr = errors.Join(returnErr, iterator.Close()) }()
	if len(cursor) == 0 {
		iterator.First()
	} else {
		// Pebble keys are byte strings. Appending zero seeks strictly beyond the
		// last visited key, including when it was concurrently deleted.
		iterator.SeekGE(append(bytes.Clone(cursor), 0))
	}
	for iterator.Valid() {
		if err := ctx.Err(); err != nil {
			return LiveReferencePage{}, err
		}
		if len(iterator.Key()) > maxLiveReferenceKeyBytes() ||
			len(iterator.Value()) > maxRecordReferenceBytes {
			return LiveReferencePage{}, errors.New("current record reference exceeds key or value bound")
		}
		key := bytes.Clone(iterator.Key())
		value := bytes.Clone(iterator.Value())
		ref, err := decodeRecordReference(key, value, s.groupID, page.Cut.Index)
		if err != nil {
			return LiveReferencePage{}, fmt.Errorf("decode current record reference: %w", err)
		}
		if err := validateCutReference(view, ref); err != nil {
			return LiveReferencePage{}, fmt.Errorf("validate current record reference: %w", err)
		}
		if err := visit(ref); err != nil {
			return LiveReferencePage{}, err
		}
		page.Cut.Count++
		page.Cursor = key
		iterator.Next()
		if limit != 0 && page.Cut.Count == uint64(limit) {
			break
		}
	}
	if err := iterator.Error(); err != nil {
		return LiveReferencePage{}, err
	}
	page.Done = !iterator.Valid()
	return page, nil
}
