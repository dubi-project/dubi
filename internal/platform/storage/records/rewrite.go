package records

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/dubi-project/dubi/internal/platform/resource"
)

type rewriteItem struct {
	id      string
	current entry
}

// RewriteCompaction yields between immutable records. IO shaping happens
// outside Store.mu, so a throttled compaction cannot sleep holding up reads.
func (s *Store) RewriteCompaction(ctx context.Context) (CompactReport, error) {
	if ctx == nil {
		return CompactReport{}, errors.New("nil compaction context")
	}
	s.mu.Lock()
	if err := s.usable(); err != nil {
		s.mu.Unlock()
		return CompactReport{}, err
	}
	if sealer, ok := s.packs.(interface{ SealAll(context.Context) error }); ok {
		if err := sealer.SealAll(ctx); err != nil {
			s.mu.Unlock()
			return CompactReport{}, err
		}
	}
	s.mu.Unlock()
	cursor := ""
	for {
		page, err := s.rewritePage(ctx, cursor)
		if err != nil {
			return CompactReport{}, err
		}
		if len(page) == 0 {
			break
		}
		for _, item := range page {
			if err := resource.WaitIO(ctx, item.current.Descriptor.Size*3); err != nil {
				return CompactReport{}, err
			}
			if err := s.rewriteRecord(ctx, item.id); err != nil {
				return CompactReport{}, err
			}
			cursor = string(indexKey(item.id))
		}
	}
	return s.Compact(ctx)
}

func (s *Store) rewritePage(ctx context.Context, after string) ([]rewriteItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return nil, err
	}
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: []byte("record/"), UpperBound: []byte("record0")})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	valid := iter.First()
	if after != "" {
		valid = iter.SeekGE([]byte(after))
		if valid && string(iter.Key()) == after {
			valid = iter.Next()
		}
	}
	var page []rewriteItem
	for ; valid && len(page) < 64; valid = iter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var current entry
		if len(iter.Value()) > 16<<10 || strictJSON(iter.Value(), &current) != nil {
			return nil, ErrCorrupt
		}
		id := strings.TrimPrefix(string(iter.Key()), "record/")
		if err := s.validateEntry(id, current); err != nil {
			return nil, err
		}
		page = append(page, rewriteItem{id: id, current: current})
	}
	return page, iter.Error()
}

func (s *Store) rewriteRecord(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.usable(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	current, found, err := s.lookup(id)
	if err != nil {
		return err
	}
	if !found {
		return ErrCorrupt
	}
	physical, err := s.packs.Read(current.Location)
	if err != nil {
		return err
	}
	location, err := s.packs.Append(ctx, physical)
	if err != nil {
		return err
	}
	replacement := entry{Version: current.Version, Descriptor: current.Descriptor.clone(), Location: location}
	if _, _, err := s.read(id, replacement.Descriptor, replacement); err != nil {
		return err
	}
	encoded, err := json.Marshal(replacement)
	if err != nil {
		return err
	}
	batch := s.db.NewBatch()
	defer batch.Close()
	if err := batch.Set(indexKey(id), encoded, nil); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		s.failed = fmt.Errorf("compaction index Sync uncertain; reopen required: %w", err)
		return s.failed
	}
	return nil
}
