package enginestate

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"
)

type InlineEntry struct{ Key, Value []byte }

// ScanInline reads bounded, small engine metadata in the authorized Keyspace.
// Large Pack-backed values are not enumerable here; engines must use it only
// for their inline metadata prefixes. Caller holds the fenced ReadView lock.
func (v ReadView) ScanInline(prefix, after []byte, limit int) ([]InlineEntry, bool, error) {
	if !v.fullRange || v.state == nil || len(prefix) == 0 || validateEngineKey(prefix) != nil || limit < 1 || limit > 512 || (len(after) > 0 && (!bytes.HasPrefix(after, prefix) || validateEngineKey(after) != nil)) {
		return nil, false, errors.New("invalid metadata scan")
	}
	base := engineKey(v.keyspace, prefix)
	// Prefix successor also covers arbitrary byte keys, including 0xff suffixes.
	end := bytes.Clone(base)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xff {
			end[i]++
			end = end[:i+1]
			break
		}
	}
	it, err := v.state.db.NewIter(&pebble.IterOptions{LowerBound: base, UpperBound: end})
	if err != nil {
		return nil, false, err
	}
	defer it.Close()
	seek := base
	if len(after) > 0 {
		seek = engineKey(v.keyspace, after)
	}
	valid := it.SeekGE(seek)
	if valid && len(after) > 0 && bytes.Equal(it.Key(), seek) {
		valid = it.Next()
	}
	out := make([]InlineEntry, 0, limit)
	total := 0
	scope := engineKey(v.keyspace, nil)
	for ; valid; valid = it.Next() {
		if v.ctx != nil {
			if err := v.ctx.Err(); err != nil {
				return nil, false, err
			}
		}
		if len(out) == limit {
			return out, true, nil
		}
		value, err := it.ValueAndErr()
		if err != nil {
			return nil, false, err
		}
		total += len(it.Key()) + len(value)
		if len(value) > 16<<10 || total > 2<<20 {
			return nil, false, errors.New("metadata scan exceeds budget")
		}
		out = append(out, InlineEntry{Key: bytes.Clone(it.Key()[len(scope):]), Value: bytes.Clone(value)})
	}
	return out, false, it.Error()
}
