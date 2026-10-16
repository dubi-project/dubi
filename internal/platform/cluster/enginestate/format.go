package enginestate

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/cockroachdb/pebble"
)

var ErrStateFormat = errors.New("unsupported engine state format; explicit migration is required")
var stateFormatKey = []byte("meta/format")

// Old engine-local keys cannot be attributed to Tenant/Dataset safely.
// Never silently reinterpret them, hide them behind the new namespace, or
// guess ownership from the most recently active Range. Only an empty DB can
// receive a new format marker; no automatic migration happens on open.
func (s *State) checkStateFormat() error {
	version := []byte(strconv.Itoa(FormatVersion))
	encoded, found, err := s.get(stateFormatKey)
	if err != nil {
		return err
	}
	if found {
		if !bytes.Equal(encoded, version) {
			return ErrStateFormat
		}
		return nil
	}
	iterator, err := s.db.NewIter(nil)
	if err != nil {
		return err
	}
	populated := iterator.First()
	err = errors.Join(iterator.Error(), iterator.Close())
	if err != nil {
		return err
	}
	if populated {
		return ErrStateFormat
	}
	if err := s.db.Set(stateFormatKey, version, pebble.Sync); err != nil {
		return fmt.Errorf("initialize engine state format: %w", err)
	}
	return nil
}
