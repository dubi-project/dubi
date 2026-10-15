package logstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"slices"

	"github.com/cockroachdb/pebble"
	"go.etcd.io/raft/v3"
)

var groupFormationKey = []byte("meta/group-formation")
var groupStartedKey = []byte("meta/group-started")

type groupFormation struct {
	Version int      `json:"version"`
	GroupID string   `json:"group_id"`
	RaftID  uint64   `json:"raft_id"`
	Joining bool     `json:"joining"`
	Peers   []uint64 `json:"peers,omitempty"`
}

func (f groupFormation) validate() error {
	if f.Version != 1 || f.GroupID == "" || len(f.GroupID) > 128 || f.RaftID == 0 || len(f.Peers) > 4096 {
		return errors.New("invalid Raft Group formation")
	}
	if f.Joining {
		if len(f.Peers) != 0 {
			return errors.New("joining Group cannot bootstrap")
		}
		return nil
	}
	if len(f.Peers) == 0 || !slices.Contains(f.Peers, f.RaftID) {
		return errors.New("bootstrap peers must contain local RaftID")
	}
	for i, id := range f.Peers {
		if id == 0 || (i > 0 && f.Peers[i-1] >= id) {
			return errors.New("bootstrap peers must be sorted and unique")
		}
	}
	return nil
}

// BindGroup durably records the one initial formation before Raft may emit any
// Ready. Reopen uses this original intent, never a caller's replacement peers.
// Returning bootstrap peers only authorizes first initialization of this bound
// log; it does not provide a cluster-level admission or creation permit.
func (s *Storage) BindGroup(groupID string, raftID uint64, peers []uint64, joining bool) ([]uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("Raft storage is closed")
	}
	requested := slices.Clone(peers)
	slices.Sort(requested)
	if groupID == "" || len(groupID) > 128 || raftID == 0 || len(requested) > 4096 {
		return nil, errors.New("invalid Group formation request")
	}
	for i, id := range requested {
		if id == 0 || (i > 0 && requested[i-1] == id) {
			return nil, errors.New("invalid bootstrap peer set")
		}
	}
	if joining && len(requested) > 0 {
		return nil, errors.New("joining Group cannot supply bootstrap peers")
	}
	data, closer, err := s.db.Get(groupFormationKey)
	var formation groupFormation
	if errors.Is(err, pebble.ErrNotFound) {
		if s.lastIndex != 0 || !raft.IsEmptyHardState(s.hardState) || len(s.confState.GetVoters()) != 0 || len(s.confState.GetLearners()) != 0 {
			return nil, errors.New("existing Raft history lacks Group formation; refusing adoption")
		}
		formation = groupFormation{Version: 1, GroupID: groupID, RaftID: raftID, Joining: joining, Peers: requested}
		if err := formation.validate(); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(formation)
		if err != nil {
			return nil, err
		}
		if err := s.db.Set(groupFormationKey, encoded, pebble.Sync); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		if len(data) > 128<<10 {
			return nil, errors.Join(closer.Close(), errors.New("oversized Raft formation record"))
		}
		d := json.NewDecoder(bytes.NewReader(data))
		d.DisallowUnknownFields()
		decodeErr := d.Decode(&formation)
		if decodeErr == nil {
			if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
				decodeErr = errors.New("trailing Group formation data")
			}
		}
		if err := errors.Join(decodeErr, closer.Close()); err != nil {
			return nil, err
		}
		if err := formation.validate(); err != nil {
			return nil, err
		}
		if formation.GroupID != groupID || formation.RaftID != raftID || (joining && !formation.Joining) || (len(requested) > 0 && !slices.Equal(requested, formation.Peers)) {
			return nil, errors.New("Raft Group identity or bootstrap peers differ from durable formation")
		}
		// A preceding Sync failure may have left the same intent readable.
		if err := s.db.LogData(nil, pebble.Sync); err != nil {
			return nil, err
		}
	}
	if s.lastIndex != 0 {
		return nil, nil
	}
	started, closer, err := s.db.Get(groupStartedKey)
	if err == nil {
		valid := bytes.Equal(started, []byte{1})
		closeErr := closer.Close()
		if !valid {
			return nil, errors.Join(closeErr, errors.New("invalid Raft started marker"))
		}
		return nil, errors.Join(closeErr, errors.New("started Raft Group lost its log; refusing bootstrap"))
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return nil, err
	}
	if !formation.Joining && !raft.IsEmptyHardState(s.hardState) {
		return nil, errors.New("bootstrap Group has HardState without log")
	}
	return slices.Clone(formation.Peers), nil
}
