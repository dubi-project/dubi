package enginestate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/dubi-project/dubi/internal/platform/cluster/regionfence"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// RewriteSnapshotForRestore changes only the Raft membership envelope of an
// Engine state snapshot. Logical Engine records stay opaque and are validated
// by State.Restore against the destination Group and membership.
func RewriteSnapshotForRestore(encoded []byte, conf *raftpb.ConfState) ([]byte, error) {
	return RewriteSnapshotForRestoreWithIdentity(encoded, conf, rootstate.RestoreIdentityMap{})
}

// RewriteSnapshotForRestoreWithIdentity also rewrites the engine-owned
// regional fence and handoff metadata. Those records contain RaftIDs inside
// JSON values; changing only the outer ConfState would make State.Restore
// reject an otherwise valid snapshot after a new-cluster identity remap.
func RewriteSnapshotForRestoreWithIdentity(encoded []byte, conf *raftpb.ConfState, mapping rootstate.RestoreIdentityMap) ([]byte, error) {
	if len(encoded) == 0 || conf == nil {
		return nil, errors.New("Engine restore requires a snapshot and destination membership")
	}
	var image snapshotImage
	if err := decodePackJSON(encoded, &image); err != nil {
		return nil, fmt.Errorf("decode Engine restore snapshot: %w", err)
	}
	confBytes, err := proto.Marshal(conf)
	if err != nil {
		return nil, err
	}
	image.Conf = confBytes
	for index := range image.Entries {
		if !bytes.Equal(image.Entries[index].Key, regionFenceKey) && !bytes.Equal(image.Entries[index].Key, regionHandoffKey) {
			continue
		}
		value, err := rewriteFenceIdentity(image.Entries[index].Value, mapping.RaftIDs)
		if err != nil {
			return nil, fmt.Errorf("rewrite Engine regional identity: %w", err)
		}
		image.Entries[index].Value = value
	}
	if err := rewriteRegionalResultHashes(image.Entries); err != nil {
		return nil, err
	}
	return json.Marshal(image)
}

// Identity translation changes the command value embedded in regional fence
// and handoff records. Their durable Result hashes are part of the same
// integrity chain and must be recomputed along with the rewritten metadata.
func rewriteRegionalResultHashes(entries []snapshotEntry) error {
	setHash := func(requestID, hash string) error {
		for index := range entries {
			if !bytes.Equal(entries[index].Key, resultKey(requestID)) {
				continue
			}
			var result Result
			if err := decodePackJSON(entries[index].Value, &result); err != nil {
				return err
			}
			result.Hash = hash
			encoded, err := json.Marshal(result)
			if err != nil {
				return err
			}
			entries[index].Value = encoded
			return nil
		}
		// Older/minimal snapshots may retain the fence metadata without the
		// corresponding command result. There is no hash to rewrite in that
		// case; State.Restore will validate the snapshot's own record shape.
		return nil
	}
	for _, entry := range entries {
		if !bytes.Equal(entry.Key, regionFenceKey) {
			continue
		}
		var record regionfence.Record
		if err := json.Unmarshal(entry.Value, &record); err != nil {
			return err
		}
		if err := setHash(record.RequestID, commandHash("region_fence", record.RequestID, record.Fence, nil)); err != nil {
			return err
		}
	}
	for _, entry := range entries {
		if !bytes.Equal(entry.Key, regionHandoffKey) {
			continue
		}
		var handoff regionfence.Handoff
		if err := json.Unmarshal(entry.Value, &handoff); err != nil {
			return err
		}
		previous := handoff.Intent.Previous
		if err := setHash(previous.RequestID, commandHash("region_fence", previous.RequestID, previous.Fence, nil)); err != nil {
			return err
		}
		seal := regionfence.HandoffCommand{Action: regionfence.Seal, Intent: &handoff.Intent}
		if err := setHash(handoff.SealRequestID, commandHash("region_handoff", handoff.SealRequestID, seal, nil)); err != nil {
			return err
		}
		if handoff.Activation != nil {
			activate := regionfence.HandoffCommand{Action: regionfence.Activate, Activation: handoff.Activation}
			if err := setHash(handoff.ActivationRequestID, commandHash("region_handoff", handoff.ActivationRequestID, activate, nil)); err != nil {
				return err
			}
		}
	}
	return nil
}

func rewriteFenceIdentity(encoded []byte, mapping map[uint64]uint64) ([]byte, error) {
	if len(mapping) == 0 {
		return encoded, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing regional identity JSON")
	}
	rewritten, err := rewriteFenceValue(value, "", mapping)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rewritten)
}

func rewriteFenceValue(value any, field string, mapping map[uint64]uint64) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			rewritten, err := rewriteFenceValue(child, key, mapping)
			if err != nil {
				return nil, err
			}
			typed[key] = rewritten
		}
		return typed, nil
	case []any:
		for index, child := range typed {
			rewritten, err := rewriteFenceValue(child, field, mapping)
			if err != nil {
				return nil, err
			}
			typed[index] = rewritten
		}
		if field == "nodes" {
			for _, child := range typed {
				number, ok := child.(json.Number)
				if !ok {
					return nil, errors.New("regional nodes list contains a non-numeric identity")
				}
				if _, err := strconv.ParseUint(string(number), 10, 64); err != nil {
					return nil, err
				}
			}
			sort.Slice(typed, func(i, j int) bool {
				left, _ := strconv.ParseUint(string(typed[i].(json.Number)), 10, 64)
				right, _ := strconv.ParseUint(string(typed[j].(json.Number)), 10, 64)
				return left < right
			})
		} else if field == "members" {
			for _, child := range typed {
				member, ok := child.(map[string]any)
				if !ok {
					return nil, errors.New("regional members list contains a non-object identity")
				}
				number, ok := member["raft_id"].(json.Number)
				if !ok {
					return nil, errors.New("regional member has no numeric raft_id")
				}
				if _, err := strconv.ParseUint(string(number), 10, 64); err != nil {
					return nil, err
				}
			}
			sort.Slice(typed, func(i, j int) bool {
				left, _ := strconv.ParseUint(string(typed[i].(map[string]any)["raft_id"].(json.Number)), 10, 64)
				right, _ := strconv.ParseUint(string(typed[j].(map[string]any)["raft_id"].(json.Number)), 10, 64)
				return left < right
			})
		}
		return typed, nil
	case json.Number:
		if field != "raft_id" && !strings.HasSuffix(field, "_raft_id") && field != "nodes" {
			return typed, nil
		}
		old, err := strconv.ParseUint(string(typed), 10, 64)
		if err != nil {
			return nil, err
		}
		if mapped, found := mapping[old]; found {
			return json.Number(strconv.FormatUint(mapped, 10)), nil
		}
		return typed, nil
	default:
		return value, nil
	}
}
