package enginestate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

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
	return json.Marshal(image)
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
