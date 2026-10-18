package rootstate

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// RestoreIdentityMap describes the identity translation used when a cluster
// backup is installed into a new cluster. Every old Node record that remains
// in the snapshot must have a replacement; historical records are retained so
// their references remain internally consistent.
type RestoreIdentityMap struct {
	RaftIDs map[uint64]uint64
	Nodes   map[string]Node
}

// RewriteSnapshotForRestore rewrites a Root snapshot's identity-bearing
// fields and replaces its Raft membership with the new cluster membership.
// Logical datasets, Groups, Ranges and Repository records remain unchanged.
// The destination State.Restore call performs the complete semantic snapshot
// validation before publishing the rewritten image.
func RewriteSnapshotForRestore(encoded []byte, mapping RestoreIdentityMap, conf *raftpb.ConfState) ([]byte, error) {
	if len(encoded) == 0 || conf == nil {
		return nil, errors.New("Root restore requires a snapshot and destination membership")
	}
	if err := validateRestoreIdentityMap(mapping); err != nil {
		return nil, err
	}
	var image snapshotImage
	if err := decodeStreamJSON(encoded, &image); err != nil {
		return nil, fmt.Errorf("decode Root restore snapshot: %w", err)
	}
	confBytes, err := proto.Marshal(conf)
	if err != nil {
		return nil, err
	}
	image.Conf = confBytes
	for index := range image.Entries {
		entry := &image.Entries[index]
		key, err := rewriteSnapshotKey(string(entry.Key), mapping)
		if err != nil {
			return nil, err
		}
		entry.Key = []byte(key)
		if strings.HasPrefix(key, "data/node/") {
			var node Node
			if err := decodeStreamJSON(entry.Value, &node); err != nil {
				return nil, fmt.Errorf("decode Root Node record: %w", err)
			}
			replacement, found := mapping.Nodes[node.NodeID]
			if !found {
				return nil, fmt.Errorf("Root restore has no replacement for Node %s", node.NodeID)
			}
			encodedNode, err := json.Marshal(replacement)
			if err != nil {
				return nil, err
			}
			entry.Value = encodedNode
			continue
		}
		if strings.HasPrefix(key, "data/node-id/") {
			old, err := strconv.ParseUint(string(entry.Value), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("decode Root Node reverse index: %w", err)
			}
			if mapped, found := mapping.RaftIDs[old]; found {
				entry.Value = []byte(strconv.FormatUint(mapped, 10))
			}
			continue
		}
		value, err := rewriteJSONIdentity(entry.Value, "", mapping)
		if err != nil {
			// Several reverse indexes intentionally store a plain key rather
			// than JSON. Leave those opaque bytes unchanged; Restore's semantic
			// validator will reject a stale identity-bearing reference.
			continue
		}
		entry.Value = value
	}
	return json.Marshal(image)
}

func validateRestoreIdentityMap(mapping RestoreIdentityMap) error {
	if len(mapping.RaftIDs) == 0 || len(mapping.Nodes) == 0 {
		return errors.New("Root restore identity map is empty")
	}
	seenRaft := make(map[uint64]struct{}, len(mapping.RaftIDs))
	for oldID, newID := range mapping.RaftIDs {
		if oldID == 0 || newID == 0 {
			return errors.New("Root restore identity map contains zero RaftID")
		}
		if _, found := seenRaft[newID]; found {
			return errors.New("Root restore identity map has duplicate destination RaftID")
		}
		seenRaft[newID] = struct{}{}
	}
	seenNode := make(map[string]struct{}, len(mapping.Nodes))
	for oldID, node := range mapping.Nodes {
		if !nodeIdentity.MatchString(oldID) || nodeIdentity.MatchString(node.NodeID) == false || node.RaftID == 0 {
			return errors.New("Root restore identity map contains invalid Node")
		}
		if _, found := seenNode[node.NodeID]; found {
			return errors.New("Root restore identity map has duplicate destination NodeID")
		}
		seenNode[node.NodeID] = struct{}{}
		if mapped, found := mapping.RaftIDs[node.RaftID]; found && mapped != node.RaftID {
			return errors.New("Root restore identity map mixes destination RaftIDs")
		}
	}
	return nil
}

func rewriteSnapshotKey(key string, mapping RestoreIdentityMap) (string, error) {
	for _, prefix := range []string{"data/node/", "data/node-fence/", "data/node-ready/"} {
		if strings.HasPrefix(key, prefix) {
			old, err := strconv.ParseUint(strings.TrimPrefix(key, prefix), 10, 64)
			if err != nil {
				return "", fmt.Errorf("invalid Root identity key %q", key)
			}
			if mapped, found := mapping.RaftIDs[old]; found {
				return prefix + fmt.Sprintf("%020d", mapped), nil
			}
			return key, nil
		}
	}
	if strings.HasPrefix(key, "data/node-id/") {
		old := strings.TrimPrefix(key, "data/node-id/")
		for oldID, node := range mapping.Nodes {
			if oldID == old {
				return "data/node-id/" + node.NodeID, nil
			}
		}
	}
	return key, nil
}

func rewriteJSONIdentity(encoded []byte, field string, mapping RestoreIdentityMap) ([]byte, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	rewritten, err := rewriteJSONValue(value, field, mapping)
	if err != nil {
		return nil, err
	}
	return json.Marshal(rewritten)
}

func rewriteJSONValue(value any, field string, mapping RestoreIdentityMap) (any, error) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			rewritten, err := rewriteJSONValue(child, key, mapping)
			if err != nil {
				return nil, err
			}
			typed[key] = rewritten
		}
		return typed, nil
	case []any:
		for index, child := range typed {
			rewritten, err := rewriteJSONValue(child, field, mapping)
			if err != nil {
				return nil, err
			}
			typed[index] = rewritten
		}
		return typed, nil
	case string:
		if isNodeIdentityField(field) {
			for oldID, node := range mapping.Nodes {
				if typed == oldID {
					return node.NodeID, nil
				}
			}
		}
		return typed, nil
	case json.Number:
		if isRaftIdentityField(field) || isRaftListField(field) {
			old, err := strconv.ParseUint(string(typed), 10, 64)
			if err != nil {
				return nil, err
			}
			if mapped, found := mapping.RaftIDs[old]; found {
				return json.Number(strconv.FormatUint(mapped, 10)), nil
			}
		}
		return typed, nil
	default:
		return value, nil
	}
}

func isNodeIdentityField(field string) bool {
	return field == "node_id" || strings.HasSuffix(field, "_node_id")
}

func isRaftIdentityField(field string) bool {
	return field == "raft_id" || strings.HasSuffix(field, "_raft_id")
}

func isRaftListField(field string) bool {
	switch field {
	case "nodes", "ready_members", "previous_nodes", "target_nodes":
		return true
	default:
		return false
	}
}
