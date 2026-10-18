package enginestate

import (
	"encoding/json"
	"errors"
	"fmt"

	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// RewriteSnapshotForRestore changes only the Raft membership envelope of an
// Engine state snapshot. Logical Engine records stay opaque and are validated
// by State.Restore against the destination Group and membership.
func RewriteSnapshotForRestore(encoded []byte, conf *raftpb.ConfState) ([]byte, error) {
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
	return json.Marshal(image)
}
