package server

import (
	"context"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enrollment"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

// rootControlHost makes management writes independent of the Node that
// accepted the request. Raft proposals are local-only in the Host API; a
// follower therefore has to forward Root control commands to the currently
// observed leader. Data-plane callers already use GroupClient routing, but
// enrollment/grant management used to bypass it and could wait forever on a
// follower.
type rootControlHost struct {
	local  rootControlLocal
	client rootControlClient
	raftID uint64
}

type rootControlLocal interface {
	enrollment.Host
	Status(string) (raft.GroupStatus, error)
}

type rootControlClient interface {
	Submit(context.Context, uint64, string, []byte) (uint64, error)
	ReadIndex(context.Context, uint64, string) (uint64, error)
}

var _ enrollment.Host = (*rootControlHost)(nil)

func newRootControlHost(local rootControlLocal, client rootControlClient, raftID uint64) (*rootControlHost, error) {
	if local == nil || client == nil || raftID == 0 {
		return nil, errors.New("Root control forwarding requires local Host, Group client and RaftID")
	}
	return &rootControlHost{local: local, client: client, raftID: raftID}, nil
}

func (h *rootControlHost) Submit(groupID string, command []byte) error {
	if groupID != raft.RootGroupID {
		return h.local.Submit(groupID, command)
	}
	status, err := h.local.Status(groupID)
	if err != nil {
		return err
	}
	if target := status.LeaderID; target != 0 && target != h.raftID {
		return h.forwardSubmit(target, groupID, command)
	}
	if err := h.local.Submit(groupID, command); err == nil {
		return nil
	} else if target, refreshErr := h.leader(); refreshErr == nil && target != 0 && target != h.raftID {
		// A stale local SoftState can briefly report this Node as leader during
		// an election. Retry once against the refreshed leader.
		if forwardErr := h.forwardSubmit(target, groupID, command); forwardErr == nil {
			return nil
		}
		return err
	} else {
		return err
	}
}

func (h *rootControlHost) ReadIndex(ctx context.Context, groupID string) (uint64, error) {
	if groupID != raft.RootGroupID {
		return h.local.ReadIndex(ctx, groupID)
	}
	status, err := h.local.Status(groupID)
	if err != nil {
		return 0, err
	}
	if target := status.LeaderID; target != 0 && target != h.raftID {
		return h.client.ReadIndex(ctx, target, groupID)
	}
	return h.local.ReadIndex(ctx, groupID)
}

func (h *rootControlHost) leader() (uint64, error) {
	status, err := h.local.Status(raft.RootGroupID)
	if err != nil {
		return 0, err
	}
	return status.LeaderID, nil
}

func (h *rootControlHost) forwardSubmit(target uint64, groupID string, command []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := h.client.Submit(ctx, target, groupID, command)
	return err
}
