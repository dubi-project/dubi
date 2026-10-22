package enginedata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
)

type ownerReadIndexClient interface {
	GroupCandidates(groupID string) []uint64
	ReadIndex(ctx context.Context, target uint64, groupID string) (uint64, error)
}

type ownerReadIndexLocal interface {
	ReadIndex(ctx context.Context, groupID string) (uint64, error)
	Status(groupID string) (raft.GroupStatus, error)
}

const ownerLocalReadIndexTimeout = 2 * time.Second
const ownerRemoteReadIndexTimeout = 8 * time.Second

// engineReadBarrier gives local Owner reads a short fast path, then asks an
// exact current Group member for a quorum-confirmed cut. This prevents a
// follower with a stale leader view from consuming the whole user read budget
// before the ordinary EngineData forwarding path can try another member.
type engineReadBarrier struct {
	runtime *node.Runtime
	client  ownerReadIndexClient
}

func (barrier engineReadBarrier) ReadIndex(ctx context.Context, groupID string) (uint64, error) {
	if barrier.runtime == nil || barrier.runtime.Host == nil {
		return 0, errors.New("engine read barrier has no local Node")
	}
	if groupID == raft.RootGroupID {
		return barrier.runtime.ReadRootIndex(ctx)
	}
	return readOwnerGroupIndex(ctx, barrier.runtime.Host, barrier.client,
		barrier.runtime.Identity.RaftID, groupID)
}

func readOwnerGroupIndex(ctx context.Context, local ownerReadIndexLocal, client ownerReadIndexClient,
	localID uint64, groupID string,
) (uint64, error) {
	if ctx == nil || local == nil || localID == 0 || groupID == "" {
		return 0, errors.New("Owner read barrier requires a valid local Node and Group")
	}
	status, statusErr := local.Status(groupID)
	candidates := client.GroupCandidates(groupID)
	ordered := make([]uint64, 0, len(candidates))
	add := func(id uint64) {
		if id == 0 || id == localID {
			return
		}
		for _, existing := range ordered {
			if existing == id {
				return
			}
		}
		ordered = append(ordered, id)
	}
	if statusErr == nil && status.LeaderID != 0 {
		for _, candidate := range candidates {
			if candidate == status.LeaderID {
				add(candidate)
				break
			}
		}
	}
	for _, candidate := range candidates {
		add(candidate)
	}
	if statusErr == nil && status.Role == etcdraft.StateLeader && status.LeaderID == localID {
		index, err := readLocalOwnerIndex(ctx, local, groupID)
		if err == nil {
			return index, nil
		}
		if err := ctx.Err(); err != nil {
			return 0, errors.Join(fmt.Errorf("local Owner Group read: %w", err), ctx.Err())
		}
	}
	if client == nil {
		return 0, errors.Join(statusErr, errors.New("Owner Group has no remote read client"))
	}
	if len(ordered) == 0 {
		// There is no known remote member. A short local attempt still allows a
		// newly elected or single-member Group to serve the read without making
		// a follower's stale ReadIndex consume the caller's full deadline.
		index, err := readLocalOwnerIndex(ctx, local, groupID)
		if err == nil {
			return index, nil
		}
		return 0, errors.Join(err, statusErr, errors.New("Owner Group has no confirmed remote read candidates"))
	}

	var failures []error
	for _, candidate := range ordered {
		if err := ctx.Err(); err != nil {
			return 0, errors.Join(append(failures, err)...)
		}
		attempt, cancel := context.WithTimeout(ctx, ownerRemoteReadIndexTimeout)
		remoteIndex, err := client.ReadIndex(attempt, candidate, groupID)
		cancel()
		if err == nil && remoteIndex != 0 {
			return remoteIndex, nil
		}
		if err == nil {
			err = errors.New("Owner Group member returned an empty read cut")
		}
		failures = append(failures, fmt.Errorf("Owner Group member %d: %w", candidate, err))
	}
	// Unknown/stale leader hints can make every remote candidate reject the
	// request. Try the local member last, after the current Group membership
	// has had a chance to return a quorum-confirmed cut.
	if err := ctx.Err(); err == nil {
		if index, localErr := readLocalOwnerIndex(ctx, local, groupID); localErr == nil {
			return index, nil
		} else {
			failures = append(failures, fmt.Errorf("local Owner Group read: %w", localErr))
		}
	}
	return 0, errors.Join(failures...)
}

func readLocalOwnerIndex(ctx context.Context, local ownerReadIndexLocal, groupID string) (uint64, error) {
	localCtx, cancel := context.WithTimeout(ctx, ownerLocalReadIndexTimeout)
	defer cancel()
	index, err := local.ReadIndex(localCtx, groupID)
	if err != nil {
		return 0, err
	}
	if index == 0 {
		return 0, errors.New("local Owner Group returned an empty read cut")
	}
	return index, nil
}
