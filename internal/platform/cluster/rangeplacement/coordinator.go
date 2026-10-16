// Package rangeplacement publishes deterministic Range-to-Region Group
// mappings from already confirmed Root cohorts. It never creates cohorts or
// shrinks a placement set based on liveness.
package rangeplacement

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	etcdraft "go.etcd.io/raft/v3"
)

type Host interface {
	Status(string) (raft.GroupStatus, error)
	ReadIndex(context.Context, string) (uint64, error)
	Submit(string, []byte) error
}

type Coordinator struct {
	host Host
	root *rootstate.State
}

func New(host Host, root *rootstate.State) (*Coordinator, error) {
	if host == nil || root == nil {
		return nil, errors.New("Range placement requires Host and Root state")
	}
	return &Coordinator{host: host, root: root}, nil
}

// PlanRange derives a placement only from stable, confirmed Region views.
// The supplied progress set is expected to come from one Root applied cut.
func PlanRange(r rootstate.Range, progress []rootstate.RegionProgress) (rootstate.RangePlacement, error) {
	if r.Validate() != nil || r.State != rootstate.RangeStateActive || len(progress) == 0 {
		return rootstate.RangePlacement{}, errors.New("Range placement requires an active Range and confirmed Regions")
	}
	placement := rootstate.RangePlacement{RangeID: r.RangeID, RangeEpoch: r.Epoch, OwnerGroupID: r.OwnerGroupID}
	seenRegions, seenGroups := make(map[string]bool), make(map[string]bool)
	ownerFound := false
	for _, view := range progress {
		if !view.Eligible || view.Pending != nil || view.Current.State != rootstate.RegionCohortConfirmed ||
			view.Current.GroupID != view.Group.GroupID || view.Group.StateKind != rootstate.GroupStateEngine ||
			view.Group.Epoch != view.Current.GroupEpoch || seenRegions[view.Current.RegionID] || seenGroups[view.Group.GroupID] {
			continue
		}
		seenRegions[view.Current.RegionID] = true
		seenGroups[view.Group.GroupID] = true
		placement.Regions = append(placement.Regions, rootstate.RangeRegionPlacement{RegionID: view.Current.RegionID,
			GroupID: view.Group.GroupID, Generation: view.Current.Generation})
		if view.Group.GroupID == r.OwnerGroupID {
			ownerFound = true
		}
	}
	sort.Slice(placement.Regions, func(i, j int) bool { return placement.Regions[i].RegionID < placement.Regions[j].RegionID })
	if !ownerFound {
		return rootstate.RangePlacement{}, errors.New("Range Owner has no stable confirmed Region cohort")
	}
	if err := placement.Validate(); err != nil {
		return rootstate.RangePlacement{}, err
	}
	return placement, nil
}

func (c *Coordinator) Reconcile(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	status, err := c.host.Status(raft.RootGroupID)
	if err != nil {
		return err
	}
	if status.Role != etcdraft.StateLeader || status.LeaderID == 0 || status.LeaderID != status.RaftID {
		return nil
	}
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	ranges, err := c.root.ActiveRanges()
	if err != nil {
		return err
	}
	groups, err := c.root.Groups()
	if err != nil {
		return err
	}
	for _, r := range ranges {
		prior, found, err := c.root.RangePlacement(r.RangeID)
		if err != nil {
			return err
		}
		var progress []rootstate.RegionProgress
		for _, group := range groups {
			if group.GroupID == raft.RootGroupID || group.StateKind != rootstate.GroupStateEngine {
				continue
			}
			view, found, err := c.root.RegionProgress(group.GroupID)
			if err != nil {
				return err
			}
			if found {
				progress = append(progress, view)
			}
		}
		placement, err := PlanRange(r, progress)
		if err != nil {
			// A Range can exist before all Region cohorts are confirmed. Leave it
			// pending for the next reconciliation round rather than publishing a
			// partial placement or generation refresh.
			continue
		}
		if found && rangePlacementEqual(prior, placement) {
			continue
		}
		return c.publish(ctx, placement)
	}
	return nil
}

func rangePlacementEqual(a, b rootstate.RangePlacement) bool {
	if a.RangeID != b.RangeID || a.RangeEpoch != b.RangeEpoch || a.OwnerGroupID != b.OwnerGroupID || len(a.Regions) != len(b.Regions) {
		return false
	}
	for i := range a.Regions {
		if a.Regions[i] != b.Regions[i] {
			return false
		}
	}
	return true
}

func (c *Coordinator) publish(ctx context.Context, placement rootstate.RangePlacement) error {
	command := rootstate.Command{Version: rootstate.FormatVersion, Type: rootstate.SetRangePlacement,
		RequestID: "range-placement-intent", RangePlacement: &placement}
	encoded, err := rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(encoded)
	command.RequestID = "range-placement-" + hex.EncodeToString(digest[:])
	encoded, err = rootstate.EncodeCommand(command)
	if err != nil {
		return err
	}
	if err := c.host.Submit(raft.RootGroupID, encoded); err != nil {
		return err
	}
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	result, found, err := c.root.ResultForCommand(encoded)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("Range placement command not yet applied")
	}
	if result.Error != "" {
		return errors.New(result.Error)
	}
	return nil
}
