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
	"time"

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

// A Region generation change invalidates every placement that references it.
// Publish a bounded batch per round instead of making each Range wait for a
// separate background timer. The caller's deadline bounds the entire round.
const placementPublicationsPerRound = 16

func New(host Host, root *rootstate.State) (*Coordinator, error) {
	if host == nil || root == nil {
		return nil, errors.New("Range placement requires Host and Root state")
	}
	return &Coordinator{host: host, root: root}, nil
}

// EnsureInitial lets a first writer finish the existing placement publication
// without waiting for the leader's periodic reconciliation. Any authenticated
// Node may propose; Root Apply still validates the active Range and cohorts.
// Existing placements are never refreshed or expanded by this path.
func (c *Coordinator) EnsureInitial(ctx context.Context, rangeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
		return err
	}
	r, found, err := c.root.Range(rangeID)
	if err != nil {
		return err
	}
	if !found || r.State != rootstate.RangeStateActive {
		return errors.New("initial placement requires an active Range")
	}
	if prior, found, err := c.root.RangePlacement(rangeID); err != nil {
		return err
	} else if found {
		if prior.RangeEpoch != r.Epoch || prior.OwnerGroupID != r.OwnerGroupID {
			return errors.New("existing placement differs from active Range")
		}
		return nil
	}
	groups, err := c.root.Groups()
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
		// Formation/growth remains a background responsibility. The caller
		// continues its bounded wait, without weakening cohort eligibility.
		return nil
	}
	return c.publish(ctx, placement)
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
	published := 0
	for _, r := range ranges {
		if err := ctx.Err(); err != nil {
			return err
		}
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
		var placement rootstate.RangePlacement
		if found {
			placement, err = refreshPlacement(r, prior, progress)
		} else {
			placement, err = PlanRange(r, progress)
		}
		if err != nil {
			// A Range can exist before all Region cohorts are confirmed. Leave it
			// pending for the next reconciliation round rather than publishing a
			// partial placement or generation refresh.
			continue
		}
		if found && rangePlacementEqual(prior, placement) {
			continue
		}
		if err := c.publish(ctx, placement); err != nil {
			return err
		}
		published++
		if published >= placementPublicationsPerRound {
			return nil
		}
	}
	return nil
}

// Refresh only the generations of the fixed Region/Group set. Later Regions
// must not expand old placements, and an unavailable/pending member must not
// silently shrink them. Root's existing SetRangePlacement independently
// validates every generation against the current confirmed cohort.
func refreshPlacement(r rootstate.Range, prior rootstate.RangePlacement, progress []rootstate.RegionProgress) (rootstate.RangePlacement, error) {
	if prior.Validate() != nil || prior.RangeID != r.RangeID || prior.RangeEpoch != r.Epoch || prior.OwnerGroupID != r.OwnerGroupID {
		return rootstate.RangePlacement{}, errors.New("placement refresh differs from the active Range")
	}
	selected := make([]rootstate.RegionProgress, 0, len(prior.Regions))
	for _, region := range prior.Regions {
		for _, view := range progress {
			if view.Current.RegionID == region.RegionID && view.Current.GroupID == region.GroupID {
				selected = append(selected, view)
				break
			}
		}
	}
	updated, err := PlanRange(r, selected)
	if err != nil || len(updated.Regions) != len(prior.Regions) {
		return rootstate.RangePlacement{}, errors.Join(err, errors.New("placement refresh lacks its complete fixed Region set"))
	}
	for i, region := range updated.Regions {
		old := prior.Regions[i]
		if region.RegionID != old.RegionID || region.GroupID != old.GroupID || region.Generation < old.Generation {
			return rootstate.RangePlacement{}, errors.New("placement refresh changes or regresses its Region set")
		}
	}
	return updated, nil
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
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}
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
	// An admitted proposal can commit after the first following ReadIndex.
	// Wait for this exact durable result without resubmitting the mutation.
	for {
		if _, err := c.host.ReadIndex(ctx, raft.RootGroupID); err != nil {
			return fmt.Errorf("confirm Range placement: %w", err)
		}
		result, found, err := c.root.ResultForCommand(encoded)
		if err != nil {
			return err
		}
		if found {
			if result.Error != "" {
				return errors.New(result.Error)
			}
			return nil
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
