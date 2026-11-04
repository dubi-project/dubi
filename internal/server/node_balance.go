package server

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type nodeBalanceWorker struct {
	runtime         *node.Runtime
	client          *raftrpc.GroupClient
	io              *resource.Maintenance
	report          func(error)
	lastGC          time.Time
	retentionCursor string
	advanced        bool
}

func (w *nodeBalanceWorker) run(ctx context.Context) {
	idle := 30*time.Second + time.Duration(w.runtime.Identity.RaftID%5000)*time.Millisecond
	timer := time.NewTimer(idle)
	defer timer.Stop()
	burst := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		maintenance, finish, err := w.io.TryStart(ctx, "capacity-balance")
		if err != nil {
			timer.Reset(5 * time.Second)
			continue
		}
		attempt, cancel := context.WithTimeout(maintenance, 2*time.Minute)
		err = w.step(attempt)
		cancel()
		finish()
		if err != nil && ctx.Err() == nil && w.report != nil {
			w.report(fmt.Errorf("capacity balance: %w", err))
		}
		if err == nil && w.advanced {
			burst++
			if burst < 8 {
				timer.Reset(100 * time.Millisecond)
			} else {
				burst = 0
				timer.Reset(5 * time.Second)
			}
		} else {
			burst = 0
			timer.Reset(idle)
		}
	}
}

func (w *nodeBalanceWorker) step(ctx context.Context) error {
	w.advanced = false
	if err := w.cleanupFencedPins(ctx); err != nil {
		return err
	}
	if err := w.reclaim(ctx); err != nil {
		return err
	}
	task, err := w.runtime.Records.NodeBalanceTask()
	if err != nil {
		return err
	}
	if task == nil {
		if w.runtime.Packs.PinnedPacks() != 0 {
			return nil
		}
		report, err := w.runtime.Volumes.Status()
		if err != nil {
			return err
		}
		local := volume.WritableDiskCapacity(report, w.runtime.Packs.WriteErrors())
		if local.TotalBytes == 0 {
			return nil
		}
		candidates, err := w.runtime.Records.BalanceCandidates(ctx)
		if err != nil {
			return err
		}
		index, err := w.runtime.ReadRootIndex(ctx)
		if err != nil {
			return err
		}
		self, member, err := w.runtime.Root.RootMember(w.runtime.Identity.RaftID)
		if err != nil || !member || self.NodeID != w.runtime.Identity.NodeID {
			return errors.Join(err, errors.New("balance source is not a current Node"))
		}
		samples := make(map[uint64]raftrpc.RepairCapacityObservation)
		for _, descriptor := range candidates {
			plan, err := recordCapacityPlan(w.runtime, index, descriptor, self.RegionID)
			if err != nil {
				continue // Pending/obsolete/orphan records grant no deletion authority.
			}
			keepers := w.selectKeepers(ctx, plan, descriptor, local, samples)
			if len(keepers) != plan.Required {
				continue
			}
			task, err = w.runtime.Records.BeginNodeBalance(ctx, descriptor, plan.RetentionScope(), keepers)
			if errors.Is(err, records.ErrRetained) || errors.Is(err, records.ErrNotHeld) {
				continue
			}
			if err != nil {
				return err
			}
			break
		}
		if task == nil {
			return nil
		}
	}
	if err := w.advance(ctx, task); err != nil {
		return err
	}
	w.advanced = task.Phase == records.BalanceDropped
	return w.reclaim(ctx)
}

func (w *nodeBalanceWorker) reclaim(ctx context.Context) error {
	through, needed, err := w.runtime.Records.NodeBalanceReclaim()
	if err != nil || !needed {
		return err
	}
	if time.Since(w.lastGC) < time.Minute || w.runtime.Packs.PinnedPacks() != 0 {
		return nil
	}
	if err := w.runtime.Packs.SealAll(ctx); err != nil {
		return err
	}
	if _, err := w.runtime.Records.Compact(ctx); err != nil {
		return err
	}
	if err := w.runtime.Records.CompleteNodeBalanceReclaim(ctx, through); err != nil {
		return err
	}
	w.lastGC = time.Now()
	return nil
}

func (w *nodeBalanceWorker) cleanupFencedPins(ctx context.Context) error {
	active, next, _, err := w.runtime.Records.RetentionPage(ctx, w.retentionCursor)
	if err != nil {
		return err
	}
	if len(active) != 0 {
		if _, err := w.runtime.ReadRootIndex(ctx); err != nil {
			return err
		}
		for _, pin := range active {
			if pin.SourceRaftID == 0 {
				continue
			}
			fence, found, err := w.runtime.Root.NodeFence(pin.SourceRaftID)
			if err != nil {
				return err
			}
			if found && fence.NodeID == pin.SourceNodeID {
				if err := w.runtime.Records.ReleaseRetention(ctx, pin.SourceNodeID, pin.Sequence); err != nil {
					return err
				}
			}
		}
	}
	w.retentionCursor = next
	return nil
}

func (w *nodeBalanceWorker) selectKeepers(ctx context.Context, plan placement.LocalPlan, descriptor records.Descriptor,
	local volume.Capacity, samples map[uint64]raftrpc.RepairCapacityObservation) []rootstate.Node {
	type candidate struct {
		node rootstate.Node
		free float64
	}
	var candidates []candidate
	improvement := false
	for _, n := range plan.Nodes() {
		if n.RaftID == w.runtime.Identity.RaftID {
			continue
		}
		sample, found := samples[n.RaftID]
		if !found {
			var err error
			sample, err = w.client.RepairCapacity(ctx, n)
			if err != nil {
				continue
			}
			samples[n.RaftID] = sample
		}
		// Old peers can still serve ordinary Repair, but cannot authorize
		// discretionary deletion using capacity telemetry they do not expose.
		if !sample.Supported || sample.TotalBytes == 0 || sample.AvailableBytes > sample.TotalBytes {
			continue
		}
		budget := capacityMoveBudget(local, volume.Capacity{TotalBytes: sample.TotalBytes, AvailableBytes: sample.AvailableBytes})
		if budget >= max(descriptor.Size, 8<<20) && sample.WritableBytes >= descriptor.Size+(5<<20) {
			improvement = true
		}
		candidates = append(candidates, candidate{node: n, free: float64(sample.AvailableBytes) / float64(sample.TotalBytes)})
	}
	if !improvement || len(candidates) < plan.Required {
		return nil
	}
	slices.SortFunc(candidates, func(a, b candidate) int {
		if a.free > b.free {
			return -1
		}
		if a.free < b.free {
			return 1
		}
		if a.node.RaftID < b.node.RaftID {
			return -1
		}
		if a.node.RaftID > b.node.RaftID {
			return 1
		}
		return 0
	})
	var keepers []rootstate.Node
	for _, candidate := range candidates[:plan.Required] {
		keepers = append(keepers, candidate.node)
	}
	return keepers
}

func capacityMoveBudget(source, target volume.Capacity) uint64 {
	if source.TotalBytes == 0 || target.TotalBytes == 0 || source.AvailableBytes > source.TotalBytes || target.AvailableBytes > target.TotalBytes {
		return 0
	}
	s, t := float64(source.TotalBytes), float64(target.TotalBytes)
	a, b := float64(source.AvailableBytes), float64(target.AvailableBytes)
	if b/t-a/s < .10 {
		return 0
	}
	return uint64((b*s - a*t) / (s + t) / 2)
}

func (w *nodeBalanceWorker) advance(ctx context.Context, task *records.NodeBalanceTask) error {
	if task.Phase == records.BalancePrepared {
		plan, err := w.currentPlan(ctx, task)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := w.runtime.Records.CancelNodeBalance(ctx, task.Sequence); err != nil {
				return err
			}
			task.Phase = records.BalanceCanceling
		} else {
			holdings := make([]placement.Holding, 0, len(task.Keepers))
			for _, keeper := range task.Keepers {
				h, pinErr := w.client.RetainRecordOnNode(ctx, keeper, task.Sequence, task.Scope, task.Descriptor)
				if pinErr != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if err := w.runtime.Records.CancelNodeBalance(ctx, task.Sequence); err != nil {
						return err
					}
					task.Phase = records.BalanceCanceling
					break
				}
				holdings = append(holdings, placement.Holding{RaftID: keeper.RaftID, NodeID: keeper.NodeID, StoreID: h.StoreID, RecordID: h.RecordID})
			}
			if task.Phase == records.BalancePrepared {
				id, _ := task.Descriptor.ID()
				count, err := plan.Count(id, holdings)
				if err != nil || count < plan.Required {
					// An invalid/duplicate Store proof can never authorize deletion.
					if cancelErr := w.runtime.Records.CancelNodeBalance(ctx, task.Sequence); cancelErr != nil {
						return cancelErr
					}
					task.Phase = records.BalanceCanceling
				} else {
					current, err := w.currentPlan(ctx, task)
					if err != nil || !plan.Matches(current) {
						if ctx.Err() != nil {
							return ctx.Err()
						}
						if err := w.runtime.Records.CancelNodeBalance(ctx, task.Sequence); err != nil {
							return err
						}
						task.Phase = records.BalanceCanceling
					} else if err := w.runtime.Records.DropBalanceHolding(ctx, task.Sequence); errors.Is(err, records.ErrRetained) {
						// Another source retained us. Keep our own bytes and release
						// our outbound promises so that source can make progress.
						if err := w.runtime.Records.CancelNodeBalance(ctx, task.Sequence); err != nil {
							return err
						}
						task.Phase = records.BalanceCanceling
					} else if err != nil {
						return err // Uncertain Sync retains every keeper pin.
					} else {
						task.Phase = records.BalanceDropped
					}
				}
			}
		}
	}
	for _, keeper := range task.Keepers {
		if current, member, err := w.runtime.Root.RootMember(keeper.RaftID); err != nil {
			return err
		} else if member && current.NodeID == keeper.NodeID {
			keeper = current // A rename/address update does not strand pin release.
		}
		if err := w.client.ReleaseRecordRetention(ctx, keeper, task.Sequence); err != nil {
			if task.Phase == records.BalanceCanceling && status.Code(err) == codes.Unimplemented {
				continue // An old peer could never have accepted Retain.
			}
			if fence, found, ferr := w.runtime.Root.NodeFence(keeper.RaftID); ferr == nil && found && fence.NodeID == keeper.NodeID {
				continue // Permanent administrative Fence, never a health timeout.
			}
			return err
		}
	}
	return w.runtime.Records.FinishNodeBalance(ctx, task.Sequence)
}

func (w *nodeBalanceWorker) currentPlan(ctx context.Context, task *records.NodeBalanceTask) (placement.LocalPlan, error) {
	index, err := w.runtime.ReadRootIndex(ctx)
	if err != nil {
		return placement.LocalPlan{}, err
	}
	jobs, err := w.runtime.Root.BackupJobs()
	if err != nil {
		return placement.LocalPlan{}, err
	}
	for _, job := range jobs {
		if job.State == rootstate.BackupJobPending || job.State == rootstate.BackupJobRunning {
			// This check is repeated AFTER every keeper's durable pin ACK.
			// If a backup starts after this Root barrier, the promised majority
			// already held the bytes before that backup's creation cut. No new
			// deletion can pass a barrier which sees the active backup.
			return placement.LocalPlan{}, errors.New("capacity deletion waits for the active cluster backup")
		}
	}
	self, member, err := w.runtime.Root.RootMember(w.runtime.Identity.RaftID)
	if err != nil || !member || self.NodeID != w.runtime.Identity.NodeID {
		return placement.LocalPlan{}, errors.Join(err, errors.New("capacity source is no longer a current Node"))
	}
	plan, err := recordCapacityPlan(w.runtime, index, task.Descriptor, self.RegionID)
	if err != nil || plan.RetentionScope() != task.Scope || len(task.Keepers) != plan.Required {
		return placement.LocalPlan{}, errors.Join(err, errors.New("capacity handoff Region plan changed"))
	}
	for _, keeper := range task.Keepers {
		if keeper.RaftID == self.RaftID || !slices.Contains(plan.Nodes(), keeper) {
			return placement.LocalPlan{}, errors.New("capacity keeper is not another current Region Node")
		}
	}
	return plan, nil
}
