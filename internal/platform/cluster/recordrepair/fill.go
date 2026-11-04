// Package recordrepair provides bounded, physical backfill for a sealed
// immutable-record cut. It does not publish a Region generation or a quorum
// receipt; the caller must authenticate the cut and recheck Root authority.
package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
)

type Report struct {
	References  uint64
	AlreadyHeld uint64
	Fetched     uint64
	// ReferencedBytes includes both already-held and newly fetched records.
	ReferencedBytes uint64
}

// FillLocalCut fetches each missing physical record in a fully verified cut
// from old Region participants. Ordinary growth requires the exact old cohort;
// an explicitly authorized maintenance-recovery seal may use only the live
// old-member subset. Sources never count as durable votes. A source is
// individually authenticated by FetchRecord, and every local result is
// physically re-read. The supplied cut must already have trusted provenance;
// its local file digest alone does not establish authority or retention.
func FillLocalCut(ctx context.Context, artifacts *snapshot.Store, cut enginestate.RecordCut, localRaftID uint64, sources []rootstate.Node, destination *records.Store, client *raftrpc.GroupClient) (Report, error) {
	if ctx == nil || artifacts == nil || destination == nil || client == nil || cut.Validate() != nil {
		return Report{}, errors.New("record fill requires a valid cut, Store and authenticated client")
	}
	region := cut.Seal.Intent.Previous.Fence.RegionID
	for _, source := range sources {
		if source.RegionID != region || source.NodeID == "" || source.Address == "" {
			return Report{}, errors.New("record fill source differs from the sealed Region")
		}
	}
	report, err := fillModePipelined(ctx, cut.Seal.Intent.Previous.Fence.Nodes(), cut.Seal.Intent.Next.Nodes, localRaftID, sources, cut.Seal.Intent.Recovery,
		func(visit func(enginestate.RecordReference) error) error {
			return enginestate.WalkVerifiedRecordCut(ctx, artifacts, cut, visit)
		},
		func(d records.Descriptor) error { _, err := destination.Verify(ctx, d); return err },
		func(attempt context.Context, source rootstate.Node, d records.Descriptor) error {
			_, err := client.FetchRecord(attempt, source, d, destination)
			return err
		}, func(attempt context.Context, source rootstate.Node, ds []records.Descriptor) error {
			return client.FetchRecords(attempt, source, ds, destination)
		})
	if err != nil {
		return Report{}, err
	}
	if report.References != cut.Count || report.ReferencedBytes != cut.PayloadBytes {
		return Report{}, errors.New("record fill differs from the verified cut summary")
	}
	return report, nil
}

type cutWalker func(func(enginestate.RecordReference) error) error
type verifier func(records.Descriptor) error
type fetcher func(context.Context, rootstate.Node, records.Descriptor) error

func fill(ctx context.Context, oldIDs, nextIDs []uint64, localRaftID uint64, sources []rootstate.Node, walk cutWalker, verify verifier, fetch fetcher) (Report, error) {
	return fillMode(ctx, oldIDs, nextIDs, localRaftID, sources, false, walk, verify, fetch)
}

func fillMode(ctx context.Context, oldIDs, nextIDs []uint64, localRaftID uint64, sources []rootstate.Node, recoverySources bool, walk cutWalker, verify verifier, fetch fetcher) (Report, error) {
	return fillModeBatched(ctx, oldIDs, nextIDs, localRaftID, sources, recoverySources, walk, verify, fetch, nil)
}

type batchFetcher func(context.Context, rootstate.Node, []records.Descriptor) error

func fillModeBatched(ctx context.Context, oldIDs, nextIDs []uint64, localRaftID uint64, sources []rootstate.Node, recoverySources bool, walk cutWalker, verify verifier, fetch fetcher, batch batchFetcher) (Report, error) {
	var report Report
	if ctx == nil || walk == nil || verify == nil || fetch == nil || localRaftID == 0 || !slices.Contains(nextIDs, localRaftID) || len(oldIDs) == 0 || len(sources) == 0 || (!recoverySources && len(sources) != len(oldIDs)) || (recoverySources && len(sources) < 2) {
		return report, errors.New("record fill requires an old cohort and a next-generation destination")
	}
	remaining := make(map[uint64]bool, len(oldIDs))
	for _, id := range oldIDs {
		if id == 0 || remaining[id] {
			return report, errors.New("invalid sealed old cohort")
		}
		remaining[id] = true
	}
	for _, source := range sources {
		if !remaining[source.RaftID] {
			return report, errors.New("record fill has a duplicate or foreign source")
		}
		delete(remaining, source.RaftID)
	}
	if len(remaining) != 0 && !recoverySources {
		return report, errors.New("record fill omits an old participant")
	}

	// Keep only a bounded page of missing descriptors. When no single source
	// holds the whole page, ordinary per-record source selection remains valid.
	var pending []enginestate.RecordReference
	var pendingBytes uint64
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		ds := make([]records.Descriptor, len(pending))
		for i, ref := range pending {
			ds[i] = ref.Descriptor
		}
		remaining := 0
		for _, source := range sources {
			if source.RaftID != localRaftID {
				remaining++
			}
		}
		for _, source := range sources {
			if source.RaftID == localRaftID {
				continue
			}
			attempt, cancel := sourceAttemptContext(ctx, remaining)
			remaining--
			err := batch(attempt, source, ds)
			cancel()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err == nil {
				for _, d := range ds {
					if err := verify(d); err != nil {
						return fmt.Errorf("verify fetched batch record: %w", err)
					}
				}
				report.Fetched += uint64(len(ds))
				pending = nil
				pendingBytes = 0
				return nil
			}
			if !retryableRecordSource(err) {
				return fmt.Errorf("fetch sealed batch from Node %d: %w", source.RaftID, err)
			}
		}
		// Disjoint source holdings and partial local I/O are handled by the same
		// verified per-record resume path. No batch success is treated as a vote.
		result, err := fillMode(ctx, oldIDs, nextIDs, localRaftID, sources, recoverySources, func(visit func(enginestate.RecordReference) error) error {
			for _, ref := range pending {
				if err := visit(ref); err != nil {
					return err
				}
			}
			return nil
		}, verify, fetch)
		if err != nil {
			return err
		}
		report.AlreadyHeld += result.AlreadyHeld
		report.Fetched += result.Fetched
		pending = nil
		pendingBytes = 0
		return nil
	}
	err := walk(func(ref enginestate.RecordReference) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		d := ref.Descriptor
		if err := d.Validate(); err != nil {
			return err
		}
		if err := verify(d); err == nil {
			report.AlreadyHeld++
		} else if !errors.Is(err, records.ErrNotHeld) {
			// A damaged local index or Pack is not an invitation to overwrite it.
			return fmt.Errorf("verify local record: %w", err)

		} else if batch != nil && d.Size <= raftrpc.FetchRecordBatchBytes {
			if len(pending) == raftrpc.FetchRecordBatchLimit || d.Size > raftrpc.FetchRecordBatchBytes-pendingBytes {
				if err := flush(); err != nil {
					return err
				}
			}
			// The cut walker may reuse its frame; retain only our own descriptor key.
			d.Key = slices.Clone(d.Key)
			pending = append(pending, enginestate.RecordReference{Descriptor: d})
			pendingBytes += d.Size
		} else {
			if err := flush(); err != nil {
				return err
			}
			var last error
			copied := false
			remaining := 0
			for _, source := range sources {
				if source.RaftID != localRaftID {
					remaining++
				}
			}
			for _, source := range sources {
				if source.RaftID == localRaftID {
					continue
				}
				if err := ctx.Err(); err != nil {
					return err
				}
				attempt, cancel := sourceAttemptContext(ctx, remaining)
				remaining--
				last = fetch(attempt, source, d)
				cancel()
				if err := ctx.Err(); err != nil {
					return err
				}
				if last == nil {
					if err := verify(d); err != nil {
						return fmt.Errorf("verify fetched record: %w", err)
					}
					report.Fetched++
					copied = true
					break
				}
				if !retryableRecordSource(last) {
					return fmt.Errorf("fetch sealed record from Node %d: %w", source.RaftID, last)
				}
			}
			if !copied {
				if last == nil {
					return errors.New("no independent old participant can supply sealed record")
				}
				return fmt.Errorf("no old participant supplied sealed record: %w", last)
			}
		}
		if report.ReferencedBytes > ^uint64(0)-d.Size {
			return errors.New("record fill byte count overflow")
		}
		report.References++
		report.ReferencedBytes += d.Size
		return nil
	})
	if err != nil {
		return Report{}, err
	}
	if err := flush(); err != nil {
		return Report{}, err
	}
	return report, nil
}
