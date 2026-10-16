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
	report, err := fillMode(ctx, cut.Seal.Intent.Previous.Fence.Nodes(), cut.Seal.Intent.Next.Nodes, localRaftID, sources, cut.Seal.Intent.Recovery,
		func(visit func(enginestate.RecordReference) error) error {
			return enginestate.WalkVerifiedRecordCut(ctx, artifacts, cut, visit)
		},
		func(d records.Descriptor) error { _, err := destination.Verify(ctx, d); return err },
		func(attempt context.Context, source rootstate.Node, d records.Descriptor) error {
			_, err := client.FetchRecord(attempt, source, d, destination)
			return err
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
		} else {
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
	return report, nil
}
