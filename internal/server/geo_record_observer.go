package server

import (
	"context"
	"errors"

	"github.com/dubi-project/dubi/internal/platform/cluster/recordrepair"
	"github.com/dubi-project/dubi/internal/platform/cluster/recordwrite"
	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/identity"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

type geoObservationRoot interface {
	AppliedIndex() uint64
	RootMember(uint64) (rootstate.Node, bool, error)
}

// Remote holding RPCs deliberately reject self-targets. Count this Node only
// after the same confirmed Root cut and an actual local Pack verification;
// membership, an index entry or a successful repair request is not a holding.
func geoRepairObserver(local identity.Identity, root geoObservationRoot, store recordwrite.Verifier,
	remote recordrepair.GeoRecordObserver) recordrepair.GeoRecordObserver {
	return func(ctx context.Context, target rootstate.Node, descriptor records.Descriptor) (raftrpc.HoldingObservation, error) {
		if target.RaftID != local.RaftID {
			return remote(ctx, target, descriptor)
		}
		cut, confirmed := router.ConfirmedRootIndex(ctx)
		qualify := func() error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !confirmed || cut == 0 || root.AppliedIndex() < cut || target.NodeID != local.NodeID {
				return errors.New("local Geo observation requires an applied confirmed Root cut and exact identity")
			}
			node, member, err := root.RootMember(local.RaftID)
			if err != nil {
				return err
			}
			if !member || node.RaftID != local.RaftID || node.NodeID != local.NodeID || node.RegionID != target.RegionID {
				return errors.New("local Geo observer is not the planned current Root member")
			}
			return nil
		}
		if err := qualify(); err != nil {
			return raftrpc.HoldingObservation{}, err
		}
		holding, err := store.Verify(ctx, descriptor)
		if err != nil {
			return raftrpc.HoldingObservation{}, err
		}
		if err := qualify(); err != nil {
			return raftrpc.HoldingObservation{}, err
		}
		return raftrpc.HoldingObservation{RaftID: local.RaftID, NodeID: local.NodeID, RootConfirmedIndex: cut, Holding: holding}, nil
	}
}
