package recordwrite

import (
	"context"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Only the reconfirmation coordinator constructs this transport, after its
// current publication and physical Region-majority checks. An Owner checkpoint
// can use that same local Group runtime instead of forwarding to another member.
// All other destinations retain the authenticated remote transport.
type geoReconfirmationMirrors struct{ writer Writer }

func (m geoReconfirmationMirrors) preferredGeoMirrorNode(group string) uint64 {
	if group == m.writer.Owner.GroupID() {
		return m.writer.LocalID
	}
	return 0
}

func (m geoReconfirmationMirrors) MirrorGeo(ctx context.Context, target uint64, group string, encoded []byte) (enginestate.Result, error) {
	w := m.writer
	if target != w.LocalID {
		return w.MirrorGeo.MirrorGeo(ctx, target, group, encoded)
	}
	started := time.Now()
	defer Trace(ctx, "repair.mirror_region", started)
	if err := ctx.Err(); err != nil {
		return enginestate.Result{}, err
	}
	command, err := routedstate.DecodeCommand(encoded)
	if err != nil || command.Type != routedstate.MirrorGeo || command.GeoMetadata == nil ||
		group != w.Owner.GroupID() || command.GeoMetadata.DestinationGroupID != group || command.GeoMetadata.Target.OwnerGroupID != group {
		return enginestate.Result{}, status.Error(codes.InvalidArgument, "invalid local Owner Geo mirror")
	}
	if err := w.Submit(ctx, group, encoded); err != nil {
		return enginestate.Result{}, err
	}
	// Submit only queues a proposal. Match the exact deterministic command hash
	// after a quorum-confirmed Owner cut; a durable rejection remains a rejection.
	return w.awaitResult(ctx, group, func() (enginestate.Result, bool, error) {
		return w.Owner.ResultForGeoMetadataMirror(command.RequestID, *command.GeoMetadata)
	})
}
