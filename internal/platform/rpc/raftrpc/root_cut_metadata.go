package raftrpc

import (
	"context"
	"errors"
	"strconv"

	"github.com/dubi-project/dubi/internal/platform/router"
	"google.golang.org/grpc/metadata"
)

const confirmedRootCutMetadataKey = "x-dubi-root-read-cut"

func appendConfirmedRootCut(ctx context.Context) context.Context {
	if index, confirmed := router.ConfirmedRootIndex(ctx); confirmed {
		return metadata.AppendToOutgoingContext(ctx, confirmedRootCutMetadataKey, strconv.FormatUint(index, 10))
	}
	return ctx
}

func confirmedRootCutFromMetadata(ctx context.Context) (uint64, bool, error) {
	values, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return 0, false, nil
	}
	cuts := values.Get(confirmedRootCutMetadataKey)
	if len(cuts) == 0 {
		return 0, false, nil
	}
	if len(cuts) != 1 {
		return 0, false, errors.New("multiple Root cuts")
	}
	index, err := strconv.ParseUint(cuts[0], 10, 64)
	if err != nil || index == 0 {
		return 0, false, errors.New("invalid Root cut")
	}
	return index, true, nil
}
