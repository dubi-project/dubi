package raftrpc

import (
	"errors"
	"fmt"

	"github.com/dubi-project/dubi/internal/platform/storage/diskpressure"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Keep pre-proposal capacity rejection distinct from peer unavailability. A
// client must not immediately route the same load to another member just
// because this node has reached the Store reserve or cannot verify its disk.
func mutationError(operation string, err error, fallback codes.Code) error {
	if status.Code(err) == codes.ResourceExhausted {
		return err
	}
	if errors.Is(err, diskpressure.ErrPressure) || errors.Is(err, diskpressure.ErrProbe) {
		s := status.New(codes.ResourceExhausted, fmt.Sprintf("%s: %v", operation, err))
		withRetry, detailErr := s.WithDetails(&errdetails.RetryInfo{RetryDelay: durationpb.New(diskpressure.RetryDelay)})
		if detailErr == nil {
			return withRetry.Err()
		}
		return s.Err()
	}
	return status.Errorf(fallback, "%s: %v", operation, err)
}
