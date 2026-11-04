package enginedata

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxRerouteAttempts = 4

// Root followers may still name the failed leader while an election is in
// flight. Retry a new barrier, but never hide a permanent error in a joined
// list of otherwise transient voter failures. No mutation has run yet.
func retryableRouteRead(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !retryableRouteRead(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return retryableRouteRead(wrapped.Unwrap())
	}
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, raft.ErrNoLeader) || errors.Is(err, router.ErrCatalogBehind) || status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded
}

func retryStaleRoute[T any](ctx context.Context, run func(context.Context) (T, error), mayRetry func(T, error) bool) (T, error) {
	var result T
	var err error
	for attempt := 0; attempt < maxRerouteAttempts; attempt++ {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		result, err = run(ctx)
		if err == nil || !mayRetry(result, err) || attempt+1 == maxRerouteAttempts {
			return result, err
		}
		ctx = router.WithoutConfirmedRootIndex(ctx)
		timer := time.NewTimer(time.Duration(attempt+1) * routeRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return result, ctx.Err()
		case <-timer.C:
		}
	}
	return result, err
}

// Every failed write attempt must prove it rejected the request BEFORE
// submission. One timeout or uncertain publication forbids changing Owners.
func safeWriteReroute(err error) bool {
	if err == nil {
		return false
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		children := joined.Unwrap()
		if len(children) == 0 {
			return false
		}
		for _, child := range children {
			if !safeWriteReroute(child) {
				return false
			}
		}
		return true
	}
	if wrapped, ok := err.(interface{ Unwrap() error }); ok {
		return safeWriteReroute(wrapped.Unwrap())
	}
	return errors.Is(err, router.ErrStaleTarget) || errors.Is(err, ErrOwnerNotLocal) || status.Code(err) == codes.Aborted
}

func safeReadReroute(err error) bool {
	stale, blocked := false, false
	var visit func(error)
	visit = func(err error) {
		if err == nil {
			return
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			for _, child := range joined.Unwrap() {
				visit(child)
			}
			return
		}
		if wrapped, ok := err.(interface{ Unwrap() error }); ok {
			visit(wrapped.Unwrap())
			return
		}
		switch status.Code(err) {
		case codes.PermissionDenied, codes.Unauthenticated, codes.InvalidArgument, codes.DataLoss:
			blocked = true
		case codes.Aborted:
			stale = true
		}
		if errors.Is(err, records.ErrCorrupt) {
			blocked = true
		}
		if errors.Is(err, router.ErrStaleTarget) || errors.Is(err, router.ErrRouteFenced) || errors.Is(err, router.ErrWrongOwner) || errors.Is(err, ErrOwnerNotLocal) {
			stale = true
		}
	}
	visit(err)
	return stale && !blocked
}

// This is a cheap local catalog check, not another global Root round trip.
// The final Owner state-machine fence remains authoritative at Apply.
func (service *Service) checkExecutionRoute(target router.Target) error {
	current, found, err := service.runtime.Root.RouteKeyspace(target.Keyspace, target.RoutingKey)
	if err != nil {
		return err
	}
	if !found {
		return router.ErrNoRoute
	}
	if err := router.CheckOwner(target.OwnerGroupID, current, target); err != nil {
		return fmt.Errorf("%w: %v", router.ErrStaleTarget, err)
	}
	return nil
}
