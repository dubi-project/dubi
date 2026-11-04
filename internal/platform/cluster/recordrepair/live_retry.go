package recordrepair

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/enginestate"
	"github.com/dubi-project/dubi/internal/platform/placement"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Only exhaustion of temporarily unavailable source observations/transfers is
// eligible. Missing/corrupt bytes, identity failures and remote-only repair
// remain ordinary durable debt, never a reason to waive a full clean sweep.
type transientSourceFailure struct{ error }

func (e *transientSourceFailure) Unwrap() error { return e.error }
func transientSourceError(err error) bool {
	if errors.Is(err, errRemoteRepairCompleted) || errors.Is(err, records.ErrCorrupt) || errors.Is(err, records.ErrUnavailable) {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.Unavailable || status.Code(err) == codes.DeadlineExceeded
}

const liveRetryRows = 8
const liveRetryBytes = 4 << 20
const liveRetryTimeout = 3 * time.Second

// Retry only this page's failed rows, after healthy rows have progressed. The
// worker has not advanced its durable cursor. There is no persistent format,
// global queue or background goroutine; older debt is untouched by the caller.
func retryTransientPage(ctx context.Context, barrier placement.ReadBarrier, owner *enginestate.State, plan placement.LocalPlan,
	local uint64, store *records.Store, client SourceClient, report *LivePageReport) error {
	if len(report.Failures) != len(report.Unresolved) {
		return errors.New("repair retry failure/reference count mismatch")
	}
	attempt, cancel := context.WithTimeout(ctx, liveRetryTimeout)
	defer cancel()
	failures := make([]error, 0, len(report.Failures))
	unresolved := make([]enginestate.RecordReference, 0, len(report.Unresolved))
	count := 0
	var size uint64
	for i, ref := range report.Unresolved {
		failure := report.Failures[i]
		var transient *transientSourceFailure
		if errors.As(failure, &transient) && count < liveRetryRows && ref.Descriptor.Size <= liveRetryBytes-size && attempt.Err() == nil {
			count++
			size += ref.Descriptor.Size
			target := router.Target{Keyspace: ref.Descriptor.Keyspace, RangeID: ref.RangeID, OwnerGroupID: ref.OwnerGroupID, Epoch: ref.RangeEpoch, RoutingKey: ref.RoutingKey}
			_, err := owner.ReadRouted(attempt, barrier, target, func(view enginestate.ReadView) error {
				current, found, err := view.RecordReference(ref.Descriptor.Key)
				if err != nil {
					return err
				}
				if !found || !reflect.DeepEqual(current, ref) {
					return errors.New("publication changed before failed-row retry")
				}
				return nil
			})
			if err == nil {
				fetched, retryErr := repairReference(attempt, plan, ref, local, store, client)
				if retryErr == nil {
					if fetched {
						report.Fetched++
					} else {
						report.AlreadyHeld++
					}
					report.Unrepaired--
					report.References = append(report.References, ref)
					continue
				}
				err = retryErr
			}
			failure = fmt.Errorf("reference %s retry: %w", ref.RequestID, err)
		}
		failures = append(failures, failure)
		unresolved = append(unresolved, ref)
	}
	report.Failures, report.Unresolved = failures, unresolved
	return ctx.Err()
}
