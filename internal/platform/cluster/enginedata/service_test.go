package enginedata

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/rootstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/router"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
)

func TestIncompleteEngineDataEntryFailsClosed(t *testing.T) {
	if service, err := New(nil, nil); err == nil || service != nil {
		t.Fatal("incomplete Node constructed a engine data entry", service, err)
	}
	space := rootstate.Keyspace{TenantID: "tenant", EngineID: "example", DatasetID: "data"}
	var incomplete Service
	if result, err := incomplete.WriteLocal(context.Background(), WriteRequest{Keyspace: space,
		Key: []byte("key"), Publish: func(records.Descriptor) ([]byte, error) { return nil, nil }}); err == nil || result.State != WriteUnknown {
		t.Fatal("incomplete entry accepted a write", result, err)
	}
	if result, err := incomplete.ReadLocal(context.Background(), space, []byte("key")); err == nil || len(result.Body) != 0 {
		t.Fatal("incomplete entry accepted a read", result, err)
	}
}

func TestRetryRouteAfterTransientRootCut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	want := router.Target{RangeID: "after-new-root-cut"}
	got, err := retryRoute(ctx, 10*time.Millisecond, func(attempt context.Context) (router.Target, error) {
		calls++
		switch calls {
		case 1:
			<-attempt.Done()
			return router.Target{}, attempt.Err()
		case 2:
			return router.Target{}, raft.ErrNoLeader
		case 3:
			return router.Target{}, router.ErrCatalogBehind
		default:
			return want, nil
		}
	})
	if err != nil || got.RangeID != want.RangeID || calls != 4 {
		t.Fatal("transient Root cuts were not retried", got, calls, err)
	}
}

func TestRetryRouteDoesNotHideStableFailure(t *testing.T) {
	calls := 0
	_, err := retryRoute(context.Background(), time.Second, func(context.Context) (router.Target, error) {
		calls++
		return router.Target{}, router.ErrNoRoute
	})
	if !errors.Is(err, router.ErrNoRoute) || calls != 1 {
		t.Fatal("stable routing error was retried or rewritten", calls, err)
	}
}

func TestReadCandidateContextReservesTimeForNextOwner(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	first, done := readCandidateContext(parent, 3)
	defer done()
	parentDeadline, _ := parent.Deadline()
	firstDeadline, _ := first.Deadline()
	if gap := parentDeadline.Sub(firstDeadline); gap < 150*time.Millisecond {
		t.Fatal("first Owner read consumed time reserved for other candidates", gap)
	}
	noDeadline, release := readCandidateContext(context.Background(), 2)
	defer release()
	if deadline, ok := noDeadline.Deadline(); !ok || time.Until(deadline) > readAttemptTimeout {
		t.Fatal("unbounded Owner read attempt", deadline, ok)
	}
}

func TestWriteCandidateContextReservesTimeForNextOwner(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	first, done := writeCandidateContext(parent, 3)
	defer done()
	parentDeadline, _ := parent.Deadline()
	firstDeadline, _ := first.Deadline()
	if gap := parentDeadline.Sub(firstDeadline); gap < 150*time.Millisecond {
		t.Fatal("first Owner write consumed time reserved for other candidates", gap)
	}
	noDeadline, release := writeCandidateContext(context.Background(), 2)
	defer release()
	if deadline, ok := noDeadline.Deadline(); !ok || time.Until(deadline) > writeAttemptTimeout {
		t.Fatal("unbounded Owner write attempt", deadline, ok)
	}
}
