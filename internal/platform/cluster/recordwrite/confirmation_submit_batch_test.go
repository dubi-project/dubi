package recordwrite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dubi-project/dubi/internal/platform/cluster/routedstate"
	"github.com/dubi-project/dubi/internal/platform/raft"
	"github.com/dubi-project/dubi/internal/platform/rpc/raftrpc"
)

func submitPoolTest(t *testing.T, batch func(context.Context, string, [][]byte) ([]error, error)) *ownerSubmitBatch {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	b := &ownerSubmitBatch{ctx: ctx, cancel: cancel, group: "owner", batch: batch, waits: make(chan ownerSubmitWait, 64), done: make(chan struct{})}
	return b
}
func waitSubmitRow(t *testing.T, w ownerSubmitWait) error {
	t.Helper()
	select {
	case e := <-w.done:
		return e
	case <-time.After(3 * time.Second):
		t.Fatal("submit row never completed")
		return nil
	}
}
func TestOwnerSubmitBatchBoundsAndIndependentErrors(t *testing.T) {
	for _, size := range []int{1, 100 << 10} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			denied := errors.New("one row denied")
			var counts []int
			b := submitPoolTest(t, func(_ context.Context, _ string, cmds [][]byte) ([]error, error) {
				total := 0
				rows := make([]error, len(cmds))
				for i, c := range cmds {
					total += len(c)
					if c[0] == 3 {
						rows[i] = denied
					}
				}
				if total > raft.SubmitBatchBytes || len(cmds) > raft.SubmitBatchEntries {
					t.Errorf("unbounded submission: %d / %d", len(cmds), total)
				}
				counts = append(counts, len(cmds))
				return rows, nil
			})
			var waits []ownerSubmitWait
			for i := 0; i < 40; i++ {
				w := ownerSubmitWait{ctx: context.Background(), command: bytes.Repeat([]byte{byte(i)}, size), done: make(chan error, 1)}
				b.waits <- w
				waits = append(waits, w)
			}
			go b.run()
			for i, w := range waits {
				e := waitSubmitRow(t, w)
				if (i == 3 && !errors.Is(e, denied)) || (i != 3 && e != nil) {
					t.Fatal(i, e)
				}
			}
			b.close()
			want := 2
			if size > 1 {
				want = 20
			}
			if len(counts) != want {
				t.Fatal(counts)
			}
		})
	}
}
func TestOwnerSubmitBatchCancellationJoinsInFlight(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	b := submitPoolTest(t, func(context.Context, string, [][]byte) ([]error, error) {
		close(entered)
		<-release
		return []error{nil}, nil
	})
	go b.run()
	caller := make(chan error, 1)
	go func() { caller <- b.Submit(context.Background(), "owner", []byte("a")) }()
	<-entered
	closed := make(chan struct{})
	go func() { b.close(); close(closed) }()
	select {
	case e := <-caller:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("caller did not cancel")
	}
	select {
	case <-closed:
		t.Fatal("page returned with submission still running")
	default:
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("collector leaked")
	}
}
func TestOwnerSubmitBatchFallbackCancellationAndBadResponse(t *testing.T) {
	for _, mode := range []string{"cancel", "oversize", "foreign", "bad-response", "failure"} {
		t.Run(mode, func(t *testing.T) {
			var singles, batches atomic.Int64
			injected := errors.New("runtime failed")
			b := submitPoolTest(t, func(context.Context, string, [][]byte) ([]error, error) {
				batches.Add(1)
				if mode == "failure" {
					return nil, injected
				}
				return nil, nil
			})
			b.single = func(context.Context, string, []byte) error { singles.Add(1); return nil }
			go b.run()
			defer b.close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			group := "owner"
			command := []byte("a")
			if mode == "cancel" {
				cancel()
			}
			if mode == "oversize" {
				command = make([]byte, raft.SubmitBatchBytes+1)
			}
			if mode == "foreign" {
				group = "other"
			}
			err := b.Submit(ctx, group, command)
			switch mode {
			case "cancel":
				if !errors.Is(err, context.Canceled) || singles.Load() != 0 || batches.Load() != 0 {
					t.Fatal(err)
				}
			case "oversize", "foreign":
				if err != nil || singles.Load() != 1 || batches.Load() != 0 {
					t.Fatal(err)
				}
			case "failure":
				if !errors.Is(err, injected) {
					t.Fatal(err)
				}
			default:
				if err == nil {
					t.Fatal("invalid batch result accepted")
				}
			}
		})
	}
}
func TestOwnerSubmitBatchPageKeepsPerRecordDurableResults(t *testing.T) {
	for _, reject := range []bool{false, true} {
		t.Run(fmt.Sprint(reject), func(t *testing.T) {
			w, refs, _, _, _, _ := geoBatchFixture(t, 32)
			single := w.Submit
			var submitted, calls atomic.Int64
			w.SubmitBatch = func(ctx context.Context, g string, commands [][]byte) ([]error, error) {
				calls.Add(1)
				rows := make([]error, len(commands))
				for i, c := range commands {
					submitted.Add(1)
					cmd, e := routedstate.DecodeCommand(c)
					if e != nil {
						return nil, e
					}
					if reject && cmd.Geo != nil && cmd.Geo.ReferenceRequestID == refs[0].RequestID {
						rows[i] = errors.New("injected per-row rejection")
					} else {
						rows[i] = single(ctx, g, c)
					}
				}
				return rows, nil
			}
			original := w.MirrorGeo
			w.MirrorGeo = batchMirrorFunc{single: original.MirrorGeo, batch: func(ctx context.Context, n uint64, g string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
				rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
				for i, c := range commands {
					rows[i].Result, rows[i].Err = original.MirrorGeo(ctx, n, g, c)
				}
				return rows, nil
			}}
			rows, e := w.RefreshGeoConfirmationPage(context.Background(), refs, 3, 3)
			if e != nil {
				t.Fatal(e)
			}
			for i, row := range rows {
				if reject && i == 0 {
					if row.Err == nil || row.Confirmed {
						t.Fatal("failed proposal counted as confirmed", row)
					}
				} else if row.Err != nil || !row.Confirmed {
					t.Fatal(i, row)
				}
			}
			if submitted.Load() != 64 || calls.Load() == 0 {
				t.Fatal(submitted.Load(), calls.Load())
			}
			t.Logf("page proposals=%d batches=%d", submitted.Load(), calls.Load())
			// The existing page path still obtains fresh cuts and exact per-command
			// results through the real engine fixture, not a batch-wide success flag.
		})
	}
}

func TestOwnerSubmitBatchAdmissionAloneNeverConfirmsPage(t *testing.T) {
	w, refs, _, _, _, _ := geoBatchFixture(t, 2)
	var admitted atomic.Int64
	w.SubmitBatch = func(_ context.Context, _ string, commands [][]byte) ([]error, error) {
		admitted.Add(int64(len(commands)))
		return make([]error, len(commands)), nil
	}
	original := w.MirrorGeo
	w.MirrorGeo = batchMirrorFunc{single: original.MirrorGeo, batch: func(ctx context.Context, n uint64, g string, commands [][]byte) ([]raftrpc.GeoMirrorBatchResult, error) {
		rows := make([]raftrpc.GeoMirrorBatchResult, len(commands))
		for i, c := range commands {
			rows[i].Result, rows[i].Err = original.MirrorGeo(ctx, n, g, c)
		}
		return rows, nil
	}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	rows, err := w.RefreshGeoConfirmationPage(ctx, refs, 3, 3)
	if admitted.Load() == 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(admitted.Load(), rows, err)
	}
	for _, r := range rows {
		if r.Confirmed {
			t.Fatal("proposal admission used as durable proof")
		}
	}
}
