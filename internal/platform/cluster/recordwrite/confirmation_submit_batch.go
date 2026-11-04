package recordwrite

import (
	"context"
	"errors"
	"time"

	"github.com/dubi-project/dubi/internal/platform/raft"
)

type ownerSubmitWait struct {
	ctx     context.Context
	command []byte
	done    chan error
}

// A page owns this bounded collector. It drains only currently ready commands;
// there is no timer, durable queue, retry, or change to the result/read barrier.
type ownerSubmitBatch struct {
	ctx    context.Context
	cancel context.CancelFunc
	group  string
	single SubmitCommand
	batch  func(context.Context, string, [][]byte) ([]error, error)
	waits  chan ownerSubmitWait
	done   chan struct{}
}

func newOwnerSubmitBatch(ctx context.Context, w Writer) *ownerSubmitBatch {
	ctx, cancel := context.WithCancel(ctx)
	b := &ownerSubmitBatch{ctx: ctx, cancel: cancel, group: w.Owner.GroupID(), single: w.Submit, batch: w.SubmitBatch,
		waits: make(chan ownerSubmitWait, GeoConfirmationRepairPageLimit), done: make(chan struct{})}
	go b.run()
	return b
}
func (b *ownerSubmitBatch) close() { b.cancel(); <-b.done }
func (b *ownerSubmitBatch) Submit(ctx context.Context, group string, command []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if group != b.group || len(command) > raft.SubmitBatchBytes {
		return b.single(ctx, group, command)
	}
	// Keep the existing per-command timing inclusive of collector queue waits;
	// timing only the shared batch would hide contention from comparisons.
	started := time.Now()
	defer Trace(ctx, "repair.owner_submit", started)
	w := ownerSubmitWait{ctx: ctx, command: command, done: make(chan error, 1)}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
		return b.ctx.Err()
	case b.waits <- w:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.ctx.Done():
		return b.ctx.Err()
	case err := <-w.done:
		return err
	}
}
func (b *ownerSubmitBatch) run() {
	defer close(b.done)
	var carry *ownerSubmitWait
	for {
		if b.ctx.Err() != nil {
			return
		}
		var first ownerSubmitWait
		if carry != nil {
			first = *carry
			carry = nil
		} else {
			select {
			case <-b.ctx.Done():
				return
			case first = <-b.waits:
			}
		}
		rows := []ownerSubmitWait{first}
		size := len(first.command)
	collect:
		for len(rows) < raft.SubmitBatchEntries {
			select {
			case w := <-b.waits:
				if len(w.command) > raft.SubmitBatchBytes-size {
					carry = &w
					break collect
				}
				rows = append(rows, w)
				size += len(w.command)
			default:
				break collect
			}
		}
		live := rows[:0]
		var commands [][]byte
		for _, w := range rows {
			if w.ctx.Err() == nil {
				live = append(live, w)
				commands = append(commands, w.command)
			}
		}
		if len(live) == 0 {
			continue
		}
		results, err := b.batch(b.ctx, b.group, commands)
		if err == nil && len(results) != len(live) {
			err = errors.New("Owner proposal batch returned invalid result count")
		}
		for i, w := range live {
			if err != nil {
				w.done <- err
			} else {
				w.done <- results[i]
			}
		}
	}
}
