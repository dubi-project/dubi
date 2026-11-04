package recordwrite

import "context"

type ownerConfirmationWait struct {
	ctx  context.Context
	done chan ownerConfirmationCut
}
type ownerConfirmationCut struct {
	index uint64
	err   error
}

// Only requests admitted BEFORE starting ReadIndex may share that cut. A
// caller arriving while it is in flight must join the next batch, even if the
// older result has not returned yet. No time-based cache or cross-page reuse.
type ownerConfirmationBatch struct {
	Barrier
	group  string
	ctx    context.Context
	cancel context.CancelFunc
	waits  chan ownerConfirmationWait
	done   chan struct{}
}

func newOwnerConfirmationBatch(ctx context.Context, base Barrier, group string) *ownerConfirmationBatch {
	ctx, cancel := context.WithCancel(ctx)
	b := &ownerConfirmationBatch{Barrier: base, group: group, ctx: ctx, cancel: cancel,
		waits: make(chan ownerConfirmationWait, GeoConfirmationRepairPageLimit), done: make(chan struct{})}
	go b.run()
	return b
}

func (b *ownerConfirmationBatch) close() { b.cancel(); <-b.done }

func (b *ownerConfirmationBatch) ReadIndex(ctx context.Context, group string) (uint64, error) {
	if group != b.group {
		return b.Barrier.ReadIndex(ctx, group)
	}
	w := ownerConfirmationWait{ctx: ctx, done: make(chan ownerConfirmationCut, 1)}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case b.waits <- w:
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case r := <-w.done:
		return r.index, r.err
	}
}

func (b *ownerConfirmationBatch) run() {
	defer close(b.done)
	for {
		var batch []ownerConfirmationWait
		select {
		case <-b.ctx.Done():
			return
		case w := <-b.waits:
			batch = append(batch, w)
		}
	collect:
		for len(batch) < GeoConfirmationRepairPageLimit {
			select {
			case w := <-b.waits:
				batch = append(batch, w)
			default:
				break collect
			}
		}
		live := batch[:0]
		for _, w := range batch {
			if w.ctx.Err() == nil {
				live = append(live, w)
			}
		}
		if len(live) == 0 {
			continue
		}
		index, err := b.Barrier.ReadIndex(b.ctx, b.group)
		for _, w := range live {
			w.done <- ownerConfirmationCut{index, err}
		}
	}
}
