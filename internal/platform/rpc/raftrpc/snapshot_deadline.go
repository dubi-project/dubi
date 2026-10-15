package raftrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dubi-project/dubi/internal/platform/rpc/raftwire"
	"github.com/dubi-project/dubi/internal/platform/storage/snapshot"
)

// Automatic internal policy, not per-node deployment knobs. Transfer progress
// is a successful bounded gRPC Send, NOT a durable receiver acknowledgement.
// Install gets a separate grace after EOF; it has no progress messages in the
// current client-streaming protocol. All durations have a finite ceiling.
type snapshotDeadlines struct {
	idle    time.Duration
	install time.Duration
	total   time.Duration
}

func deadlinesForSnapshot(size uint64) snapshotDeadlines {
	size = min(size, snapshot.MaxStreamBytes+uint64(maxSnapshotEnvelope+snapshotWirePrefixSize))
	secondsAt := func(rate uint64) time.Duration {
		seconds := size / rate
		if size%rate != 0 {
			seconds++
		}
		return time.Duration(seconds) * time.Second
	}
	install := min(6*time.Hour, 15*time.Minute+secondsAt(1<<20))
	return snapshotDeadlines{
		idle:    2 * time.Minute,
		install: install,
		total:   min(24*time.Hour, 15*time.Minute+secondsAt(256<<10)+install),
	}
}

type snapshotProgress struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelCauseFunc
	stopTotal  context.CancelFunc
	timer      *time.Timer
	limits     snapshotDeadlines
	deadline   time.Time
	installing bool
	closed     bool
}

func newSnapshotProgress(parent context.Context, limits snapshotDeadlines) *snapshotProgress {
	total, stop := context.WithTimeout(parent, limits.total)
	ctx, cancel := context.WithCancelCause(total)
	progress := &snapshotProgress{ctx: ctx, cancel: cancel, stopTotal: stop, limits: limits,
		deadline: time.Now().Add(limits.idle)}
	progress.mu.Lock()
	progress.timer = time.AfterFunc(limits.idle, progress.expire)
	progress.mu.Unlock()
	return progress
}

func (p *snapshotProgress) expire() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.ctx.Err() != nil {
		return
	}
	// A prior callback can already be waiting for mu when progress resets its
	// timer. Recheck the absolute deadline before canceling the current phase.
	if remaining := time.Until(p.deadline); remaining > 0 {
		p.timer.Reset(remaining)
		return
	}
	p.cancelExpired()
}

func (p *snapshotProgress) cancelExpired() {
	phase := "transfer made no progress"
	if p.installing {
		phase = "durable installation exceeded its grace"
	}
	p.cancel(fmt.Errorf("snapshot %s: %w", phase, context.DeadlineExceeded))
}

func (p *snapshotProgress) renew(installation bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ctx.Err() != nil {
		return context.Cause(p.ctx)
	}
	if p.closed {
		return errors.New("snapshot progress tracker is closed")
	}
	// A late Send may win scheduling against its timeout callback. It must not
	// resurrect an already expired phase.
	if !time.Now().Before(p.deadline) {
		p.cancelExpired()
		return context.Cause(p.ctx)
	}
	if p.installing {
		return errors.New("snapshot transfer already ended")
	}
	delay := p.limits.idle
	if installation {
		p.installing = true
		delay = p.limits.install
	}
	p.deadline = time.Now().Add(delay)
	p.timer.Reset(delay)
	return nil
}

func (p *snapshotProgress) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	p.timer.Stop()
	p.cancel(context.Canceled)
	p.stopTotal()
}

// Wrapping the generated client centralizes progress for both legacy and file
// paths. Nothing renews the install grace while waiting for the durable ACK.
type progressSnapshotClient struct {
	raftwire.RaftService_InstallSnapshotClient
	progress *snapshotProgress
}

func (s *progressSnapshotClient) Send(frame *raftwire.InstallSnapshotRequest) error {
	if err := s.RaftService_InstallSnapshotClient.Send(frame); err != nil {
		return err
	}
	return s.progress.renew(false)
}

func (s *progressSnapshotClient) CloseAndRecv() (*raftwire.InstallSnapshotResponse, error) {
	if err := s.progress.renew(true); err != nil {
		return nil, err
	}
	return s.RaftService_InstallSnapshotClient.CloseAndRecv()
}
