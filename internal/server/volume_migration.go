package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/resource"
	"github.com/dubi-project/dubi/internal/platform/storage/records"
	"github.com/dubi-project/dubi/internal/platform/storage/volume"
)

// volumeMigrationWorker resumes a durable node-local task after restart. It
// deliberately processes one bounded page per interval so foreground Raft and
// data IO retain priority. Operator removal may still call the same Store API
// synchronously and is serialized by the Store lock.
type volumeMigrationWorker struct {
	runtime            *node.Runtime
	report             func(error)
	budget             *resource.ByteBudget
	io                 *resource.Maintenance
	lastBalanceAttempt time.Time
	advanced           bool
}

func (worker *volumeMigrationWorker) run(ctx context.Context) {
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			stepCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
			maintenanceCtx, finish, admissionErr := worker.io.TryStart(stepCtx, "volume-balance")
			if admissionErr != nil {
				cancel()
				timer.Reset(5 * time.Second)
				continue
			}
			err := worker.step(maintenanceCtx)
			finish()
			cancel()
			if err != nil && ctx.Err() == nil && worker.report != nil {
				worker.report(fmt.Errorf("resume Volume migration: %w", err))
			}
			if err == nil && worker.advanced {
				timer.Reset(100 * time.Millisecond)
			} else {
				timer.Reset(5 * time.Second)
			}
		}
	}
}

func (worker *volumeMigrationWorker) step(ctx context.Context) error {
	if worker == nil || worker.runtime == nil || worker.runtime.Records == nil {
		return nil
	}
	worker.advanced = false
	status, err := worker.runtime.Records.VolumeMigration()
	if err != nil {
		return err
	}
	if !status.Active {
		if worker.runtime.Packs != nil && worker.runtime.Packs.PinnedPacks() != 0 {
			return nil
		}
		if worker.runtime.Volumes == nil || time.Since(worker.lastBalanceAttempt) < time.Minute {
			return nil
		}
		worker.lastBalanceAttempt = time.Now()
		report, err := worker.runtime.Volumes.Status()
		if err != nil {
			return err
		}
		plan, needed := volume.PlanBalance(report)
		if !needed {
			return nil
		}
		started, err := worker.runtime.Records.BeginVolumeBalance(ctx, plan)
		if errors.Is(err, records.ErrBalanceScanPending) {
			worker.advanced = true
			worker.lastBalanceAttempt = time.Time{}
			return nil
		}
		if err != nil || !started {
			return err
		}
		status, err = worker.runtime.Records.VolumeMigration()
		if err != nil {
			return err
		}
	}
	if status.TargetVolumeID != "" && worker.runtime.Volumes != nil {
		report, err := worker.runtime.Volumes.Status()
		if err != nil {
			return err
		}
		healthySource, healthyTarget := false, false
		for _, v := range report.Volumes {
			if v.Health == "healthy" && v.State == volume.Active {
				healthySource = healthySource || v.ID == status.SourceVolumeID
				healthyTarget = healthyTarget || v.ID == status.TargetVolumeID
			}
		}
		if !healthySource || !healthyTarget {
			return worker.runtime.Records.CancelVolumeBalance()
		}
	}
	if worker.budget != nil {
		release, err := worker.budget.TryAcquire(64 << 20)
		if err != nil {
			return err
		}
		defer release()
	}
	moved, done, err := worker.runtime.Records.MigrateVolumeStep(ctx, status.SourceVolumeID)
	if err == nil {
		after, readErr := worker.runtime.Records.VolumeMigration()
		if readErr != nil {
			return readErr
		}
		worker.advanced = moved > 0 || done || after.Cursor != status.Cursor
	}
	if err == nil && done && status.TargetVolumeID != "" {
		_, err = worker.runtime.Records.Compact(ctx)
		if err != nil {
			return errors.Join(err, worker.runtime.Records.NoteVolumeBalanceFailure(err.Error()))
		}
		err = worker.runtime.Records.CompleteVolumeBalance(status.SourceVolumeID, status.TargetVolumeID, status.SourcePackID)
	}
	return err
}
