package server

import (
	"context"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/resource"
)

// volumeMigrationWorker resumes a durable node-local task after restart. It
// deliberately processes one bounded page per interval so foreground Raft and
// data IO retain priority. Operator removal may still call the same Store API
// synchronously and is serialized by the Store lock.
type volumeMigrationWorker struct {
	runtime *node.Runtime
	report  func(error)
	budget  *resource.ByteBudget
}

func (worker *volumeMigrationWorker) run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stepCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
			err := worker.step(stepCtx)
			cancel()
			if err != nil && ctx.Err() == nil && worker.report != nil {
				worker.report(fmt.Errorf("resume Volume migration: %w", err))
			}
		}
	}
}

func (worker *volumeMigrationWorker) step(ctx context.Context) error {
	if worker == nil || worker.runtime == nil || worker.runtime.Records == nil {
		return nil
	}
	status, err := worker.runtime.Records.VolumeMigration()
	if err != nil || !status.Active {
		return err
	}
	if worker.budget != nil {
		release, err := worker.budget.TryAcquire(64 << 20)
		if err != nil {
			return err
		}
		defer release()
	}
	_, _, err = worker.runtime.Records.MigrateVolumeStep(ctx, status.SourceVolumeID)
	return err
}
