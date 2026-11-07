package server

import (
	"context"
	"fmt"
	"time"

	"github.com/dubi-project/dubi/internal/platform/management"
	"github.com/dubi-project/dubi/internal/platform/node"
	"github.com/dubi-project/dubi/internal/platform/raft"
)

func notificationWorker(runtime *node.Runtime, service *management.Notifications, ready <-chan struct{}, report func(error)) func(context.Context) {
	return func(ctx context.Context) {
		select {
		case <-ctx.Done():
			return
		case <-ready:
		}
		timer := time.NewTimer(15 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			status, e := runtime.Host.Status(raft.RootGroupID)
			if e == nil && status.LeaderID == runtime.Identity.RaftID {
				call, cancel := context.WithTimeout(ctx, 20*time.Second)
				e = service.Scan(call)
				cancel()
				if e != nil {
					service.Reset()
					if report != nil && ctx.Err() == nil {
						report(fmt.Errorf("notification scan: %w", e))
					}
				}
			} else {
				service.Reset()
			}
			timer.Reset(30 * time.Second)
		}
	}
}
